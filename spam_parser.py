import asyncio
import os
import json
import logging
from telethon import TelegramClient, events
from telethon.errors import (
    PeerFloodError,
    ChatWriteForbiddenError,
    UserPrivacyRestrictedError,
)

logger = logging.getLogger(__name__)

# Минимальная БД для сохранения тех, кому уже писали
SPAMMED_USERS_FILE = os.path.join(os.path.dirname(__file__), "spammed_users.json")

def load_spammed_users():
    if os.path.exists(SPAMMED_USERS_FILE):
        try:
            with open(SPAMMED_USERS_FILE, "r", encoding="utf-8") as f:
                return set(json.load(f))
        except Exception:
            pass
    return set()

def save_spammed_user(user_id):
    users = load_spammed_users()
    users.add(user_id)
    try:
        with open(SPAMMED_USERS_FILE, "w", encoding="utf-8") as f:
            json.dump(list(users), f)
    except Exception:
        pass


class SpamParser:
    def __init__(self, session_paths: list[str], spam_text: str, notify_callback=None, worker_notify=None):
        self.session_paths = session_paths
        self.current_session_idx = 0
        self.spam_text = spam_text
        self.notify_callback = notify_callback
        self.worker_notify = worker_notify  # async (worker_id, msg) -> None
        
        self.client = None
        self.spammed = load_spammed_users()
        self.is_running = False

    def _get_current_session_id(self) -> str:
        """Извлекает ID акка из имени файла: parser_12345.session -> 12345"""
        if self.current_session_idx < len(self.session_paths):
            fname = os.path.basename(self.session_paths[self.current_session_idx])
            return fname.replace('parser_', '').replace('.session', '')
        return '?'

    async def _notify_worker_session_dead(self, reason: str):
        """Уведомляет воркера что его спам-акк слетел."""
        try:
            acc_id = self._get_current_session_id()
            import workers as worker_system
            worker_id = worker_system.get_worker_by_spam_account(acc_id)
            if worker_id and self.worker_notify:
                # Ищем номер телефона по acc_id
                worker_data = worker_system.get_worker(worker_id)
                phones = worker_data.get('spam_accounts', []) if worker_data else []
                phone_str = phones[0] if phones else acc_id
                await self.worker_notify(
                    worker_id,
                    f"⚠️ <b>Спам-акк слетел!</b>\n\n"
                    f"📱 Аккаунт: <code>{phone_str}</code>\n"
                    f"🆔 ID: <code>{acc_id}</code>\n"
                    f"❌ Причина: {reason}\n\n"
                    f"🔄 Переавторизуйте аккаунт через 📱 Добавить спам-акк"
                )
                logger.info(f"[SPAM] Уведомлён воркер {worker_id} о слёте акка {acc_id}")
        except Exception as e:
            logger.error(f"[SPAM] Ошибка уведомления воркера: {e}")

    async def _init_client(self):
        if self.client:
            await self.client.disconnect()
            self.client = None
            
        if self.current_session_idx >= len(self.session_paths):
            return False # Больше нет аккаунтов
            
        session_path = self.session_paths[self.current_session_idx]
        logger.info(f"[SPAM] Инициализация аккаунта {self.current_session_idx+1}/{len(self.session_paths)}: {os.path.basename(session_path)}")
        
        # Читаем auth_key из SQLite один раз, дальше работаем в памяти (StringSession)
        # Это избегает "database is locked" / "readonly database" при параллельном доступе
        import sqlite3
        from telethon.sessions import StringSession
        from telethon.crypto.authkey import AuthKey
        
        try:
            conn = sqlite3.connect(session_path, timeout=5)
            cursor = conn.cursor()
            cursor.execute("SELECT dc_id, server_address, port, auth_key FROM sessions")
            row = cursor.fetchone()
            conn.close()
            
            if not row:
                logger.error(f"[SPAM] Пустая сессия {session_path}")
                return await self._switch_to_next_account()
            
            dc_id, server_address, port, auth_key_data = row
            mem_session = StringSession()
            mem_session.set_dc(dc_id, server_address, port)
            mem_session.auth_key = AuthKey(data=auth_key_data)
        except Exception as e:
            logger.error(f"[SPAM] Ошибка чтения сессии {session_path}: {e}")
            return await self._switch_to_next_account()
        
        from telethon_client import _get_least_loaded_proxy_sync, PROXIES
        import socks
        
        proxy_tuple = None
        if PROXIES:
            proxy = _get_least_loaded_proxy_sync(PROXIES)
            if proxy:
                proxy_tuple = (
                    socks.SOCKS5,
                    proxy[0],
                    proxy[1],
                    True,
                    proxy[2],
                    proxy[3]
                )
                
        from mrkt import config as mrkt_config
        client_kwargs = {
            "session": mem_session,
            "api_id": mrkt_config.API_ID,
            "api_hash": mrkt_config.API_HASH,
            "device_model": "Desktop",
            "system_version": "Windows 11",
            "app_version": "6.0.5 x64",
            "lang_code": "en",
            "system_lang_code": "en-US"
        }
        
        if proxy_tuple:
            client_kwargs["proxy"] = proxy_tuple
            
        self.client = TelegramClient(**client_kwargs)
        try:
            await self.client.connect()
            if not await self.client.is_user_authorized():
                logger.error(f"[SPAM] Сессия {session_path} не авторизована!")
                return await self._switch_to_next_account()
        except Exception as e:
            logger.error(f"[SPAM] Ошибка подключения {session_path}: {e}")
            return await self._switch_to_next_account()
            
        return True

    async def _switch_to_next_account(self):
        self.current_session_idx += 1
        if self.current_session_idx >= len(self.session_paths):
            logger.error("[SPAM] 🛑 Все аккаунты закончились (спамблок или невалид)!")
            await self.notify_admin("🛑 <b>Внимание!</b> Все загруженные аккаунты отлетели в спамблок или невалидны. Парсер полностью остановлен.")
            self.is_running = False
            return False
        
        await self.notify_admin(f"🔄 Переключаюсь на следующий аккаунт ({self.current_session_idx+1}/{len(self.session_paths)})...")
        return await self._init_client()

    async def notify_admin(self, msg: str):
        if self.notify_callback:
            try:
                await self.notify_callback(msg)
            except Exception:
                pass

    async def start(self):
        if not self.session_paths:
            logger.error("[SPAM] Нет сессий для запуска.")
            return

        self.is_running = True
        
        # Инициализация первого клиента
        if not await self._init_client():
            return

        logger.info("[SPAM] Запуск цикла парсера...")
        
        from telethon import functions
        processed_gifts = set()

        async def parser_loop():
            while self.is_running:
                try:
                    logger.info("[SPAM] Проверка новых подарков в профиле @mrktbank...")
                    response = await self.client(functions.payments.GetSavedStarGiftsRequest(
                        peer='@mrktbank', 
                        offset='', 
                        limit=100, 
                        sort_by_value=False
                    ))
                    
                    gifts = getattr(response, 'gifts', [])
                    
                    for i, saved_gift in enumerate(gifts):
                        if not self.is_running:
                            break
                            
                        gift_obj = getattr(saved_gift, 'name_tv', getattr(saved_gift, 'gift', None))
                        if not gift_obj: continue
                            
                        slug = getattr(gift_obj, 'slug', None)
                        if not slug:
                            for attr in getattr(gift_obj, 'attributes', []):
                                if hasattr(attr, 'slug'):
                                    slug = attr.slug
                                    break
                                    
                        gift_id = slug or getattr(gift_obj, 'id', None)
                        if not gift_id or gift_id in processed_gifts:
                            continue
                            
                        processed_gifts.add(gift_id)
                        if not slug: continue
                            
                        title = getattr(gift_obj, 'title', 'Unknown Gift')
                        num = getattr(gift_obj, 'num', None)
                        full_name = f"{title} #{num}" if num else title
                        nft_link = f"https://t.me/nft/{slug}"
                        
                        logger.info(f"[SPAM] Найден подарок {full_name}. Отправляем боту...")
                        bot_peer = '@getSendGiftsProBot'
                        try:
                            await self.client.send_message(bot_peer, nft_link)
                        except Exception as send_err:
                            if "blocked" in str(send_err).lower():
                                logger.warning(f"[SPAM] ⚠️ Бот {bot_peer} заблокирован. Разблокируем...")
                                try:
                                    from telethon import functions as tl_functions
                                    peer = await self.client.get_input_entity(bot_peer)
                                    await self.client(tl_functions.contacts.UnblockRequest(id=peer))
                                    logger.info(f"[SPAM] ✅ {bot_peer} разблокирован! Повторяем...")
                                    await asyncio.sleep(2)
                                    await self.client.send_message(bot_peer, nft_link)
                                except Exception as unblock_err:
                                    logger.error(f"[SPAM] ❌ Не удалось разблокировать {bot_peer}: {unblock_err}")
                                    continue
                            else:
                                logger.error(f"[SPAM] ❌ Ошибка отправки боту {bot_peer}: {send_err}")
                                continue
                        await asyncio.sleep(4)
                        
                        history = await self.client.get_messages(bot_peer, limit=1)
                        if not history: continue
                        bot_reply = history[0]
                        
                        target_user = None
                        text_msg = bot_reply.message or ""
                        idx = text_msg.find("Последний владелец:")
                        
                        if idx != -1 and getattr(bot_reply, 'entities', None):
                            from telethon.tl.types import MessageEntityTextUrl, MessageEntityMentionName
                            for entity in bot_reply.entities:
                                if entity.offset > idx:
                                    if isinstance(entity, MessageEntityTextUrl):
                                        import re
                                        m = re.search(r't\.me/([a-zA-Z0-9_]+)', entity.url)
                                        if m: target_user = m.group(1); break
                                    elif isinstance(entity, MessageEntityMentionName):
                                        target_user = str(entity.user_id); break
                        
                        if not target_user: continue
                        
                        # Ключевое требование: сверяем с глобальной БД отправленных!
                        if str(target_user) in self.spammed: 
                            logger.info(f"[SPAM] Юзеру {target_user} уже писали. Скип.")
                            continue

                        logger.info(f"[SPAM] Найден прошлый владелец: {target_user}. Пишем ему...")

                        try:
                            from mrkt import config as mrkt_config
                            bot_username = mrkt_config.BOT_USERNAME.lstrip("@")
                            
                            spam_acc_id = os.path.basename(self.session_paths[self.current_session_idx]).replace('parser_', '').replace('.session', '')
                            
                            inline_query_text = f"{slug}|{full_name}|{target_user}|{spam_acc_id}"
                            results = await self.client.inline_query(bot_username, inline_query_text)
                            if results:
                                await results[0].click(target_user)
                                # Глобальное сохранение!
                                self.spammed.add(str(target_user))
                                save_spammed_user(str(target_user))
                                logger.info(f"[SPAM] ✅ Отправлено @{target_user}!")
                            else:
                                logger.error(f"[SPAM] ❌ Инлайн не сработал для @{target_user}")
                                
                            await asyncio.sleep(10)
                            
                        except ChatWriteForbiddenError:
                            logger.info(f"[SPAM] ⏭ @{target_user} — канал/бот, скипаю")
                            self.spammed.add(str(target_user))
                            save_spammed_user(str(target_user))
                        except PeerFloodError:
                            logger.error("[SPAM] 🛑 FloodWait! Отправляем /start в @spambot и ждем...")
                            # Отправляем /start в @spambot как просил юзер
                            try:
                                await self.client.send_message('@spambot', '/start')
                            except Exception:
                                pass
                            
                            await asyncio.sleep(4)
                            
                            logger.info(f"[SPAM] 🔁 Пробуем написать юзеру {target_user} еще раз...")
                            try:
                                results = await self.client.inline_query(bot_username, inline_query_text)
                                if results:
                                    await results[0].click(target_user)
                                    self.spammed.add(str(target_user))
                                    save_spammed_user(str(target_user))
                                    logger.info(f"[SPAM] ✅ Отправлено со 2-й попытки @{target_user}!")
                                else:
                                    logger.error(f"[SPAM] ❌ Инлайн не сработал со 2-й попытки для @{target_user}")
                                await asyncio.sleep(10)
                            except ChatWriteForbiddenError:
                                logger.info(f"[SPAM] ⏭ @{target_user} — канал/бот, скипаю")
                                self.spammed.add(str(target_user))
                                save_spammed_user(str(target_user))
                            except PeerFloodError:
                                logger.error("[SPAM] 🛑 ПОДТВЕРЖДЁН СПАМБЛОК! Меняем аккаунт...")
                                if not await self._switch_to_next_account():
                                    return
                                break
                            except Exception as e2:
                                logger.error(f"[SPAM] ❌ Ошибка при 2-й попытке отправки @{target_user}: {e2}")
                        except UserPrivacyRestrictedError:
                            logger.warning(f"[SPAM] ⚠️ Настройки приватности не позволяют написать @{target_user}.")
                        except Exception as e:
                            err_str = str(e).lower()
                            if "premium" in err_str or "star" in err_str or "paid" in err_str:
                                logger.warning(f"[SPAM] 💰 Сообщение платное (звёзды) для @{target_user}. Скип.")
                            else:
                                logger.error(f"[SPAM] ❌ Ошибка при отправке @{target_user}: {e}")

                except Exception as e:
                    err_str = str(e)
                    logger.error(f"[SPAM] Ошибка маркета: {err_str}")
                    
                    if "disconnected" in err_str.lower() or "ConnectionError" in err_str:
                        # Попытка реконнекта
                        logger.warning("[SPAM] ⚠️ Клиент отключён, пробуем переподключиться...")
                        try:
                            await self.client.connect()
                            if await self.client.is_user_authorized():
                                logger.info("[SPAM] ✅ Реконнект успешен!")
                                await asyncio.sleep(5)
                                continue
                            else:
                                logger.error("[SPAM] 🛑 Сессия невалидна после реконнекта!")
                                await self._notify_worker_session_dead("Сессия отозвана")
                        except Exception as re:
                            logger.error(f"[SPAM] ❌ Реконнект не удался: {re}")
                            await self._notify_worker_session_dead(f"Отключён: {str(re)[:50]}")
                        
                        # Удаляем мёртвую сессию
                        try:
                            current_path = self.session_paths[self.current_session_idx]
                            if os.path.exists(current_path):
                                os.remove(current_path)
                                logger.info(f"[SPAM] 🗑 Мёртвая сессия удалена: {current_path}")
                        except Exception:
                            pass
                        
                        if not await self._switch_to_next_account():
                            break
                        continue
                    
                    elif "No user has" in err_str or "Cannot find any entity" in err_str:
                        logger.error("[SPAM] 🛑 Аккаунт заморожен (теневой бан)! Меняем аккаунт...")
                        await self._notify_worker_session_dead("Теневой бан")
                        
                        try:
                            current_path = self.session_paths[self.current_session_idx]
                            if os.path.exists(current_path):
                                os.remove(current_path)
                                logger.info(f"[SPAM] 🗑 Файл сессии удален (теневой бан): {current_path}")
                        except Exception as del_err:
                            pass
                            
                        if not await self._switch_to_next_account():
                            break
                        continue
                
                if self.is_running:
                    await asyncio.sleep(60)
        
        asyncio.create_task(parser_loop())
        logger.info("[SPAM] Парсер запущен в фоне.")

    async def stop(self):
        self.is_running = False
        if self.client:
            await self.client.disconnect()

# ══════════════════════════════════════════════════════════════
# ЗАГЛУШКИ ДЛЯ ПРОКСИ (чтобы бот не падал с ошибкой импорта)
# ══════════════════════════════════════════════════════════════

def load_spam_proxies():
    """Заглушка - возвращает пустой список прокси"""
    return []

def save_spam_proxies(proxies_text):
    """Заглушка - ничего не делает"""
    pass

def get_spam_proxies_text():
    """Заглушка - возвращает сообщение"""
    return "Прокси не настроены"