# ══════════════════════════════════════════════════════════════
# MRKT API Client — все взаимодействия с api.tgmrkt.io
# ══════════════════════════════════════════════════════════════

import asyncio
import json
import logging
import time
from typing import Optional, List, Dict, Any
from urllib.parse import unquote

import aiohttp

logger = logging.getLogger("mrkt.api")

MRKT_API = "https://api.tgmrkt.io/api/v1"


class MrktAPI:
    """
    Async-клиент для MRKT marketplace.

    Авторизация:
        1. Получаем init_data через Telethon (RequestAppWebView на @mrkt)
        2. POST /auth {"data": init_data} → {"token": "..."}
        3. Все запросы с Header  Authorization: <token>
    """

    def __init__(self, token: str, proxy: str | None = None, cookie_jar: aiohttp.CookieJar | None = None):
        self.token = token
        self.proxy = proxy
        self._session: Optional[aiohttp.ClientSession] = None
        self._user_info: dict = {}

        # MRKT требует cookie access_token= помимо заголовка Authorization.
        # Если cookie_jar не передан (например, при загрузке сохранённого токена),
        # создаём его автоматически из токена.
        if cookie_jar is None:
            from yarl import URL
            cookie_jar = aiohttp.CookieJar()
            cookie_jar.update_cookies(
                {"access_token": token},
                response_url=URL("https://api.tgmrkt.io/"),
            )
        self._cookie_jar = cookie_jar

    # ── helpers ──────────────────────────────────────────────

    def _headers(self) -> dict:
        return {
            "Authorization": self.token,
            "Referer": "https://cdn.tgmrkt.io/",
            "Origin": "https://cdn.tgmrkt.io",
            "Content-Type": "application/json",
            "Accept": "application/json",
            "User-Agent": (
                "Mozilla/5.0 (Linux; Android 14; SM-A536B) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/125.0.0.0 Mobile Safari/537.36"
            ),
        }

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                headers=self._headers(),
                cookie_jar=self._cookie_jar,
            )
        return self._session

    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()

    async def _request(
        self,
        method: str,
        endpoint: str,
        payload: dict | None = None,
        retries: int = 2,
    ) -> dict:
        """Универсальный HTTP-клиент с retry."""
        url = f"{MRKT_API}{endpoint}"
        last_err = None
        for attempt in range(retries + 1):
            try:
                session = await self._get_session()
                kwargs: dict = {"proxy": self.proxy} if self.proxy else {}
                
                if method == "GET":
                    async with session.get(url, **kwargs) as resp:
                        text = await resp.text()
                        if resp.status != 200:
                            logger.debug(f"[MRKT] {method} {endpoint} → {resp.status} | {text[:300]}")
                            # Возвращаем ошибку с кодом статуса чтобы вызывающий код мог отличить 401 от успеха
                            if not text:
                                return {"error": str(resp.status), "status": resp.status}
                        else:
                            logger.debug(f"[MRKT] {method} {endpoint} → {resp.status} | {text[:300]}")
                            if not text: return {}
                        parsed = json.loads(text)
                        return parsed if isinstance(parsed, dict) else {"error": parsed}
                else:
                    async with session.post(url, json=payload or {}, **kwargs) as resp:
                        text = await resp.text()
                        if resp.status != 200:
                            logger.debug(f"[MRKT] {method} {endpoint} → {resp.status} | {text[:300]}")
                            if not text:
                                return {"error": str(resp.status), "status": resp.status}
                        else:
                            logger.debug(f"[MRKT] {method} {endpoint} → {resp.status} | {text[:300]}")
                            if not text: return {}
                        parsed = json.loads(text)
                        return parsed if isinstance(parsed, dict) else {"error": parsed}
            except Exception as e:
                last_err = e
                logger.warning(f"[MRKT] {method} {endpoint} attempt {attempt+1} failed: {e}")
                if attempt < retries:
                    await asyncio.sleep(1.5)
        raise RuntimeError(f"MRKT API {method} {endpoint} failed after {retries+1} attempts: {last_err}")

    # ══════════════════════════════════════════════════════════
    #  AUTH
    # ══════════════════════════════════════════════════════════

    @classmethod
    async def auth_with_init_data(cls, init_data: str, proxy: str | None = None) -> "MrktAPI":
        """Создаёт клиент, авторизовавшись через init_data.
        Сохраняет cookies из auth-ответа для последующих запросов."""
        cookie_jar = aiohttp.CookieJar()
        async with aiohttp.ClientSession(cookie_jar=cookie_jar) as s:
            headers = {
                "Content-Type": "application/json",
                "Referer": "https://cdn.tgmrkt.io/",
                "Origin": "https://cdn.tgmrkt.io",
                "User-Agent": (
                    "Mozilla/5.0 (Linux; Android 14; SM-A536B) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/125.0.0.0 Mobile Safari/537.36"
                ),
            }
            async with s.post(
                f"{MRKT_API}/auth",
                json={"data": init_data},
                headers=headers,
            ) as resp:
                # Логируем cookies из ответа
                for cookie in resp.cookies.values():
                    pass  # cookies captured silently
                data = await resp.json()
                token = data.get("token")
                if not token:
                    raise RuntimeError(f"MRKT auth failed: {data}")
        # Передаём cookie_jar в инстанс — cookies сохранятся для всех запросов
        api = cls(token, proxy, cookie_jar=cookie_jar)
        api._user_info = data.get("user", {})
        return api

    # ══════════════════════════════════════════════════════════
    #  PROFILE / BALANCE
    # ══════════════════════════════════════════════════════════

    async def get_profile(self) -> dict:
        """GET /me — информация о пользователе."""
        data = await self._request("GET", "/me")
        if "id" in data:
            self._user_info["id"] = data["id"]
        return data

    async def get_balance(self) -> dict:
        """GET /balance — баланс пользователя (TON, Stars и тд)."""
        return await self._request("GET", "/balance")

    async def get_balance_ton(self) -> float:
        """Возвращает баланс в TON."""
        data = await self.get_balance()
        # Пробуем разные ключи — точную структуру узнаем в рантайме
        if isinstance(data, dict):
            # Новый формат: "hard" (в наноТОНах) - это текущий доступный баланс!
            if "hard" in data:
                return float(data["hard"]) / 1_000_000_000
                
            for key in ("balance", "ton", "tonBalance", "amount"):
                if key in data:
                    return float(data[key])
            # Может быть вложенный объект
            if "wallet" in data and isinstance(data["wallet"], dict):
                return float(data["wallet"].get("balance", 0))
        return 0.0

    # ══════════════════════════════════════════════════════════
    #  VAULT / INVENTORY
    # ══════════════════════════════════════════════════════════

    async def get_vault(self) -> List[dict]:
        """
        POST /gifts — подарки в хранилище (инвентарь).
        """
        payload = {
            "isListed": False,
            "count": 100,
            "ModelNames": [],
            "SymbolNames": [],
            "BackdropNames": [],
            "CollectionNames": []
        }
        
        try:
            data = await self._request("POST", "/gifts", payload)
            if isinstance(data, list):
                return data
            return data.get("gifts", data.get("items", []))
        except Exception as e:
            logger.warning(f"[MRKT] /gifts failed: {e}")
            return []

    async def get_my_saling(self) -> List[dict]:
        """
        POST /gifts (isListed=True) — подарки ЮЗЕРА на продаже.
        """
        payload = {
            "isListed": True,
            "count": 100,
            "ModelNames": [],
            "SymbolNames": [],
            "BackdropNames": [],
            "CollectionNames": []
        }
        
        try:
            data = await self._request("POST", "/gifts", payload)
            if isinstance(data, list):
                return data
            return data.get("gifts", data.get("items", []))
        except Exception as e:
            logger.warning(f"[MRKT] /gifts(saling) failed: {e}")
            return []

    # ══════════════════════════════════════════════════════════
    #  SELL / DELIST
    # ══════════════════════════════════════════════════════════

    async def sell_gift(self, gift_id: str, price: float, currency: str = "TON") -> dict:
        """
        POST /gifts/sale — выставить подарок на продажу.
        Ретрай при пустом ответе (MRKT иногда отказывает с первой попытки).
        """
        price_nano = int(round(price * 1_000_000_000))
        payload = {"ids": [gift_id], "price": str(price_nano)}
        if currency != "TON":
            payload["currency"] = currency
        
        logger.info(f"[MRKT] sell_gift payload: {payload}")
        
        for attempt in range(3):
            result = await self._request("POST", "/gifts/sale", payload)
            listed_ids = result.get("ids", []) if isinstance(result, dict) else []
            if listed_ids:
                return result
            # Пустой ответ — ждём и пробуем снова
            if attempt < 2:
                logger.warning(f"[MRKT] sell_gift empty response (attempt {attempt+1}), retrying in 3s...")
                import asyncio
                await asyncio.sleep(3)
        
        return result  # Вернуть последний результат даже если пустой

    async def instant_sell(self, gift_id: str, gift: dict = None) -> dict:
        """
        POST /gifts/instant-sell — моментальная продажа (если есть).
        Fallback: sell по цене QuickSale или FloorPrice.
        """
        try:
            result = await self._request("POST", "/gifts/instant-sell", {"id": gift_id})
            # Проверяем реальный успех, а не просто "не пустой ответ"
            if not result or result.get("error") or result.get("status") in [404, 400, 500]:
                raise ValueError(f"instant-sell failed: {result}")
            return result
        except Exception as e:
            logger.warning(f"[MRKT] instant-sell failed: {e}, trying quickSale price...")
            if not gift:
                try:
                    vault = await self.get_vault()
                    for g in vault:
                        if g.get("id") == gift_id or g.get("giftIdString") == gift_id:
                            gift = g
                            break
                except Exception:
                    pass

            qs = await self.get_quick_sale_price(gift_id, gift)
            
            # Если нет быстрых покупок, берем минимальную цену с рынка
            if qs <= 0 and gift:
                floor_nano = gift.get("floorPriceNanoTONsByBackdropModel") or gift.get("floorPriceNanoTONsByCollection")
                if floor_nano:
                    qs = float(floor_nano) / 1_000_000_000
                    qs = round(qs * 0.95, 2)  # -5% для быстрой продажи
                    logger.warning(f"[MRKT] Used floor price fallback: {qs} TON")
                    
            logger.info(f"[MRKT] instant_sell fallback: gift_id={gift_id}, qs_price={qs} TON")
                    
            if qs > 0:
                # Пробуем sell с ретраями
                for sell_attempt in range(3):
                    sell_result = await self.sell_gift(gift_id, qs)
                    listed_ids = sell_result.get("ids", []) if isinstance(sell_result, dict) else []
                    if listed_ids:
                        return sell_result
                    logger.warning(f"[MRKT] sell_gift empty in instant_sell fallback (attempt {sell_attempt+1})")
                    await asyncio.sleep(5)
                return sell_result  # вернём последний результат даже если пустой
            raise

    async def cancel_sale(self, gift_id: str) -> dict:
        """POST /gifts/sale/cancel — снять подарок с продажи."""
        return await self._request("POST", "/gifts/sale/cancel", {"ids": [gift_id]})

    async def delist_gift(self, gift_id: str) -> dict:
        """Alias for cancel_sale."""
        return await self.cancel_sale(gift_id)

    # ══════════════════════════════════════════════════════════
    #  PRICING
    # ══════════════════════════════════════════════════════════

    async def get_quick_sale_price(self, gift_id: str, gift: dict = None) -> float:
        """
        Пробуем получить цену быстрой продажи через ордера.
        """
        # Сначала проверяем конкретный подарок по ID (если кто-то выставил оффер именно на него)
        try:
            data = await self._request("POST", "/orders/top", {
                "giftId": gift_id,
                "type": "buy",
                "count": 1,
            })
            orders = data.get("orders", []) if isinstance(data, dict) else []
            if data and not orders and "priceMaxNanoTONs" in data:
                # Берём 99% от максимальной цены, округляем вниз до 0.01 TON
                price_nano = int(float(data["priceMaxNanoTONs"]) * 0.99)
                price_nano = price_nano // 10_000_000 * 10_000_000  # кратно 0.01 TON
                return price_nano / 1_000_000_000
            if orders:
                return float(orders[0].get("price", 0))
        except Exception:
            pass

        # Если не нашли, ищем по коллекции (как это делает интерфейс маркета)
        if not gift:
            # Попробуем найти его в vault (защита от рассинхрона файлов у юзера)
            try:
                vault = await self.get_vault()
                for g in vault:
                    if g.get("id") == gift_id or g.get("giftIdString") == gift_id:
                        gift = g
                        break
            except Exception:
                pass

        if gift:
            collection_name = gift.get("collectionName")
            logger.info(f"[MRKT-DBG] collection_name: {collection_name}")
            if collection_name:
                try:
                    data = await self._request("POST", "/orders/top", {
                        "collectionName": collection_name,
                        "type": "buy",
                        "count": 1,
                    })
                    if isinstance(data, dict) and "priceMaxNanoTONs" in data:
                        # Берём 99% от максимальной цены, округляем вниз до 0.01 TON
                        price_nano = int(float(data["priceMaxNanoTONs"]) * 0.99)
                        price_nano = price_nano // 10_000_000 * 10_000_000  # кратно 0.01 TON
                        return price_nano / 1_000_000_000
                    
                    orders = data.get("orders", []) if isinstance(data, dict) else []
                    if orders:
                        return float(orders[0].get("price", 0))
                except Exception as e:
                    logger.warning(f"[MRKT-DBG] orders/top failed: {e}")
                    pass
        else:
            logger.info("[MRKT-DBG] gift object not passed to get_quick_sale_price")

        # Вариант 3: посмотреть минимальную цену через saling
        try:
            gift_info = await self._request("GET", f"/gifts/{gift_id}")
            floor = gift_info.get("floorPrice") or gift_info.get("quickSalePrice")
            if floor:
                return float(floor)
        except Exception:
            pass

        return 0.0

    async def get_gift_info(self, gift_id: str) -> dict:
        """GET /gifts/<id> — детали подарка."""
        return await self._request("GET", f"/gifts/{gift_id}")

    # ══════════════════════════════════════════════════════════
    #  BUY
    # ══════════════════════════════════════════════════════════

    async def buy_gift(self, gift_id: str, price_nanotons: int) -> dict:
        """
        POST /gifts/buy — купить подарок с маркетплейса.
        gift_id: UUID подарка на продаже
        price_nanotons: цена в наноТОНах
        """
        payload = {
            "Ids": [gift_id],
            "prices": {gift_id: price_nanotons},
        }
        return await self._request("POST", "/gifts/buy", payload)

    # ══════════════════════════════════════════════════════════
    #  WITHDRAW
    # ══════════════════════════════════════════════════════════

    async def withdraw_ton(self, wallet: str, amount: float) -> dict:
        """GET /wallet/withdraw/tons — вывод TON на кошелёк (передаются nanoTONs)."""
        # Безопасный вывод: вычитаем 0.05 TON на комиссию, если сумма слишком близка к макс. балансу
        current_bal = await self.get_balance_ton()
        if amount > current_bal - 0.05:
            amount = current_bal - 0.05
            
        if amount < 0.1:
            return {"error": True, "message": f"Недостаточно средств для вывода (нужно минимум 0.15 TON с учетом комиссии). Доступно: {current_bal}"}
            
        nanotons = int(round(amount * 1_000_000_000))
        ep = f"/wallet/withdraw/tons?nanoTONs={nanotons}&wallet={wallet}"
        
        try:
            # Делаем GET-запрос
            result = await self._request("GET", ep)
            
            if isinstance(result, dict):
                # Проверяем явные признаки ошибки от API
                if result.get("error"):
                    return {"error": True, "message": result.get("message", str(result)), "full": result}
                if result.get("status") and int(result.get("status", 0)) >= 400:
                    return {"error": True, "message": f"HTTP {result.get('status')}: {result}", "full": result}
                if result.get("title") and "Bad Request" in str(result.get("title", "")):
                    return {"error": True, "message": f"Bad Request: {result}", "full": result}
                # Успех
                return result or {"success": True}
            else:
                return {"error": True, "message": f"unexpected response: {result}"}
        except Exception as e:
            return {"error": True, "message": str(e)}

    # ══════════════════════════════════════════════════════════
    #  MARKETPLACE (browse)
    # ══════════════════════════════════════════════════════════

    async def get_saling_gifts(
        self,
        collection_names: list | None = None,
        count: int = 20,
        cursor: str = "",
        ordering: str = "Price",
        low_to_high: bool = True,
    ) -> dict:
        """POST /gifts/saling — все подарки на маркетплейсе."""
        payload = {
            "collectionNames": collection_names or [],
            "modelNames": [],
            "backdropNames": [],
            "symbolNames": [],
            "ordering": ordering,
            "lowToHigh": low_to_high,
            "maxPrice": None,
            "minPrice": None,
            "mintable": None,
            "number": None,
            "count": count,
            "cursor": cursor,
            "query": None,
            "promotedFirst": False,
        }
        return await self._request("POST", "/gifts/saling", payload)

    # ══════════════════════════════════════════════════════════
    #  OFFERS
    # ══════════════════════════════════════════════════════════

    async def get_gift_sale_info(self, gift_id: str) -> dict:
        """GET /gifts/gift/<id> — информация о гифте (включая saleId если на продаже)."""
        return await self._request("GET", f"/gifts/gift/{gift_id}")

    async def create_offer(self, gift_sale_id: str, price_nano: int) -> dict:
        """
        POST /offers/create — создать оффер на гифт.
        gift_sale_id — это saleId (не UUID гифта), берётся из get_gift_sale_info().
        price_nano — цена в наноТОНах.
        """
        try:
            result = await self._request("POST", "/offers/create", {
                "price": price_nano,
                "giftSaleId": gift_sale_id,
            })
            return result or {"ok": True}
        except Exception as e:
            return {"error": True, "message": str(e)}

    async def get_activities(self, is_active: bool = True, offset: int = 0, count: int = 20) -> list:
        """GET /activities — получить активности (входящие оферы и тд)."""
        try:
            qs = f"?offset={offset}&count={count}&isActive={str(is_active).lower()}"
            data = await self._request("GET", f"/activities{qs}")
            if isinstance(data, list):
                return data
            if isinstance(data, dict):
                # _request оборачивает списки как {"error": [list]}
                err = data.get("error")
                if isinstance(err, list):
                    return err
                return data.get("items", data.get("activities", []))
            return []
        except Exception as e:
            logger.warning(f"[MRKT] get_activities error: {e}")
            return []

    async def accept_offer(self, offer_id: str) -> dict:
        """POST /offers/accept — принять оффер (пробуем несколько форматов)."""
        formats = [
            # (endpoint, payload)
            ("/offers/accept", {"offerId": offer_id}),
            ("/offers/accept", {"id": offer_id}),
            (f"/offers/accept?offerId={offer_id}", None),
            (f"/offers/{offer_id}/accept", None),
            ("/gifts/offer/accept", {"offerId": offer_id}),
            ("/activities/respond", {"offerId": offer_id, "action": "accept"}),
        ]
        last_error = ""
        for endpoint, payload in formats:
            try:
                result = await self._request("POST", endpoint, payload)
                result_str = str(result)
                # Считаем неудачей: пустой ответ, OFFER_NOT_EXIST, любой error/Unauthorized
                if not result:
                    last_error = f"{endpoint} → empty"
                    continue
                if "OFFER_NOT_EXIST" in result_str:
                    last_error = f"{endpoint} → OFFER_NOT_EXIST"
                    continue
                if "not found" in result_str.lower() or "Not Found" in result_str:
                    last_error = f"{endpoint} → not found"
                    continue
                if isinstance(result, dict) and result.get("error"):
                    last_error = f"{endpoint} → {result.get('error')}"
                    continue
                # Если дошли сюда — реальный успех
                logger.info(f"[MRKT] accept_offer OK via {endpoint}: {result_str[:200]}")
                return result
            except Exception as e:
                last_error = f"{endpoint} → exception: {e}"
                continue
        
        # Все форматы провалились
        return {"error": True, "message": f"accept failed: {last_error}"}

    async def cancel_offer(self, offer_id: str) -> dict:
        """POST /offers/cancel — отменить оффер."""
        try:
            result = await self._request("POST", "/offers/cancel", {"offerId": offer_id})
            return result or {"ok": True}
        except Exception as e:
            return {"error": True, "message": str(e)}
