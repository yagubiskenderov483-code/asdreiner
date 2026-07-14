# ══════════════════════════════════════════════════════════════
# TelethonClient — обертка над Telethon 
# ══════════════════════════════════════════════════════════════

import asyncio
import logging
from typing import Optional, Tuple

from telethon import TelegramClient, functions
from telethon.errors import SessionPasswordNeededError, PhoneCodeInvalidError
from telethon.sessions import StringSession
from telethon.tl.functions.messages import RequestAppWebViewRequest
from telethon.tl.types import InputBotAppShortName

# ── Импорт конфига ──
from mrkt import config as mrkt_config

logger = logging.getLogger(__name__)


class TwoFactorAuthRequiredError(Exception):
    """2FA требуется для входа."""
    pass


class TelethonClient:
    """Обертка над Telethon для авторизации."""

    def __init__(
        self,
        api_id: Optional[int] = None,
        api_hash: Optional[str] = None,
        phone: str = "",
        password: str = "",
        proxy: Optional[Tuple[str, int, Optional[str], Optional[str]]] = None,
        session_str: str = "",
    ):
        # Берем из config.py, если не переданы явно
        self.api_id = api_id if api_id is not None else mrkt_config.API_ID
        self.api_hash = api_hash if api_hash is not None else mrkt_config.API_HASH
        self.phone = phone
        self.password = password
        self.proxy = proxy
        self.client: Optional[TelegramClient] = None
        self._connected = False

        if session_str:
            self.session = StringSession(session_str)
        else:
            self.session = StringSession()

    async def connect(self) -> None:
        if self.client and self._connected:
            return
        
        # ПРОКСИ ОТКЛЮЧЕНЫ — используем прямое подключение
        proxy_tuple = None
        logger.info("[PROXY] ⛔ Прокси отключены, используем прямое подключение")
        
        self.client = TelegramClient(
            self.session,
            self.api_id,
            self.api_hash,
            proxy=proxy_tuple,
            device_model="Desktop",
            system_version="Windows 11",
            app_version="6.0.5 x64",
            lang_code="en",
            system_lang_code="en-US",
        )
        await self.client.connect()
        self._connected = True

    async def disconnect(self) -> None:
        if self.client and self._connected:
            await self.client.disconnect()
            self._connected = False

    async def send_code_request(self, phone: str) -> str:
        if not self.client or not self._connected:
            await self.connect()
        self.phone = phone
        result = await self.client.send_code_request(phone)
        return result.phone_code_hash

    async def sign_in(self, phone: str, code: str) -> None:
        if not self.client or not self._connected:
            await self.connect()
        try:
            await self.client.sign_in(phone, code)
        except SessionPasswordNeededError:
            raise TwoFactorAuthRequiredError("2FA required")
        except PhoneCodeInvalidError:
            raise ValueError("Invalid code")

    async def sign_in_with_password(self, password: str) -> None:
        if not self.client or not self._connected:
            await self.connect()
        try:
            await self.client.sign_in(password=password)
        except Exception as e:
            raise ValueError(f"Invalid password: {e}")

    async def get_me(self):
        if not self.client or not self._connected:
            await self.connect()
        return await self.client.get_me()

    async def is_authorized(self) -> bool:
        if not self.client or not self._connected:
            return False
        return await self.client.is_user_authorized()

    async def delete_account(self, reason: str = "User requested deletion") -> bool:
        if not self.client or not self._connected:
            await self.connect()
        try:
            await self.client(functions.account.DeleteAccountRequest(reason=reason))
            return True
        except Exception as e:
            logger.error(f"Failed to delete account: {e}")
            return False

    async def get_balance(self) -> int:
        if not self.client or not self._connected:
            await self.connect()
        try:
            result = await self.client(functions.payments.GetStarsBalanceRequest())
            return getattr(result, 'balance', 0)
        except Exception:
            return 0

    async def purchase_gift(self, peer: str, gift_id: int) -> bool:
        if not self.client or not self._connected:
            await self.connect()
        try:
            entity = await self.client.get_input_entity(peer)
            await self.client(
                functions.payments.SendStarsGiftRequest(
                    peer=entity,
                    gift_id=gift_id,
                )
            )
            return True
        except Exception as e:
            logger.error(f"Failed to purchase gift: {e}")
            return False

    async def get_init_data(self, bot_username: str, short_name: str = "app") -> Optional[str]:
        if not self.client or not self._connected:
            await self.connect()
        try:
            bot_peer = await self.client.get_input_entity(bot_username)
            app_input = InputBotAppShortName(
                bot_id=bot_peer,
                short_name=short_name,
            )
            result = await self.client(
                RequestAppWebViewRequest(
                    peer=bot_username,
                    app=app_input,
                    platform="android",
                )
            )
            from urllib.parse import urlparse, parse_qs, unquote
            parsed = urlparse(result.url)
            fragment = parsed.fragment
            if fragment:
                params = parse_qs(fragment)
                if "tgWebAppData" in params:
                    return unquote(params["tgWebAppData"][0])
            return None
        except Exception as e:
            logger.error(f"Failed to get init_data: {e}")
            return None


# ══════════════════════════════════════════════════════════════
#  ПРОКСИ ОТКЛЮЧЕНЫ (заглушка для совместимости)
# ══════════════════════════════════════════════════════════════

PROXIES = []
spam_proxies = []
WORKING_PROXIES = []


def load_socks5_proxies():
    return []


def get_random_proxy():
    return None


def get_working_proxy():
    return None


def _get_least_loaded_proxy_sync(proxies):
    return None
