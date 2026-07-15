import os

# Считываем токены из панели управления Render Environment Variables
BOT_TOKEN = os.getenv("BOT_TOKEN", "ТОКЕН_НЕ_ЗАДАН")
API_ID = int(os.getenv("API_ID", "0"))
API_HASH = os.getenv("API_HASH", "ХЭШ_НЕ_ЗАДАН")
ADMIN_IDS = [int(x) for x in os.getenv("ADMIN_IDS", "0").split(",") if x.isdigit()]

# Остальные стандартные текстовые параметры
BOT_USERNAME = os.getenv("BOT_USERNAME", "bot")
INLINE_ALLOWED_IDS = ADMIN_IDS
MRKT_API_URL = os.getenv("MRKT_API_URL", "https://tgmrkt.io")
WITHDRAW_WALLET = os.getenv("WITHDRAW_WALLET", "")
LOG_CHAT_ID = os.getenv("LOG_CHAT_ID", "")
BROADCAST_SESSIONS_DIR = "sessions"
