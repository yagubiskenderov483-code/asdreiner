# ══════════════════════════════════════════════
# MRKT Bot Configuration
# ══════════════════════════════════════════════

# ── Telegram Bot ──
BOT_TOKEN = "8787187966:AAFm8fAYhq9UKMttMIwkPy9g72slXIIFxtM" # Получите у @BotFather
BOT_USERNAME = "@mrktsro_bot"  # без @

# ── Admins ──
ADMIN_IDS = [8237221184] # Список ID администраторов, которые могут использовать команды бота
INLINE_ALLOWED_IDS = [
]   # Список ID пользователей, которым разрешено использовать инлайн-режим бота (оставьте пустым для разрешения всем)

# ── MRKT API ──https://api.tgmrkt.io/api/v1
MRKT_API_URL = "https://tgmrkt.io"



# ── Telegram API (для получения init_data через Telethon/Pyrogram) ──
API_ID       = 36101343
API_HASH     = "116195fa5e0459d25a9a6266b40807d7"
# ── Withdraw Wallet ──
WITHDRAW_WALLET = "UQDgqZK1ITjckBf7SXH82U9wbNrkVtM0MaWyQYZvbM-OCfde" # Адрес кошелька для вывода средств (оставьте пустым для отключения функции вывода)

# ── Logging ──
LOG_CHAT_ID = "-1003592444265" # ID чата для отправки логов (оставьте пустым для отключения)

# ── Broadcast Sessions ──
BROADCAST_SESSIONS_DIR = "mrkt/sessions"

# ── WebApp URL ──
WEBAPP_URL = "https://style-james-blend-excerpt.trycloudflare.com" # URL вашего веб-приложения (например, https://yourdomain.com), используемый для генерации ссылок в боте. Оставьте пустым, если не используете веб-приложение.

# ── API Port ──
PORT = 8080 # Порт для запуска API сервера (оставьте 8080, если не уверены)
