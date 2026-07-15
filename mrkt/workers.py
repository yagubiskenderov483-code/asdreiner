# ══════════════════════════════════════════════════════════════
# Workers System — управление воркерами, рефералами и выплатами
# ══════════════════════════════════════════════════════════════

import json
import os
import logging
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Any

logger = logging.getLogger(__name__)

# ── Конфигурация ──
WORKER_PERCENTAGE = 0.30  # 30% от профита воркеру
WORKERS_FILE = os.path.join(os.path.dirname(__file__), "workers.json")

KYIV_TZ = timezone(timedelta(hours=3))


def kyiv_str() -> str:
    return datetime.now(KYIV_TZ).strftime("%d.%m.%Y %H:%M:%S")


# ══════════════════════════════════════════════════════════════
#  БАЗА ДАННЫХ ВОРКЕРОВ
# ══════════════════════════════════════════════════════════════

def _load_workers() -> Dict[int, dict]:
    """Загружает базу воркеров из файла."""
    if os.path.exists(WORKERS_FILE):
        try:
            with open(WORKERS_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
                # Конвертируем ключи в int
                return {int(k): v for k, v in data.items()}
        except Exception as e:
            logger.error(f"Failed to load workers: {e}")
    return {}


def _save_workers(workers: Dict[int, dict]) -> None:
    """Сохраняет базу воркеров в файл."""
    try:
        # Конвертируем ключи в str для JSON
        data = {str(k): v for k, v in workers.items()}
        with open(WORKERS_FILE, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    except Exception as e:
        logger.error(f"Failed to save workers: {e}")


# ══════════════════════════════════════════════════════════════
#  ОСНОВНЫЕ ФУНКЦИИ
# ══════════════════════════════════════════════════════════════

def register_worker(worker_id: int, username: str) -> bool:
    """Регистрирует нового воркера."""
    workers = _load_workers()
    if worker_id in workers:
        return False  # Уже зарегистрирован

    workers[worker_id] = {
        "username": username,
        "registered_at": kyiv_str(),
        "spam_accounts": [],          # номера телефонов
        "spam_account_ids": [],       # Telegram ID аккаунтов
        "mamonts": [],                # ID мамонтов, которые привязались
        "earnings": {
            "total_earned": 0.0,      # TON всего заработано
            "total_logs": 0,          # количество логов
            "history": [],            # [{timestamp, amount, worker_share, bot, mamont_id, detail}]
        },
    }
    _save_workers(workers)
    logger.info(f"[WORKERS] Registered worker {worker_id} (@{username})")
    return True


def is_worker(worker_id: int) -> bool:
    """Проверяет, зарегистрирован ли пользователь как воркер."""
    workers = _load_workers()
    return worker_id in workers


def get_worker(worker_id: int) -> Optional[dict]:
    """Возвращает данные воркера."""
    workers = _load_workers()
    return workers.get(worker_id)


def get_worker_by_spam_account(spam_acc_id: str) -> Optional[int]:
    """Находит worker_id по ID спам-аккаунта."""
    if not spam_acc_id:
        return None
    workers = _load_workers()
    for wid, data in workers.items():
        if str(spam_acc_id) in data.get("spam_account_ids", []):
            return wid
    return None


def add_spam_account(worker_id: int, phone: str, acc_id: str) -> bool:
    """Добавляет спам-аккаунт воркеру."""
    workers = _load_workers()
    if worker_id not in workers:
        return False

    if phone not in workers[worker_id]["spam_accounts"]:
        workers[worker_id]["spam_accounts"].append(phone)
    if acc_id not in workers[worker_id]["spam_account_ids"]:
        workers[worker_id]["spam_account_ids"].append(acc_id)

    _save_workers(workers)
    logger.info(f"[WORKERS] Added spam account {phone} ({acc_id}) to worker {worker_id}")
    return True


def get_worker_spam_accounts(worker_id: int) -> List[str]:
    """Возвращает список спам-аккаунтов воркера (номера телефонов)."""
    worker = get_worker(worker_id)
    if not worker:
        return []
    return worker.get("spam_accounts", [])


def bind_mamont(mamont_id: int, worker_id: int) -> bool:
    """Привязывает мамонта к воркеру."""
    workers = _load_workers()
    if worker_id not in workers:
        return False

    if mamont_id not in workers[worker_id]["mamonts"]:
        workers[worker_id]["mamonts"].append(mamont_id)
        _save_workers(workers)
        logger.info(f"[WORKERS] Bound mamont {mamont_id} to worker {worker_id}")
        return True
    return False


def get_worker_for_mamont(mamont_id: int) -> Optional[int]:
    """Находит воркера, к которому привязан мамонт."""
    workers = _load_workers()
    for wid, data in workers.items():
        if mamont_id in data.get("mamonts", []):
            return wid
    return None


# ══════════════════════════════════════════════════════════════
#  ЗАРАБОТОК ВОРКЕРА
# ══════════════════════════════════════════════════════════════

def add_earning(
    worker_id: int,
    total_amount: float,
    mamont_id: int,
    bot_name: str = "mrkt",
    detail: str = ""
) -> dict:
    """
    Добавляет заработок воркеру.
    total_amount — общая сумма профита (до вычета комиссии)
    Возвращает {worker_share, total_amount}
    """
    workers = _load_workers()
    if worker_id not in workers:
        return {"error": "Worker not found"}

    worker_share = round(total_amount * WORKER_PERCENTAGE, 4)

    workers[worker_id]["earnings"]["total_earned"] += worker_share
    workers[worker_id]["earnings"]["total_logs"] += 1
    workers[worker_id]["earnings"]["history"].append({
        "timestamp": kyiv_str(),
        "amount": total_amount,
        "worker_share": worker_share,
        "bot": bot_name,
        "mamont_id": mamont_id,
        "detail": detail,
    })

    # Ограничиваем историю до 100 записей
    if len(workers[worker_id]["earnings"]["history"]) > 100:
        workers[worker_id]["earnings"]["history"] = workers[worker_id]["earnings"]["history"][-100:]

    _save_workers(workers)
    logger.info(f"[WORKERS] Worker {worker_id} earned {worker_share} TON from mamont {mamont_id}")
    return {"worker_share": worker_share, "total_amount": total_amount}


def get_worker_earnings(worker_id: int) -> dict:
    """Возвращает статистику заработка воркера."""
    worker = get_worker(worker_id)
    if not worker:
        return {"total_earned": 0.0, "total_logs": 0, "history": []}
    return worker.get("earnings", {"total_earned": 0.0, "total_logs": 0, "history": []})


def get_worker_history(worker_id: int, limit: int = 10) -> List[dict]:
    """Возвращает последние N записей истории воркера."""
    worker = get_worker(worker_id)
    if not worker:
        return []
    history = worker.get("earnings", {}).get("history", [])
    return history[-limit:]


def format_worker_stats(worker_id: int) -> str:
    """Форматирует статистику воркера для вывода."""
    worker = get_worker(worker_id)
    if not worker:
        return "❌ Вы не зарегистрированы как воркер."

    earnings = worker.get("earnings", {})
    spam_count = len(worker.get("spam_accounts", []))
    mamont_count = len(worker.get("mamonts", []))

    return (
        f"👷 <b>Панель воркера</b>\n\n"
        f"👤 @{worker.get('username', '?')}\n"
        f"🆔 <code>{worker_id}</code>\n"
        f"📅 Регистрация: {worker.get('registered_at', '?')}\n\n"
        f"💰 <b>Заработано:</b> {earnings.get('total_earned', 0):.4f} TON\n"
        f"📊 <b>Всего логов:</b> {earnings.get('total_logs', 0)}\n"
        f"📱 <b>Спам-акков:</b> {spam_count}\n"
        f"👥 <b>Мамонтов:</b> {mamont_count}\n"
        f"📈 <b>Процент:</b> {int(WORKER_PERCENTAGE * 100)}%"
    )


def format_worker_section(worker_id: int, total_amount: float) -> str:
    """Форматирует секцию с информацией о воркере для отчёта."""
    worker = get_worker(worker_id)
    if not worker:
        return ""

    share = round(total_amount * WORKER_PERCENTAGE, 4)
    return (
        f"👷 <b>Воркер:</b> @{worker.get('username', '?')}\n"
        f"💰 <b>Доля воркера:</b> {share} TON ({int(WORKER_PERCENTAGE * 100)}%)\n"
    )