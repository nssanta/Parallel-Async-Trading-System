import asyncio
import json
from pathlib import Path
import httpx
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes
from config import TELEGRAM_TOKEN
import logging

# Logger Setup
logger = logging.getLogger('SignalLogger')

# Path to subscribers file
SUBSCRIBERS_FILE = Path(__file__).parent.parent / "subscribers.json"

# Global cache for subscribers list
_subscribers_cache = None

def load_subscribers() -> list[int]:
    """
    Loads subscribers list from subscribers.json and caches it.
    Called once at startup or when cache update is needed.
    :return: List of subscriber chat IDs.
    """
    global _subscribers_cache
    if _subscribers_cache is not None:
        return _subscribers_cache
    try:
        with SUBSCRIBERS_FILE.open('r', encoding='utf-8') as f:
            data = json.load(f)
        _subscribers_cache = [int(chat_id) for chat_id in data]
        logger.info("[load_subscribers] Subscribers loaded into cache")
        return _subscribers_cache
    except Exception as e:
        logger.error(f"[load_subscribers] Error loading subscribers: {e}")
        _subscribers_cache = []
        return _subscribers_cache

def save_subscribers(subscribers: list[int]) -> None:
    """
    Saves subscribers list to file and updates cache.
    :param subscribers: List of subscriber chat IDs.
    """
    global _subscribers_cache
    try:
        with SUBSCRIBERS_FILE.open('w', encoding='utf-8') as f:
            json.dump(subscribers, f, ensure_ascii=False, indent=2)
        _subscribers_cache = subscribers
        logger.info("[save_subscribers] Subscribers saved and cache updated")
    except Exception as e:
        logger.error(f"[save_subscribers] Error saving subscribers: {e}")

async def send_to_all_async(text: str) -> None:
    """
    Asynchronously sends a message to all subscribers from cache.
    :param text: Message text.
    """
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        async with httpx.AsyncClient() as client:
            semaphore = asyncio.Semaphore(10)
            async def send_message(chat_id: int) -> None:
                async with semaphore:
                    try:
                        await client.post(url, data={"chat_id": chat_id, "text": text}, timeout=5.0)
                    except Exception as e:
                        logger.error(f"[send_to_all_async] Error sending to chat_id {chat_id}: {e}")
            tasks = [send_message(chat_id) for chat_id in load_subscribers()]
            await asyncio.gather(*tasks, return_exceptions=True)
    except Exception as e:
        logger.error(f"[send_to_all_async] Error sending messages: {e}")

async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Handles /start command, checking if chat_id is authorized.
    """
    try:
        chat_id = update.effective_chat.id
        subscribers = load_subscribers()
        if chat_id not in subscribers:
            await update.message.reply_text("Access denied. 📛")
            logger.info(f"[start_command] Subscription rejected for chat_id {chat_id}")
            return
        await update.message.reply_text("You are subscribed to notifications! 📈")
        logger.info(f"[start_command] Subscription confirmed for chat_id {chat_id}")
    except Exception as e:
        logger.error(f"[start_command] Error handling /start: {e}")
        await update.message.reply_text("Error subscribing. Try again later.")

def run_telegram_bot() -> None:
    """
    Starts the Telegram bot for command handling.
    """
    try:
        # Load subscribers into cache at bot start
        load_subscribers()
        app = Application.builder().token(TELEGRAM_TOKEN).build()
        app.add_handler(CommandHandler("start", start_command))
        app.run_polling()
    except Exception as e:
        logger.error(f"[run_telegram_bot] Bot start error: {e}")
