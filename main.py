import multiprocessing
import asyncio
import sys
from config import BUFFER_SIZE
from Agregator.Server import run_server
from Manager.BotMaster import BotMaster
from Telegram.UtilsTG import run_telegram_bot


def run_bot_master(signal_queue: multiprocessing.Queue):
    """
    Starts the BotMaster process which manages the trading logic.
    :param signal_queue: Multiprocessing queue to receive signals
    """
    try:
        bot_master = BotMaster(signal_queue)
        asyncio.run(bot_master.start())
    except Exception as e:
        print(f"[run_bot_master] Error: {e}")
        raise


def main():
    """
    Main entry point of the application.
    Initializes and starts the Server, BotMaster, and Telegram Bot processes.
    """
    # Signal queue from FastAPI
    signal_queue = multiprocessing.Queue(maxsize=BUFFER_SIZE)

    # API Server Process
    server_process = multiprocessing.Process(
        target=run_server,
        args=(signal_queue,),
        name="ServerProcess"
    )

    # Trading Logic Process
    bot_master_process = multiprocessing.Process(
        target=run_bot_master,
        args=(signal_queue,),
        name="BotMasterProcess"
    )

    # Telegram Bot Process (polling)
    telegram_process = multiprocessing.Process(
        target=run_telegram_bot,
        name="TelegramBotProcess"
    )

    # Start all processes
    server_process.start()
    bot_master_process.start()
    telegram_process.start()

    # Wait for completion (usually Ctrl+C)
    server_process.join()
    bot_master_process.join()
    telegram_process.join()


if __name__ == "__main__":
    multiprocessing.set_start_method('spawn', force=True)
    main()