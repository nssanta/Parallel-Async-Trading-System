import asyncio
import logging
import multiprocessing
import os
import time
import sys
from Manager.Cache import Cache
from Manager.SignalManager import SignalManager
from Telegram.UtilsTG import send_to_all_async
from Trader.OkxWebSocketClient import OkxWebSocketClient
from Trader.OkxTradingBot import TraderManager, OkxTradingBot


# Logger Setup
log_file = os.path.join(os.path.dirname(__file__), 'BotMaster.log')
logger = logging.getLogger('BotMaster')
logger.setLevel(logging.INFO)
os.makedirs(os.path.dirname(log_file), exist_ok=True)
open(log_file, 'a', encoding='utf-8').close()

if not logger.handlers:
    file_handler = logging.FileHandler(log_file, 'a', 'utf-8')
    file_handler.setLevel(logging.ERROR)
    formatter = logging.Formatter('%(asctime)s - [%(funcName)s] - %(levelname)s - %(message)s')
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    
    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(logging.INFO)
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)


class BotMaster:
    def __init__(self, signal_queue: multiprocessing.Queue):
        self.signal_queue = signal_queue
        self.cache = Cache()  # Initialize cache
        self.ws_client = OkxWebSocketClient(self.cache)  # WebSocket Client with cache
        self.trader = None
        self.manager = None

    async def start(self):
        try:
            logger.info("[BotMaster] Starting WebSocket Client...")
            # Start WS (private + public + heartbeat)
            ws_task = asyncio.create_task(self.ws_client.start())

            # Wait for login confirmation and private subscription
            await self.wait_for_ws_connection()
            await asyncio.sleep(5)  # Give WS 2–3 sec for initial push

            # Wait for balance in cache
            timeout_bal = 15
            start_bal = time.time()
            while True:
                bal = await self.cache.get_balance("cashBal")
                if bal and float(bal) > 0:
                    logger.info(f"[BotMaster] Balance loaded: {bal}")
                    break
                if time.time() - start_bal > timeout_bal:
                    logger.warning("[BotMaster] Balance timeout, continuing without it")
                    break
                await asyncio.sleep(0.5)

            # Initialize Trader
            self.trader = TraderManager(self.signal_queue, self.cache)
            await self.trader.load_symbols()

            # Load Signal Manager
            self.manager = SignalManager(self.signal_queue, self.trader, self.cache, self.ws_client)

            # ===== Wait for private positions cache population =====
            timeout = 15  # wait up to 15 sec
            start = time.time()
            while True:
                open_pos = await self.cache.get_all_open_positions()
                if open_pos:
                    logger.info(f"[BotMaster] Positions cache populated: {list(open_pos.keys())}")
                    break
                if time.time() - start > timeout:
                    logger.warning(
                        "[BotMaster] Position wait timeout, proceeding with empty cache or whatever is available")
                    break
                await asyncio.sleep(0.5)
            # ====================================================

            # ===== Start background tasks before position initialization =====
            balance_task = asyncio.create_task(self.manager.update_balance_periodically())
            
            # Initialize positions (which triggers monitors)
            await self.manager.initialize_positions()

            # ===== Subscribe to tickers for open positions =====
            for position_key in open_pos.keys():
                symbol, pos_side = position_key
                instId = f"{symbol}-USDT-SWAP"
                await self.ws_client.subscribe_to_ticker(instId)
            logger.info(f"[BotMaster] Subscribed to tickers for positions: {list(open_pos.keys())}")
            # =========================================================

            await asyncio.sleep(3)
            # 🚀 Now start signal processing
            manager_task = asyncio.create_task(self.manager.process_signals())

            # ===== Start all background tasks =====
            await asyncio.gather(ws_task, manager_task, balance_task)

        except Exception as e:
            logger.error(f"[BotMaster] Critical Error: {e}")
            await send_to_all_async(f"❌ [BotMaster] Critical Error: {e}")
            await self.ws_client.stop()
            sys.exit(1)

    async def wait_for_ws_connection(self, timeout=60):
        start_time = time.time()
        while not self.ws_client.is_authenticated:
            if time.time() - start_time > timeout:
                logger.error("[BotMaster] Failed to connect WebSocket, exiting...")
                await send_to_all_async("❌ [BotMaster] Failed to connect WebSocket, exiting...")
                await self.ws_client.stop()
                sys.exit(1)
            await asyncio.sleep(1)
        logger.info("[BotMaster] WebSocket Client connected successfully")
        await send_to_all_async("🟢 [BotMaster] WebSocket Client connected successfully")
