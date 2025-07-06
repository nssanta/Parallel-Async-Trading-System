import asyncio
import datetime
import json
import time
import hmac
import base64
import logging
import websockets
import ssl
import certifi
import os

from config import API_KEY, API_SECRET, PASSPHRASE
from Manager.Cache import Cache
from Telegram.UtilsTG import send_to_all_async

# Logger Setup
log_file = os.path.join(os.path.dirname(__file__), 'OkxWebSocketClient.log')
logger = logging.getLogger('OkxWebSocketClient')
logger.setLevel(logging.ERROR)
os.makedirs(os.path.dirname(log_file), exist_ok=True)
if not logger.handlers:
    file_handler = logging.FileHandler(log_file, 'a', 'utf-8')
    file_handler.setLevel(logging.ERROR)
    formatter = logging.Formatter('%(asctime)s - [%(funcName)s] - %(levelname)s - %(message)s')
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(logging.ERROR)
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

class OkxWebSocketClient:
    """
    WebSocket client for OKX private and public channels.
    Handles tickers, positions, account updates, and syncs with Cache.
    """
    def __init__(self, cache: Cache):
        """
        Initialize OKX WebSocket client.
        :param cache: Cache instance.
        """
        self.cache = cache
        self.api_key = API_KEY
        self.api_secret = API_SECRET
        self.passphrase = PASSPHRASE
        self.private_ws_url = "wss://ws.okx.com:8443/ws/v5/private"
        self.public_ws_url = "wss://ws.okx.com:8443/ws/v5/public"
        self.private_websocket = None
        self.public_websocket = None
        self.reconnect_interval = 5
        self.is_authenticated = False
        
        # Ticker management
        self.subscribed_tickers = set()
        self.active_tickers = set()
        
        # Health monitoring
        self.last_ticker_time = {}   # symbol -> timestamp
        self.last_ticker_price = {}  # symbol -> last price
        self.ticker_resubscribe_state = {}
        self.last_public_restart_time = 0.0
        
        # Config
        self.TICKER_STALE_SECONDS = 30
        self.TICKER_RESUBSCRIBE_COOLDOWN = 30
        self.TICKER_MAX_RESUBSCRIBE_ATTEMPTS = 3
        self.GLOBAL_STALE_SECONDS = 90
        self.PUBLIC_RESTART_COOLDOWN = 120
        
        self.tasks = []
        self._last_account_state = None
        logger.info("[__init__] WebSocket Client Initialized")

        if not all([self.api_key, self.api_secret, self.passphrase]):
            logger.error("[__init__] Missing API credentials in config")
            raise ValueError("API key, secret, or passphrase is missing")

    def generate_signature(self, timestamp: str, method: str, path: str) -> str:
        """Generates HMAC SHA256 signature."""
        try:
            message = f"{timestamp}{method}{path}"
            signature = hmac.new(
                self.api_secret.encode('utf-8'),
                message.encode('utf-8'),
                digestmod='sha256'
            ).digest()
            return base64.b64encode(signature).decode('utf-8')
        except Exception as e:
            logger.error(f"[generate_signature] Error: {e}")
            asyncio.create_task(send_to_all_async(f"❌ [generate_signature] Error: {e}"))
            return ""

    async def login(self) -> bool:
        """Authenticates with private WebSocket."""
        try:
            timestamp = str(round(time.time() * 1000) / 1000)
            sign = self.generate_signature(timestamp, 'GET', '/users/self/verify')
            login_message = {
                "op": "login",
                "args": [{
                    "apiKey": self.api_key,
                    "passphrase": self.passphrase,
                    "timestamp": timestamp,
                    "sign": sign
                }]
            }
            await self.private_websocket.send(json.dumps(login_message))
            logger.info("[login] Sent authentication request")

            response = await asyncio.wait_for(self.private_websocket.recv(), timeout=5.0)
            response_data = json.loads(response)
            if response_data.get('event') == 'login' and response_data.get('code') == '0':
                logger.info("[login] Authentication successful")
                asyncio.create_task(send_to_all_async("🟢 [login] Authentication successful"))
                self.is_authenticated = True
                return True
            else:
                logger.error(f"[login] Auth failed: {response_data.get('msg', 'Unknown error')}")
                asyncio.create_task(send_to_all_async(f"❌ [login] Auth failed: {response_data.get('msg', 'Unknown error')}"))
                self.is_authenticated = False
                return False
        except asyncio.TimeoutError:
            logger.error("[login] Auth timeout")
            asyncio.create_task(send_to_all_async("❌ [login] Auth timeout"))
            return False
        except Exception as e:
            logger.error(f"[login] Auth error: {e}")
            asyncio.create_task(send_to_all_async(f"❌ [login] Auth error: {e}"))
            return False

    async def subscribe_to_ticker(self, instId: str) -> None:
        """Subscribes to ticker channel."""
        try:
            timeout = 30
            start_time = time.time()
            while self.public_websocket is None:
                if time.time() - start_time > timeout:
                    raise TimeoutError(f"Public WS timeout for {instId}")
                logger.info(f"[subscribe_to_ticker] Waiting for public WS for {instId}...")
                await asyncio.sleep(1)
            
            if instId not in self.subscribed_tickers:
                self.subscribed_tickers.add(instId)

            if self.public_websocket is not None and instId not in self.active_tickers:
                subscribe_message = {"op": "subscribe", "args": [{"channel": "tickers", "instId": instId}]}
                await self.public_websocket.send(json.dumps(subscribe_message))
                self.active_tickers.add(instId)
                logger.info(f"[subscribe_to_ticker] Subscribed to {instId}")
                await asyncio.sleep(1)
        except Exception as e:
            logger.error(f"[subscribe_to_ticker] Error {instId}: {e}")
            asyncio.create_task(send_to_all_async(f"❌ [subscribe_to_ticker] Error {instId}: {e}"))

    async def unsubscribe_from_ticker(self, instId: str) -> None:
        """Unsubscribes from ticker channel."""
        try:
            if instId in self.subscribed_tickers:
                unsubscribe_message = {"op": "unsubscribe", "args": [{"channel": "tickers", "instId": instId}]}
                if self.public_websocket is not None:
                    await self.public_websocket.send(json.dumps(unsubscribe_message))
                self.subscribed_tickers.remove(instId)
                self.active_tickers.discard(instId)
                logger.info(f"[unsubscribe_from_ticker] Unsubscribed from {instId}")
                await asyncio.sleep(1)
        except Exception as e:
            logger.error(f"[unsubscribe_from_ticker] Error {instId}: {e}")
            asyncio.create_task(send_to_all_async(f"❌ [unsubscribe_from_ticker] Error {instId}: {e}"))

    async def _handle_ticker(self, data: dict) -> None:
        """Handles ticker messages."""
        try:
            for ticker in data.get("data", []):
                inst_id = ticker["instId"]
                symbol = inst_id.replace("-USDT-SWAP", "")
                price = float(ticker.get("last", 0))
                if price > 0:
                    await self.cache.update_price(symbol, price)
                    self._mark_ticker_received(symbol, price, inst_id)
        except Exception as e:
            logger.error(f"[_handle_ticker] Error: {e}")
            asyncio.create_task(send_to_all_async(f"❌ [_handle_ticker] Error: {e}"))

    def _mark_ticker_received(self, symbol: str, price: float, inst_id: str | None = None) -> None:
        """Updates last ticker time."""
        try:
            now = time.time()
            self.last_ticker_time[symbol] = now
            self.last_ticker_price[symbol] = price
            if inst_id is not None and inst_id in self.ticker_resubscribe_state:
                state = self.ticker_resubscribe_state.get(inst_id, {})
                state["attempts"] = 0
                state["last_attempt"] = now
                state["alert_sent"] = False
                self.ticker_resubscribe_state[inst_id] = state
        except Exception as e:
            logger.error(f"[_mark_ticker_received] Error: {e}")

    async def subscribe_private(self) -> None:
        """Subscribes to private channels."""
        try:
            subscribe_message = {
                "op": "subscribe",
                "args": [
                    {"channel": "positions", "instType": "SWAP"},
                    {"channel": "account"},
                    {"channel": "orders", "instType": "SWAP"}
                ]
            }
            await self.private_websocket.send(json.dumps(subscribe_message))
            logger.info("[subscribe_private] Subscribing to positions, account, orders")
            asyncio.create_task(send_to_all_async("🟢 [subscribe_private] Subscribing to private channels"))
        except Exception as e:
            logger.error(f"[subscribe_private] Error: {e}")
            asyncio.create_task(send_to_all_async(f"❌ [subscribe_private] Error: {e}"))

    async def handle_message(self, message: str, is_public: bool = False) -> None:
        """Handles incoming WS messages."""
        try:
            if not message or message.strip() in ("ping", "pong"):
                return

            data = json.loads(message)
            channel = data.get("arg", {}).get("channel")
            
            if data.get("event") == "error":
                logger.error(f"[handle_message] OKX Error: {data.get('msg')}, code: {data.get('code')}")
                asyncio.create_task(send_to_all_async(f"❌ [handle_message] OKX Error: {data.get('msg')}"))
            elif data.get("event") == "subscribe":
                logger.info(f"[handle_message] Subscribed: {data.get('arg')}")
            elif channel == "tickers" and is_public:
                await self._handle_ticker(data)
            elif channel == "positions" and not is_public:
                await self._handle_position(data)
            elif channel == "account" and not is_public:
                await self._handle_account(data)
            elif channel == "orders" and not is_public:
                await self._handle_order(data)
        except json.JSONDecodeError as e:
            logger.error(f"[handle_message] JSON Error: {e}")
        except Exception as e:
            logger.error(f"[handle_message] Error: {e}")

    async def _handle_order(self, data: dict) -> None:
        """Handles order updates."""
        try:
            for order in data.get("data", []):
                symbol = order["instId"].replace("-USDT-SWAP", "")
                sf = lambda v: float(v) if v and v != '' else 0.0
                pos_side = order.get("posSide", "").lower()
                if pos_side not in ["long", "short"]:
                    continue

                reduce_only_raw = order.get("reduceOnly")
                reduce_only = False
                if isinstance(reduce_only_raw, bool):
                    reduce_only = reduce_only_raw
                else:
                    try:
                        reduce_only = str(reduce_only_raw).lower() in ("true", "1", "yes")
                    except Exception:
                        reduce_only = False

                if order.get("state") == "filled" and reduce_only:
                    close_data = {
                        "fillPx": sf(order.get("fillPx")),
                        "fillPnl": sf(order.get("fillPnl")),
                        "fillTime": int(order.get("fillTime", 0)),
                        "posSide": pos_side,
                        "fillSz": sf(order.get("fillSz", 1))
                    }
                    await self.cache.update_close(symbol, pos_side, close_data)
                else:
                    order_data = {
                        "algoId": order.get("algoId", ""),
                        "instId": order.get("instId", ""),
                        "posSide": pos_side,
                        "tpTriggerPx": sf(order.get("tpTriggerPx")),
                        "state": order.get("state", ""),
                        "reduceOnly": order.get("reduceOnly", "false")
                    }
                    await self.cache.update_order(symbol, pos_side, order_data)
        except Exception as e:
            logger.error(f"[_handle_order] Error: {e}")

    async def _handle_position(self, data: dict) -> None:
        """Handles position updates."""
        try:
            for pos in data.get("data", []):
                symbol = pos["instId"].replace("-USDT-SWAP", "")
                sf = lambda v: float(v) if v else 0.0
                pos_side = pos.get("posSide", "").lower()
                
                if pos_side not in ["long", "short"]:
                    continue
                
                current = {
                    "avgPx": sf(pos.get("avgPx")),
                    "posSide": pos_side,
                    "pos": sf(pos.get("pos")),
                    "margin": sf(pos.get("margin")),
                    "mgnMode": pos.get("mgnMode", "cross"),
                    "cTime": int(pos.get("cTime", 0)),
                    "lever": sf(pos.get("lever")),
                    "markPx": sf(pos.get("markPx")),
                    "liqPx": sf(pos.get("liqPx")),
                    "notionalUsd": sf(pos.get("notionalUsd")),
                    "uplLastPx": sf(pos.get("uplLastPx")),
                    "uplRatioLastPx": sf(pos.get("uplRatioLastPx")),
                    "posId": pos.get("posId", ""),
                    "realizedPnl": sf(pos.get("realizedPnl"))
                }

                if current["pos"] != 0:
                    await self.cache.update_position(symbol, pos_side, current)
                else:
                    existing_close = await self.cache.get_closed_position(symbol, pos_side) or {}
                    close_data = {
                        **existing_close,
                        "pos": 0.0,
                        "closeTime": int(pos.get("uTime", pos.get("cTime", int(datetime.datetime.now().timestamp() * 1000))))
                    }
                    await self.cache.update_close(symbol, pos_side, close_data)
                    await self.cache.remove_position(symbol, pos_side)

                    open_positions = await self.cache.get_all_open_positions()
                    if not any(sym == symbol for (sym, _) in open_positions.keys()):
                        await self.unsubscribe_from_ticker(f"{symbol}-USDT-SWAP")

        except Exception as e:
            logger.error(f"[_handle_position] Error: {e}")

    async def _handle_account(self, data: dict) -> None:
        """Handles account updates."""
        def safe_float(val, default=0.0):
            try:
                return float(val or default)
            except (ValueError, TypeError):
                return default

        try:
            for account_data in data.get("data", []):
                details = account_data.get("details", [{}])[0]
                current_key_fields = {
                    "availBal": safe_float(details.get("availBal")),
                    "cashBal": safe_float(details.get("cashBal")),
                    "totalEq": safe_float(account_data.get("totalEq")),
                    "mmr": safe_float(details.get("mmr", 100.0))
                }

                if self._last_account_state is None or any(
                    self._last_account_state.get(key) != current_key_fields.get(key)
                    for key in current_key_fields
                ):
                    snapshot_path = os.path.join(os.path.dirname(__file__), 'account_snapshot.log')
                    with open(snapshot_path, 'a', encoding='utf-8') as f:
                        f.write(f"{datetime.datetime.now().isoformat()} - {json.dumps(account_data)}\n")
                    self._last_account_state = current_key_fields

                await self.cache.update_balance(current_key_fields)

        except Exception as e:
            logger.error(f"[_handle_account] Error: {e}")

    async def connect_private(self) -> None:
        """Connects to private WebSocket."""
        ssl_context = ssl.create_default_context()
        ssl_context.load_verify_locations(certifi.where())

        while True:
            try:
                logger.info("[connect_private] Connecting...")
                async with websockets.connect(self.private_ws_url, ssl=ssl_context) as ws:
                    self.private_websocket = ws
                    logger.info("[connect_private] Connected")
                    asyncio.create_task(send_to_all_async("🟢 [connect_private] Connected"))

                    if not await self.login():
                        logger.error(f"[connect_private] Auth failed, retry in {self.reconnect_interval}s")
                        await asyncio.sleep(self.reconnect_interval)
                        continue

                    await self.subscribe_private()

                    async for message in ws:
                        await self.handle_message(message, is_public=False)

            except Exception as e:
                logger.error(f"[connect_private] Error: {e}. Retry in {self.reconnect_interval}s")
                asyncio.create_task(send_to_all_async(f"❌ [connect_private] Error: {e}"))
                await asyncio.sleep(self.reconnect_interval)

    async def connect_public(self) -> None:
        """Connects to public WebSocket."""
        ssl_context = ssl.create_default_context()
        ssl_context.load_verify_locations(certifi.where())

        while True:
            try:
                logger.info("[connect_public] Connecting...")
                self.active_tickers.clear()
                async with websockets.connect(self.public_ws_url, ssl=ssl_context) as ws:
                    self.public_websocket = ws
                    logger.info("[connect_public] Connected")
                    asyncio.create_task(send_to_all_async("🟢 [connect_public] Connected"))

                    # Resubscribe tickers
                    for inst_id in list(self.subscribed_tickers):
                        if inst_id not in self.active_tickers:
                            subscribe_message = {"op": "subscribe", "args": [{"channel": "tickers", "instId": inst_id}]}
                            await self.public_websocket.send(json.dumps(subscribe_message))
                            self.active_tickers.add(inst_id)
                            await asyncio.sleep(1)

                    async for message in ws:
                        await self.handle_message(message, is_public=True)

            except Exception as e:
                logger.error(f"[connect_public] Error: {e}. Retry in {self.reconnect_interval}s")
                asyncio.create_task(send_to_all_async(f"❌ [connect_public] Error: {e}"))
                await asyncio.sleep(self.reconnect_interval)

    async def start(self) -> None:
        """Starts WebSocket client tasks."""
        try:
            self.tasks = [
                asyncio.create_task(self.connect_private()),
                asyncio.create_task(self.connect_public()),
                asyncio.create_task(self.send_heartbeat()),
                asyncio.create_task(self.monitor_tickers_health())
            ]
            logger.info("[start] Client started")
            asyncio.create_task(send_to_all_async("🟢 [start] Client started"))
            await asyncio.gather(*self.tasks, return_exceptions=True)
        except Exception as e:
            logger.error(f"[start] Start error: {e}")

    async def send_heartbeat(self) -> None:
        """Sends pings."""
        while True:
            try:
                if self.private_websocket and self.is_authenticated:
                    await self.private_websocket.send("ping")
                if self.public_websocket:
                    await self.public_websocket.send("ping")
                await asyncio.sleep(20)
            except Exception as e:
                logger.error(f"[send_heartbeat] Error: {e}")
                await asyncio.sleep(5)

    async def monitor_tickers_health(self) -> None:
        """Monitors ticker freshness."""
        await asyncio.sleep(10)
        while True:
            try:
                now = time.time()
                stale_symbols_global = []

                for inst_id in list(self.subscribed_tickers):
                    symbol = inst_id.replace("-USDT-SWAP", "")
                    last_ts = self.last_ticker_time.get(symbol)

                    if not last_ts:
                        continue

                    age = now - last_ts
                    if age > self.TICKER_STALE_SECONDS:
                        state = self.ticker_resubscribe_state.get(inst_id, {"attempts": 0, "last_attempt": 0.0, "alert_sent": False})

                        if (now - state["last_attempt"] >= self.TICKER_RESUBSCRIBE_COOLDOWN and state["attempts"] < self.TICKER_MAX_RESUBSCRIBE_ATTEMPTS):
                            state["attempts"] += 1
                            state["last_attempt"] = now
                            self.ticker_resubscribe_state[inst_id] = state
                            
                            try:
                                if inst_id in self.active_tickers:
                                    await self.unsubscribe_from_ticker(inst_id)
                                await asyncio.sleep(1)
                                await self.subscribe_to_ticker(inst_id)
                            except Exception as e:
                                logger.error(f"[monitor_tickers_health] Resub error {inst_id}: {e}")

                    if last_ts and (now - last_ts) > self.GLOBAL_STALE_SECONDS:
                        stale_symbols_global.append(symbol)

                if stale_symbols_global and self.public_websocket is not None and (now - self.last_public_restart_time) > self.PUBLIC_RESTART_COOLDOWN:
                    self.last_public_restart_time = now
                    try:
                        await self.public_websocket.close()
                    except Exception:
                        pass

                await asyncio.sleep(5)
            except Exception as e:
                logger.error(f"[monitor_tickers_health] Error: {e}")
                await asyncio.sleep(5)

    async def stop(self) -> None:
        """Stops client."""
        try:
            for task in self.tasks:
                task.cancel()
            await asyncio.gather(*self.tasks, return_exceptions=True)
            self.tasks.clear()

            if self.private_websocket:
                await self.private_websocket.close()
            if self.public_websocket:
                await self.public_websocket.close()

            self.is_authenticated = False
            self.subscribed_tickers.clear()
            self.active_tickers.clear()
            logger.info("[stop] Stopped")
        except Exception as e:
            logger.error(f"[stop] Error: {e}")