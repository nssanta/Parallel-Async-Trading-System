import hashlib
import time
import json
from urllib.parse import urlencode
from decimal import Decimal, ROUND_DOWN
import httpx
import logging
import websockets
import asyncio
import hmac
import base64
import ssl
import certifi
import uuid
import os
from datetime import datetime, timezone

# Import configuration instead of loading dotenv directly
from config import API_KEY, API_SECRET, PASSPHRASE, LOG_LEVEL

# Logger Setup
log_file = os.path.join(os.path.dirname(__file__), 'okx.log')
logger = logging.getLogger('Okx')
logger.setLevel(getattr(logging, LOG_LEVEL))
os.makedirs(os.path.dirname(log_file), exist_ok=True)
open(log_file, 'a', encoding='utf-8').close()

if not logger.handlers:
    file_handler = logging.FileHandler(log_file, 'a', 'utf-8')
    file_handler.setLevel(logging.ERROR)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(logging.INFO)
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)


class Okx:
    """
    Class for interacting with OKX API (REST & WebSocket).
    """

    def __init__(self):
        """
        Initialize OKX client.
        """
        self.api_key = API_KEY
        self.api_secret = API_SECRET
        self.pass_key = PASSPHRASE

        self.base_url = 'https://www.okx.com'
        self.base_ws = "wss://ws.okx.com:8443/ws/v5/public"
        self.private_ws = 'wss://ws.okx.com:8443/ws/v5/private'

        self.private_websocket = None
        self.ticker_websocket = None

        self.last_price = 0
        self.info_for_spot_instrument = None
        self.info_for_futures_instrument = None

        self.logger = logger

    def disable_stream_handler(self):
        """Disables console logging."""
        for handler in self.logger.handlers:
            if isinstance(handler, logging.StreamHandler):
                self.logger.removeHandler(handler)

    def disable_all_logging(self):
        """Disables all logging."""
        for handler in self.logger.handlers[:]:
            self.logger.removeHandler(handler)

    def create_headers(self, request_type="GET", endpoint="", body=None):
        """
        Creates headers for API requests.
        :param request_type: HTTP method (GET, POST).
        :param endpoint: Request endpoint.
        :param body: Request body.
        :return: Dictionary of headers.
        """
        timestamp = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        body_str = json.dumps(body) if body else ''
        message = f"{timestamp}{request_type.upper()}{endpoint}{body_str}"
        mac = hmac.new(bytes(self.api_secret, 'utf-8'), bytes(message, 'utf-8'), digestmod=hashlib.sha256)
        signature = base64.b64encode(mac.digest()).decode('utf-8')

        headers = {
            'Content-Type': 'application/json',
            'OK-ACCESS-KEY': self.api_key,
            'OK-ACCESS-SIGN': signature,
            'OK-ACCESS-TIMESTAMP': timestamp,
            'OK-ACCESS-PASSPHRASE': self.pass_key,
            'x-simulated-trading': '0',
        }
        return headers

    def create_headers_2(self, request_type="GET", endpoint="", body=None, use_milliseconds=False):
        """
        Alternative header creation with different timestamp format and JSON separation.
        """
        if use_milliseconds:
            timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
        else:
            timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S") + "Z"

        body_str = json.dumps(body, separators=(',', ':')) if body else ''
        message = f"{timestamp}{request_type.upper()}{endpoint}{body_str}"
        mac = hmac.new(bytes(self.api_secret, 'utf-8'), bytes(message, 'utf-8'), digestmod=hashlib.sha256)
        signature = base64.b64encode(mac.digest()).decode('utf-8')

        headers = {
            'Content-Type': 'application/json',
            'OK-ACCESS-KEY': self.api_key,
            'OK-ACCESS-SIGN': signature,
            'OK-ACCESS-TIMESTAMP': timestamp,
            'OK-ACCESS-PASSPHRASE': self.pass_key,
            'x-simulated-trading': '0',
        }
        return headers

    async def set_leverage(self, lever, mgn_mode, inst_id=None, ccy=None, pos_side=None):
        """
        Sets leverage for instruments.
        :param lever: Leverage value.
        :param mgn_mode: Margin mode (isolated or cross).
        :param inst_id: Instrument ID.
        :param ccy: Currency.
        :param pos_side: Position side.
        :return: API response data or None.
        """
        endpoint = "/api/v5/account/set-leverage"
        body = {"lever": lever, "mgnMode": mgn_mode}
        if inst_id: body["instId"] = inst_id
        if ccy: body["ccy"] = ccy
        if pos_side and mgn_mode == "isolated": body["posSide"] = pos_side

        headers = self.create_headers_2(request_type="POST", endpoint=endpoint, body=body)

        try:
            async with httpx.AsyncClient() as client:
                response = await client.post(f"{self.base_url}{endpoint}", json=body, headers=headers)
                if response.status_code == 200:
                    data = response.json()
                    if data.get('code') == "0":
                        self.logger.info(f'[set_leverage] Leverage set successfully: {body}')
                        return data.get('data', [])
                    else:
                        self.logger.error(f"[set_leverage] Error setting leverage: {data.get('msg')}")
                        return None
                else:
                    self.logger.error(f"[set_leverage] Request error: {response.status_code}")
                    return None
        except Exception as e:
            self.logger.error(f"[set_leverage] Exception: {e}")
            return None

    async def set_position_mode(self, pos_mode):
        """
        Sets position mode (long_short_mode or net_mode).
        """
        endpoint = "/api/v5/account/set-position-mode"
        url = f"{self.base_url}{endpoint}"
        body = {"posMode": pos_mode}
        headers = self.create_headers_2(request_type="POST", endpoint=endpoint, body=body)

        try:
            async with httpx.AsyncClient() as client:
                response = await client.post(url=url, headers=headers, json=body)
                if response.status_code == 200:
                    data = response.json()
                    if data.get('code') == "0":
                        self.logger.info(f'[set_position_mode] Position mode set: {body}')
                        return data.get('data', [])
                    else:
                        self.logger.error(f"[set_position_mode] Error setting mode: {data.get('msg')}")
                        return None
                else:
                    self.logger.error(f"[set_position_mode] Request error: {response.status_code}")
                    return None
        except Exception as e:
            self.logger.error(f"[set_position_mode] Exception: {e}")
            return None

    async def get_pair_price(self, pair: str):
        """
        Gets the last price for a pair.
        :param pair: Instrument ID (e.g. 'BTC-USDT').
        :return: Last price or None.
        """
        endpoint = "/api/v5/market/ticker"
        params = {"instId": pair}
        async with httpx.AsyncClient() as client:
            try:
                response = await client.get(self.base_url + endpoint, params=params)
                if response.status_code == 200:
                    data = response.json()
                    if data["code"] == "0":
                        return data["data"][0]["last"]
                    else:
                        self.logger.error(f"[get_pair_price] API Error: {data['msg']}")
                else:
                    self.logger.error(f"[get_pair_price] HTTP Error: {response.status_code}")
            except Exception as e:
                self.logger.error(f"[get_pair_price] Exception: {e}")
        return None

    async def get_instruments_info(self, category: str = 'SPOT', symbol: str = None):
        """
        Gets instrument information.
        """
        endpoint = "/api/v5/public/instruments"
        params = {"instType": category}
        if symbol:
            params["instId"] = symbol

        async with httpx.AsyncClient() as client:
            try:
                response = await client.get(self.base_url + endpoint, params=params)
                if response.status_code == 200:
                    data = response.json()
                    if data["code"] == "0":
                        return data["data"]
                    else:
                        self.logger.error(f"[get_instruments_info] API Error: {data['msg']}")
                else:
                    self.logger.error(f"[get_instruments_info] HTTP Error: {response.status_code}")
            except Exception as e:
                self.logger.error(f"[get_instruments_info] Exception: {e}")
        return None

    async def get_save_info_instrumen(self, pair: str = "BTC-USDT", category: str = "SPOT",
                                      pair_futures: str = 'BTC-USDT-SWAP', category_futures: str = 'SWAP') -> bool:
        """
        Fetches and saves instrument info for futures.
        """
        max_attempts = 5
        required_keys = ['tickSz', 'lotSz', 'minSz', 'ctVal']

        for attempt in range(1, max_attempts + 1):
            try:
                await asyncio.sleep(0.2)
                data_futures = await self.get_instruments_info(symbol=pair_futures, category=category_futures)
                
                if not data_futures or not isinstance(data_futures, list) or not data_futures[0]:
                    if attempt < max_attempts: await asyncio.sleep(1)
                    continue

                if not all(key in data_futures[0] for key in required_keys):
                    self.logger.warning(f"Missing keys in instrument info: {required_keys}")
                    if attempt < max_attempts: await asyncio.sleep(1)
                    continue

                self.info_for_futures_instrument = data_futures[0]
                price_futures = await self.get_pair_price(pair=pair_futures)
                if price_futures:
                    self.info_for_futures_instrument['price'] = price_futures
                    return True
            except Exception as e:
                self.logger.error(f"Attempt {attempt} failed: {e}")
                if attempt < max_attempts: await asyncio.sleep(1)

        self.logger.error(f"Failed to load instrument info for {pair_futures}")
        return False

    async def create_order(self, instId: str, tdMode: str, side: str, ordType: str, sz: float, px: float = None,
                          ccy: str = None, clOrdId: str = None, tag: str = None, posSide: str = None,
                          reduceOnly: bool = None, attachAlgoOrds: list = None) -> dict:
        """
        Creates an order.
        """
        url = f"{self.base_url}/api/v5/trade/order"
        body = {
            "instId": instId,
            "tdMode": tdMode,
            "side": side,
            "ordType": ordType,
            "sz": str(sz),
        }
        if px is not None: body["px"] = str(px)
        if ccy: body["ccy"] = ccy
        if clOrdId is None: clOrdId = uuid.uuid4().hex
        body["clOrdId"] = clOrdId
        if tag: body["tag"] = tag
        if posSide: body["posSide"] = posSide
        if reduceOnly is not None: body["reduceOnly"] = reduceOnly
        if attachAlgoOrds: body["attachAlgoOrds"] = attachAlgoOrds

        headers = self.create_headers(request_type="POST", endpoint="/api/v5/trade/order", body=body)
        payload = json.dumps(body)

        try:
            async with httpx.AsyncClient() as client:
                response = await client.post(url, data=payload, headers=headers)
                data = response.json()
                if data.get("code") == '0':
                    return data
                else:
                    self.logger.error(f"[create_order] Error: {data}")
                    return data
        except Exception as err:
            self.logger.error(f"[create_order] Exception: {err}")
            return None

    async def cancel_order(self, instId: str, ordId: str = None, clOrdId: str = None) -> dict:
        """
        Cancels an order.
        """
        url = f"{self.base_url}/api/v5/trade/cancel-order"
        body = {'instId': instId}
        if ordId: body["ordId"] = ordId
        elif clOrdId: body["clOrdId"] = clOrdId
        else: return {"status": "error", "message": "ordId or clOrdId required"}

        payload = json.dumps(body)
        headers = self.create_headers(request_type="POST", endpoint="/api/v5/trade/cancel-order", body=body)

        try:
            async with httpx.AsyncClient() as client:
                response = await client.post(url, data=payload, headers=headers)
                return response.json()
        except Exception as err:
            return {"status": "error", "message": str(err)}

    async def close_position(self, instId: str, mgnMode: str, posSide: str = None, ccy: str = None,
                             autoCxl: bool = False) -> dict:
        """
        Closes a position via market order.
        """
        url = f"{self.base_url}/api/v5/trade/close-position"
        body = {
            "instId": instId,
            "mgnMode": mgnMode,
            "autoCxl": str(autoCxl).lower()
        }
        if posSide: body["posSide"] = posSide
        if ccy: body["ccy"] = ccy

        payload = json.dumps(body)
        headers = self.create_headers(request_type="POST", endpoint="/api/v5/trade/close-position", body=body)

        try:
            async with httpx.AsyncClient() as client:
                response = await client.post(url, data=payload, headers=headers)
                data = response.json()
                if data.get('code') == "0":
                    return {"status": "success", "data": data.get('data', [])}
                return {"status": "error", "message": data.get('msg', 'Unknown error')}
        except Exception as err:
            return {"status": "error", "message": str(err)}

    async def create_algo_order(self, instId: str, tdMode: str, side: str, ordType: str, sz: str,
                                posSide: str = None, tpTriggerPx: str = None, tpOrdPx: str = None,
                                slTriggerPx: str = None, slOrdPx: str = None, tag: str = None) -> dict:
        """
        Creates an algorithmic order (TP/SL).
        """
        url = f"{self.base_url}/api/v5/trade/order-algo"
        body = {
            "instId": instId,
            "tdMode": tdMode,
            "side": side,
            "ordType": ordType,
            "sz": sz
        }
        if posSide: body["posSide"] = posSide
        if tpTriggerPx: body["tpTriggerPx"] = tpTriggerPx
        if tpOrdPx: body["tpOrdPx"] = tpOrdPx
        if slTriggerPx: body["slTriggerPx"] = slTriggerPx
        if slOrdPx: body["slOrdPx"] = slOrdPx
        if tag: body["tag"] = tag

        headers = self.create_headers_2(request_type="POST", endpoint="/api/v5/trade/order-algo", body=body)

        try:
            async with httpx.AsyncClient() as client:
                response = await client.post(url, json=body, headers=headers)
                return response.json()
        except Exception as err:
            return {"status": "error", "message": str(err)}

    async def cancel_algo_order(self, algoId: str, instId: str) -> dict:
        """
        Cancels an algo order.
        """
        url = f"{self.base_url}/api/v5/trade/cancel-algos"
        body = [{'algoId': algoId, 'instId': instId}]
        headers = self.create_headers(request_type="POST", endpoint="/api/v5/trade/cancel-algos", body=body)
        payload = json.dumps(body)

        try:
            async with httpx.AsyncClient() as client:
                response = await client.post(url, data=payload, headers=headers)
                return response.json()
        except Exception as err:
            return {"status": "error", "message": str(err)}

    async def get_pending_trade_orders(self, instId: str) -> dict:
        """
        Gets pending trade orders.
        """
        url = f"{self.base_url}/api/v5/trade/orders-pending"
        params = {'instId': instId}
        query = urlencode(params)
        headers = self.create_headers(request_type="GET", endpoint=f"/api/v5/trade/orders-pending?{query}")

        try:
            async with httpx.AsyncClient() as client:
                response = await client.get(f"{url}?{query}", headers=headers)
                return response.json()
        except Exception as err:
            return {"status": "error", "message": str(err)}

    async def get_pending_algo_orders(self, ordType: str = 'conditional', algoId: str = None) -> dict:
        """
        Gets pending algo orders.
        """
        url = f"{self.base_url}/api/v5/trade/orders-algo-pending"
        params = {'ordType': ordType}
        if algoId: params['algoId'] = algoId
        query = urlencode(params)
        headers = self.create_headers(request_type="GET", endpoint=f"/api/v5/trade/orders-algo-pending?{query}")

        try:
            async with httpx.AsyncClient() as client:
                response = await client.get(f"{url}?{query}", headers=headers)
                return response.json()
        except Exception as err:
            return {"status": "error", "message": str(err)}

    async def get_position_details(self, instId: str) -> dict:
        """
        Gets position details.
        """
        url = f"{self.base_url}/api/v5/account/positions"
        params = {'instId': instId}
        query = urlencode(params)
        headers = self.create_headers(request_type="GET", endpoint=f"/api/v5/account/positions?{query}")

        try:
            async with httpx.AsyncClient() as client:
                response = await client.get(f"{url}?{query}", headers=headers)
                return response.json()
        except Exception as err:
            return {"status": "error", "message": str(err)}

    async def get_position_history(self, instId: str, limit: int = 10) -> dict:
        """
        Gets position history.
        """
        url = f"{self.base_url}/api/v5/account/positions-history"
        params = {'instId': instId, 'limit': limit}
        query = urlencode(params)
        headers = self.create_headers(request_type="GET", endpoint=f"/api/v5/account/positions-history?{query}")

        try:
            async with httpx.AsyncClient() as client:
                response = await client.get(f"{url}?{query}", headers=headers)
                return response.json()
        except Exception as err:
            return {"status": "error", "message": str(err)}

    async def amend_algo_order(self, instId: str, algoId: str, newTpTriggerPx: str = None, newTpOrdPx: str = None,
                               newSlTriggerPx: str = None, newSlOrdPx: str = None, newSz: str = None) -> dict:
        """
        Amends an algo order.
        """
        body = {"instId": instId, "algoId": algoId}
        if newTpTriggerPx: body["newTpTriggerPx"] = newTpTriggerPx
        if newTpOrdPx: body["newTpOrdPx"] = newTpOrdPx
        if newSlTriggerPx: body["newSlTriggerPx"] = newSlTriggerPx
        if newSlOrdPx: body["newSlOrdPx"] = newSlOrdPx
        if newSz: body["newSz"] = newSz

        headers = self.create_headers_2(request_type="POST", endpoint="/api/v5/trade/amend-algos", body=body)

        try:
            async with httpx.AsyncClient() as client:
                response = await client.post(f"{self.base_url}/api/v5/trade/amend-algos", json=body, headers=headers)
                return response.json()
        except Exception as err:
            return {"status": "error", "message": str(err)}

    async def set_global_position_mode(self):
        """Sets position mode to long/short."""
        await self.set_position_mode("long_short_mode")