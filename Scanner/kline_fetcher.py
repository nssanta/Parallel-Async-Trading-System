import asyncio
import httpx
import logging
import os
import time
from datetime import datetime, timedelta
from typing import List, Dict, Any, Callable, Optional

# =====================================================================================================================
# // Logger Setup
# =====================================================================================================================
def setup_logger():
    """
    Configures and returns the logger for the module.
    """
    log_file = os.path.join(os.path.dirname(__file__), 'KlinesFetcher.log')
    logger = logging.getLogger('KlinesFetcher')
    logger.setLevel(logging.INFO)
    os.makedirs(os.path.dirname(log_file), exist_ok=True)

    if not logger.handlers:
        # File Handler
        file_handler = logging.FileHandler(log_file, 'a', 'utf-8')
        file_handler.setLevel(logging.INFO)
        formatter = logging.Formatter('%(asctime)s - [%(funcName)s] - %(levelname)s - %(message)s')
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

        # Stream Handler
        stream_handler = logging.StreamHandler()
        stream_handler.setLevel(logging.INFO)
        stream_handler.setFormatter(formatter)
        logger.addHandler(stream_handler)

    return logger

logger = setup_logger()

# =====================================================================================================================
# // Binance Klines Fetcher Class
# =====================================================================================================================
class BinanceKlinesFetcher:
    """
    Manages asynchronous fetching, validation, and scheduling of Kline requests from Binance Futures API.
    """
    def __init__(self, timeframes: List[str], symbols: List[str], klines_limit: int = 100, kline_processor: Optional[Callable] = None):
        """
        Initializes fetcher with given parameters.
        """
        # // Core Parameters
        self.base_url = "https://fapi.binance.com/fapi/v1/klines"
        self.timeframes = timeframes
        self.initial_symbols = symbols
        self.klines_limit = klines_limit
        self.valid_symbols: List[str] = []
        self.kline_processor = kline_processor

        # Safety buffer
        self.candle_close_safety_ms = 1000 
        self.fetch_delay_seconds = 2

        # // Client Session and Semaphore
        self.session: Optional[httpx.AsyncClient] = None
        self.semaphore = asyncio.Semaphore(15) 

        # // Request Weights
        self.request_weights = {
            (1, 99): 1,
            (100, 499): 2,
            (500, 1000): 5,
            (1001, 1500): 10,
        }
        self.request_weight = self._get_request_weight()

        logger.info(f"BinanceKlinesFetcher Initialized.")
        logger.info(f"Timeframes: {self.timeframes}")
        logger.info(f"Limit: {self.klines_limit}, Weight: {self.request_weight}")

    def _get_request_weight(self) -> int:
        for (low, high), weight in self.request_weights.items():
            if low <= self.klines_limit <= high:
                return weight
        return 1

    async def start_session(self):
        if self.session is None or self.session.is_closed:
            self.session = httpx.AsyncClient()
            logger.info("New httpx session created.")

    async def close_session(self):
        if self.session:
            await self.session.aclose()
            logger.info("Httpx session closed.")

    async def validate_symbols(self) -> List[str]:
        if not self.session:
            await self.start_session()

        logger.info(f"Validating {len(self.initial_symbols)} symbols.")
        valid_symbols = []

        for symbol in self.initial_symbols:
            try:
                params = {"symbol": symbol, "interval": "1m", "limit": 1}
                async with self.semaphore:
                    response = await self.session.get(self.base_url, params=params, timeout=10)
                    if response.status_code == 200:
                        data = response.json()
                        if data:
                            valid_symbols.append(symbol)
                            logger.info(f"Symbol {symbol} valid.")
                        else:
                            logger.warning(f"Symbol {symbol}: Empty API response.")
                    else:
                        logger.warning(f"Symbol {symbol}: HTTP {response.status_code}")
            except Exception as e:
                logger.error(f"Validation error for {symbol}: {e}")

        self.valid_symbols = valid_symbols
        logger.info(f"Validation complete. Valid symbols: {len(valid_symbols)}")
        return valid_symbols

    async def fetch_single_kline(self, symbol: str, interval: str) -> List[List[Any]]:
        if not self.session:
            await self.start_session()

        params = {
            "symbol": symbol,
            "interval": interval,
            "limit": self.klines_limit
        }

        max_retries = 3
        for attempt in range(max_retries):
            try:
                async with self.semaphore:
                    response = await self.session.get(self.base_url, params=params, timeout=10)
                    if response.status_code == 200:
                        data = response.json()
                        if data:
                            return self._filter_open_candle(data, symbol, interval)
                        else:
                            logger.warning(f"Empty response for {symbol} {interval}.")
                            return []
                    elif response.status_code == 400:
                        logger.error(f"HTTP 400 for {symbol} {interval}")
                        return []
                    elif response.status_code in (418, 429):
                        retry_after = int(response.headers.get("Retry-After", 60))
                        logger.warning(f"IP Ban. Waiting {retry_after} sec...")
                        await asyncio.sleep(retry_after)
                        continue
                    else:
                        logger.error(f"HTTP {response.status_code} for {symbol} {interval}")
                        return []

            except Exception as e:
                logger.error(f"Exception for {symbol} {interval}: {e}")
                if attempt < max_retries - 1:
                    await asyncio.sleep(1)

        return []

    def _filter_open_candle(self, data: List[List[Any]], symbol: str, interval: str) -> List[List[Any]]:
        """
        Filters candle list.
        Logs debug data for comparison.
        """
        if not data:
            return []

        last_candle = data[-1]
        
        # Binance format: [OpenTime, Open, High, Low, Close, Volume, CloseTime...]
        open_time_ms = int(last_candle[0])
        c_open = last_candle[1]
        c_high = last_candle[2]
        c_low = last_candle[3]
        c_close = last_candle[4]
        close_time_ms = int(last_candle[6])
        
        now_ms = int(time.time() * 1000)

        open_str = datetime.fromtimestamp(open_time_ms / 1000).strftime('%H:%M:%S')
        close_str = datetime.fromtimestamp(close_time_ms / 1000).strftime('%H:%M:%S.%f')[:-3]
        now_str = datetime.fromtimestamp(now_ms / 1000).strftime('%H:%M:%S.%f')[:-3]
        
        diff_ms = close_time_ms - now_ms 
        
        # Remove logic
        should_remove = close_time_ms > (now_ms - self.candle_close_safety_ms)
        
        status = "REMOVED (OPEN)" if should_remove else "KEPT (CLOSED)"
        
        is_fresh = abs(now_ms - close_time_ms) < 60000 
        
        if should_remove or is_fresh:
             logger.debug(f"[DATA_DEBUG] {symbol} {interval} | Action: {status} | "
                            f"Now: {now_str} | Candle: [OT: {open_str} -> CT: {close_str}] | "
                            f"Price: O={c_open} H={c_high} L={c_low} C={c_close} | Diff: {diff_ms}ms")

        if should_remove:
            return data[:-1]
        
        return data

    def _parse_timeframe_to_minutes(self, timeframe: str) -> int:
        if timeframe.endswith('m'):
            return int(timeframe[:-1])
        elif timeframe.endswith('h'):
            return int(timeframe[:-1]) * 60
        elif timeframe.endswith('d'):
            return int(timeframe[:-1]) * 1440
        return 1

    def _should_fetch_timeframe(self, timeframe: str, current_minute: int) -> bool:
        tf_minutes = self._parse_timeframe_to_minutes(timeframe)
        return current_minute % tf_minutes == 0

    def calculate_request_schedule(self, current_time: datetime) -> List[tuple]:
        current_minute = current_time.minute
        schedule = []
        active_timeframes = [tf for tf in self.timeframes if self._should_fetch_timeframe(tf, current_minute)]

        if not active_timeframes:
            return schedule

        logger.info(f"Active timeframes for minute {current_minute}: {active_timeframes}")
        all_requests = [(symbol, tf) for tf in active_timeframes for symbol in self.valid_symbols]

        current_weight = 0
        max_weight = 2400

        for symbol, tf in all_requests:
            if current_weight + self.request_weight > max_weight:
                break
            schedule.append((symbol, tf))
            current_weight += self.request_weight

        logger.info(f"Scheduled {len(schedule)} requests.")
        return schedule

    async def run(self):
        logger.info("BinanceKlinesFetcher Loop Started.")

        if not self.valid_symbols:
            logger.error("No valid symbols.")
            return

        await self.start_session()

        try:
            while True:
                current_time = datetime.utcnow()
                
                schedule = self.calculate_request_schedule(current_time)

                if schedule:
                    tasks = [self.fetch_single_kline(symbol, tf) for symbol, tf in schedule]
                    results = await asyncio.gather(*tasks, return_exceptions=True)

                    for (symbol, tf), result in zip(schedule, results):
                        if isinstance(result, Exception):
                            logger.error(f"Error {symbol} {tf}: {result}")
                        elif result and self.kline_processor:
                            await self.kline_processor(symbol, tf, result)

                now_after = datetime.utcnow()
                next_minute = now_after.replace(second=0, microsecond=0) + timedelta(minutes=1)
                sleep_time = (next_minute - now_after).total_seconds() + self.fetch_delay_seconds
                
                logger.debug(f"Sleeping {sleep_time:.2f} sec...")
                await asyncio.sleep(sleep_time)

        except KeyboardInterrupt:
            logger.info("Stopping.")
        finally:
            await self.close_session()

# =====================================================================================================================
async def main():
    pass

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass