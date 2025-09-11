import functools
import json
import os
import logging
import time
from collections import deque
from typing import List, Dict, Any
import asyncio
import httpx
from pydantic import BaseModel

import pandas as pd

from kline_fetcher import BinanceKlinesFetcher
from info.indicator import compute_indicator

# =====================================================================================================================
# // Settings
# =====================================================================================================================
TARGET_IP = "127.0.0.1"
TARGET_PORT = 8760
TARGET_URL = f"http://{TARGET_IP}:{TARGET_PORT}/public/webhook"

# =====================================================================================================================
# // Logging
# =====================================================================================================================
def setup_loggers():
    """
    Configures loggers:
    - signals_logger: writes signals to signals.log
    - error_logger: writes errors to errors.log
    """
    log_dir = os.path.join(os.path.dirname(__file__), 'logs')
    os.makedirs(log_dir, exist_ok=True)

    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

    # --- Logger for Signals ---
    sig_logger = logging.getLogger('Signals')
    sig_logger.setLevel(logging.INFO)
    sig_handler = logging.FileHandler(os.path.join(log_dir, 'signals.log'), encoding='utf-8')
    sig_handler.setFormatter(formatter)
    sig_logger.addHandler(sig_handler)
    sig_logger.propagate = False

    # --- Logger for Errors ---
    err_logger = logging.getLogger('Errors')
    err_logger.setLevel(logging.ERROR)
    err_handler = logging.FileHandler(os.path.join(log_dir, 'errors.log'), encoding='utf-8')
    err_handler.setFormatter(formatter)
    err_logger.addHandler(err_handler)
    err_logger.propagate = False

    return sig_logger, err_logger

signals_logger, error_logger = setup_loggers()

# =====================================================================================================================
# // Signal Model
# =====================================================================================================================
class Signal(BaseModel):
    symbol: str
    timeframe: str
    signal: str
    timestamp: int

# =====================================================================================================================
# // Indicator Settings
# =====================================================================================================================

DEFAULT_INDICATOR_SETTINGS = {
    "macd_fast": 12,
    "macd_slow": 26,
    "macd_signal": 9,
}

INDICATOR_SETTINGS_BY_TIMEFRAME = {
    "15m": {
        "macd_fast": 12,
        "macd_slow": 26,
        "macd_signal": 9,
    },
    "4h": {
        "macd_fast": 12,
        "macd_slow": 26,
        "macd_signal": 9,
    },
}

def get_indicator_settings(timeframe: str) -> dict:
    settings = INDICATOR_SETTINGS_BY_TIMEFRAME.get(timeframe)
    if settings is None:
        return DEFAULT_INDICATOR_SETTINGS.copy()
    return settings

# =====================================================================================================================
# // Indicator Scanner
# =====================================================================================================================
class IndicatorScanner:
    """
    Orchestrates Fetcher and Indicator.
    1. Receives klines from BinanceKlinesFetcher.
    2. Stores kline history.
    3. Runs indicator calculation on updates.
    4. Notifies about signals.
    """
    def __init__(self, timeframes: List[str], symbols: List[str], history_size: int = 300):
        self.timeframes = timeframes
        self.symbols = symbols
        self.history_size = history_size
        self.klines_history: Dict[str, Dict[str, deque]] = {}
        self.last_processed_ts: Dict[str, Dict[str, int]] = {}

        self.fetcher = BinanceKlinesFetcher(
            timeframes=self.timeframes,
            symbols=self.symbols,
            klines_limit=self.history_size,
            kline_processor=self.process_kline_update
        )
        
        self.valid_symbols = []
        self.http_client = httpx.AsyncClient(timeout=5.0)

        print(f"IndicatorScanner initialized.")

    async def _fetch_initial_history(self):
        """Loads initial kline history."""
        print("Loading initial history...")
        tasks = []
        for symbol in self.valid_symbols:
            for tf in self.timeframes:
                tasks.append(self.fetcher.fetch_single_kline(symbol, tf))

        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        task_idx = 0
        for symbol in self.valid_symbols:
            for tf in self.timeframes:
                result = results[task_idx]
                if isinstance(result, Exception):
                    error_logger.error(f"Error loading history for {symbol} {tf}: {result}")
                elif result:
                    self.klines_history[symbol][tf].extend(result)
                task_idx += 1
        print("Initial history loaded.")

    async def process_kline_update(self, symbol: str, timeframe: str, klines: List[List[Any]]):
        if not klines:
            return

        history = self.klines_history[symbol][timeframe]
        last_history_ts = history[-1][0] if history else 0

        new_candles = [k for k in klines if k[0] > last_history_ts]

        if not new_candles:
            if history and klines and history[-1][0] == klines[-1][0]:
                if history[-1] != klines[-1]:
                    history[-1] = klines[-1]
                    await self._run_indicator(symbol, timeframe)
            return

        new_candles.sort(key=lambda x: x[0])

        for candle in new_candles:
            history.append(candle)

        await self._run_indicator(symbol, timeframe)

    def _prepare_dataframe(self, history: deque) -> pd.DataFrame:
        if not history:
            return pd.DataFrame()

        df = pd.DataFrame(list(history), columns=[
            'Open time', 'Open', 'High', 'Low', 'Close', 'Volume',
            'Close time', 'Quote asset volume', 'Number of trades',
            'Taker buy base asset volume', 'Taker buy quote asset volume', 'Ignore'
        ])
        df['Open time'] = pd.to_datetime(df['Open time'], unit='ms')
        df['Close'] = pd.to_numeric(df['Close'])
        df['High'] = pd.to_numeric(df['High'])
        df['Low'] = pd.to_numeric(df['Low'])
        return df

    async def send_signal(self, signal_data: Signal):
        try:
            response = await self.http_client.post(TARGET_URL, json=signal_data.model_dump())
            if response.status_code != 200:
                error_logger.error(f"Signal send error {signal_data.symbol}: HTTP {response.status_code} - {response.text}")
            else:
                signals_logger.info(f"Signal sent: {signal_data.symbol} {signal_data.timeframe} {signal_data.signal}")
        except Exception as e:
            error_logger.error(f"Connection error sending signal {signal_data.symbol}: {e}")

    async def _run_indicator(self, symbol: str, timeframe: str):
        history = self.klines_history[symbol][timeframe]
        if len(history) < 50: 
            return

        last_ts = history[-1][0]
        symbol_ts = self.last_processed_ts.get(symbol)
        if symbol_ts is not None and symbol_ts.get(timeframe) == last_ts:
            return

        df = self._prepare_dataframe(history)

        try:
            indicator_settings = get_indicator_settings(timeframe)
            
            loop = asyncio.get_running_loop()
            indicator_task = functools.partial(compute_indicator, df=df, **indicator_settings)
            
            df_with_signals = await loop.run_in_executor(None, indicator_task)

            self.last_processed_ts.setdefault(symbol, {})[timeframe] = last_ts

            last_row = df_with_signals.iloc[-1]
            
            signal_type = None
            if last_row['buy_signal']:
                signal_type = "long" # Changed to lowercase to match Bot expectations
            elif last_row['sell_signal']:
                signal_type = "short" # Changed to lowercase to match Bot expectations

            if signal_type:
                sig = Signal(
                    symbol=symbol,
                    timeframe=timeframe,
                    signal=signal_type,
                    timestamp=int(time.time() * 1000)
                )
                
                signals_logger.info(f"SIGNAL: {signal_type} on {symbol} [{timeframe}] at {last_row['Close']}")
                await self.send_signal(sig)

        except Exception as e:
            error_logger.error(f"Error calculating indicator for {symbol} {timeframe}: {e}")

    async def run(self):
        print("Starting IndicatorScanner...")
        await self.fetcher.start_session()

        self.valid_symbols = await self.fetcher.validate_symbols()
        if not self.valid_symbols:
            error_logger.error("No valid symbols. Exiting.")
            await self.fetcher.close_session()
            return
            
        self.klines_history = {
            symbol: {tf: deque(maxlen=self.history_size) for tf in self.timeframes}
            for symbol in self.valid_symbols
        }
        self.last_processed_ts = {
            symbol: {tf: 0 for tf in self.timeframes}
            for symbol in self.valid_symbols
        }

        await self._fetch_initial_history()
        await self.fetcher.run()

        await self.fetcher.close_session()
        await self.http_client.aclose()


# =====================================================================================================================
# // Main
# =====================================================================================================================
async def main():
    print("Scanner App Launching.")

    timeframes_to_scan = ['15m', '4h']

    pairs_file = os.path.join(os.path.dirname(__file__), 'info', 'binance_pairs.json')
    try:
        with open(pairs_file, 'r', encoding='utf-8') as f:
            symbols_to_scan = json.load(f)
        print(f"Loaded {len(symbols_to_scan)} symbols from {pairs_file}")
    except FileNotFoundError:
        error_logger.warning(f"{pairs_file} not found. Using default list.")
        symbols_to_scan = [
            "BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT"
        ]
    except Exception as e:
        error_logger.error(f"Error loading pairs file: {e}")
        return

    scanner = IndicatorScanner(
        timeframes=timeframes_to_scan,
        symbols=symbols_to_scan,
        history_size=500
    )
    await scanner.run()

    print("Scanner App Finished.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Stopped by user.")