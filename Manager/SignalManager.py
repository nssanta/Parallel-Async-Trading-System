import asyncio
import math
import time
import os
import logging
import json
import multiprocessing

from datetime import datetime, timedelta
from typing import Any, Dict

from Manager.Utils import Signal
from Manager.Cache import Cache
from Manager.StateManager import StateManager
from Manager.HedgeManager import HedgeManager
from Trader.OkxWebSocketClient import OkxWebSocketClient
from config import MAX_OPEN_POSITIONS, MIN_BALANCE, MAX_CONCURRENT_TASKS, TRADING_STAGES, HEDGE_CONFIG
from Trader.OkxTradingBot import TraderManager
from Telegram.UtilsTG import send_to_all_async
from collections import defaultdict

from asyncio import Semaphore

# Add dynamically:
LIST_TF = [stage['tf'] for stage in TRADING_STAGES]
ALLOWED_TFS = set(LIST_TF)

# Logger Setup
log_file = os.path.join(os.path.dirname(__file__), 'Signal.log')
logger = logging.getLogger('SignalLogger')
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


class SignalManager:
    """
    Signal Manager for processing trading signals using Cache and WebSocket.
    Uses StateManager for state handling and HedgeManager for hedging.
    """

    def __init__(self, signal_queue: multiprocessing.Queue, trader: TraderManager, cache: Cache,
                 ws_client: OkxWebSocketClient):
        self.signal_queue = signal_queue
        self.trader = trader
        self.cache = cache
        self.ws_client = ws_client
        self.signal_async_queue = asyncio.Queue()
        self.positions_lock = asyncio.Lock()
        self.symbol_locks = defaultdict(asyncio.Lock)
        self.logger = logger
        self.semaphore = asyncio.Semaphore(MAX_CONCURRENT_TASKS)
        self._state_file = os.path.join(os.path.dirname(__file__), 'state.json')
        self.balance_notification_timestamp = datetime.utcnow() - timedelta(minutes=11)
        self._daily_balance_file = os.path.join(os.path.dirname(__file__), 'daily_balance.json')
        self.daily_balance = self._load_daily_balance()
        self.hedge_mode_active = False
        self.position_closed_flags = defaultdict(bool)
        self.state_file_lock = asyncio.Lock()
        self.last_reopen_notification = defaultdict(lambda: datetime.utcnow() - timedelta(minutes=15))
        self.hedge_tasks: Dict[tuple, asyncio.Task] = {}

        # Create StateManager
        self.state_manager = StateManager(
            cache=self.cache,
            trader=self.trader,
            state_file=self._state_file,
            state_file_lock=self.state_file_lock,
            symbol_locks=self.symbol_locks,
            position_closed_flags=self.position_closed_flags,
            hedge_tasks=self.hedge_tasks,
            get_empty_shield_state_func=self.get_empty_shield_state
        )

        # Create HedgeManager
        self.hedge_manager = HedgeManager(
            cache=self.cache,
            trader=self.trader,
            state_manager=self.state_manager,
            symbol_locks=self.symbol_locks,
            hedge_tasks=self.hedge_tasks,
            position_closed_flags=self.position_closed_flags,
            last_reopen_notification=self.last_reopen_notification,
            hedge_mode_active=self.hedge_mode_active
        )

    def get_empty_shield_state(self) -> dict:
        """Create empty hedge state for main position."""
        return {
            "levels_reached": -1,
            "target_coef": 0.0,
            "current_coef": 0.0,
            "last_update_ts": 0,
            "active_levels": {},
            "close_price_safe": 0.0,
            "wait_safe_zone_exit": False,
            "stop_is_move": False
        }

    def _load_daily_balance(self) -> dict:
        """Loads daily balance snapshot."""
        if os.path.exists(self._daily_balance_file):
            try:
                with open(self._daily_balance_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except Exception as e:
                self.logger.error(f"[_load_daily_balance] Error: {e}")
                return {{}}
        return {{}}

    async def _save_daily_balance(self) -> None:
        """Saves totalEq and cashBal at midnight."""
        try:
            now = datetime.utcnow()
            if now.hour == 0 and now.minute == 0 and now.second < 5:
                total_eq = await self.cache.get_balance("totalEq")
                cash_bal = await self.cache.get_balance("cashBal")
                date_str = now.strftime('%Y-%m-%d')
                self.daily_balance[date_str] = {
                    "totalEq": float(total_eq or 0),
                    "cashBal": float(cash_bal or 0)
                }
                with open(self._daily_balance_file, 'w', encoding='utf-8') as f:
                    json.dump(self.daily_balance, f, indent=2)
                self.logger.info(f"[_save_daily_balance] Balance saved for {date_str}")
        except Exception as e:
            self.logger.error(f"[_save_daily_balance] Error: {e}")
            await send_to_all_async(f"[_save_daily_balance] Error: {e}")

    async def feeder(self):
        """Transfers signals from multiprocessing.Queue to asyncio.Queue."""
        loop = asyncio.get_event_loop()
        while True:
            try:
                sig = await loop.run_in_executor(None, self.signal_queue.get)
                await self.signal_async_queue.put(sig)
                self.logger.info(f"[feeder] Signal transferred: {sig.symbol}")
                await asyncio.sleep(0.01)
            except Exception as e:
                self.logger.error(f"[feeder] Error: {e}")
                await send_to_all_async(f"[feeder] Error: {e}")
                await asyncio.sleep(0.05)

    async def process_signals(self) -> None:
        """Main loop for processing signals from async queue."""
        self.logger.info("[process_signals] Signal Manager started")
        await send_to_all_async("[process_signals] Signal Manager started")

        asyncio.create_task(self.feeder())

        while True:
            try:
                balance = await self.get_initial_balance()
                max_coins = await self.calculate_max_coins(balance)
                self.logger.debug(f"[process_signals] max_coins={max_coins}")

                semaphore = Semaphore(max_coins if max_coins > 0 else 1)
                self.logger.debug(f"[process_signals] Created semaphore with limit {max_coins}")

                signals = []
                try:
                    if max_coins == 0:
                        sig = self.signal_async_queue.get_nowait()
                        if sig.timeframe != LIST_TF[0]:
                            signals.append(sig)
                            self.logger.debug(
                                f"[process_signals] Extracted averaging signal: {sig.symbol}, timeframe={sig.timeframe}")
                        else:
                            self.logger.info(
                                f"[process_signals] Signal {sig.symbol} (timeframe={LIST_TF[0]}) rejected: position limit reached")
                            await send_to_all_async(
                                f"[process_signals] Signal rejected: {sig.symbol} {sig.signal} (timeframe={sig.timeframe}) — limit reached (max_coins={max_coins})")
                            self.signal_async_queue.task_done()
                    else:
                        for _ in range(max_coins):
                            sig = self.signal_async_queue.get_nowait()
                            signals.append(sig)
                            self.logger.debug(
                                f"[process_signals] Extracted signal: {sig.symbol}, timeframe={sig.timeframe}")
                except asyncio.QueueEmpty:
                    pass
                self.logger.info(f"[process_signals] Extracted signals: {len(signals)}")

                filtered_signals = []
                for sig in signals:
                    role = await self.cache.get_position_role(sig.symbol, sig.signal)
                    if role == "child":
                        self.logger.info(
                            f"[process_signals] Signal {sig.symbol} ({sig.signal}) rejected: position is child")
                        self.signal_async_queue.task_done()
                        continue
                    filtered_signals.append(sig)
                signals = filtered_signals
                self.logger.info(f"[process_signals] Signals after filter: {len(signals)}")

                tasks = []

                for sig in signals:
                    if sig.timeframe == LIST_TF[0]:
                        if max_coins <= 0:
                            self.logger.info(
                                f"[process_signals] Signal {sig.symbol} (timeframe={LIST_TF[0]}) dropped: limit reached")
                            self.signal_async_queue.task_done()
                            continue
                        async with semaphore:
                            task = asyncio.create_task(self.handle_signal(sig))
                            tasks.append(task)
                            self.signal_async_queue.task_done()
                            self.logger.debug(
                                f"[process_signals] Started handle_signal for {sig.symbol} (timeframe={LIST_TF[0]})")
                    else:
                        task = asyncio.create_task(self.handle_signal(sig))
                        tasks.append(task)
                        self.signal_async_queue.task_done()
                        self.logger.debug(
                            f"[process_signals] Started handle_signal for {sig.symbol} (timeframe={sig.timeframe}) for averaging")

                if tasks:
                    await asyncio.gather(*tasks, return_exceptions=True)
                    self.logger.debug(f"[process_signals] All tasks ({len(tasks)}) completed")

                await asyncio.sleep(5)

            except Exception as e:
                self.logger.error(f"[process_signals] Critical error: {e}")
                await send_to_all_async(f"[process_signals] Critical error: {e}")
                await asyncio.sleep(5)

    async def update_balance_periodically(self) -> None:
        """Periodically updates balance and sends position info every 10 minutes."""
        while True:
            try:
                data_balance = await self.cache.get_balance("")
                avail_bal = float(data_balance.get("availBal", 0))
                cash_bal = float(data_balance.get("cashBal", 0))
                total_eq = float(data_balance.get("totalEq", 0))
                mmr = float(data_balance.get("mmr", 0))

                y = 0.0
                z = 0.0
                x = 0.0

                positions = await self.cache.get_all_open_positions()

                valid_positions = {}
                stale_count = 0
                for (symbol, pos_side), pos in positions.items():
                    pos_size = float(pos.get("pos", 0))
                    avg_px = pos.get("avgPx", 0.0)

                    if pos_size <= 0 or not avg_px or avg_px <= 0:
                        self.logger.debug(
                            f"[update_balance_periodically] Skipping stale {symbol}:{pos_side} "
                            f"(pos={pos_size}, avgPx={avg_px})")
                        await self.cache.remove_position(symbol, pos_side)
                        stale_count += 1
                        continue

                    valid_positions[(symbol, pos_side)] = pos

                if stale_count > 0:
                    self.logger.info(
                        f"[update_balance_periodically] Cleaned {stale_count} stale positions")

                positions = valid_positions
                bots = self.trader.bots

                for (symbol, pos_side), pos in positions.items():
                    notional_usd = float(pos.get("notionalUsd", 0))
                    lever = float(pos.get("lever", 1))
                    margin = notional_usd / lever if lever > 0 else 0.0
                    role = pos.get("role", "unknown")
                    upl = float(pos.get("uplLastPx", 0))
                    contracts = float(pos.get("pos", 0))
                    current_price = await self.cache.get_price(symbol=symbol) or 0.0

                    if lever > 0:
                        if role == "main":
                            x += margin
                            y += upl

                    multiplier = 1.0
                    if multiplier is None or not isinstance(multiplier, (int, float)):
                        self.logger.error(
                            f"[update_balance_periodically] Invalid multiplier for {pos_side} in {symbol}, using 1.0")
                        multiplier = 1.0

                    ct_val = 0

                    if role == "main":
                        bot = self.trader.bots.get((symbol, pos_side))
                        if current_price == 0:
                            current_price = await bot.get_price_with_fallback(symbol)

                        if current_price > 0 and notional_usd > 0:
                            if bot:
                                ct_val = float(bot.okx.info_for_futures_instrument.get('ctVal', 1.0))
                                z_contribution = (contracts * current_price * ct_val * multiplier) / lever
                            else:
                                self.logger.error(
                                    f"[update_balance_periodically] Bot for {symbol}:{pos_side} not found")
                                await send_to_all_async(
                                    f"[update_balance_periodically] Bot for {symbol}:{pos_side} not found")
                                z_contribution = margin
                            z += z_contribution
                        else:
                            await send_to_all_async(
                                f"[update_balance_periodically] Current price ({current_price}) or notionalUsd ({notional_usd}) for {symbol} invalid. Hedge not accounted.")

                risk_pct = ((abs(x) + abs(y) + abs(z)) * 100 / cash_bal) if cash_bal > 0 else 0.0
                await self.cache.update_balance({"risk_pct": risk_pct})

                self.logger.debug(
                    f"[update_balance_periodically] x={x:.4f}, y={y:.4f}, z={z:.4f}, risk={risk_pct:.2f}%, cash_bal={cash_bal:.2f}")

                max_coins = await self.calculate_max_coins(avail_bal)

                now = datetime.utcnow()
                today_date_str = now.strftime('%Y-%m-%d')
                today_balance = self.daily_balance.get(today_date_str, {{}})
                prev_total_eq = float(today_balance.get("totalEq", total_eq or 0))
                prev_cash_bal = float(today_balance.get("cashBal", cash_bal or 0))
                eq_change_pct = ((total_eq - prev_total_eq) / prev_total_eq * 100) if prev_total_eq > 0 else 0
                cash_change_pct = ((cash_bal - prev_cash_bal) / prev_cash_bal * 100) if prev_cash_bal > 0 else 0

                if (datetime.utcnow() - self.balance_notification_timestamp) >= timedelta(minutes=5):
                    header = (
                        f"Balance Updated:\n\n"
                        f"Available: {avail_bal:.2f} USDT\n"
                        f"Cash Bal: {cash_bal:.2f} USDT ({cash_change_pct:+.2f}%)\n"
                        f"Equity: {total_eq:.2f} USDT ({eq_change_pct:+.2f}%)\n"
                        f"In Deals: {x:.4f} USDT\n"
                        f"Required for Hedge: {z:.4f} USDT\n"
                        f"Drawdown: {y:.4f} USDT\n"
                        f"Risk: {risk_pct:.2f}% (mmr: {float(mmr):.2f})\n\n"
                        f"Coins: {max_coins} | Positions: {len(positions)}\n\n"
                    )
                    if positions:
                        grouped = defaultdict(list)
                        for (sym, pos_side), pos in positions.items():
                            grouped[sym].append(((sym, pos_side), pos))

                        sorted_symbols = sorted(grouped.keys())

                        symbol_prices = {{}}
                        for sym in sorted_symbols:
                            price = await self.cache.get_price(sym)
                            bot = self.trader.bots.get((sym, "long")) or self.trader.bots.get((sym, "short"))
                            if bot:
                                symbol_prices[sym] = f"{round(price, bot.round_decimal)}$" if price and price > 0 else "N/A"
                            else:
                                symbol_prices[sym] = f"{price}$" if price and price > 0 else "N/A"

                        body_lines = ["Positions:"]
                        max_pos_header_len = 0

                        for idx, sym in enumerate(sorted_symbols):
                            positions_for_sym = grouped[sym]
                            positions_for_sym.sort(key=lambda x: x[0][1])

                            body_lines.append(f"{sym}:")

                            for (sym, pos_side), pos in positions_for_sym:
                                side_emoji = "LONG" if pos.get("posSide") == "long" else "SHORT"
                                wait_reopen = pos.get("wait_reopen", False)
                                wait_emoji = "[W]" if wait_reopen and pos.get("role") == "main" else ""
                                pnl_usd = round(float(pos.get("uplLastPx", 0.0)), 4)
                                pnl_pct = round(float(pos.get("uplRatioLastPx", 0.0)) * 100, 2)
                                avg_cnt = pos.get("averages", 0)
                                role = pos.get("role", "unknown")
                                contracts = float(pos.get("pos", 0.0))
                                notional_usd = float(pos.get("notionalUsd", 0))
                                lever = float(pos.get("lever", 1))
                                margin = notional_usd / lever if lever > 0 else 0.0
                                price_str = symbol_prices.get(sym, "N/A")

                                header_line = f"  {pos_side.upper()} {wait_emoji}{side_emoji} | Role: {role}"
                                body_lines.append(header_line)
                                max_pos_header_len = max(max_pos_header_len, len(header_line))

                                body_lines.append(
                                    f"    -> PnL: {pnl_pct:+.2f}% ({pnl_usd:+.2f}$) | AVG={avg_cnt} | Ctr: {contracts} | $: {round(margin, 4)}$ | Price: {price_str}")

                            if idx != len(sorted_symbols) - 1:
                                divider = "-" * 22
                                body_lines.append(divider)

                        batch_size = 20
                        for i in range(0, len(body_lines), batch_size):
                            batch = body_lines[i:i + batch_size]
                            if i == 0:
                                message = header + "\n\n" + "\n".join(batch)
                            else:
                                message = "Positions (continued):\n\n" + "\n".join(batch)
                            await send_to_all_async(message)

                    else:
                        await send_to_all_async(header + "Positions: None")

                    self.balance_notification_timestamp = datetime.utcnow()

                    try:
                        await asyncio.wait_for(self.state_manager._save_state(), timeout=5)
                    except Exception as e:
                        self.logger.error(f"[update_balance_periodically] _save_state timeout/error: {e}")

                await self._save_daily_balance()

            except Exception as e:
                self.logger.error(f"[update_balance_periodically] Error: {e}")
                await send_to_all_async(f"[update_balance_periodically] Error: {e}")

            await asyncio.sleep(1)

    async def get_initial_balance(self, key: str = "availBal") -> float:
        """Get initial balance from cache."""
        try:
            balance = await self.cache.get_balance(key)
            if balance is None:
                self.logger.error(f"[get_initial_balance] Balance ({key}) not found in cache")
                await send_to_all_async(f"[get_initial_balance] Balance ({key}) not found in cache")
                return 0.0
            return float(balance)
        except Exception as e:
            self.logger.error(f"[get_initial_balance] Error getting balance ({key}): {e}")
            await send_to_all_async(f"[get_initial_balance] Error getting balance ({key}): {e}")
            return 0.0

    def normalize_symbol(self, symbol: str) -> str:
        """Normalize symbol from TradingView format."""
        symbol = symbol.replace('OKX:', '').replace('.P', '').replace('-', '')
        base_currencies = ['USDT', 'USDC', 'USD', 'BTC', 'ETH']
        for base in base_currencies:
            if symbol.endswith(base):
                return symbol[:-len(base)]
        self.logger.error(f"[normalize_symbol] Failed to normalize symbol: {symbol}")
        asyncio.run_coroutine_threadsafe(
            send_to_all_async(f"[normalize_symbol] Failed to normalize symbol: {symbol}"),
            asyncio.get_event_loop()
        )
        return symbol

    async def calculate_max_coins(self, balance: float) -> int:
        """Calculate max coins for opening new positions."""
        async with self.positions_lock:
            try:
                positions = await self.cache.get_all_open_positions()
                num_positions = len([pos for (sym, pos_side), pos in positions.items() if pos.get("role") == "main"])

                if num_positions >= MAX_OPEN_POSITIONS:
                    self.logger.info(f"[calculate_max_coins] Limit reached: {num_positions}/{MAX_OPEN_POSITIONS}")
                    return 0
                max_coins = MAX_OPEN_POSITIONS - num_positions
                self.logger.info(
                    f"[calculate_max_coins] Available coins: {max_coins}, positions: {num_positions}/{MAX_OPEN_POSITIONS}")
                return max_coins
            except Exception as e:
                self.logger.error(f"[calculate_max_coins] Error: {e}")
                await send_to_all_async(f"[calculate_max_coins] Error: {e}")
                return 0

    async def should_average(self, sig: Signal, pos: dict) -> bool:
        """Check if position needs averaging."""
        try:
            role = await self.cache.get_position_role(sig.symbol, sig.signal)
            if role != "main":
                self.logger.info(
                    f"[should_average] Position {sig.symbol} ({sig.signal}) is not main (role={role}), averaging rejected")
                await send_to_all_async(
                    f"[should_average] Position {sig.symbol} ({sig.signal}) is not main (role={role}), averaging rejected")
                return False

            bot = self.trader.bots.get((sig.symbol, sig.signal))
            if not bot:
                self.logger.error(f"[should_average] Bot not found for {(sig.symbol, sig.signal)}")
                return False

            current_price = await self.cache.get_price(sig.symbol)
            avg_price = float(pos.get("avgPx", 0.0))

            if current_price is None or avg_price <= 0:
                self.logger.error(
                    f"[should_average] Invalid prices for {sig.symbol}, current_price={current_price}, avg_price={avg_price}")
                try:
                    _, pos_side, api_avg_price, _, position_size = await bot.get_pnl_info()
                    if api_avg_price and api_avg_price > 0:
                        pos["avgPx"] = api_avg_price
                        pos["pos"] = position_size
                        pos["posSide"] = pos_side
                        await self.cache.update_position(sig.symbol, sig.signal, pos)
                        avg_price = api_avg_price
                        self.logger.info(f"[should_average] Restored avgPx for {sig.symbol} via API: {avg_price}")
                    else:
                        self.logger.error(f"[should_average] API did not return valid avgPx for {sig.symbol}")
                        await send_to_all_async(f"[should_average] Failed to get avgPx for {sig.symbol}")
                        return False
                except Exception as e:
                    self.logger.error(f"[should_average] API Error for {sig.symbol}: {e}")
                    await send_to_all_async(f"[should_average] API Error for {sig.symbol}: {e}")
                    return False

            if sig.signal != pos.get("posSide"):
                self.logger.info(
                    f"[should_average] Signal {sig.signal} mismatch with posSide {pos.get('posSide')} for {sig.symbol}")
                return False

            current_averages = pos.get("averages", 0)
            if current_averages >= len(bot.averaging_stages):
                self.logger.info(
                    f"[should_average] Max averages reached for {sig.symbol}: {current_averages}")
                return False

            if current_averages < len(bot.averaging_stages) and sig.timeframe == bot.averaging_stages[current_averages]['tf']:
                last_avg_price = await self.cache.get_last_avg_price(sig.symbol, sig.signal)
                required_price_change = bot.averaging_stages[current_averages].get("required_price_change", 0.0)

                price_change_pct = None

                if required_price_change > 0 and last_avg_price is not None and last_avg_price > 0:
                    price_change_pct = abs(current_price - last_avg_price) / last_avg_price * 100
                    await send_to_all_async(
                        f"[DEBUG] Price comparison for averaging {sig.symbol}:\n"
                        f"Current Price: {current_price:.8f}\n"
                        f"Last Avg Price: {last_avg_price:.8f}\n"
                        f"Change %: {price_change_pct:.2f}%\n"
                        f"Required %: {required_price_change}%\n"
                        f"Current Step: {current_averages}\n"
                        f"Signal TF: {sig.timeframe}\n"
                        f"Averaging TF: {bot.averaging_stages[current_averages]['tf'] if current_averages < len(bot.averaging_stages) else 'N/A'}")

                    if price_change_pct < required_price_change:
                        await send_to_all_async(
                            f"[should_average] Price change {price_change_pct:.2f}% < {required_price_change}% for {sig.symbol}\n"
                            f"Averaging rejected for {sig.symbol} ({sig.signal})")
                        return False

                await send_to_all_async(
                    f"[should_average] Averaging allowed for {sig.symbol}\n"
                    f"Step: {current_averages + 1}, timeframe={sig.timeframe}\n"
                    f"Price Change: {f'{price_change_pct:.2f}%' if price_change_pct is not None else 'N/A'}")
                return True

            return False
        except Exception as e:
            self.logger.error(f"[should_average] Error for {sig.symbol}: {e}")
            await send_to_all_async(f"[should_average] Error for {sig.symbol}: {e}")
            return False

    async def handle_signal(self, sig: Signal) -> None:
        """Process trading signal."""
        await send_to_all_async(f"[handle_signal] Signal received: {sig.symbol} {sig.timeframe} {sig.signal}")
        self.logger.info(f"[handle_signal] Signal received: {sig.symbol} {sig.timeframe} {sig.signal}")

        sig.symbol = self.normalize_symbol(sig.symbol)
        inst_id = f"{sig.symbol}-USDT-SWAP"

        if sig.timeframe not in ALLOWED_TFS:
            self.logger.info(f"[handle_signal] Signal {sig.symbol} rejected: invalid timeframe")
            return

        balance = await self.get_initial_balance()
        if balance < MIN_BALANCE:
            self.logger.error(f"[handle_signal] Insufficient balance: {balance} < {MIN_BALANCE}")
            await send_to_all_async(f"[handle_signal] Insufficient balance: {balance} < {MIN_BALANCE}")
            return

        bot = self.trader.bots.get((sig.symbol, sig.signal))
        if not bot:
            self.logger.error(f"[handle_signal] Bot for {sig.symbol}:{sig.signal} not found")
            await send_to_all_async(f"[handle_signal] Bot for {sig.symbol}:{sig.signal} not found")
            return

        lock_key = sig.symbol
        async with self.symbol_locks[lock_key]:
            pos = await self.cache.get_position(sig.symbol, sig.signal) or {{}}

            open_positions = await self.cache.get_all_open_positions()
            opposite_found = None
            for (sym, pos_side), p in open_positions.items():
                if sym != sig.symbol:
                    continue
                if p.get("role") != "main":
                    continue
                existing_side = p.get("posSide")
                if existing_side and existing_side != sig.signal:
                    opposite_found = existing_side
                    break
                if existing_side == sig.signal:
                    pos = p
                    break

            if opposite_found:
                self.logger.info(
                    f"[handle_signal] Rejected open {sig.symbol} {sig.signal}: main {opposite_found} exists")
                await send_to_all_async(
                    f"Signal {sig.symbol} rejected: main position {opposite_found} already open")
                return

            if not pos:
                max_coins = await self.calculate_max_coins(balance)
                if max_coins == 0:
                    self.logger.warning(f"[handle_signal] Position limit reached: {max_coins}/{MAX_OPEN_POSITIONS}")
                    await send_to_all_async(
                        f"Signal {sig.symbol} rejected: limit reached (open: {len(await self.cache.get_all_open_positions())}/{MAX_OPEN_POSITIONS})")
                    return

                entry_tf = TRADING_STAGES[0]['tf']
                if sig.timeframe != entry_tf:
                    self.logger.info(
                        f"[handle_signal] Signal {sig.symbol} rejected: timeframe {entry_tf} required")
                    await send_to_all_async(f"Signal {sig.symbol} rejected: timeframe {entry_tf} required")
                    return

                try:
                    if inst_id not in self.ws_client.subscribed_tickers:
                        self.logger.info(f"[handle_signal] Subscribing to ticker for {inst_id}")
                        await self.ws_client.subscribe_to_ticker(inst_id)

                    max_attempts = 5
                    price = None
                    for attempt in range(1, max_attempts + 1):
                        price = await self.cache.get_price(sig.symbol)
                        if price is not None and price > 0:
                            self.logger.info(f"[handle_signal] Price for {sig.symbol} obtained: {price}")
                            break
                        self.logger.debug(f"[handle_signal] Attempt {attempt}: Price for {sig.symbol} not yet in cache")
                        await asyncio.sleep(0.5)
                    else:
                        self.logger.warning(
                            f"[handle_signal] Failed to get price for {sig.symbol} after {max_attempts} attempts")
                        await send_to_all_async(f"[handle_signal] Failed to get price for {sig.symbol}")
                        return
                except Exception as e:
                    self.logger.error(f"[handle_signal] Error subscribing to ticker {inst_id}: {e}")
                    await send_to_all_async(f"[handle_signal] Error subscribing to ticker {inst_id}: {e}")
                    return

                cash_bal = await self.cache.get_balance("cashBal")
                cmd = {
                    "symbol": sig.symbol,
                    "action": "open_main",
                    "signal": sig,
                    "balance": cash_bal,
                    "side": sig.signal
                }

                result = await self.trader.process_command(cmd)

                if result.get("status") != "success":
                    self.logger.error(
                        f"[handle_signal] Failed to open position for {sig.symbol}: {result.get('message')}")
                    await send_to_all_async(f"Failed to open position {sig.symbol}: {result.get('message')}")
                    return

                c_time = int(datetime.utcnow().timestamp() * 1000)
                position_id = f"{sig.symbol}-{c_time}"

                max_attempts = 5
                pos_data = None
                for attempt in range(1, max_attempts + 1):
                    pos_data = await self.cache.get_position(sig.symbol, sig.signal)
                    if pos_data and pos_data.get("avgPx", 0.0) > 0 and pos_data.get("pos", 0.0) > 0:
                        self.logger.info(
                            f"[handle_signal] Position for {sig.symbol} found in cache: avgPx={pos_data['avgPx']}, pos={pos_data['pos']}")
                        break
                    self.logger.debug(
                        f"[handle_signal] Attempt {attempt}/{max_attempts}: Position for {sig.symbol} not yet in cache")
                    await asyncio.sleep(0.5)
                else:
                    self.logger.error(
                        f"[handle_signal] Failed to get position for {sig.symbol} after {max_attempts} attempts")
                    await send_to_all_async(f"[handle_signal] Failed to get position for {sig.symbol}")
                    return

                exclusive_fields = {
                    "averages": 0,
                    "is_new": True,
                    "position_id": position_id,
                    "round_decimal": bot.round_decimal,
                    "initial_po": result.get("usdt_amount", 0.0),
                    "role": "main",
                    "posSide": sig.signal,
                    "last_avg_price": result.get("fill_price", price),
                    "shield_state": self.get_empty_shield_state()
                }
                pos_data = {{**pos_data, **exclusive_fields}}
                await self.cache.update_position(sig.symbol, sig.signal, pos_data)

                max_wait = 10
                for _ in range(max_wait):
                    pos_check = await self.cache.get_position(sig.symbol, sig.signal)
                    if pos_check and float(pos_check.get("avgPx", 0)) > 0 and float(pos_check.get("pos", 0)) > 0:
                        self.logger.info(f"[handle_signal] Position {sig.symbol}:{sig.signal} updated in cache")
                        break
                    await asyncio.sleep(1)
                else:
                    self.logger.warning(f"[handle_signal] Position {sig.symbol}:{sig.signal} not updated timely")
                await asyncio.sleep(1)

                try:
                    await self.cache.clear_closed(sig.symbol, sig.signal)
                except Exception:
                    pass
                asyncio.create_task(
                    self.monitor_position(sig.symbol, sig.signal, datetime.fromtimestamp(c_time / 1000)))
                asyncio.create_task(self.hedge_manager.start_hedge_supervisor(sig.symbol, sig.signal))

                await asyncio.sleep(0.5)
                await self.state_manager._save_state()

                self.logger.info(
                    f"[handle_signal] Position opened for {sig.symbol}, ID: {position_id}, avgPx: {pos_data['avgPx']}, last_avg_price: {pos_data['last_avg_price']}")
                return

            role = await self.cache.get_position_role(sig.symbol, sig.signal)
            if role == "child":
                self.logger.info(
                    f"[handle_signal] Position {sig.symbol} ({sig.signal}) is child, signal ignored")
                await send_to_all_async(
                    f"[handle_signal] Position {sig.symbol} ({sig.signal}) is child, signal ignored")
                return

            if role == "main" and await self.should_average(sig, pos):
                current_price = await self.cache.get_price(sig.symbol)
                if current_price is None or current_price <= 0:
                    self.logger.error(f"[handle_signal] Failed to get current price for {sig.symbol}")
                    await send_to_all_async(f"[handle_signal] Failed to get current price for {sig.symbol}")
                    return

                cmd = {
                    "symbol": sig.symbol,
                    "action": "average_main",
                    "signal": sig,
                    "balance": balance,
                    "pos": pos
                }
                max_attempts = 10
                for attempt in range(1, max_attempts + 1):
                    cmd['balance'] = await self.get_initial_balance('cashBal')
                    result = await self.trader.process_command(cmd)
                    if result.get("status") == "success":
                        pos_data = await self.cache.get_position(sig.symbol, sig.signal)
                        current_step = pos_data.get("averages", 0)
                        multiplier = next(
                            (stage['multiplier'] for stage in bot.averaging_stages if stage['step'] == current_step), 0)
                        usdt_amount = round(pos_data.get("initial_po", 0.0) * multiplier, 4)
                        fill_price = result.get("fill_price", current_price)

                        await send_to_all_async(
                            f"Averaging main position {sig.symbol} ({sig.signal}).\n"
                            f"Reason: Averaging signal (TF={sig.timeframe}).\n"
                            f"Action: Closing all related hedge positions.")
                        await self.hedge_manager.close_all_hedges_on_averaging(sig.symbol, sig.signal)
                        await self.state_manager._save_state()

                        self.logger.info(
                            f"[handle_signal] Position {sig.symbol} averaged, step: {pos_data['averages']}, avgPx: {pos_data.get('avgPx')}, last_avg_price: {fill_price}")
                        await send_to_all_async(
                            f"[handle_signal] Position {sig.symbol} averaged\n"
                            f"Side: {sig.signal}\n"
                            f"Contracts: {pos_data['pos']}\n"
                            f"Step: {pos_data['averages']}\n"
                            f"avgPx: {pos_data.get('avgPx'):.8f}\n"
                            f"Amount: {usdt_amount:.4f} USDT\n"
                            f"Price: {fill_price:.8f}")
                        break
                    else:
                        self.logger.error(
                            f"[handle_signal] Attempt {attempt}/{max_attempts} failed for {sig.symbol}: {result.get('message')}")
                        await send_to_all_async(
                            f"Attempt {attempt}/{max_attempts} averaging {sig.symbol} failed")
                        if attempt < max_attempts:
                            await asyncio.sleep(2)
                else:
                    self.logger.error(
                        f"[handle_signal] All {max_attempts} averaging attempts for {sig.symbol} failed")
                    await send_to_all_async(f"All {max_attempts} averaging attempts for {sig.symbol} failed")

    async def initialize_positions(self) -> None:
        """Initialize existing open positions."""
        try:
            self.logger.info("[initialize_positions] Starting position initialization")
            await send_to_all_async("[initialize_positions] Position initialization started")

            self.logger.info("[initialize_positions] Cleaning stale positions...")
            positions_before = await self.cache.get_all_open_positions()
            removed = 0
            for (sym, side), pos in positions_before.items():
                if float(pos.get("pos", 0)) <= 0 or not pos.get("avgPx", 0):
                    await self.cache.remove_position(sym, side)
                    removed += 1
            if removed > 0:
                await self.state_manager._save_state()
                self.logger.info(f"[initialize_positions] Removed {removed} stale positions")
                await send_to_all_async(f"Removed {removed} stale positions on startup")

            updated_positions = await self.state_manager.sync_positions_with_state()
            if updated_positions is None:
                self.logger.error("[initialize_positions] sync_positions_with_state returned None")
                await send_to_all_async("[initialize_positions] sync_positions_with_state returned None")
                updated_positions = {{}}

            self.logger.info(f"[initialize_positions] Synchronized {len(updated_positions)} positions")

            for (sym, side), _ in updated_positions.items():
                closed = await self.cache.get_closed_position(sym, side)
                if closed:
                    await self.cache.clear_closed(sym, side)
                    self.logger.info(f"[initialize_positions] Cleared stale closed_data for {sym}:{side}")

            await self.state_manager._save_state()
            await asyncio.sleep(2)

            for (symbol, pos_side), pos_data in updated_positions.items():
                bot = self.trader.bots.get((symbol, pos_side))
                if not bot:
                    self.logger.warning(f"[initialize_positions] Bot for {symbol}:{pos_side} not found")
                    await send_to_all_async(f"[initialize_positions] Bot for {symbol}:{pos_side} not found")
                    continue

                try:
                    success = await bot.fetch_algo_id(pos_data["posSide"])
                    if success:
                        bot.position = pos_data["posSide"]
                        bot.current_position_size = pos_data["pos"]
                        self.logger.info(f"[initialize_positions] algo_id_tp synced for {symbol}:{pos_side}")
                        await send_to_all_async(
                            f"[initialize_positions] algo_id_tp synced for {symbol}:{pos_side}")
                    else:
                        self.logger.warning(
                            f"[initialize_positions] Failed to sync algo_id_tp for {symbol}:{pos_side}")
                        await send_to_all_async(
                            f"[initialize_positions] Failed to sync algo_id_tp for {symbol}:{pos_side}")
                except Exception as e:
                    self.logger.error(
                        f"[initialize_positions] Error syncing algo_id_tp for {symbol}:{pos_side}: {e}")
                    await send_to_all_async(
                        f"[initialize_positions] Error syncing algo_id_tp for {symbol}:{pos_side}: {e}")

                if pos_data["role"] == "child":
                    await self.cache.update_hedge(symbol, pos_side, {{}})
                    self.logger.info(
                        f"[initialize_positions] Marked hedge {symbol}:{pos_side}, keys in opposite main")

                await send_to_all_async(
                    f"[initialize_positions] Loaded position: {symbol} ({pos_side}), Role: {pos_data['role']}, Side: {'LONG' if pos_data['posSide'] == 'long' else 'SHORT'}, Contracts: {pos_data['pos']}, ID: {pos_data['position_id']}")

            await self.state_manager._save_state()

            for (symbol, pos_side), pos_data in updated_positions.items():
                c_time = datetime.fromtimestamp(
                    int(pos_data.get("cTime", int(datetime.now().timestamp() * 1000))) / 1000)
                if pos_data["role"] == "child":
                    pass
                else:
                    asyncio.create_task(self.hedge_manager.start_hedge_supervisor(symbol, pos_side))
                    asyncio.create_task(self.monitor_position(symbol, pos_side, c_time))
                    self.logger.info(f"[initialize_positions] Started monitor_position and hedge_supervisor for {symbol}:{pos_side}")
                    await send_to_all_async(
                        f"[initialize_positions] Started monitor_position and hedge_supervisor for {symbol}:{pos_side}")

            await asyncio.sleep(5)
            updated_again = await self.state_manager.sync_positions_with_state()
            self.logger.info(f"[initialize_positions] Re-sync: {len(updated_again)} positions")
            await self.state_manager._save_state()

            self.logger.info(
                f"[initialize_positions] Initialization complete, loaded {len(updated_positions)} positions")
            await send_to_all_async(
                f"[initialize_positions] Initialization complete, loaded {len(updated_positions)} positions")
            await self.get_all_open_positions(send_notification=True)

            if self.state_manager.cleanup_task is None or self.state_manager.cleanup_task.done():
                self.state_manager.cleanup_task = asyncio.create_task(self.state_manager.periodic_cleanup())
                self.logger.info("[initialize_positions] Started periodic_cleanup")
            else:
                self.logger.info("[initialize_positions] periodic_cleanup already running, skipping")

        except Exception as e:
            self.logger.error(f"[initialize_positions] Critical initialization error: {e}")
            await send_to_all_async(f"[initialize_positions] Critical initialization error: {e}")
            raise

    async def get_all_open_positions(self, send_notification: bool = True) -> dict:
        """Get all open positions from cache and format for notifications."""
        try:
            positions = await self.cache.get_all_open_positions()
            self.logger.info(f"[get_all_open_positions] Positions found: {len(positions)}")
            position_texts = []
            for (symbol, pos_side), pos in positions.items():
                bot = self.trader.bots.get((symbol, pos_side), list(self.trader.bots.values())[0])
                mark_px = await self.cache.get_price(symbol) or 0.0
                upl_last_px = pos.get("uplLastPx", 0.0)
                upl_ratio_last_px = pos.get("uplRatioLastPx", 0.0)
                notional_usd = pos.get("notionalUsd", 0.0)
                initial_po = pos.get("initial_po", 0.0)
                lever = pos.get("lever", "N/A")
                c_time = pos.get("cTime", int(datetime.now().timestamp() * 1000))
                pos_id = pos.get("position_id", f"{symbol}-{c_time}")
                contracts = pos.get("pos", 0.0)
                last_avg_price = pos.get("last_avg_price", 0.0)
                formatted_time = datetime.fromtimestamp(c_time / 1000).strftime(
                    '%Y-%m-%d %H:%M:%S') if c_time > 0 else "N/A"
                text = (
                    f"Position: {symbol} ({pos_side})\n"
                    f"ID: {pos_id}\n"
                    f"Role: {pos.get('role', None)}\n"
                    f"Side: {'LONG' if pos['posSide'] == 'long' else 'SHORT'}\n"
                    f"Contracts: {contracts}\n"
                    f"Last Price: {round(mark_px, bot.round_decimal) if mark_px > 0 else 'N/A'}\n"
                    f"Open Price: {round(pos['avgPx'], bot.round_decimal) if pos.get('avgPx', 0.0) > 0 else 'N/A'}\n"
                    f"Last Avg Price: {round(last_avg_price, bot.round_decimal) if last_avg_price > 0 else 'N/A'}\n"
                    f"PNL USD: {round(upl_last_px, 4)}\n"
                    f"PNL %: {round(upl_ratio_last_px * 100, 4)} %\n"
                    f"Value USD: {round(notional_usd, 4) if notional_usd > 0 else 'N/A'}\n"
                    f"PO Amount: {round(initial_po, 4) if initial_po > 0 else 'N/A'} USDT\n"
                    f"Leverage: {lever}\n"
                    f"Pair: {symbol}-USDT-SWAP\n"
                )
                position_texts.append(text)

            if send_notification and position_texts and (
                    datetime.utcnow() - self.balance_notification_timestamp >= timedelta(minutes=1)
            ):
                for i in range(0, len(position_texts), 5):
                    await send_to_all_async(
                        f"[get_all_open_positions] Positions found: {len(positions)}\n\n" +
                        "\n".join(position_texts[i:i + 5])
                    )
                self.balance_notification_timestamp = datetime.utcnow()

            return positions
        except Exception as e:
            self.logger.error(f"[get_all_open_positions] Error getting positions: {e}")
            await send_to_all_async(f"[get_all_open_positions] Error getting positions: {e}")
            return {{}}

    async def monitor_position(self, symbol: str, pos_side: str, c_time: datetime) -> None:
        """Monitor position, checking for closure via cache."""
        bot = self.trader.bots.get((symbol, pos_side))
        if not bot:
            self.logger.error(f"[monitor_position] Bot for {symbol} not found")
            await send_to_all_async(f"[monitor_position] Bot for {symbol} not found")
            return

        c_time_ts = int(c_time.timestamp() * 1000)
        self.logger.info(f"[monitor_position] Monitoring started for {symbol}, cTime={c_time}")

        required_keys = {"fillPx", "fillPnl", "fillTime", "posSide"}
        wait_interval = 3
        open_positions_check_interval = timedelta(minutes=2)
        last_open_positions_check = datetime.utcnow()

        try:
            _start_closed = await self.cache.get_closed_position(symbol, pos_side)
            if isinstance(_start_closed, dict):
                _fill_time = int(_start_closed.get("fillTime", 0))
                now_ms = int(datetime.utcnow().timestamp() * 1000)
                tolerance = 5000
                max_age_ms = 30 * 60 * 1000
                if _fill_time and ((_fill_time < (c_time_ts - tolerance)) or (now_ms - _fill_time) > max_age_ms):
                    await self.cache.clear_closed(symbol, pos_side)
        except Exception as e:
            self.logger.error(f"[monitor_position] Failed to clear old cache (non-critical): {e}")

        while True:
            try:
                async with self.symbol_locks[symbol]:

                    closed_data = await self.cache.get_closed_position(symbol, pos_side)
                    if not isinstance(closed_data, dict):
                        self.logger.debug(
                            f"[monitor_position] closed_data not dict: type={type(closed_data)}, value={closed_data}")
                        closed_data = {{}}
                    pos_amount = float(closed_data.get("pos", 1)) if closed_data else 1.0

                    if closed_data and all(key in closed_data for key in required_keys) and pos_amount == 0:
                        fill_time = int(closed_data.get("fillTime", 0))
                        tolerance = 5000
                        now_ms = int(datetime.utcnow().timestamp() * 1000)
                        max_age_ms = 30 * 60 * 1000
                        if fill_time and (fill_time < (c_time_ts - tolerance) or (now_ms - fill_time) > max_age_ms):
                            await self.cache.clear_closed(symbol, pos_side)
                            await asyncio.sleep(wait_interval)
                            continue

                        self.logger.info(
                            f"[monitor_position] Position {symbol} closed via closed_data, pos_amount={pos_amount}")
                        position_data = await self.state_manager._fetch_position_history(symbol, bot, pos_side, c_time_ts)
                        message = await self.state_manager._build_close_message(symbol, pos_side, bot, position_data, closed_data,
                                                                  c_time_ts, role='main')
                        self.logger.info(f"[monitor_position] {message}")
                        await send_to_all_async(message)
                        await self.state_manager._cleanup_position(symbol, pos_side, bot)
                        return

                    if (datetime.utcnow() - last_open_positions_check) >= open_positions_check_interval:
                        self.logger.info(
                            f"[monitor_position] 2 mins passed: checking open positions cache for {symbol}")
                        open_positions = await self.cache.get_all_open_positions()
                        self.logger.debug(f"[monitor_position] Open positions: {list(open_positions.keys())}")
                        if (symbol, pos_side) not in open_positions:
                            self.logger.info(
                                f"[monitor_position] Position {symbol} missing from open positions cache — assuming closed, killing monitor")
                            await send_to_all_async(
                                f"[monitor_position] Position {symbol} missing from cache — monitor stopped")
                            position_data = await self.state_manager._fetch_position_history(symbol, bot, pos_side, c_time_ts)
                            message = await self.state_manager._build_close_message(symbol, pos_side, bot, position_data, closed_data,
                                                                      c_time_ts, role='main')
                            self.logger.info(f"[monitor_position] {message}")
                            await send_to_all_async(message)
                            await self.state_manager._cleanup_position(symbol, pos_side, bot)
                            return
                        last_open_positions_check = datetime.utcnow()

                    pos_data = await self.cache.get_position(symbol, pos_side)
                    if not isinstance(pos_data, dict):
                        self.logger.debug(
                            f"[monitor_position] pos_data not dict: type={type(pos_data)}, value={pos_data}")
                        pos_data = {{}}

                    if pos_data == {{}}:
                        if closed_data and all(key in closed_data for key in required_keys) and float(
                                closed_data.get("pos", 1)) == 0:
                            fill_time = int(closed_data.get("fillTime", 0))
                            if fill_time < c_time_ts:
                                await self.cache.clear_closed(symbol, pos_side)
                                await asyncio.sleep(wait_interval)
                                continue
                            else:
                                position_data = await self.state_manager._fetch_position_history(symbol, bot, pos_side, c_time_ts)
                                message = await self.state_manager._build_close_message(symbol, pos_side, bot, position_data,
                                                                          closed_data,
                                                                          c_time_ts, role='main')

                                await self.state_manager._cleanup_position(symbol, pos_side, bot)
                                return
                        else:
                            self.logger.info(
                                f"[monitor_position] CONDITION 3: pos_data empty, but closed_data doesn't confirm — waiting")
                            await asyncio.sleep(wait_interval)
                            continue

                    self.logger.debug(
                        f"[monitor_position] Position {symbol} still open: pos={pos_data.get('pos', 'N/A')}")

                    await asyncio.sleep(wait_interval)

            except Exception as e:
                self.logger.error(f"[monitor_position] Error monitoring {symbol}: {e}")
                await send_to_all_async(f"[monitor_position] Error monitoring {symbol}: {e}")
                await asyncio.sleep(wait_interval)