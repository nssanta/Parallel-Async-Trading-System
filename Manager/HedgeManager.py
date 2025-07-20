import asyncio
import os
import logging
from datetime import datetime, timedelta
from typing import Any, Dict
from collections import defaultdict

from Manager.Utils import Signal
from config import HEDGE_CONFIG
from Telegram.UtilsTG import send_to_all_async

# Logger Setup
log_file = os.path.join(os.path.dirname(__file__), 'HedgeManager.log')
logger = logging.getLogger('HedgeManager')
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


class HedgeManager:
    """
    Hedge Manager for managing hedging positions.
    """

    def __init__(
        self,
        cache,
        trader,
        state_manager,
        symbol_locks: Dict,
        hedge_tasks: Dict,
        position_closed_flags: Dict,
        last_reopen_notification: Dict,
        hedge_mode_active: bool = False
    ):
        """
        :param cache: Cache instance.
        :param trader: TraderManager instance.
        :param state_manager: StateManager instance.
        :param symbol_locks: Dictionary of symbol locks.
        :param hedge_tasks: Dictionary of hedge supervisor tasks.
        :param position_closed_flags: Closed position flags.
        :param last_reopen_notification: Dictionary of last reopen notification times.
        :param hedge_mode_active: Flag for hedge mode activity.
        """
        self.cache = cache
        self.trader = trader
        self.state_manager = state_manager
        self.symbol_locks = symbol_locks
        self.hedge_tasks = hedge_tasks
        self.position_closed_flags = position_closed_flags
        self.last_reopen_notification = last_reopen_notification
        self.hedge_mode_active = hedge_mode_active
        self.logger = logger

    def get_empty_shield_state(self) -> dict:
        """Creates empty shield state for main position."""
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

    async def start_hedge_supervisor(self, symbol: str, main_side: str) -> None:
        """Starts a persistent hedge supervisor for the specified main position."""
        key = (symbol, main_side)

        async with self.symbol_locks[symbol]:
            task = self.hedge_tasks.get(key)
            if task and not task.done():
                self.logger.debug(f"[start_hedge_supervisor] Supervisor for {key} already running")
                return
            self.hedge_tasks[key] = asyncio.create_task(self.hedge_supervisor(symbol, main_side))

    async def hedge_supervisor(self, symbol: str, main_side: str) -> None:
        """
        Hedge supervisor: lives as long as main or open child exists, manages hedge lifecycle.
        """
        hedge_side = "short" if main_side == "long" else "long"
        wait_interval = 1
        anti_spam_seconds = int(HEDGE_CONFIG.get("anti_spam_seconds", 5))
        last_tick = 0
        last_safe_zone_log = 0
        safe_zone_log_interval_ms = 3 * 60 * 1000
        open_positions_check_interval = timedelta(minutes=2)
        last_open_positions_check = datetime.utcnow()
        child_was_active = False
        key = (symbol, main_side)
        try:
            while True:
                await asyncio.sleep(wait_interval)
                # FAST-PATH: Fix instant SL before entering anti_spam window.
                try:
                    async with self.symbol_locks[symbol]:
                        main = await self.cache.get_position(symbol, main_side)
                        if main:
                            child = await self.cache.get_position(symbol, hedge_side)
                            child_open = False
                            try:
                                child_open = bool(
                                    isinstance(child, dict)
                                    and float(child.get("pos", 0)) > 0
                                    and float(child.get("avgPx", 0)) > 0
                                )
                            except Exception:
                                child_open = False

                            if not child_open and not child_was_active:
                                closed_data = await self.cache.get_closed_position(symbol, hedge_side)
                                if isinstance(closed_data, dict):
                                    pos_amount = float(closed_data.get("pos", 1.0))
                                    fill_time = int(closed_data.get("fillTime", 0))
                                    required_keys = {"fillPx", "fillPnl", "fillTime", "posSide"}
                                    now_ts = int(datetime.utcnow().timestamp() * 1000)
                                    max_age_ms = 30 * 60 * 1000
                                    is_fresh = fill_time > 0 and (now_ts - fill_time) <= max_age_ms
                                    if pos_amount == 0 and is_fresh and all(k in closed_data for k in required_keys):
                                        await self._record_hedge_close_price(symbol, hedge_side, main_side, locked=True)
                                        try:
                                            await self.cache.clear_closed(symbol, hedge_side)
                                        except Exception:
                                            pass
                except Exception as e:
                    self.logger.error(f"[hedge_supervisor] fast close detection error: {e}")

                now_ms = int(datetime.utcnow().timestamp() * 1000)
                if now_ms - last_tick < anti_spam_seconds * 1000:
                    continue
                last_tick = now_ms

                async with self.symbol_locks[symbol]:
                    main = await self.cache.get_position(symbol, main_side)
                    child = await self.cache.get_position(symbol, hedge_side)

                    child_open = False
                    try:
                        child_open = bool(
                            isinstance(child, dict)
                            and float(child.get("pos", 0)) > 0
                            and float(child.get("avgPx", 0)) > 0
                        )
                    except Exception:
                        child_open = False

                    if child_open and not child_was_active:
                        child_was_active = True
                        try:
                            await self.cache.clear_closed(symbol, hedge_side)
                        except Exception:
                            pass
                        try:
                            if isinstance(main, dict):
                                await self.cache.update_position(symbol, main_side, {"wait_reopen": True})
                            await self.cache.update_position(symbol, hedge_side, {"wait_reopen": True})
                            await self.state_manager._save_state()
                        except Exception:
                            pass

                    if not child_open and not child_was_active:
                        last_open_positions_check = datetime.utcnow()

                    if not child_open and child_was_active:
                        bot_child = self.trader.bots.get((symbol, hedge_side))
                        if bot_child:
                            allow_reopen = bool(self.hedge_mode_active) and bool(main)
                            hedge_pos_for_ctime = await self.cache.get_position(symbol, hedge_side)
                            hedge_c_time_ts = int(hedge_pos_for_ctime.get("cTime", int(datetime.utcnow().timestamp() * 1000))) if hedge_pos_for_ctime else int(datetime.utcnow().timestamp() * 1000)

                            is_closed, last_open_positions_check, reopened = await self.check_position_closed(
                                symbol, hedge_side, bot_child,
                                last_open_positions_check, open_positions_check_interval,
                                await self._get_initial_balance("cashBal") or 0.0,
                                hedge_c_time_ts,
                                allow_reopen=allow_reopen,
                                is_pending=False
                            )

                            if is_closed and not reopened and not allow_reopen:
                                await self.state_manager._cleanup_position(symbol, hedge_side, bot_child)
                                child_was_active = False
                                continue
                            if reopened:
                                child_was_active = True

                    if not main and (not child or float(child.get("pos", 0)) <= 0):
                        break

                    if main and isinstance(main, dict):
                        if "wait_reopen" not in main or not main.get("wait_reopen", False):
                            await self.cache.update_position(symbol, main_side, {"wait_reopen": True})

                    if not main and child and float(child.get("pos", 0)) > 0:
                        bot_child = self.trader.bots.get((symbol, hedge_side))
                        if bot_child:
                            if await self.check_pnl_and_close(symbol, hedge_side, bot_child, child):
                                continue
                            await self.check_price_and_set_take_profit(symbol, hedge_side, bot_child, child)
                        continue

                    if main:
                        if not child or float(child.get("pos", 0)) <= 0 or float(child.get("avgPx", 0)) <= 0:
                            shield = main.get("shield_state", {})
                            try:
                                closed_data = await self.cache.get_closed_position(symbol, hedge_side)
                                if isinstance(main, dict) and isinstance(shield, dict) and isinstance(closed_data, dict):
                                    pos_amount = float(closed_data.get("pos", 1.0))
                                    fill_time = int(closed_data.get("fillTime", 0)) if closed_data else 0
                                    max_age_ms = 30 * 60 * 1000
                                    is_fresh = fill_time > 0 and (int(datetime.utcnow().timestamp() * 1000) - fill_time) <= max_age_ms

                                    if pos_amount == 0 and not is_fresh:
                                        await self.cache.clear_closed(symbol, hedge_side)
                            except Exception:
                                pass

                            wait_safe_zone = bool(shield.get("wait_safe_zone_exit", False))

                            if wait_safe_zone:
                                now_ms = int(datetime.utcnow().timestamp() * 1000)
                                if now_ms - last_safe_zone_log >= safe_zone_log_interval_ms:
                                    last_safe_zone_log = now_ms

                                    close_price_safe = float(shield.get("close_price_safe", 0.0))
                                    safe_zone_pct = HEDGE_CONFIG.get("price_safe_zone", 0.2)

                                    bot_child = self.trader.bots.get((symbol, hedge_side))
                                    if bot_child:
                                        try:
                                            current_price = await bot_child.get_price_with_fallback(symbol)

                                            if hedge_side == "long":
                                                price_change_pct = ((current_price - close_price_safe) / close_price_safe) * 100
                                            else:
                                                price_change_pct = ((close_price_safe - current_price) / close_price_safe) * 100

                                            remaining_pct = safe_zone_pct - price_change_pct
                                            remaining_pct = max(0, remaining_pct)

                                            status_emoji = "[WAIT]" if price_change_pct < safe_zone_pct else "[READY]"
                                            direction = "LONG" if hedge_side == "long" else "SHORT"

                                            log_message = (
                                                f"{status_emoji} [SAFE-ZONE] Waiting for {symbol} ({direction} hedge):\n"
                                                f