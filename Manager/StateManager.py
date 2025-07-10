import asyncio
import os
import json
import logging
from datetime import datetime, timedelta
from typing import Any, Dict, Optional
from collections import defaultdict

from config import TRADING_STAGES, HEDGE_CONFIG
from Telegram.UtilsTG import send_to_all_async

# Logger Setup
log_file = os.path.join(os.path.dirname(__file__), 'StateManager.log')
logger = logging.getLogger('StateManager')
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


class StateManager:
    """
    State Manager for handling state.json, position cleanup, and history.
    """

    def __init__(
        self,
        cache,
        trader,
        state_file: str,
        state_file_lock: asyncio.Lock,
        symbol_locks: Dict,
        position_closed_flags: Dict,
        hedge_tasks: Dict,
        get_empty_shield_state_func
    ):
        """
        :param cache: Cache instance.
        :param trader: TraderManager instance for bot access.
        :param state_file: Path to state.json.
        :param state_file_lock: Lock for safe writing to state.json.
        :param symbol_locks: Dictionary of symbol locks.
        :param position_closed_flags: Closed position flags (anti-spam).
        :param hedge_tasks: Dictionary of hedge supervisor tasks.
        :param get_empty_shield_state_func: Function to get empty shield_state.
        """
        self.cache = cache
        self.trader = trader
        self._state_file = state_file
        self.state_file_lock = state_file_lock
        self.symbol_locks = symbol_locks
        self.position_closed_flags = position_closed_flags
        self.hedge_tasks = hedge_tasks
        self.get_empty_shield_state = get_empty_shield_state_func
        self.logger = logger
        self.cleanup_task = None
        self.cleanup_running = False
        self.last_reopen_notification = defaultdict(lambda: datetime.utcnow() - timedelta(minutes=15))

    def _load_state(self) -> dict:
        """
        Loads state from state.json without automatic role correction.
        :return: Dictionary {symbol:pos_side: {...}}.
        """
        if not os.path.exists(self._state_file):
            self.logger.info(f"[_load_state] File {self._state_file} not found, returning empty dict")
            return {}

        try:
            with open(self._state_file, 'r', encoding='utf-8') as f:
                content = f.read().strip()

                if not content:
                    self.logger.info(f"[_load_state] File {self._state_file} is empty, returning empty dict")
                    return {}
                state = json.loads(content)
                if not isinstance(state, dict):
                    self.logger.error(f"[_load_state] Invalid format in state.json: not a dict, returning empty")
                    return {}
                self.logger.info(f"[_load_state] Loaded {len(state)} records from {self._state_file}")

                validated_state = {}
                for key, data in state.items():
                    try:
                        if ':' not in key:
                            self.logger.warning(f"[_load_state] Invalid key {key}, skipping")
                            continue
                        symbol, pos_side = key.split(':')

                        if len(key.split(':')) != 2:
                            self.logger.error(f"[_load_state] Invalid key format: {key}")
                            continue

                        if pos_side not in ['long', 'short']:
                            self.logger.warning(f"[_load_state] Invalid pos_side {pos_side} for {key}, skipping")
                            continue

                        required_fields = ['averages', 'position_id', 'initial_po', 'role']
                        if not all(field in data for field in required_fields):
                            self.logger.warning(
                                f"[_load_state] Missing required fields in {key}: {data}, skipping")
                            continue

                        if not isinstance(data['averages'], int):
                            self.logger.warning(
                                f"[_load_state] Invalid averages for {key}: {data['averages']}, skipping")
                            continue
                        if not isinstance(data['position_id'], str):
                            self.logger.warning(
                                f"[_load_state] Invalid position_id for {key}: {data['position_id']}, skipping")
                            continue
                        if not isinstance(data['initial_po'], (int, float)):
                            self.logger.warning(
                                f"[_load_state] Invalid initial_po for {key}: {data['initial_po']}, skipping")
                            continue
                        if data['role'] not in ['main', 'child']:
                            self.logger.warning(f"[_load_state] Invalid role {data['role']} for {key}, skipping")
                            continue

                        if data['role'] == 'main':
                            shield_state = data.get('shield_state', {})
                            if not isinstance(shield_state, dict):
                                self.logger.warning(
                                    f"[_load_state] Invalid shield_state for {key}, creating new one"
                                )
                                data['shield_state'] = self.get_empty_shield_state()
                            else:
                                required_fields_shield = ['close_price_safe', 'wait_safe_zone_exit']
                                for field in required_fields_shield:
                                    if field not in shield_state:
                                        self.logger.warning(
                                            f"[_load_state] Missing {field} in shield_state for {key}"
                                        )
                                        shield_state[field] = 0.0 if field == 'close_price_safe' else False

                        validated_state[key] = data
                    except Exception as e:
                        self.logger.error(f"[_load_state] Error processing record {key}: {e}, skipping")
                        continue

                return validated_state

        except Exception as e:
            self.logger.error(f"[_load_state] Failed to load state: {e}, returning empty dict")
            asyncio.run_coroutine_threadsafe(
                send_to_all_async(f"[_load_state] Failed to load state: {e}"),
                asyncio.get_event_loop()
            )
            return {}

    async def _save_state(self) -> None:
        """
        Saves current averaging counters, position IDs, and hedge data to state.json.
        """
        async with self.state_file_lock:
            try:
                saved_state = self._load_state()
                positions = await self.cache.get_all_open_positions()
                hedges = await self.cache.get_all_hedges()
                payload = {}

                for (symbol, pos_side), pos in positions.items():
                    state_key = f"{symbol}:{pos_side}"

                    pos_size = float(pos.get("pos", 0))
                    if pos_size <= 0:
                        self.logger.debug(f"[_save_state] Skipping {symbol}:{pos_side} (pos={pos_size})")
                        continue

                    avg_px = pos.get("avgPx", 0.0)
                    if not avg_px or avg_px <= 0:
                        self.logger.debug(f"[_save_state] Skipping {symbol}:{pos_side} (avgPx={avg_px})")
                        continue

                    is_hedge = (symbol, pos_side) in hedges
                    _role = 'child' if is_hedge else (
                        pos.get('role') if pos.get('role') in ('main', 'child') else 'main')

                    saved_data = saved_state.get(state_key, {})
                    pos_data = {
                        "averages": pos.get("averages", 0),
                        "position_id": pos.get("position_id",
                                               f"{symbol}-{pos_side}-{int(datetime.now().timestamp() * 1000)}"),
                        "initial_po": pos.get("initial_po", 0.0),
                        "role": _role,
                        "last_avg_price": pos.get("last_avg_price", pos.get("avgPx", 0.0)),
                        "shield_state": pos.get("shield_state", self.get_empty_shield_state()) if _role == "main" else None
                    }

                    if _role == "main":
                        pos_data["wait_reopen"] = pos.get("wait_reopen", False)
                    else:
                        hedge_data = hedges.get((symbol, pos_side), {})
                        pos_data["stop_is_move"] = hedge_data.get("stop_is_move", pos.get("stop_is_move", False))
                        pos_data["wait_reopen"] = hedge_data.get("wait_reopen", pos.get("wait_reopen", False))

                    payload[state_key] = pos_data

                try:
                    with open(self._state_file, 'w', encoding='utf-8') as f:
                        json.dump(payload, f, indent=2)
                    self.logger.info(f"[_save_state] State saved to {self._state_file} ({len(payload)} records)")
                except Exception as save_e:
                    self.logger.error(f"[_save_state] JSON Write Error: {save_e}")
                    await send_to_all_async(f"[_save_state] Write Error: {save_e}")
            except Exception as e:
                self.logger.error(f"[_save_state] Failed to save state: {e}")
                await send_to_all_async(f"[_save_state] Failed to save state: {e}")

    async def sync_positions_with_state(self) -> dict:
        """
        Syncs positions from cache with data from state.json, assigns main/child roles, and updates cache.
        :return: Dictionary of updated positions {(symbol, pos_side): pos_data}.
        """
        self.logger.info("[sync_positions_with_state] Starting synchronization with state.json")
        updated_positions = {}
        try:
            saved_state = self._load_state()

            max_retry = 10
            positions = await self.cache.get_all_open_positions()
            for _ in range(max_retry):
                incomplete = [(s, ps) for (s, ps), pd in positions.items() if
                              not pd.get("avgPx", 0.0) or float(pd.get("pos", 0.0)) <= 0]
                if not incomplete:
                    self.logger.info(f"[sync_positions_with_state] Cache fully loaded: {len(positions)} positions")
                    break
                self.logger.debug(f"[sync_positions_with_state] Waiting for {len(incomplete)} positions to load...")
                await asyncio.sleep(1)
                positions = await self.cache.get_all_open_positions()
            else:
                self.logger.warning(
                    f"[sync_positions_with_state] Not all positions loaded timely, continuing with {len(positions)}")

            symbol_positions = defaultdict(list)

            self.logger.debug(f"[sync_positions_with_state] positions: {positions}")

            for (symbol, pos_side), pos_data in positions.items():
                c_time = int(pos_data.get("cTime", 0))
                pos_size = float(pos_data.get("pos", 0.0))
                avg_px = pos_data.get("avgPx", 0.0)

                if pos_size <= 0:
                    self.logger.warning(
                        f"[sync_positions_with_state] Dead position {symbol}:{pos_side} (pos={pos_size}), removing from cache")
                    await self.cache.remove_position(symbol, pos_side)
                    continue

                if not avg_px or avg_px <= 0:
                    self.logger.warning(
                        f"[sync_positions_with_state] Incomplete data {symbol}:{pos_side} (avgPx={avg_px}), skipping")
                    continue

                symbol_positions[symbol].append((pos_side, pos_data))
                self.logger.info(
                    f"[sync_positions_with_state] Added {symbol}:{pos_side} to {symbol}, total {len(symbol_positions[symbol])}")

            for symbol, pos_list in symbol_positions.items():
                state_roles = {pos_side: saved_state.get(f"{symbol}:{pos_side}", {}).get("role") for pos_side, _ in
                               pos_list}
                valid_positions = pos_list
                self.logger.info(
                    f"[sync_positions_with_state] For {symbol}: state_roles={state_roles}, len(pos_list)={len(pos_list)}")

                num_main = sum(1 for r in state_roles.values() if r == "main")
                if len(valid_positions) > 1 and (num_main > 1 or num_main == 0 or not all(
                        state_roles.get(pos_side) in ["main", "child"] for pos_side, _ in valid_positions)):
                    await send_to_all_async("[sync_positions_with_state] Missing roles or duplicate main, fallback")
                    state_roles = {}

                if len(valid_positions) > 1 and not state_roles:
                    def get_ctime(item):
                        ctime = item[1].get("cTime")
                        try:
                            return int(ctime) if ctime else float('inf')
                        except ValueError:
                            self.logger.warning(f"[sync_positions_with_state] Bad cTime {symbol}:{item[0]}: {ctime}")
                            return float('inf')

                    valid_positions.sort(key=get_ctime)
                    main_assigned = False
                    for pos_side, pos_data in valid_positions:
                        state_key = f"{symbol}:{pos_side}"
                        saved_data = saved_state.get(state_key, {})
                        defaults = {
                            "averages": 0,
                            "position_id": f"{symbol}-{pos_side}-{pos_data.get('cTime', int(datetime.now().timestamp() * 1000))}",
                            "initial_po": 0.0,
                            "role": "main" if not main_assigned else "child",
                            "last_avg_price": float(pos_data.get("avgPx", 0.0)),
                            "last_price_open": float(pos_data.get("avgPx", 0.0)) if not main_assigned else 0.0,
                            "stop_is_move": False,
                            "wait_reopen": False
                        }
                        saved_po = saved_data.get("initial_po", 0.0)
                        if saved_po == 0.0:
                            cash_bal = await self.cache.get_balance("cashBal") or 0.0
                            po_pct = TRADING_STAGES[0]["po_percent"].get(pos_side, 0.15)
                            defaults["initial_po"] = (cash_bal * po_pct / 100) if cash_bal > 0 else 0.0
                        else:
                            defaults["initial_po"] = saved_po
                        pos_data.update({
                            "averages": saved_data.get("averages", defaults["averages"]),
                            "position_id": saved_data.get("position_id", defaults["position_id"]),
                            "initial_po": defaults["initial_po"] if saved_po == 0.0 else saved_po,
                            "role": defaults["role"],
                            "last_avg_price": saved_data.get("last_avg_price", defaults["last_avg_price"]),
                            "last_price_open": saved_data.get("last_price_open", defaults["last_price_open"]) if
                            defaults["role"] == "child" else 0.0,
                            "stop_is_move": saved_data.get("stop_is_move", defaults["stop_is_move"]) if defaults[
                                                                                                            "role"] == "child" else False,
                            "wait_reopen": saved_data.get("wait_reopen", defaults["wait_reopen"]),
                            "shield_state": pos_data.get("shield_state") or saved_data.get("shield_state", self.get_empty_shield_state()) if defaults["role"] == "main" else None,
                        })
                        updated_positions[(symbol, pos_side)] = pos_data
                        await self.cache.update_position(symbol, pos_side, pos_data)
                        self.logger.info(
                            f"[sync_positions_with_state] Position {symbol}:{pos_side} updated, role={pos_data['role']}")

                        if len(valid_positions) > 1:
                            main_side = valid_positions[0][0]
                            main_pos_data = valid_positions[0][1]
                            child_side = valid_positions[-1][0]
                            child_key = f"{symbol}:{child_side}"
                            child_saved = saved_state.get(child_key, {})
                            if child_saved and (
                                    "last_price_open" in child_saved or "stop_is_move" in child_saved):
                                shield_state = main_pos_data.get("shield_state", {})
                                if not isinstance(shield_state, dict):
                                    shield_state = self.get_empty_shield_state()
                                shield_state["last_price_open"] = child_saved.get("last_price_open",
                                                                                   main_pos_data.get("avgPx", 0.0))
                                shield_state["stop_is_move"] = child_saved.get("stop_is_move", False)
                                main_pos_data["shield_state"] = shield_state
                                await self.cache.update_position(symbol, main_side, main_pos_data)
                                self.logger.info(
                                    f"[sync_positions_with_state] Transferred old hedge keys to main.shield_state {symbol}:{main_side}")

                        main_assigned = True
                else:
                    for pos_side, pos_data in valid_positions:
                        state_key = f"{symbol}:{pos_side}"
                        saved_data = saved_state.get(state_key, {})
                        role = saved_data.get("role", "unknown") if saved_data else "unknown"
                        if role == "unknown":
                            role = "main"
                        defaults = {
                            "averages": 0,
                            "position_id": f"{symbol}-{pos_side}-{pos_data.get('cTime', int(datetime.now().timestamp() * 1000))}",
                            "initial_po": 0.0,
                            "role": role,
                            "last_avg_price": float(pos_data.get("avgPx", 0.0)),
                            "last_price_open": float(pos_data.get("avgPx", 0.0)) if role == "child" else 0.0,
                            "stop_is_move": False,
                            "wait_reopen": False
                        }
                        saved_po = saved_data.get("initial_po", 0.0)
                        if saved_po == 0.0:
                            cash_bal = await self.cache.get_balance("cashBal") or 0.0
                            po_pct = TRADING_STAGES[0]["po_percent"].get(pos_side, 0.15)
                            defaults["initial_po"] = (cash_bal * po_pct / 100) if cash_bal > 0 else 0.0
                        else:
                            defaults["initial_po"] = saved_po
                        pos_data.update({
                            "averages": saved_data.get("averages", defaults["averages"]),
                            "position_id": saved_data.get("position_id", defaults["position_id"]),
                            "initial_po": defaults["initial_po"] if saved_po == 0.0 else saved_po,
                            "role": defaults["role"],
                            "last_avg_price": saved_data.get("last_avg_price", defaults["last_avg_price"]),
                            "wait_reopen": saved_data.get("wait_reopen", defaults["wait_reopen"]),
                            "shield_state": pos_data.get("shield_state") or saved_data.get("shield_state", self.get_empty_shield_state()) if defaults["role"] == "main" else None,
                        })
                        if defaults["role"] == "child":
                            pos_data["last_price_open"] = saved_data.get("last_price_open",
                                                                         float(pos_data.get("avgPx", 0.0)))
                            pos_data["stop_is_move"] = saved_data.get("stop_is_move", False)

                        updated_positions[(symbol, pos_side)] = pos_data
                        await self.cache.update_position(symbol, pos_side, pos_data)
                        self.logger.info(
                            f"[sync_positions_with_state] Position {symbol}:{pos_side} updated, role={pos_data['role']}")

            for key in saved_state:
                if key not in [f"{sym}:{side}" for (sym, side) in updated_positions]:
                    self.logger.warning(f"[sync_positions_with_state] State {key} not in cache, ignoring")

            self.logger.info(f"[sync_positions_with_state] Final updated_positions: {len(updated_positions)}")

            return updated_positions
        except Exception as e:
            self.logger.error(f"[sync_positions_with_state] Sync error: {e}")
            await send_to_all_async(f"[sync_positions_with_state] Sync error: {e}")
            return {}

    async def _fetch_position_history(self, symbol: str, bot, pos_side: str, c_time_ts: int) -> dict:
        """
        Fetches position history via API with side and time filtering.
        """
        max_attempts = 15
        base_delay = 0.5
        max_delay = 10.0

        for attempt in range(1, max_attempts + 1):
            try:
                position_data = await bot.okx.get_position_history(instId=f"{symbol}-USDT-SWAP", limit=10)
                self.logger.debug(f"[_fetch_position_history] API response (attempt {attempt}): {position_data}")

                if position_data and position_data.get("status") == "success" and position_data.get("data"):
                    for record in position_data["data"]:
                        record_side = record.get("posSide")
                        record_ctime = int(record.get("cTime", 0))
                        record_utime = int(record.get("uTime", 0))

                        time_tolerance = 5000
                        if (record_side == pos_side and
                            record_ctime >= (c_time_ts - time_tolerance) and
                            record_utime > c_time_ts):

                            self.logger.info(
                                f"[_fetch_position_history] Valid record found: {symbol}:{pos_side}, "
                                f"cTime={record_ctime}, uTime={record_utime} (attempt {attempt})"
                            )
                            return {"status": "success", "data": [record]}

                    self.logger.warning(
                        f"[_fetch_position_history] No valid records for {symbol}:{pos_side} "
                        f"(c_time>={c_time_ts}) in {len(position_data['data'])} records (attempt {attempt}/{max_attempts})"
                    )
                else:
                    self.logger.warning(f"[_fetch_position_history] API returned no data (attempt {attempt})")

                delay = min(base_delay * (2 ** (attempt - 1)), max_delay)
                self.logger.debug(f"[_fetch_position_history] Waiting {delay:.1f}s before retry...")
                await asyncio.sleep(delay)

            except Exception as e:
                self.logger.error(f"[_fetch_position_history] API Error (attempt {attempt}/{max_attempts}): {e}")
                delay = min(base_delay * (2 ** (attempt - 1)), max_delay)
                await asyncio.sleep(delay)

        self.logger.error(
            f"[_fetch_position_history] All {max_attempts} attempts failed for {symbol}:{pos_side}"
        )
        return {}

    async def _build_close_message(self, symbol: str, pos_side: str, bot, position_data: dict, closed_data: dict,
                                   c_time_ts: int, role: str) -> str:
        """
        Builds the close position message using API or cache data.
        """
        sf = lambda v: float(v) if v else 0.0

        role_text = "Main" if role == "main" else "Hedge" if role == "child" else "Unknown"

        if position_data and position_data.get("status") == "success" and position_data.get("data"):
            api_data = position_data["data"][0]
            realized_pnl = sf(api_data.get("realizedPnl", 0.0))
            pnl_ratio = sf(api_data.get("pnlRatio", 0.0)) * 100
            close_price = sf(api_data.get("closeAvgPx", 0.0))
            fee = sf(api_data.get("fee", 0.0))
            side = api_data.get("posSide", "unknown")
            close_timestamp = int(api_data.get("uTime", 0))
            close_time = datetime.fromtimestamp(close_timestamp / 1000).strftime(
                '%Y-%m-%d %H:%M:%S') if close_timestamp > 0 else "N/A"
            contracts = sf(api_data.get("closeSz", closed_data.get("fillSz", 0.0)))
            message = (
                f"[_build_close_message] {role_text} position closed: {symbol} ({pos_side})\n"
                f"Side: {'LONG' if side == 'long' else 'SHORT'}\n"
                f"Contracts: {contracts}\n"
                f"PNL: {round(realized_pnl, 6)} USDT\n"
                f"PNL (%): {round(pnl_ratio, 2)}%\n"
                f"Close Price: {round(close_price, bot.round_decimal) if close_price > 0 else 'N/A'}\n"
                f"Fee: {round(fee, 6)} USDT\n"
                f"Close Time: {close_time}"
            )
        else:
            self.logger.warning(
                f"[_build_close_message] Using cache data for {symbol}:{pos_side}, API did not return data")
            pnl = float(closed_data.get("fillPnl", 0.0)) if closed_data else 0.0
            close_price = float(closed_data.get("fillPx", 0.0)) if closed_data else 0.0
            side = closed_data.get("posSide", "unknown") if closed_data else "unknown"
            close_timestamp = c_time_ts
            if closed_data:
                close_timestamp = int(closed_data.get("fillTime", closed_data.get("closeTime", c_time_ts)))
            close_time = datetime.fromtimestamp(close_timestamp / 1000).strftime('%Y-%m-%d %H:%M:%S')
            contracts = float(closed_data.get("fillSz", 0.0)) if closed_data else 0.0
            message = (
                f"[_build_close_message] {role_text} position closed: {symbol} ({pos_side})\n"
                f"Side: {'LONG' if side == 'long' else 'SHORT'}\n"
                f"Contracts: {contracts}\n"
                f"PNL: {round(pnl, 6)} USDT\n"
                f"Close Price: {round(close_price, bot.round_decimal) if close_price > 0 else 'N/A'}\n"
                f"Close Time: {close_time}"
            )
        return message

    async def _cleanup_position(self, symbol: str, pos_side: str, bot) -> None:
        """
        Cleans up position data after closure.
        """
        key = f"{symbol}:{pos_side}"

        self.logger.info(f"[_cleanup_position] Starting cleanup for {symbol}:{pos_side}")
        await send_to_all_async(f'[_cleanup_position] called for {symbol} {pos_side}')

        pos_in_cache = await self.cache.get_position(symbol, pos_side)
        if not pos_in_cache and self.position_closed_flags.get(key, False):
            self.logger.info(f"[_cleanup_position] Position {key} already cleaned, skipping")
            return

        bot.position = None
        bot.algo_id_tp = None
        bot.current_position_size = 0.0

        await self.cache.remove_position(symbol, pos_side)
        await self.cache.clear_closed(symbol, pos_side)
        await self.cache.clear_hedge(symbol, pos_side)

        try:
            await self._save_state()
        except Exception as e:
            self.logger.error(f"[_cleanup_position] _save_state error: {e}")

        self.position_closed_flags.pop(key, None)
        self.last_reopen_notification.pop(key, None)

        hedge_key = (symbol, pos_side)
        if hedge_key in self.hedge_tasks:
            self.hedge_tasks.pop(hedge_key, None)
            self.logger.info(f"[_cleanup_position] Supervisor task removed for {hedge_key}")

        self.logger.info(f"[_cleanup_position] Cleaned {symbol}:{pos_side}, flags popped")
        await send_to_all_async(f'[_cleanup_position] Cleanup finished: {symbol}:{pos_side}')

    async def periodic_cleanup(self) -> None:
        """Background task for cleaning up stale positions every 5 minutes."""
        self.cleanup_running = True

        try:
            while self.cleanup_running:
                try:
                    await asyncio.sleep(5 * 60)

                    if not self.cleanup_running:
                        break

                    positions = await self.cache.get_all_open_positions()
                    removed = 0

                    for (symbol, pos_side), pos in positions.items():
                        pos_size = float(pos.get("pos", 0))
                        avg_px = pos.get("avgPx", 0.0)

                        if pos_size <= 0 or not avg_px or avg_px <= 0:
                            await self.cache.remove_position(symbol, pos_side)
                            removed += 1
                            self.logger.debug(
                                f"[periodic_cleanup] Removed stale {symbol}:{pos_side} (pos={pos_size}, avgPx={avg_px})")

                    if removed > 0:
                        self.logger.info(f"[periodic_cleanup] Removed {removed} stale positions")
                        await self._save_state()
                        await send_to_all_async(f"[periodic_cleanup] Periodic cleanup: removed {removed} stale positions")

                except asyncio.CancelledError:
                    self.logger.info("[periodic_cleanup] CancelledError received, exiting gracefully")
                    break

                except Exception as e:
                    self.logger.error(f"[periodic_cleanup] Error: {e}")
                    await asyncio.sleep(60)

        finally:
            self.cleanup_running = False
            self.logger.info("[periodic_cleanup] Task finished")

    async def stop_periodic_cleanup(self) -> None:
        """Stops periodic_cleanup gracefully."""
        if self.cleanup_task and not self.cleanup_task.done():
            self.logger.info("[stop_periodic_cleanup] Stopping periodic_cleanup...")
            self.cleanup_running = False
            self.cleanup_task.cancel()

            try:
                await self.cleanup_task
            except asyncio.CancelledError:
                pass

            self.logger.info("[stop_periodic_cleanup] Cleanup stopped successfully")
        else:
            self.logger.info("[stop_periodic_cleanup] Cleanup already stopped or not started")