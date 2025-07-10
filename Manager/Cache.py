import asyncio
import logging
import os
from typing import Dict, Optional, Tuple, Union
from datetime import datetime

# Logger Setup
log_file = os.path.join(os.path.dirname(__file__), 'Cache.log')
logger = logging.getLogger('Cache')
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


class Cache:
    """
    Asynchronous in-memory cache for storing data on prices, positions, balances, closed positions, and orders.

    :ivar prices: Dictionary with last prices by symbol (symbol: price).
    :ivar positions: Dictionary with open position data ((symbol, pos_side): position_data).
    :ivar balance: Current balance in USDT.
    :ivar closed_positions: Dictionary with closed position data ((symbol, pos_side): dict).
    :ivar last_orders: Dictionary with last order data ((symbol, pos_side): order_data).
    :ivar last_fills: Dictionary with last fill data (symbol: fill_data).
    :ivar prices_lock: Lock for safe access to prices.
    :ivar positions_lock: Lock for safe access to positions.
    :ivar balance_lock: Lock for safe access to balance.
    :ivar closed_positions_lock: Lock for safe access to closed positions.
    :ivar last_orders_lock: Lock for safe access to orders.
    """

    def __init__(self):
        # Initialize data storage
        self.prices: Dict[str, float] = {}  # Stores last prices by symbol (key: symbol)
        self.positions: Dict[Tuple[str, str], dict] = {}
        self.closed_positions: Dict[Tuple[str, str], dict] = {}
        self.last_orders: Dict[Tuple[str, str], dict] = {}
        self.last_fills: Dict[str, dict] = {}
        
        self.prices_lock = asyncio.Lock()
        self.positions_lock = asyncio.Lock()
        self.balance_lock = asyncio.Lock()
        self.closed_positions_lock = asyncio.Lock()
        self.last_orders_lock = asyncio.Lock()
        
        self.hedges: Dict[Tuple[str, str], dict] = {}  # Stores hedge data: (symbol, pos_side) -> {last_price_open, stop_is_move, ...}
        self.hedges_lock = asyncio.Lock()  # Lock for safe access to hedges
        
        self.balance: Dict[str, float] = {
            "availBal": 0.0,
            "cashBal": 0.0,
            "totalEq": 0.0,
            "mmr": 100.0,
            "risk_pct": 0.0  # Added risk_pct
        }

        logger.info("[__init__] Cache initialized")

    async def update_risk_pct(self, risk_pct: float) -> None:
        """
        Updates the risk percentage in the cache.

        :param risk_pct: Risk percentage (float).
        :return: None
        """
        try:
            async with self.balance_lock:
                self.balance["risk_pct"] = risk_pct
                logger.info(f"[update_risk_pct] Risk pct updated: {risk_pct:.2f}%")
        except Exception as e:
            logger.error(f"[update_risk_pct] Error updating risk_pct: {e}")

    async def get_risk_pct(self) -> float:
        """
        Retrieves the current risk percentage from the cache.

        :return: Risk percentage (float) or 0.0 on error.
        """
        try:
            async with self.balance_lock:
                return self.balance.get("risk_pct", 0.0)
        except Exception as e:
            logger.error(f"[get_risk_pct] Error retrieving risk_pct: {e}")
            return 0.0

    async def update_price(self, symbol: str, price: float) -> None:
        """
        Updates the price for the specified symbol in the cache.

        :param symbol: Trading pair symbol (e.g., 'BTC').
        :param price: New price.
        :return: None
        """
        try:
            async with self.prices_lock:
                self.prices[symbol] = price
        except Exception as e:
            logger.error(f"[update_price] Error updating price for {symbol}: {e}")

    async def get_price(self, symbol: str) -> Optional[float]:
        """
        Retrieves the current price for the specified symbol from the cache.

        :param symbol: Trading pair symbol.
        :return: Price or None if price is missing.
        """
        try:
            async with self.prices_lock:
                return self.prices.get(symbol)
        except Exception as e:
            logger.error(f"[get_price] Error retrieving price for {symbol}: {e}")
            return None

    async def update_averaging(self, symbol: str, pos_side: str, usdt_amount: float, last_avg_price: float) -> None:
        """
        Updates averaging data for a position in the cache, including the average price.

        :param symbol: Trading pair symbol (e.g., 'BTC').
        :param pos_side: Position side ('long' or 'short').
        :param usdt_amount: USDT amount added for averaging.
        :param last_avg_price: Price at which averaging occurred.
        :return: None
        """
        position_key = (symbol, pos_side)
        try:
            async with self.positions_lock:
                pos = self.positions.get(position_key, {})
                pos['averages'] = pos.get('averages', 0) + 1
                pos['margin'] = pos.get('margin', 0.0) + usdt_amount
                pos['last_avg_price'] = last_avg_price
                self.positions[position_key] = pos
                logger.info(
                    f"[update_averaging] Averaging for {symbol} ({pos_side}): averages={pos['averages']}, margin={pos['margin']}, last_avg_price={last_avg_price}")
        except Exception as e:
            logger.error(f"[update_averaging] Error updating averaging for {symbol} ({pos_side}): {e}")

    async def get_position(self, symbol: str, pos_side: str) -> Optional[dict]:
        """
        Retrieves position data for the specified symbol and side from the cache.

        :param symbol: Trading pair symbol.
        :param pos_side: Position side ('long' or 'short').
        :return: Position data or None if position is missing.
        """
        position_key = (symbol, pos_side)
        try:
            async with self.positions_lock:
                return self.positions.get(position_key)
        except Exception as e:
            logger.error(f"[get_position] Error retrieving position for {symbol} ({pos_side}): {e}")
            return None

    async def update_balance(self, balances: Dict[str, float]) -> None:
        """
        Updates the current balance in the cache.
        :param balances: Dictionary of balances.
        :return: None
        """
        try:
            async with self.balance_lock:
                for key, value in balances.items():
                    if key in self.balance:
                        self.balance[key] = value
                logger.info(f"[update_balance] Balance updated: {self.balance}")
        except Exception as e:
            logger.error(f"[update_balance] Error updating balance: {e}")

    async def get_balance(self, key: str = "availBal") -> Union[float, dict]:
        """
        Retrieves the current balance from the cache.

        :param key: Key to retrieve specific balance value. If empty string, returns the entire dictionary.
        :return: Balance value or dictionary of balances if key is empty.
        """
        try:
            async with self.balance_lock:
                if key == "":
                    return self.balance.copy()
                return self.balance.get(key, 0.0)
        except Exception as e:
            logger.error(f"[get_balance] Error retrieving balance: {e}")
            return {} if key == "" else 0.0

    async def clear_cache(self, symbol: Optional[str] = None, pos_side: Optional[str] = None) -> None:
        """
        Clears the cache for the specified symbol/side or completely.

        :param symbol: Trading pair symbol (optional).
        :param pos_side: Position side ('long' or 'short', optional).
                         If symbol is specified but pos_side is not, clears all positions for the symbol.
                         If both symbol and pos_side are specified, clears specific position.
                         If None, clears the entire cache.
        :return: None
        """
        try:
            async with self.prices_lock, self.positions_lock, self.closed_positions_lock, self.balance_lock, self.last_orders_lock, self.hedges_lock:
                if symbol:
                    if pos_side is None:
                        self.prices.pop(symbol, None)
                        keys_to_remove_positions = [k for k in self.positions.keys() if k[0] == symbol]
                        for k in keys_to_remove_positions:
                            del self.positions[k]

                        keys_to_remove_closed = [k for k in self.closed_positions.keys() if k[0] == symbol]
                        for k in keys_to_remove_closed:
                            del self.closed_positions[k]

                        keys_to_remove_orders = [k for k in self.last_orders.keys() if k[0] == symbol]
                        for k in keys_to_remove_orders:
                            del self.last_orders[k]

                        keys_to_remove_hedges = [k for k in self.hedges.keys() if k[0] == symbol]
                        for k in keys_to_remove_hedges:
                            del self.hedges[k]

                        logger.info(f"[clear_cache] Cache cleared for symbol {symbol}")
                    else:
                        position_key = (symbol, pos_side)
                        self.positions.pop(position_key, None)
                        self.closed_positions.pop(position_key, None)
                        self.last_orders.pop(position_key, None)
                        self.hedges.pop(position_key, None)
                        logger.info(f"[clear_cache] Cache cleared for {symbol} ({pos_side})")
                else:
                    self.prices.clear()
                    self.positions.clear()
                    self.closed_positions.clear()
                    self.last_orders.clear()
                    self.hedges.clear()
                    logger.info("[clear_cache] Full cache cleared")
        except Exception as e:
            logger.error(f"[clear_cache] Error clearing cache: {e}")

    async def remove_position(self, symbol: str, pos_side: str) -> None:
        """
        Removes an active position by symbol and side from the cache.

        :param symbol: Trading pair symbol (e.g., 'ETH').
        :param pos_side: Position side ('long' or 'short').
        """
        position_key = (symbol, pos_side)
        try:
            async with self.positions_lock:
                if position_key in self.positions:
                    del self.positions[position_key]
                    logger.info(f"[remove_position] Position for {symbol} ({pos_side}) removed from active")
        except Exception as e:
            logger.error(f"[remove_position] Error removing position for {symbol} ({pos_side}): {e}")

    async def get_all_open_positions(self) -> Dict[Tuple[str, str], dict]:
        """
        Returns a copy of all open positions.

        :return: Dictionary {(symbol, pos_side): position_data}.
        """
        try:
            async with self.positions_lock:
                return dict(self.positions)
        except Exception as e:
            logger.error(f"[get_all_open_positions] Error retrieving positions: {e}")
            return {}

    async def get_closed_position(self, symbol: str, pos_side: str) -> Optional[dict]:
        """
        Returns closed position data for symbol and side if not notified.

        :param symbol: Trading pair symbol.
        :param pos_side: Position side ('long' or 'short').
        :return: Closing data or None.
        """
        position_key = (symbol, pos_side)
        try:
            async with self.closed_positions_lock:
                data = self.closed_positions.get(position_key)
                if data and not data.get('notified', False):
                    return data
                return None
        except Exception as e:
            logger.error(f"[get_closed_position] Error retrieving closure for {symbol} ({pos_side}): {e}")
            return None

    async def clear_closed(self, symbol: str, pos_side: str) -> None:
        """
        Marks closed position data as notified.

        :param symbol: Instrument symbol (e.g., 'BTC').
        :param pos_side: Position side ('long' or 'short').
        """
        position_key = (symbol, pos_side)
        try:
            async with self.closed_positions_lock:
                if position_key in self.closed_positions:
                    self.closed_positions[position_key]['notified'] = True
                    logger.info(f"[clear_closed] Closed position data marked for {symbol} ({pos_side})")
        except Exception as e:
            logger.error(f"[clear_closed] Error clearing closure for {symbol} ({pos_side}): {e}")

    async def get_all_closed(self) -> Dict[Tuple[str, str], dict]:
        """
        Returns a copy of all closed positions.

        :return: Dictionary {(symbol, pos_side): closed_position_data}.
        """
        try:
            async with self.closed_positions_lock:
                return dict(self.closed_positions)
        except Exception as e:
            logger.error(f"[get_all_closed] Error retrieving closed positions: {e}")
            return {}

    async def ensure_custom_fields(self, position_data: dict, symbol: str, pos_side: str) -> dict:
        """
        Checks and adds custom keys to position data if they are missing.

        :param position_data: Position data (e.g., {'avgPx': 0.03213, 'pos': 6.0, ...}).
        :param symbol: Trading pair symbol.
        :param pos_side: Position side ('long' or 'short').
        :return: Updated position data with custom keys.
        """
        try:
            custom_fields = {
                'averages': 0,
                'position_id': f"{symbol}-{pos_side}-{int(datetime.now().timestamp() * 1000)}",
                'initial_po': 0.0,
                'is_new': False,
                'round_decimal': 8,
                'last_avg_price': 0.0,
                'tp_error': False
            }

            updated_data = position_data.copy()

            for field, default_value in custom_fields.items():
                updated_data.setdefault(field, default_value)

            updated_data.setdefault('cTime', int(datetime.now().timestamp() * 1000))
            updated_data.setdefault('posSide', pos_side)

            return updated_data
        except Exception as e:
            logger.error(
                f"[ensure_custom_fields] Error setting custom keys for {symbol} ({pos_side}): {e}")
            return position_data

    async def update_position(self, symbol: str, pos_side: str, position_data: dict) -> None:
        """
        Updates open position data for the specified symbol and side in the cache.

        :param symbol: Trading pair symbol (e.g., 'AVAAI').
        :param pos_side: Position side ('long' or 'short').
        :param position_data: Position data.
        :return: None
        """
        position_key = (symbol, pos_side)
        try:
            async with self.positions_lock:
                if position_data.get('pos') == 0:
                    self.positions.pop(position_key, None)
                    logger.info(f"[update_position] Position for {symbol} ({pos_side}) removed from cache (closed)")
                else:
                    existing_data = self.positions.get(position_key, {})
                    merged_data = {**existing_data, **position_data}
                    updated_data = await self.ensure_custom_fields(merged_data, symbol, pos_side)
                    self.positions[position_key] = updated_data
        except Exception as e:
            logger.error(f"[update_position] Error updating position for {symbol} ({pos_side}): {e}")

    async def update_close(self, symbol: str, pos_side: str, data: dict) -> None:
        """
        Updates closed position data in the cache, preserving all passed fields.

        :param symbol: Instrument symbol (e.g., 'ACE').
        :param pos_side: Position side ('long' or 'short').
        :param data: Closed position data (fillPx, fillPnl, fillTime, posSide, pos, closeTime).
        :return: None
        """
        position_key = (symbol, pos_side)
        try:
            async with self.closed_positions_lock:
                existing_data = self.closed_positions.get(position_key, {})
                
                old_fill_time = existing_data.get('fillTime', 0)
                new_fill_time = data.get('fillTime', 0)
                
                existing_data.update(data)
                
                existing_data.setdefault('closeTime', int(datetime.now().timestamp() * 1000))
                
                if new_fill_time > 0 and new_fill_time != old_fill_time:
                    existing_data['notified'] = False
                else:
                    existing_data.setdefault('notified', False)
                
                existing_data.setdefault('posSide', pos_side)
                
                self.closed_positions[position_key] = existing_data
                logger.info(f"[update_close] For {symbol} ({pos_side}) saved: {existing_data}")

        except Exception as e:
            logger.error(f"[update_close] Error updating closure for {symbol} ({pos_side}): {e}")

    async def update_order(self, symbol: str, pos_side: str, order_data: dict) -> None:
        """
        Updates order data for the specified symbol and side in the cache.

        :param symbol: Trading pair symbol (e.g., 'BTC').
        :param pos_side: Position side ('long' or 'short').
        :param order_data: Order data.
        :return: None
        """
        position_key = (symbol, pos_side)
        try:
            async with self.last_orders_lock:
                self.last_orders[position_key] = order_data
                logger.info(f"[update_order] Order for {symbol} ({pos_side}) updated in cache: {order_data}")
        except Exception as e:
            logger.error(f"[update_order] Error updating order for {symbol} ({pos_side}): {e}")

    async def get_order(self, symbol: str, pos_side: str) -> Optional[dict]:
        """
        Retrieves order data for the specified symbol and side from the cache.

        :param symbol: Trading pair symbol.
        :param pos_side: Position side ('long' or 'short').
        :return: Order data or None if order is missing.
        """
        position_key = (symbol, pos_side)
        try:
            async with self.last_orders_lock:
                return self.last_orders.get(position_key)
        except Exception as e:
            logger.error(f"[get_order] Error retrieving order for {symbol} ({pos_side}): {e}")
            return None

    async def clear_order(self, symbol: str, pos_side: str) -> None:
        """
        Removes order data for the specified symbol and side from the cache.

        :param symbol: Trading pair symbol (e.g., 'BTC').
        :param pos_side: Position side ('long' or 'short').
        :return: None
        """
        position_key = (symbol, pos_side)
        try:
            async with self.last_orders_lock:
                self.last_orders.pop(position_key, None)
                logger.info(f"[clear_order] Order for {symbol} ({pos_side}) removed from cache")
        except Exception as e:
            logger.error(f"[clear_order] Error removing order for {symbol} ({pos_side}): {e}")

    async def get_tp_error(self, symbol: str, pos_side: str) -> bool:
        """
        Retrieves the TP set error flag for the specified symbol and side from the cache.

        :param symbol: Trading pair symbol.
        :param pos_side: Position side.
        :return: True if there was a TP set error, False otherwise.
        """
        position_key = (symbol, pos_side)
        try:
            async with self.positions_lock:
                position = self.positions.get(position_key, {})
                return position.get('tp_error', False)
        except Exception as e:
            logger.error(f"[get_tp_error] Error retrieving TP flag for {symbol} ({pos_side}): {e}")
            return False

    async def set_tp_error(self, symbol: str, pos_side: str, error: bool) -> None:
        """
        Sets or resets the TP set error flag for the specified symbol and side.

        :param symbol: Trading pair symbol.
        :param pos_side: Position side.
        :param error: True if TP failed to set, False if TP set successfully.
        :return: None
        """
        position_key = (symbol, pos_side)
        try:
            async with self.positions_lock:
                position = self.positions.get(position_key, {})
                position['tp_error'] = error
                self.positions[position_key] = position
                logger.info(f"[set_tp_error] TP flag for {symbol} ({pos_side}) set: {error}")
        except Exception as e:
            logger.error(f"[set_tp_error] Error setting TP flag for {symbol} ({pos_side}): {e}")

    async def get_positions_by_symbol(self, symbol: str) -> Dict[str, dict]:
        """
        Retrieves all open positions (long and short) for the specified symbol.

        :param symbol: Trading pair symbol.
        :return: Dictionary {pos_side: position_data}.
        """
        result = {}
        try:
            async with self.positions_lock:
                for (s, side), data in self.positions.items():
                    if s == symbol:
                        result[side] = data
        except Exception as e:
            logger.error(f"[get_positions_by_symbol] Error retrieving positions for {symbol}: {e}")
        return result

    async def get_position_size(self, symbol: str, pos_side: str) -> float:
        """
        Retrieves the position size (field 'pos') for the specified symbol and side.

        :param symbol: Trading pair symbol.
        :param pos_side: Position side.
        :return: Position size or 0.0 if position is missing.
        """
        position = await self.get_position(symbol, pos_side)
        return float(position.get('pos', 0.0)) if position else 0.0

    async def get_position_role(self, symbol: str, pos_side: str) -> str:
        """
        Retrieves the position role (field 'role') for the specified symbol and side.

        :param symbol: Trading pair symbol.
        :param pos_side: Position side.
        :return: Position role ('main', 'child') or 'unknown'.
        """
        position = await self.get_position(symbol, pos_side)
        return position.get('role', 'unknown') if position else 'unknown'
    
    async def get_last_avg_price(self, symbol: str, pos_side: str) -> Optional[float]:
        """
        Retrieves the last successful averaging price for the specified symbol and side.

        :param symbol: Trading pair symbol.
        :param pos_side: Position side.
        :return: Last averaging price or None.
        """
        position_key = (symbol, pos_side)
        try:
            async with self.positions_lock:
                position = self.positions.get(position_key)
                return position.get('last_avg_price') if position else None
        except Exception as e:
            logger.error(
                f"[get_last_avg_price] Error retrieving last averaging price for {symbol} ({pos_side}): {e}")
            return None

    async def update_hedge(self, symbol: str, pos_side: str, hedge_data: dict) -> None:
        """
        Updates hedge data for the specified symbol and side in the cache.

        :param symbol: Trading pair symbol.
        :param pos_side: Position side.
        :param hedge_data: Hedge data.
        :return: None
        """
        position_key = (symbol, pos_side)
        try:
            async with self.hedges_lock:
                existing_data = self.hedges.get(position_key, {})
                updated_data = {
                    **existing_data,
                    **hedge_data,
                    'posSide': pos_side
                }
                updated_data.setdefault('last_price_open', 0.0)
                updated_data.setdefault('stop_is_move', False)
                self.hedges[position_key] = updated_data
                logger.info(f"[update_hedge] Hedge data for {symbol} ({pos_side}) updated: {updated_data}")
        except Exception as e:
            logger.error(f"[update_hedge] Error updating hedge for {symbol} ({pos_side}): {e}")

    async def get_hedge(self, symbol: str, pos_side: str) -> Optional[dict]:
        """
        Retrieves hedge data for the specified symbol and side from the cache.

        :param symbol: Trading pair symbol.
        :param pos_side: Position side.
        :return: Hedge data or None if hedge is missing.
        """
        position_key = (symbol, pos_side)
        try:
            async with self.hedges_lock:
                return self.hedges.get(position_key)
        except Exception as e:
            logger.error(f"[get_hedge] Error retrieving hedge for {symbol} ({pos_side}): {e}")
            return None

    async def clear_hedge(self, symbol: str, pos_side: str) -> None:
        """
        Removes hedge data for the specified symbol and side from the cache.

        :param symbol: Trading pair symbol.
        :param pos_side: Position side.
        :return: None
        """
        position_key = (symbol, pos_side)
        try:
            async with self.hedges_lock:
                self.hedges.pop(position_key, None)
                logger.info(f"[clear_hedge] Hedge data for {symbol} ({pos_side}) removed from cache")
        except Exception as e:
            logger.error(f"[clear_hedge] Error removing hedge for {symbol} ({pos_side}): {e}")

    async def get_all_hedges(self) -> Dict[Tuple[str, str], dict]:
        """
        Returns a copy of all hedge data.

        :return: Dictionary {(symbol, pos_side): hedge_data}.
        """
        try:
            async with self.hedges_lock:
                return dict(self.hedges)
        except Exception as e:
            logger.error(f"[get_all_hedges] Error retrieving all hedges: {e}")
            return {}

    async def clear_all_hedges(self) -> None:
        """
        Clears all hedge data from the cache.

        :return: None
        """
        try:
            async with self.hedges_lock:
                self.hedges.clear()
                logger.info("[clear_all_hedges] All hedge data cleared")
        except Exception as e:
            logger.error(f"[clear_all_hedges] Error clearing all hedges: {e}")
