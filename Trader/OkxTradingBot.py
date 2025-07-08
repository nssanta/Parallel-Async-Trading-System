import asyncio
import logging
import random
from decimal import Decimal, ROUND_DOWN, ROUND_UP
import uuid
import os
from typing import Tuple, Optional

from Manager.Utils import Signal
from Manager.Cache import Cache
from config import LEVERAGE, MIN_BALANCE, COMMISSION, SYMBOLS_FILE, TRADING_STAGES, STOP_LOSS, HEDGE_CONFIG, \
    TP_MAX_ATTEMPTS, TP_MULTIPLIER, TP_RETRY_DELAY, SL_MULTIPLIER, SL_RETRY_DELAY, SL_MAX_ATTEMPTS

from Trader.OKX import Okx
from Telegram.UtilsTG import send_to_all_async

# Logger Setup
log_file = os.path.join(os.path.dirname(__file__), 'Trader.log')
logger = logging.getLogger('TraderLogger')
logger.setLevel(logging.ERROR)

os.makedirs(os.path.dirname(log_file), exist_ok=True)
open(log_file, 'a', encoding='utf-8').close()

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

class OkxTradingBot:
    def __init__(self,
                 pair_futures: str,
                 pos_side: str,
                 cache: Cache,
                 leverage: int = LEVERAGE,
                 time_sleep: float = 0.3,
                 ):
        """
        Initializes the OKX trading bot with cache integration.

        :param pair_futures: Futures pair (e.g., BTC-USDT-SWAP).
        :param cache: Cache instance for data access.
        :param leverage: Trading leverage.
        :param time_sleep: Delay between requests in seconds.
        """
        self.pair_futures = pair_futures
        self.cache = cache
        self.pos_side = pos_side
        self.leverage = leverage
        self.trading_stages = TRADING_STAGES
        self.stop_loss = float(STOP_LOSS)
        self.time_sleep = time_sleep
        self.round_decimal = None
        self.lot_size = None
        self.algo_id_tp = None
        self.algo_id_sl = None
        self.tick_size = None
        self.logger = logger
        self.instrument_data = {}

        # Validate averaging steps
        steps = [stage['step'] for stage in self.trading_stages]
        if sorted(steps) != list(range(len(steps))):
            self.logger.error(f"[__init__] Invalid steps in TRADING_STAGES: {steps}")
            raise ValueError("Steps in TRADING_STAGES must be sequential: 0, 1, 2, ...")

        self.entry_stage = next((stage for stage in self.trading_stages if stage['step'] == 0), None)
        if not self.entry_stage:
            raise ValueError("Missing step 0 in TRADING_STAGES")
        self.averaging_stages = [stage for stage in self.trading_stages if stage['step'] > 0]

        try:
            self.okx = Okx()
            self.okx.disable_all_logging()
            self.logger.info(f"[__init__] OKX client initialized")
        except Exception as e:
            self.logger.error(f"[__init__] OKX client initialization error: {e}")
            raise

    async def initialize(self) -> None:
        """
        Initializes settings for futures trading.
        """
        max_attempts = 5
        for attempt in range(1, max_attempts + 1):
            try:
                spot_pair = self.pair_futures.replace('-SWAP', '')
                if not await self.okx.get_save_info_instrumen(
                        pair=spot_pair,
                        category="SPOT",
                        pair_futures=self.pair_futures,
                        category_futures='SWAP'
                ):
                    self.logger.warning(f"[initialize] Attempt {attempt}: Instrument data not loaded")
                    if attempt < max_attempts:
                        await asyncio.sleep(1)
                    continue

                await self.okx.set_leverage(
                    inst_id=self.pair_futures,
                    lever=self.leverage,
                    mgn_mode='cross',
                    pos_side='long'
                )
                await self.okx.set_leverage(
                    inst_id=self.pair_futures,
                    lever=self.leverage,
                    mgn_mode='cross',
                    pos_side='short'
                )
                await self.get_decimal_price()
                await self.get_lot_size()
                await self.get_tick_size()
                self.logger.info(f"[initialize] Initialization complete for {self.pair_futures}")
                await send_to_all_async(f"🟢 [initialize] Initialization complete for {self.pair_futures}")
                return
            except Exception as e:
                self.logger.error(f"[initialize] Attempt {attempt}: Error: {e}")
                await send_to_all_async(f"❌ [initialize] Attempt {attempt}: Error: {e}")
                if attempt < max_attempts:
                    await asyncio.sleep(1)
        self.logger.error(f"[initialize] Failed to initialize {self.pair_futures} after {max_attempts} attempts")
        await send_to_all_async(f"❌ [initialize] Failed to initialize {self.pair_futures}")
        raise ValueError("Failed to initialize bot")

    async def get_price_with_fallback(self, symbol: str) -> float:
        """
        Returns the current instrument price, trying cache first, then OKX REST API.
        """
        api_symbol = f"{symbol}-USDT-SWAP" if not symbol.endswith('-USDT-SWAP') else symbol
        price = await self.cache.get_price(symbol=symbol)
        if price is None or price == 0:
            self.logger.debug(f"[get_price_with_fallback] No price in cache for {symbol}, requesting OKX")
            price = await self.okx.get_pair_price(pair=api_symbol)
        return float(price) or 0.0

    async def get_tick_size(self) -> None:
        """
        Gets tickSize for the futures pair.
        """
        try:
            tick_size = self.okx.info_for_futures_instrument.get('tickSz')
            self.tick_size = float(tick_size)
            self.logger.info(f"[get_tick_size] tickSz for {self.pair_futures}: {self.tick_size}")
        except Exception as e:
            self.logger.error(f"[get_tick_size] Error getting tickSz: {e}")
            await send_to_all_async(f"❌ [get_tick_size] Error getting tickSz: {e}")
            self.tick_size = 0.0001

    async def get_decimal_price(self) -> None:
        """
        Gets the number of decimal places for the price based on tickSz.
        """
        try:
            tick_size = self.okx.info_for_futures_instrument.get('tickSz')
            tick_str = str(tick_size).rstrip('0')
            if '.' in tick_str:
                self.round_decimal = len(tick_str.split('.')[1])
            else:
                self.round_decimal = 0
            self.round_decimal = max(self.round_decimal, 3)
            self.logger.info(
                f"[get_decimal_price] Set {self.round_decimal} decimals for {self.pair_futures} based on tickSz={tick_size}")
        except Exception as e:
            self.logger.error(f"[get_decimal_price] Error getting price decimals: {e}")
            await send_to_all_async(f"❌ [get_decimal_price] Error getting price decimals: {e}")
            self.round_decimal = 3

    async def get_lot_size(self) -> None:
        """
        Gets the lot size for the futures pair.
        """
        try:
            lot_sz = self.okx.info_for_futures_instrument.get('lotSz')
            self.lot_size = float(lot_sz)
            self.logger.info(f"[get_lot_size] Set lot size {self.lot_size} for {self.pair_futures}")
        except Exception as e:
            self.logger.error(f"[get_lot_size] Error getting lot size: {e}")
            await send_to_all_async(f"❌ [get_lot_size] Error getting lot size: {e}")
            self.lot_size = 0.01

    def round_to_lot_size(self, size: float) -> float:
        """
        Rounds position size to the nearest lot_size multiple.
        """
        try:
            lot_size = float(self.okx.info_for_futures_instrument.get('lotSz', 1.0))
            if lot_size <= 0:
                raise ValueError("lot_size must be greater than 0")
            size_dec = Decimal(str(size))
            lot_size_dec = Decimal(str(lot_size))
            rounded = (size_dec / lot_size_dec).to_integral_value(ROUND_DOWN) * lot_size_dec
            return float(rounded)
        except Exception as e:
            self.logger.error(f"[round_to_lot_size] Rounding error: {e}")
            return 0.0

    async def compute_hedge_delta(self, delta_coef: float) -> Tuple[float, float]:
        """Calculates additional hedge contracts and USDT equivalent based on coefficient."""
        try:
            delta = float(delta_coef or 0)
        except Exception:
            delta = 0.0
        if delta <= 0:
            return 0.0, 0.0

        symbol = self.pair_futures.replace("-USDT-SWAP", "")
        main_side = "long" if self.pos_side == "short" else "short"

        main_pos = await self.cache.get_position(symbol, main_side)
        main_size = 0.0
        try:
            if isinstance(main_pos, dict):
                main_size = float(main_pos.get("pos", 0.0) or 0.0)
        except Exception:
            main_size = 0.0
        if main_size <= 0:
            await send_to_all_async(f"⚠️ [compute_hedge_delta] No active main position for {symbol}:{main_side}")
            return 0.0, 0.0

        if not await self.ensure_instrument_data():
            return 0.0, 0.0

        try:
            min_sz = float(self.okx.info_for_futures_instrument.get('minSz', 0.0))
        except Exception:
            min_sz = 0.0
        try:
            lot_sz = float(self.okx.info_for_futures_instrument.get('lotSz', 1.0))
        except Exception:
            lot_sz = 1.0

        raw_contracts = main_size * delta
        delta_contracts = self.round_to_lot_size(raw_contracts)

        price = await self.get_price_with_fallback(symbol)
        try:
            ct_val = float(self.okx.info_for_futures_instrument.get('ctVal', 1.0))
        except Exception:
            ct_val = 1.0
        lever = float(self.leverage or 1.0)

        if delta_contracts <= 0 or delta_contracts < min_sz:
            try:
                lot_size_dec = Decimal(str(lot_sz))
                need_dec = (Decimal(str(min_sz)) / lot_size_dec).to_integral_value(ROUND_UP) * lot_size_dec
                delta_contracts = float(need_dec)
            except Exception:
                delta_contracts = float(min_sz or 0.0)

            usdt = (delta_contracts * price * ct_val) / lever if price > 0 and ct_val > 0 and lever > 0 else 0.0
            await send_to_all_async(
                f"⚠️ [compute_hedge_delta] Lot too small: raw={raw_contracts:.6f} < minSz={min_sz}. "
                f"Rounding up to min. Contracts={delta_contracts:.6f}, ≈{usdt:.4f} USDT"
            )
            return float(delta_contracts), float(usdt)

        usdt = (delta_contracts * price * ct_val) / lever if price > 0 and ct_val > 0 and lever > 0 else 0.0
        return float(delta_contracts), float(usdt)

    async def get_pnl_info(self) -> Tuple[float, str, float, float, float]:
        """
        Gets PNL and position info from cache or API.
        """
        try:
            symbol = self.pair_futures.replace("-USDT-SWAP", "")
            pos_data = await self.cache.get_position(symbol, self.pos_side)
            if pos_data and isinstance(pos_data.get("pos", 0), (int, float)) and pos_data.get("pos", 0) > 0:
                pos_side = pos_data.get("posSide")
                try:
                    avg_price = float(pos_data.get("avgPx", 0))
                    mark_price = float(pos_data.get("markPx", 0))
                    position_size = float(pos_data.get("pos", 0))
                    pnl = float(pos_data.get("uplLastPx", 0))
                except (ValueError, TypeError) as e:
                    self.logger.error(f"[get_pnl_info] Cache conversion error for {self.pair_futures}: {e}")
                    return 0.0, None, None, None, 0.0
                if avg_price == 0 or mark_price == 0 or pos_side is None:
                    self.logger.warning(f"[get_pnl_info] Zero prices or pos_side in cache for {self.pair_futures}")
                    return 0.0, None, None, None, 0.0
                return pnl, pos_side, avg_price, mark_price, position_size

            max_attempts = 3
            for attempt in range(1, max_attempts + 1):
                try:
                    await asyncio.sleep(random.uniform(0.3, 1.1))
                    resp = await self.okx.get_position_details(instId=self.pair_futures)

                    if not isinstance(resp, dict) or resp.get("code") != "0":
                        self.logger.error(f"[get_pnl_info] Attempt {attempt}: Invalid API response: {resp}")
                        if attempt < max_attempts:
                            await asyncio.sleep(random.uniform(0.2, 0.9))
                        continue

                    data = resp.get("data", [])
                    if not data:
                        self.logger.info(f"[get_pnl_info] Attempt {attempt}: Position closed (not found)")
                        return 0.0, None, None, None, 0.0

                    for pos in data:
                        inst_id = pos.get("instId", "")
                        pos_side = pos.get("posSide", None)
                        try:
                            position_size = float(pos.get("pos", "0") or "0")
                            avg_price = float(pos.get("avgPx", "0") or "0")
                            mark_price = float(pos.get("markPx", "0") or "0")
                            pnl = float(pos.get("upl", "0") or "0")
                        except (ValueError, TypeError) as e:
                            self.logger.error(
                                f"[get_pnl_info] Attempt {attempt}: API data conversion error for {self.pair_futures}: {e}")
                            continue
                        if inst_id == self.pair_futures and position_size > 0 and pos_side:
                            if avg_price == 0 or mark_price == 0:
                                self.logger.warning(
                                    f"[get_pnl_info] Attempt {attempt}: Zero prices for {self.pair_futures}")
                                return 0.0, None, None, None, 0.0
                            await self.cache.update_position(symbol, pos_side, {
                                "avgPx": avg_price,
                                "posSide": pos_side,
                                "pos": position_size,
                                "markPx": mark_price,
                                "upl": pnl
                            })
                            return pnl, pos_side, avg_price, mark_price, position_size

                    self.logger.info(f"[get_pnl_info] Attempt {attempt}: Position closed (not found)")
                    return 0.0, None, None, None, 0.0

                except Exception as e:
                    self.logger.error(f"[get_pnl_info] Attempt {attempt}: API Error: {e}")
                    if attempt < max_attempts:
                        await asyncio.sleep(random.uniform(0.2, 0.9))

            self.logger.error(
                f"[get_pnl_info] Failed to get data after {max_attempts} attempts, assuming closed")
            await send_to_all_async(
                f"❌ [get_pnl_info] Failed to get data for {self.pair_futures} after {max_attempts} attempts")
            return 0.0, None, None, None, 0.0

        except Exception as e:
            self.logger.error(f"[get_pnl_info] General Error: {e}")
            await send_to_all_async(f"❌ [get_pnl_info] General Error for {self.pair_futures}: {e}")
            return 0.0, None, None, None, 0.0

    async def calculate_level(self, entry_price: float, direction: str,
                              take_profit_percent: float = None,
                              stop_loss_percent: float = None) -> tuple[str, str]:
        """
        Calculates TP and SL levels.
        """
        try:
            if self.tick_size <= 0:
                self.logger.error(f"[calculate_level] Invalid tick_size: {self.tick_size}")
                await send_to_all_async(f"❌ [calculate_level] Invalid tick_size: {self.tick_size}")
                return None, None

            entry_price = Decimal(str(entry_price))
            take_profit = Decimal(str(take_profit_percent)) if take_profit_percent is not None else None
            stop_loss = Decimal(str(stop_loss_percent)) if stop_loss_percent is not None else None

            if take_profit is None and stop_loss is None:
                return None, None

            tick = Decimal(str(self.tick_size))
            take_price, stop_price = None, None

            if take_profit is not None:
                take_adjustment = entry_price * (take_profit / 100)
                if direction == 'long':
                    take_price = entry_price + take_adjustment
                    take_price = (take_price / tick).to_integral_value(ROUND_UP) * tick
                else:
                    take_price = entry_price - take_adjustment
                    take_price = (take_price / tick).to_integral_value(ROUND_DOWN) * tick

                actual_percent = abs((take_price - entry_price) / entry_price) * 100
                if actual_percent < take_profit:
                    if direction == 'long':
                        take_price = entry_price * (1 + take_profit / 100)
                        take_price = (take_price / tick).to_integral_value(ROUND_UP) * tick
                    else:
                        take_price = entry_price * (1 - take_profit / 100)
                        take_price = (take_price / tick).to_integral_value(ROUND_DOWN) * tick
            else:
                actual_percent = Decimal("0")

            if stop_loss is not None:
                stop_adjustment = entry_price * (stop_loss / 100)
                if direction == 'long':
                    stop_price = entry_price - stop_adjustment
                else:
                    stop_price = entry_price + stop_adjustment
                stop_price = (stop_price / tick).to_integral_value(ROUND_DOWN) * tick

            precision = self.round_decimal
            take_str = f"{float(take_price):.{precision}f}" if take_price is not None else None
            stop_str = f"{float(stop_price):.{precision}f}" if stop_price is not None else None

            self.logger.info(
                f"[calculate_level] Calculated levels: "
                f"TP={take_str if take_str else '-'} "
                f"({actual_percent:.2f}%), "
                f"SL={stop_str if stop_str else '-'} "
                f"for {direction}"
            )

            return take_str, stop_str

        except Exception as e:
            self.logger.error(f"[calculate_level] Error calculating levels: {e}")
            await send_to_all_async(f"❌ [calculate_level] Error calculating levels: {e}")
            return None, None

    async def fetch_algo_id(self, pos_side: str, max_attempts: int = 3, delay: float = 0.5) -> bool:
        """
        Fetches TP algo_id from cache or API.
        """
        symbol = self.pair_futures.replace("-USDT-SWAP", "")

        order_data = await self.cache.get_order(symbol, pos_side)
        if order_data and order_data.get("posSide") == pos_side:
            tp_trigger_px = order_data.get("tpTriggerPx", "")
            try:
                if tp_trigger_px and float(tp_trigger_px) > 0:
                    self.algo_id_tp = order_data["algoId"]
                    self.logger.info(f"[fetch_algo_id] algo_id_tp found in cache: {self.algo_id_tp}")
                    return True
            except (ValueError, TypeError):
                self.logger.warning(f"[fetch_algo_id] Invalid tpTriggerPx in cache: {tp_trigger_px}")

        async def try_fetch_api():
            active_orders = await self.okx.get_pending_algo_orders(ordType='conditional,oco')
            if not isinstance(active_orders, dict) or active_orders.get("status") != "success":
                self.logger.error(f"[fetch_algo_id] Invalid API response: {active_orders}")
                return False
            for order in active_orders.get('data', []):
                if (
                        order.get('instId') == self.pair_futures and
                        order.get('posSide') == pos_side and
                        order.get('tpTriggerPx', '') and
                        order.get('reduceOnly') == 'true'
                ):
                    try:
                        float(order.get("tpTriggerPx"))
                        self.algo_id_tp = order['algoId']
                        await self.cache.update_order(symbol, pos_side, order)
                        self.logger.info(f"[fetch_algo_id] algo_id_tp found via API: {self.algo_id_tp}")
                        return True
                    except ValueError as e:
                        self.logger.error(f"[fetch_algo_id] Error converting tpTriggerPx: {e}")
            return False

        for attempt in range(max_attempts):
            try:
                if await try_fetch_api():
                    return True
                await asyncio.sleep(delay)
            except Exception as e:
                self.logger.error(f"[fetch_algo_id] Attempt {attempt + 1}: Error: {e}")
                await asyncio.sleep(delay)

        self.logger.warning(f"[fetch_algo_id] algo_id_tp not found after {max_attempts} attempts")
        return False

    async def open_position(self, sig: Signal, balance: float) -> dict:
        """
        Opens a new position based on signal.
        """
        if sig.timeframe != self.entry_stage['tf']:
            self.logger.info(
                f"[open_position] Signal {sig.symbol} rejected: timeframe {TRADING_STAGES[0]['tf']} required")
            await send_to_all_async(
                f"⚠️ Signal {sig.symbol} rejected: timeframe {TRADING_STAGES[0]['tf']} required")
            return {"status": "error", "message": "Timeframe 3m required"}

        if float(balance) < float(MIN_BALANCE) or float(balance) < float(0.1 + COMMISSION):
            self.logger.error(f"[open_position] Insufficient balance: {balance} < {MIN_BALANCE}")
            await send_to_all_async(f"❌ Insufficient balance: {balance} < {MIN_BALANCE}")
            return {"status": "error", "message": "Insufficient funds"}

        try:
            price = await self.get_price_with_fallback(sig.symbol)
            if not price:
                raise ValueError("Price not retrieved from cache")
            self.logger.info(f"[open_position] Current price for {sig.symbol}: {price}")
        except Exception as e:
            self.logger.error(f"[open_position] Error getting price: {e}")
            await send_to_all_async(f"❌ Error getting price: {e}")
            return {"status": "error", "message": f"Error getting price: {e}"}

        try:
            ct_val = float(self.okx.info_for_futures_instrument.get('ctVal'))
            lot_size = float(self.okx.info_for_futures_instrument.get('lotSz'))
            min_sz = float(self.okx.info_for_futures_instrument.get('minSz'))
            leverage = float(self.leverage)
            po_percent = TRADING_STAGES[0]['po_percent'][sig.signal]
            usdt_amount = round(float(balance) * (po_percent / 100.0), 4)
            if usdt_amount < MIN_BALANCE:
                raise ValueError(f"Position amount {usdt_amount} less than min {MIN_BALANCE}")
            notional = usdt_amount * leverage
            cont = max(round((notional / (price * ct_val)) / lot_size) * lot_size, min_sz)
            cont = self.round_to_lot_size(cont)
            if cont < min_sz:
                raise ValueError(f"Volume {cont} less than min {min_sz}")
            self.logger.info(f"[open_position] Volume: {cont}, Price: {price}, Amount: {usdt_amount}")
        except Exception as e:
            self.logger.error(f"[open_position] Volume calculation error: {e}")
            await send_to_all_async(f"❌ [open_position] Volume calculation error: {e}")
            return {"status": "error", "message": f"Volume calculation error: {e}"}

        side = "buy" if sig.signal == "long" else "sell"
        pos_side = sig.signal

        try:
            max_attempts = TP_MAX_ATTEMPTS
            tp_multiplier = TP_MULTIPLIER
            retry_delay = TP_RETRY_DELAY
            tp_percent = float(self.entry_stage['tp'])

            clOrdId = uuid.uuid4().hex[:32]
            order_params = {
                "instId": self.pair_futures,
                "tdMode": "cross",
                "side": side,
                "posSide": pos_side,
                "ordType": "market",
                "sz": cont,
                "clOrdId": clOrdId
            }
            await send_to_all_async(
                f"📤 [open_position] Sending market order for {sig.symbol}: {order_params}")
            order = await self.okx.create_order(**order_params)
            await send_to_all_async(f"📥 [open_position] OKX Response: {order}")

            if not isinstance(order, dict) or (
                    order.get('sCode') not in (None, '0') and order.get('code') not in (None, '0')):
                self.logger.error(f"[open_position] Market order error: {order}")
                await send_to_all_async(f"❌ [open_position] Market order error: {order}")
                return {"status": "error", "message": f"Market order error: {order}"}

            order_data = (order.get('data') or [{}])[0]
            new_algo_id = order_data.get('algoId') or order_data.get('algoIdStr') or None
            if new_algo_id:
                self.algo_id_tp = new_algo_id
                await self.cache.update_order(self.pair_futures.replace("-USDT-SWAP", ""), pos_side, {
                    "algoId": new_algo_id,
                    "instId": self.pair_futures,
                    "posSide": pos_side,
                    "tpTriggerPx": None,
                    "state": "live",
                    "reduceOnly": "true"
                })
                self.logger.info(f"[open_position] algoId extracted: {new_algo_id}")

            placed_tp = bool(self.algo_id_tp)
            last_algo_response = None

            for attempt in range(1, max_attempts + 1):
                if placed_tp:
                    break

                take_price, _ = await self.calculate_level(price, pos_side,
                                                           take_profit_percent=tp_percent,
                                                           stop_loss_percent=None)
                if take_price is None:
                    await send_to_all_async(
                        f"⚠️ [open_position] Attempt {attempt}: Failed to calc TP for {tp_percent}%")
                    tp_percent *= tp_multiplier
                    await asyncio.sleep(retry_delay)
                    continue

                new_tp_params = {
                    "instId": self.pair_futures,
                    "tdMode": "cross",
                    "side": "sell" if pos_side == "long" else "buy",
                    "ordType": "conditional",
                    "sz": str(cont),
                    "posSide": pos_side,
                    "tpTriggerPx": str(take_price),
                    "tpOrdPx": str(take_price),
                }
                await send_to_all_async(
                    f"📤 [open_position] Attempt {attempt}/{max_attempts} creating TP: {new_tp_params}")
                resp = await self.okx.create_algo_order(**new_tp_params)
                await send_to_all_async(f"📥 [open_position] Response (attempt {attempt}): {resp}")
                last_algo_response = resp

                if isinstance(resp, dict) and (resp.get('sCode') == '0' or resp.get('code') == '0'):
                    got = await self.fetch_algo_id(pos_side)
                    if got:
                        placed_tp = True
                        self.logger.info(f"[open_position] TP created and algoId fetched (attempt {attempt})")
                        break
                    else:
                        await send_to_all_async(f"⚠️ [open_position] Attempt {attempt}: TP created but algoId not found yet")
                else:
                    error_message = resp.get('sMsg', str(resp)) if isinstance(resp, dict) else str(resp)
                    await send_to_all_async(f"❌ [open_position] Attempt {attempt}: create_algo_order error: {error_message}")

                tp_percent *= tp_multiplier
                await asyncio.sleep(retry_delay)

            if not placed_tp:
                await send_to_all_async(
                    f"❌ [open_position] Failed to set TP for {sig.symbol} after {max_attempts} attempts. Last resp: {last_algo_response}")
                self.logger.error(f"[open_position] TP not set after {max_attempts} attempts for {sig.symbol}")

            self.logger.info(f"[open_position] Position {pos_side} opened: {sig.symbol}, Contracts: {cont}")
            await send_to_all_async(f"📈 Position {pos_side} "
                                    f"\nOpened: {sig.symbol} "
                                    f"\nContracts: {cont} "
                                    f"\nAmount (PO): {usdt_amount:.4f} USDT"
                                    f"\nNotional: {notional:.4f} USDT")

            max_attempts = 5
            for attempt in range(max_attempts):
                try:
                    _, _, avg_price, _, position_size = await self.get_pnl_info()
                    if avg_price is None or avg_price == 0 or position_size is None or position_size == 0:
                        self.logger.debug(
                            f"[open_position] Attempt {attempt + 1}: Invalid pos data (avg_price={avg_price}, size={position_size})")
                        if attempt < max_attempts - 1:
                            await asyncio.sleep(0.5)
                        continue
                    success = await self.move_take_profit(avg_price, position_size, self.entry_stage['tp'], pos_side)
                    if success:
                        self.logger.info(f"[open_position] TP adjusted to {self.entry_stage['tp']}%")
                        await send_to_all_async(f"✅ TP for {sig.symbol} adjusted to {self.entry_stage['tp']}%")
                        return {
                            "status": "success",
                            "message": f"Position {pos_side} opened for {sig.symbol}",
                            "entry_price": avg_price,
                            "amount": cont,
                            "usdt_amount": usdt_amount
                        }
                    self.logger.warning(f"[open_position] Failed to adjust TP to {self.entry_stage['tp']}%")
                    await send_to_all_async(f"⚠️ Failed to adjust TP for {sig.symbol} to {self.entry_stage['tp']}% ")
                    return {
                        "status": "partial_success",
                        "message": f"Position {pos_side} opened, but TP not adjusted",
                        "entry_price": avg_price,
                        "amount": cont
                    }
                except Exception as e:
                    self.logger.error(f"[open_position] Attempt {attempt + 1}: TP adjustment error: {e}")
                    if attempt < max_attempts - 1:
                        await asyncio.sleep(0.5)

            self.logger.error(f"[open_position] Failed to adjust TP after {max_attempts} attempts")
            await send_to_all_async(f"❌ Failed to adjust TP for {sig.symbol}")
            return {
                "status": "partial_success",
                "message": f"Position {pos_side} opened, but TP not adjusted",
                "entry_price": price,
                "amount": cont
            }

        except Exception as e:
            self.logger.error(f"[open_position] Order creation error: {e}")
            await send_to_all_async(f"❌ Order creation error: {e}")
            return {"status": "error", "message": f"Order creation error: {e}"}

    async def cancel_limit_take_profit_orders(self, pos_side: str) -> bool:
        """
        Checks and cancels limit TP orders.
        """
        try:
            self.logger.info(
                f"[cancel_limit_take_profit_orders] Checking limit TP orders for {self.pair_futures}, pos_side={pos_side}")
            await send_to_all_async(
                f"ℹ️ [cancel_limit_take_profit_orders] Checking limit TP orders for {self.pair_futures} ({pos_side})")

            pending_orders = await self.okx.get_pending_trade_orders(instId=self.pair_futures)
            
            if not isinstance(pending_orders, dict) or pending_orders.get("status") != "success":
                self.logger.error(f"[cancel_limit_take_profit_orders] Invalid API response: {pending_orders}")
                await send_to_all_async(f"❌ [cancel_limit_take_profit_orders] Invalid API response: {pending_orders}")
                return False

            side = "buy" if pos_side == "short" else "sell"
            tp_orders = [
                order for order in pending_orders.get("data", [])
                if order.get("instId") == self.pair_futures
                   and order.get("isTpLimit") in ("true", True)
                   and order.get("ordType") == "limit"
                   and order.get("state") == "live"
                   and order.get("reduceOnly") in ("true", True)
                   and order.get("side") == side
            ]

            self.logger.info(f"[cancel_limit_take_profit_orders] Found {len(tp_orders)} limit TP orders")
            
            for order in tp_orders:
                ord_id = order.get("ordId")
                cancel_params = {"instId": self.pair_futures, "ordId": ord_id}
                try:
                    cancel_response = await self.okx.cancel_order(**cancel_params)
                    self.logger.debug(f"[cancel_limit_take_profit_orders] Cancel response: {cancel_response}")
                    
                    if isinstance(cancel_response, dict) and cancel_response.get("code") == "0":
                        self.logger.info(f"[cancel_limit_take_profit_orders] TP order {ord_id} cancelled")
                    else:
                        self.logger.error(
                            f"[cancel_limit_take_profit_orders] Error cancelling {ord_id}: {cancel_response}")
                        return False
                except Exception as e:
                    self.logger.error(f"[cancel_limit_take_profit_orders] Exception cancelling {ord_id}: {e}")
                    return False

            return True
        except Exception as e:
            self.logger.error(f"[cancel_limit_take_profit_orders] General error: {e}")
            return False

    async def update_take_profit(self, price_open: float, position_size: float, take_profit_percent: float,
                                 pos_side: str) -> bool:
        """
        Updates existing TP.
        """
        if not self.algo_id_tp or not await self.fetch_algo_id(pos_side):
            self.logger.error(f"[update_take_profit] algo_id_tp missing for {self.pair_futures}")
            return False

        position_size = self.round_to_lot_size(position_size)

        if not pos_side or pos_side not in ["long", "short"]:
            return False

        max_attempts = getattr(self, 'TP_MAX_ATTEMPTS', 5)
        for attempt in range(1, max_attempts + 1):
            try:
                take_price, _ = await self.calculate_level(price_open, pos_side, take_profit_percent=take_profit_percent, stop_loss_percent=None)
                if take_price is None:
                    if attempt < max_attempts: await asyncio.sleep(0.5)
                    continue

                active_orders = await self.okx.get_pending_algo_orders(ordType='conditional,oco')
                if not any(o.get('algoId') == self.algo_id_tp for o in active_orders.get('data', [])):
                    self.algo_id_tp = None
                    return False

                if self.algo_id_tp:
                    try:
                        await self.okx.cancel_algo_order(instId=self.pair_futures, algoId=self.algo_id_tp)
                    except Exception:
                        pass
                else:
                    pass

                new_tp_params = {
                    "instId": self.pair_futures,
                    "tdMode": "cross",
                    "side": "sell" if pos_side == "long" else "buy",
                    "ordType": "conditional",
                    "sz": str(position_size),
                    "posSide": pos_side,
                    "tpTriggerPx": str(take_price),
                    "tpOrdPx": str(take_price),
                }
                response = await self.okx.create_algo_order(**new_tp_params)

                if isinstance(response, dict) and (response.get('sCode') == '0' or response.get('code') == '0'):
                    self.logger.info(
                        f"[update_take_profit] TP updated for {self.pair_futures}, price: {take_price}")
                    return True

                if isinstance(response, dict) and (response.get('sCode') == '51400' or response.get('code') == '51400'):
                    self.algo_id_tp = None
                    return False

                if attempt < max_attempts: await asyncio.sleep(0.5)

            except Exception as e:
                self.logger.error(f"[update_take_profit] Attempt {attempt}: Error: {e}")
                if attempt < max_attempts: await asyncio.sleep(0.5)

        return False

    async def validate_inputs(self, price_open: float, position_size: float, take_profit_percent: float,
                              pos_side: str) -> bool:
        """Validates input parameters."""
        if not isinstance(price_open, (int, float)) or price_open <= 0:
            return False
        if not isinstance(position_size, (int, float)) or position_size <= 0:
            return False
        if not isinstance(take_profit_percent, (int, float)) or take_profit_percent <= 0:
            return False
        if not pos_side or pos_side not in ["long", "short"]:
            return False
        return True

    async def create_take_profit(self, price_open: float, position_size: float, take_profit_percent: float,
                                 pos_side: str) -> bool:
        """
        Creates new TP.
        """
        if not await self.ensure_instrument_data():
            return False

        _, _, _, _, actual_position_size = await self.get_pnl_info()
        if actual_position_size is None or actual_position_size == 0:
            return False

        min_sz = float(self.okx.info_for_futures_instrument.get('minSz', 0.01))
        position_size = self.round_to_lot_size(position_size)
        if position_size < min_sz:
            return False

        if not await self.validate_inputs(price_open, position_size, take_profit_percent, pos_side):
            return False

        take_price, _ = await self.calculate_level(price_open, pos_side, take_profit_percent=take_profit_percent, stop_loss_percent=None)
        if take_price is None or float(take_price) <= 0:
            return False

        tp_params = {
            "instId": self.pair_futures,
            "tdMode": "cross",
            "side": "sell" if pos_side == "long" else "buy",
            "ordType": "conditional",
            "sz": str(position_size),
            "posSide": pos_side,
            "tpTriggerPx": str(take_price),
            "tpOrdPx": str(take_price),
        }

        order = await self.okx.create_algo_order(**tp_params)

        if not isinstance(order, dict) or order.get('sCode') != '0':
            return False

        order_data = order.get('data', [{}])[0]
        new_algo_id = order_data.get('algoId')
        if new_algo_id:
            self.algo_id_tp = new_algo_id
            await self.cache.update_order(self.pair_futures.replace("-USDT-SWAP", ""), pos_side, {
                "algoId": new_algo_id,
                "instId": self.pair_futures,
                "posSide": pos_side,
                "tpTriggerPx": take_price,
                "state": "live",
                "reduceOnly": "true"
            })

        if await self.fetch_algo_id(pos_side):
            self.logger.info(
                f"[create_take_profit] TP created: price={take_price}, size={position_size:.4f}")
            await send_to_all_async(f"🟡 TP created for {self.pair_futures}, price: {take_price}")
            return True

        return False

    async def move_take_profit(self, price_open: float, position_size: float, take_profit_percent: float,
                               pos_side: str) -> bool:
        """
        Updates or creates TP.
        """
        if not await self.ensure_instrument_data():
            return False

        _, _, _, _, actual_position_size = await self.get_pnl_info()
        if actual_position_size is None or actual_position_size == 0:
            return False

        if not await self.validate_inputs(price_open, position_size, take_profit_percent, pos_side):
            return False

        if self.algo_id_tp and await self.update_take_profit(price_open, position_size, take_profit_percent, pos_side):
            return True

        if self.algo_id_tp:
            cancel_params = {"instId": self.pair_futures, "algoId": self.algo_id_tp}
            try:
                await self.okx.cancel_algo_order(**cancel_params)
            except Exception:
                pass
            self.algo_id_tp = None

        if not await self.cancel_limit_take_profit_orders(pos_side):
            return False

        if await self.create_take_profit(price_open, position_size, take_profit_percent, pos_side):
            return True

        return False

    async def ensure_instrument_data(self) -> bool:
        """
        Checks instrument data and loads if missing.
        """
        if self.okx.info_for_futures_instrument is not None and 'tickSz' in self.okx.info_for_futures_instrument:
            return True
        for attempt in range(5):
            try:
                await self.okx.get_save_info_instrumen(
                    pair_futures=self.pair_futures,
                    category_futures='SWAP',
                    pair=self.pair_futures.replace('-SWAP', ''),
                    category="SPOT"
                )
                if self.okx.info_for_futures_instrument is not None and 'tickSz' in self.okx.info_for_futures_instrument:
                    return True
            except Exception as e:
                await asyncio.sleep(1)
        return False

    async def average_position(self, sig: Signal, pos: dict, balance: float) -> dict:
        """
        Averages existing position.
        """
        current_step = pos["averages"] + 1
        self.logger.info(f"[average_position] Averaging: {sig.symbol}, step: {current_step}")

        if not await self.ensure_instrument_data():
            return {"status": "error", "message": "Instrument data unavailable"}

        if not self.averaging_stages:
            return {"status": "error", "message": "Averaging stages empty"}

        if pos["averages"] >= len(self.averaging_stages):
            return {"status": "error", "message": "All steps completed"}

        stage = self.averaging_stages[current_step - 1]
        multiplier = float(stage['multiplier'])
        initial_po = pos.get("initial_po", 0.0)
        usdt_amount = round(initial_po * multiplier, 4)

        if not usdt_amount or not multiplier:
            return {"status": "error", "message": "Step not found"}

        usdt_amount = round(initial_po * multiplier, 4)
        if balance < usdt_amount:
            return {"status": "error", "message": f"Insufficient funds ({usdt_amount} USDT)"}

        tp_error = await self.cache.get_tp_error(sig.symbol, sig.signal)
        if tp_error:
            return {"status": "error", "message": "Blocked due to TP error"}

        return await self.average_main_position(sig, pos, balance, usdt_amount, current_step)

    async def average_main_position(self, sig: Signal, pos: dict, balance: float, usdt_amount: float,
                                    current_step: int) -> dict:
        """
        Executes main position averaging.
        """
        if not await self.ensure_instrument_data():
            return {"status": "error", "message": "Instrument data unavailable"}

        try:
            price = await self.get_price_with_fallback(sig.symbol)
            if not price or price <= 0:
                raise ValueError("Price invalid")
        except Exception as e:
            return {"status": "error", "message": f"Price error: {e}"}

        try:
            min_sz = float(self.okx.info_for_futures_instrument.get('minSz', 0.01))
            ct_val = float(self.okx.info_for_futures_instrument.get('ctVal', 1.0))
            leverage = float(self.leverage)
            notional = usdt_amount * leverage
            cont = max(round((notional / (price * ct_val)) / self.lot_size) * self.lot_size, min_sz)
            cont = self.round_to_lot_size(cont)
            if cont < min_sz:
                return {"status": "error", "message": f"Volume less than min ({min_sz})"}
        except Exception as e:
            return {"status": "error", "message": f"Volume calc error: {e}"}

        side = "buy" if sig.signal == "long" else "sell"
        fill_px = None
        for attempt in range(1, 4):
            try:
                clOrdId = uuid.uuid4().hex[:32]
                order_params = {
                    "instId": self.pair_futures,
                    "tdMode": "cross",
                    "side": side,
                    "posSide": sig.signal,
                    "ordType": "market",
                    "sz": cont,
                    "clOrdId": clOrdId
                }
                await send_to_all_async(f"📤 [average_main_position] Sending order: {order_params}")

                order = await self.okx.create_order(**order_params)
                await send_to_all_async(f"📥 [average_main_position] Response: {order}")

                if order is None:
                    if attempt < 3: await asyncio.sleep(random.uniform(0.2, 0.9))
                    continue
                if not isinstance(order, dict) or order.get('sCode') != '0':
                    if attempt < 3: await asyncio.sleep(random.uniform(0.2, 0.9))
                    continue
                fill_px = float(order.get('fillPx', price))
                break
            except Exception as e:
                if attempt < 3:
                    await asyncio.sleep(random.uniform(0.2, 0.9))
                else:
                    return {"status": "error", "message": "All attempts failed"}

        if fill_px is None:
            return {"status": "error", "message": "Order execution failed"}

        max_attempts = 15
        prev_pos = pos.get("pos", 0.0)
        pos_data = None
        for i in range(max_attempts):
            pos_data = await self.cache.get_position(sig.symbol, sig.signal)
            if pos_data and pos_data.get("pos", 0.0) > prev_pos:
                break
            await asyncio.sleep(1.0)
        else:
            return {"status": "error", "message": "Cache update failed"}

        await self.cache.update_position(sig.symbol, sig.signal, {
            "avgPx": pos_data["avgPx"],
            "posSide": sig.signal,
            "pos": pos_data["pos"],
            "markPx": price,
            "upl": 0.0
        })

        await self.cache.update_averaging(sig.symbol, sig.signal, usdt_amount, fill_px)

        tp_attempts = 30
        stage_index = current_step - 1
        stage = self.averaging_stages[stage_index]
        tp_percent = stage['tp']
        max_tp_percent = 10.0
        tp_increment = 0.05

        for tp_attempt in range(tp_attempts):
            try:
                _, _, avg_price, _, position_size = await self.get_pnl_info()
                if avg_price is None or avg_price == 0 or position_size is None or position_size == 0:
                    if tp_attempt < tp_attempts - 1: await asyncio.sleep(0.2)
                    continue

                if await self.move_take_profit(avg_price, position_size, tp_percent, sig.signal):
                    await self.cache.set_tp_error(sig.symbol, sig.signal, False)
                    break

            except Exception as e:
                if tp_attempt < tp_attempts - 1:
                    tp_percent += tp_increment
                    if tp_percent > max_tp_percent: break
                    await asyncio.sleep(0.2)
                continue
        else:
            await self.cache.set_tp_error(sig.symbol, sig.signal, True)
            return {
                "status": "error",
                "message": "TP set failed",
                "avgPx": pos_data["avgPx"],
                "pos": pos_data["pos"],
                "averages": current_step,
                "usdt_amount": usdt_amount
            }

        return {
            "status": "success",
            "message": f"Position averaged for {sig.symbol}",
            "avgPx": pos_data["avgPx"],
            "pos": pos_data["pos"],
            "averages": current_step,
            "usdt_amount": usdt_amount
        }

    async def average_hedge(self, sig: Signal, pos: dict, balance: float, usdt_amount: float, current_step: int) -> dict:
        """
        Averages hedge position.
        """
        try:
            usdt_amount = float(usdt_amount)
        except Exception:
            usdt_amount = 0.0
        if usdt_amount <= 0:
            return {"status": "error", "message": "Invalid amount"}

        if not await self.ensure_instrument_data():
            return {"status": "error", "message": "Instrument data unavailable"}

        try:
            price = await self.get_price_with_fallback(sig.symbol)
            if not price or price <= 0:
                raise ValueError("Price invalid")
        except Exception as e:
            return {"status": "error", "message": f"Price error: {e}"}

        try:
            min_sz = float(self.okx.info_for_futures_instrument.get('minSz', 0.01))
            ct_val = float(self.okx.info_for_futures_instrument.get('ctVal', 1.0))
            leverage = float(self.leverage)
            notional = usdt_amount * leverage
            cont = max(round((notional / (price * ct_val)) / self.lot_size) * self.lot_size, min_sz)
            cont = self.round_to_lot_size(cont)
            if cont < min_sz:
                return {"status": "error", "message": f"Volume less than min ({min_sz})"}
        except Exception as e:
            return {"status": "error", "message": f"Volume calc error: {e}"}

        side = "buy" if sig.signal == "long" else "sell"
        fill_px = None
        for attempt in range(1, 4):
            try:
                clOrdId = uuid.uuid4().hex[:32]
                order_params = {
                    "instId": self.pair_futures,
                    "tdMode": "cross",
                    "side": side,
                    "posSide": sig.signal,
                    "ordType": "market",
                    "sz": cont,
                    "clOrdId": clOrdId
                }
                order = await self.okx.create_order(**order_params)

                if order is None:
                    if attempt < 3: await asyncio.sleep(random.uniform(0.2, 0.9))
                    continue
                if not isinstance(order, dict) or order.get('sCode') != '0':
                    if attempt < 3: await asyncio.sleep(random.uniform(0.2, 0.9))
                    continue
                fill_px = float(order.get('fillPx', price))
                break
            except Exception as e:
                if attempt < 3:
                    await asyncio.sleep(random.uniform(0.2, 0.9))
                else:
                    return {"status": "error", "message": "Attempts failed"}

        if fill_px is None:
            return {"status": "error", "message": "Order failed"}

        max_attempts = 15
        prev_pos_size = pos.get("pos", 0.0)
        pos_data = None
        for i in range(max_attempts):
            pos_data = await self.cache.get_position(sig.symbol, sig.signal)
            if pos_data and pos_data.get("pos", 0.0) > prev_pos_size:
                break
            await asyncio.sleep(1.0)
        else:
            return {"status": "error", "message": "Cache update failed"}

        await self.cache.update_position(sig.symbol, sig.signal, {
            "avgPx": pos_data["avgPx"],
            "posSide": sig.signal,
            "pos": pos_data["pos"],
            "markPx": price,
            "upl": 0.0,
            "role": "child"
        })

        await self.cache.update_averaging(sig.symbol, sig.signal, usdt_amount, fill_px)

        sl_attempts = 30
        main_pos_side = "long" if sig.signal == "short" else "short"
        
        for sl_attempt in range(sl_attempts):
            try:
                _, _, avg_price, _, total_size = await self.get_pnl_info()
                if avg_price is None or avg_price <= 0 or total_size is None or total_size <= 0:
                    await asyncio.sleep(0.2)
                    continue

                main_pos = await self.cache.get_position(sig.symbol, main_pos_side)
                main_shield = (main_pos or {}).get('shield_state', {}) if isinstance(main_pos, dict) else {}
                stop_is_move = bool(main_shield.get('stop_is_move', False))

                level_cfg = None
                if isinstance(current_step, dict) and 'sl_pct' in current_step:
                    level_cfg = current_step
                else:
                    try:
                        level_cfg = HEDGE_CONFIG['levels'][0]
                    except Exception:
                        level_cfg = {"sl_pct": 1.0, "move_sl_pct": 0.5}

                sl_percent = level_cfg.get('move_sl_pct') if stop_is_move else level_cfg.get('sl_pct')

                if sl_percent is None:
                    break

                if await self.move_stop_loss(avg_price, total_size, sl_percent, sig.signal):
                    break 
                else:
                    pass
            
            except Exception as e:
                await asyncio.sleep(SL_RETRY_DELAY)
        
        return {
            "status": "success",
            "message": f"Hedge averaged for {sig.symbol}",
            "avgPx": pos_data["avgPx"],
            "pos": pos_data["pos"],
            "averages": current_step,
            "usdt_amount": usdt_amount
        }

    async def open_hedge(self, sig: Signal, pos: float, real_money_for_hedge: Optional[float] = None, current_step: Optional[dict] = None) -> dict:
        """
        Opens hedge position.
        """
        symbol = self.pair_futures.replace("-USDT-SWAP", "")
        pos_side = sig.signal

        try:
            existing_positions = await self.cache.get_positions_by_symbol(symbol)
            if pos_side in existing_positions:
                role = existing_positions[pos_side].get('role', 'unknown')
                return {"status": "error", "message": f"Position {pos_side} exists (role: {role})"}
        except Exception as e:
            return {"status": "error", "message": f"Pos check error: {e}"}

        if pos_side not in ["long", "short"]:
            return {"status": "error", "message": "Invalid side"}

        if not isinstance(pos, (int, float)) or pos <= 0:
            return {"status": "error", "message": "Invalid volume"}

        if not await self.ensure_instrument_data():
            return {"status": "error", "message": "Instrument data unavailable"}

        try:
            min_sz = float(self.okx.info_for_futures_instrument.get('minSz', 0.01))
            cont = self.round_to_lot_size(pos)
            if cont < min_sz:
                return {"status": "error", "message": f"Volume less than min ({min_sz})"}
        except Exception as e:
            return {"status": "error", "message": f"Rounding error: {e}"}

        try:
            price = await self.get_price_with_fallback(symbol)
            if not price or price <= 0:
                raise ValueError("Price error")
        except Exception as e:
            return {"status": "error", "message": f"Price error: {e}"}

        try:
            balance = await self.cache.get_balance("availBal")
            if real_money_for_hedge is None:
                return {"status": "error", "message": "Hedge money missing"}
            if float(balance) < float(real_money_for_hedge):
                return {"status": "error", "message": f"Insufficient funds: {real_money_for_hedge}"}
        except Exception as e:
            return {"status": "error", "message": f"Balance check error: {e}"}

        side = "buy" if pos_side == "long" else "sell"

        try:
            clOrdId = uuid.uuid4().hex[:32]
            order_params = {
                "instId": self.pair_futures,
                "tdMode": "cross",
                "side": side,
                "posSide": pos_side,
                "ordType": "market",
                "sz": str(cont),
                "clOrdId": clOrdId
            }
            
            order = await self.okx.create_order(**order_params)

            if order is None or not isinstance(order, dict) or order.get('sCode') != '0':
                error_msg = order.get('sMsg', 'Unknown error') if isinstance(order, dict) else str(order)
                return {"status": "error", "message": f"OKX Error: {error_msg}"}

            max_attempts = 15
            prev_pos = await self.cache.get_position(symbol, pos_side) or {}
            prev_size = float(prev_pos.get("pos", 0.0)) if isinstance(prev_pos, dict) else 0.0
            pos_data = None
            for i in range(max_attempts):
                pos_data = await self.cache.get_position(symbol, pos_side)
                try:
                    if pos_data and float(pos_data.get("pos", 0.0)) > prev_size and float(pos_data.get("avgPx", 0.0)) > 0:
                        break
                except Exception:
                    pass
                await asyncio.sleep(1.0)

            try:
                if pos_data and float(pos_data.get("avgPx", 0.0)) > 0:
                    pos_data["role"] = "child"
                    await self.cache.update_position(symbol, pos_side, pos_data)
            except Exception:
                pass

            if not pos_data or float(pos_data.get("avgPx", 0.0)) <= 0:
                return {
                    "status": "success",
                    "message": f"Hedge order sent (no SL)."
                }

            main_pos_side = "long" if pos_side == "short" else "short"
            main_pos = await self.cache.get_position(symbol, main_pos_side)
            main_shield = (main_pos or {}).get('shield_state', {}) if isinstance(main_pos, dict) else {}
            stop_is_move = bool(main_shield.get('stop_is_move', False))

            if isinstance(current_step, dict) and 'sl_pct' in current_step:
                level_cfg = current_step
            else:
                try:
                    level_cfg = HEDGE_CONFIG['levels'][0]
                except Exception:
                    level_cfg = {"sl_pct": 1.0, "move_sl_pct": 0.5}

            sl_percent = level_cfg.get('move_sl_pct') if stop_is_move else level_cfg.get('sl_pct')

            _, _, avg_price, _, total_size = await self.get_pnl_info()
            if avg_price and total_size and sl_percent:
                await self.move_stop_loss(avg_price, total_size, sl_percent, pos_side)

            return {
                "status": "success",
                "message": f"Hedge opened."
            }

        except Exception as e:
            return {"status": "error", "message": f"Exception: {e}"}

    async def fetch_algo_id_sl(self, pos_side: str, max_attempts: int = 3, delay: float = 0.5) -> bool:
        """
        Fetches SL algo_id.
        """
        symbol = self.pair_futures.replace("-USDT-SWAP", "")

        order_data = await self.cache.get_order(symbol, pos_side)
        if order_data and order_data.get("posSide") == pos_side:
            sl_trigger_px = order_data.get("slTriggerPx", "")
            try:
                if sl_trigger_px and float(sl_trigger_px) > 0:
                    self.algo_id_sl = order_data["algoId"]
                    return True
            except (ValueError, TypeError):
                pass

        async def try_fetch_api():
            active_orders = await self.okx.get_pending_algo_orders(ordType='conditional,oco')
            if not isinstance(active_orders, dict) or active_orders.get("status") != "success":
                return False
            for order in active_orders.get('data', []):
                if (
                        order.get('instId') == self.pair_futures and
                        order.get('posSide') == pos_side and
                        order.get('slTriggerPx', '') and
                        order.get('reduceOnly') == 'true'
                ):
                    try:
                        float(order.get("slTriggerPx"))
                        self.algo_id_sl = order['algoId']
                        await self.cache.update_order(symbol, pos_side, order)
                        return True
                    except ValueError:
                        pass
            return False

        for attempt in range(max_attempts):
            try:
                if await try_fetch_api():
                    return True
                await asyncio.sleep(delay)
            except Exception:
                await asyncio.sleep(delay)

        return False

    async def create_stop_loss(self, price_open: float, position_size: float, stop_loss_percent: float,
                                 pos_side: str) -> bool:
        """
        Creates SL with retries.
        """
        sl_percent = stop_loss_percent
        for attempt in range(1, SL_MAX_ATTEMPTS + 1):
            _, stop_price = await self.calculate_level(price_open, pos_side, stop_loss_percent=sl_percent,
                                                       take_profit_percent=None)
            if not stop_price:
                sl_percent *= SL_MULTIPLIER
                await asyncio.sleep(SL_RETRY_DELAY)
                continue

            new_sl_params = {
                "instId": self.pair_futures,
                "tdMode": "cross",
                "side": "sell" if pos_side == "long" else "buy",
                "ordType": "conditional",
                "sz": str(position_size),
                "posSide": pos_side,
                "slTriggerPx": str(stop_price),
                "slOrdPx": "-1",
            }
            
            resp = await self.okx.create_algo_order(**new_sl_params)

            if isinstance(resp, dict) and (resp.get('sCode') == '0' or resp.get('code') == '0'):
                if await self.fetch_algo_id_sl(pos_side):
                    self.logger.info(f"[create_stop_loss] SL created")
                    return True
            
            sl_percent *= SL_MULTIPLIER
            await asyncio.sleep(SL_RETRY_DELAY)

        self.logger.error(f"[create_stop_loss] Failed to set SL after {SL_MAX_ATTEMPTS} attempts.")
        await send_to_all_async(f"❌ [create_stop_loss] Failed to set SL for {self.pair_futures}")
        return False

    async def move_stop_loss(self, price_open: float, position_size: float, stop_loss_percent: float,
                             pos_side: str) -> bool:
        """
        Updates SL.
        """
        try:
            await self.fetch_algo_id_sl(pos_side)
            if self.algo_id_sl:
                cancel_params = {"instId": self.pair_futures, "algoId": str(self.algo_id_sl)}
                await self.okx.cancel_algo_order(**cancel_params)
                self.algo_id_sl = None
                await self.cache.update_order(self.pair_futures.replace("-USDT-SWAP", ""), pos_side, None)

            await self.cancel_limit_stop_loss_orders(pos_side)
            
        except Exception as e:
            self.logger.error(f"[move_stop_loss] Error cancelling old SL: {e}")

        try:
            _, _, _, _, actual_position_size = await self.get_pnl_info()
            if actual_position_size is None or actual_position_size <= 0:
                return False
            
            size_to_set = self.round_to_lot_size(actual_position_size)

            created = await self.create_stop_loss(price_open, size_to_set, stop_loss_percent, pos_side)
            return created
        except Exception as e:
            self.logger.error(f"[move_stop_loss] Error creating SL: {e}")
            return False

    async def cancel_limit_stop_loss_orders(self, pos_side: str) -> bool:
        """
        Cancels limit SL orders.
        """
        try:
            pending_orders = await self.okx.get_pending_trade_orders(instId=self.pair_futures)
            
            if not isinstance(pending_orders, dict) or pending_orders.get("status") != "success":
                return False

            side = "buy" if pos_side == "short" else "sell"
            sl_orders = [
                order for order in pending_orders.get("data", [])
                if order.get("instId") == self.pair_futures
                   and order.get("isSlLimit") in ("true", True)
                   and order.get("ordType") == "limit"
                   and order.get("state") == "live"
                   and order.get("reduceOnly") in ("true", True)
                   and order.get("side") == side
            ]

            for order in sl_orders:
                ord_id = order.get("ordId")
                cancel_params = {"instId": self.pair_futures, "ordId": ord_id}
                try:
                    await self.okx.cancel_order(**cancel_params)
                except Exception:
                    return False

            return True
        except Exception:
            return False


class TraderManager:
    def __init__(self, signal_queue, cache: Cache):
        """
        Initializes TraderManager.
        :param signal_queue: Signal queue.
        :param cache: Cache instance.
        """
        self.signal_queue = signal_queue
        self.cache = cache
        self.bots = {}
        self.logger = logger
        self.logger.info(f"[__init__] TraderManager initialized")

    async def load_symbols(self) -> None:
        """
        Loads symbols and initializes bots.
        """
        try:
            okx = Okx()
            await okx.set_global_position_mode()

            with open(SYMBOLS_FILE, 'r') as f:
                symbols_raw = f.read().strip().split(',')

            semaphore_state = asyncio.Semaphore(5)
            success_symbols = []
            failed_symbols = []
            tasks = []
            max_attempts = 3

            async def initialize_bot(symbol_pos_side, bot, attempt=1):
                symbol, pos_side = symbol_pos_side
                async with semaphore_state:
                    try:
                        await bot.initialize()
                        success_symbols.append(f"{symbol} ({pos_side})")
                        self.logger.info(
                            f"[load_symbols] Bot initialized for {symbol} ({pos_side})")
                    except Exception as e:
                        if attempt < max_attempts:
                            await asyncio.sleep(random.uniform(0.5, 1.5))
                            await initialize_bot(symbol_pos_side, bot, attempt + 1)
                        else:
                            failed_symbols.append((f"{symbol} ({pos_side})", str(e)))
                            self.logger.error(
                                f"[load_symbols] Failed to init {symbol} ({pos_side}): {e}")

            for symbol in symbols_raw:
                if symbol.startswith('OKX:') and symbol.endswith('.P'):
                    clean_symbol = symbol.replace('OKX:', '').replace('.P', '').replace('USDT', '')
                    if clean_symbol:
                        pair_futures = f"{clean_symbol}-USDT-SWAP"
                        bot_long = OkxTradingBot(pair_futures=pair_futures, pos_side="long", cache=self.cache)
                        bot_short = OkxTradingBot(pair_futures=pair_futures, pos_side="short", cache=self.cache)
                        self.bots[(clean_symbol, "long")] = bot_long
                        self.bots[(clean_symbol, "short")] = bot_short
                        tasks.append(initialize_bot((clean_symbol, "long"), bot_long))
                        tasks.append(initialize_bot((clean_symbol, "short"), bot_short))

            await asyncio.gather(*tasks, return_exceptions=True)

            message = f"🟢 Bots initialized: {len(success_symbols)}"
            if failed_symbols:
                message += f"❌ Errors: {len(failed_symbols)}"
            await send_to_all_async(message)
            self.logger.info(f"[load_symbols] Result: {len(success_symbols)} success, {len(failed_symbols)} failed")
        except Exception as e:
            self.logger.error(f"[load_symbols] Error: {e}")
            await send_to_all_async(f"❌ [load_symbols] Error: {e}")

    async def _resolve_open_hedge_args(self, bot, symbol: str, command: dict):
        """Prepares args for open_hedge."""
        await bot.ensure_instrument_data()
        try:
            min_sz = float(bot.okx.info_for_futures_instrument.get('minSz', 0.0))
        except Exception:
            min_sz = 0.0
        try:
            lot_sz = float(bot.okx.info_for_futures_instrument.get('lotSz', 1.0))
        except Exception:
            lot_sz = 1.0
        try:
            ct_val = float(bot.okx.info_for_futures_instrument.get('ctVal', 1.0))
        except Exception:
            ct_val = 1.0
        lever = float(bot.leverage or 1.0)
        price = await bot.get_price_with_fallback(symbol)

        def ceil_to_lot(x: float) -> float:
            try:
                return float((Decimal(str(x)) / Decimal(str(lot_sz))).to_integral_value(ROUND_UP) * Decimal(str(lot_sz)))
            except Exception:
                return x

        pos = command.get('pos')
        delta_coef = command.get('delta_coef')
        money_arg = command.get('real_money_for_hedge')
        current_step = command.get('current_step')

        contracts = 0.0
        money = 0.0

        if isinstance(pos, (int, float)):
            try:
                contracts = float(pos)
            except Exception:
                contracts = 0.0
            contracts = bot.round_to_lot_size(contracts)
            if contracts < min_sz:
                contracts = ceil_to_lot(min_sz)
            if price > 0 and ct_val > 0 and lever > 0:
                money = (contracts * price * ct_val) / lever
            return contracts, money, current_step

        if delta_coef is not None:
            try:
                contracts, money = await bot.compute_hedge_delta(delta_coef)
            except Exception:
                contracts, money = 0.0, 0.0
            return contracts, money, current_step

        if money_arg is not None:
            try:
                money = float(money_arg)
            except Exception:
                money = 0.0
            if price > 0 and ct_val > 0 and lever > 0 and money > 0:
                notional = money * lever
                raw = notional / (price * ct_val)
                contracts = bot.round_to_lot_size(raw)
                if contracts < min_sz:
                    contracts = ceil_to_lot(min_sz)
            return contracts, money, current_step

        return 0.0, 0.0, current_step

    async def _resolve_average_hedge_args(self, bot, symbol: str, command: dict):
        """Prepares args for average_hedge."""
        current_step = command.get('current_step')
        raw_usdt = command.get('usdt_amount')
        delta_coef = command.get('delta_coef')

        try:
            if raw_usdt is not None:
                usdt_amount = float(raw_usdt)
                if usdt_amount > 0:
                    return usdt_amount, current_step
        except Exception:
            pass

        if delta_coef is not None:
            try:
                _, money = await bot.compute_hedge_delta(delta_coef)
                if money and money > 0:
                    return float(money), current_step
            except Exception:
                pass

        return 0.0, current_step

    async def process_command(self, command: dict) -> dict:
        """
        Processes trading command.
        """
        self.logger.info(f"[process_command] Command: {command}")
        symbol = command.get('symbol')
        if not symbol:
            self.logger.error(f"[process_command] Missing 'symbol'")
            return {"status": "error", "message": "Missing 'symbol'"}
        
        action = command['action']
        sig = command['signal']
        balance = command['balance']
        pos = command.get('pos', {})

        pos_side = sig.signal
        if pos_side not in ["long", "short"]:
            self.logger.error(f"[process_command] Invalid side: {pos_side}")
            return {"status": "error", "message": "Invalid side"}
        
        bot_key = (symbol, pos_side)
        if bot_key not in self.bots:
            self.bots[bot_key] = OkxTradingBot(pair_futures=f"{symbol}-USDT-SWAP", pos_side=pos_side, cache=self.cache)
            await self.bots[bot_key].initialize()
            self.logger.info(f"[process_command] Bot created for {symbol} ({pos_side})")

        bot = self.bots[bot_key]

        if action == 'open_main':
            result = await bot.open_position(sig, balance)
        elif action == 'average_main':
            result = await bot.average_position(sig, pos, balance)
        elif action == 'open_hedge':
            contracts, money, current_step = await self._resolve_open_hedge_args(bot, symbol, command)
            if contracts is None or contracts <= 0 or money is None or money <= 0:
                self.logger.error(f"[process_command] Invalid open_hedge args")
                result = {"status": "error", "message": "Invalid open_hedge args"}
            else:
                result = await bot.open_hedge(sig, contracts, money, current_step=current_step)
        elif action == 'average_hedge':
            usdt_amount, current_step = await self._resolve_average_hedge_args(bot, symbol, command)
            if usdt_amount is None or usdt_amount <= 0:
                self.logger.error(f"[process_command] Invalid usdt_amount for average_hedge")
                return {"status": "error", "message": "Invalid usdt_amount for average_hedge"}
            
            current_step_index = command.get('current_step_index')
            if current_step_index is not None and isinstance(pos, dict):
                actual_averages = pos.get("averages", -1)
                expected_averages = current_step_index - 1
                if actual_averages != expected_averages:
                    self.logger.warning(
                        f"[process_command] STATE MISMATCH {symbol} (Order skipped): ..."
                        f"expected averages={expected_averages}, got {actual_averages}")
                    return {"status": "error", "message": "Race condition: averages mismatch"}
            
            result = await bot.average_hedge(sig, pos, balance, usdt_amount, current_step)
        else:
            self.logger.error(f"[process_command] Invalid action: {action}")
            result = {"status": "error", "message": "Invalid action"}

        self.logger.info(f"[process_command] Result: {result}")
        return result