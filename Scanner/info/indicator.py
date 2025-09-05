import pandas as pd
import numpy as np

def ema(series: pd.Series, period: int) -> pd.Series:
    """
    Calculate Exponential Moving Average.
    :param series: Input series.
    :param period: EMA period.
    :return: EMA series.
    """
    return series.ewm(span=period, adjust=False).mean()

def compute_indicator(
    df: pd.DataFrame,
    macd_fast: int = 12,
    macd_slow: int = 26,
    macd_signal: int = 9,
    **kwargs
) -> pd.DataFrame:
    """
    Calculates MACD indicator and generating signals.
    
    :param df: DataFrame with 'Close' column.
    :param macd_fast: Fast EMA period.
    :param macd_slow: Slow EMA period.
    :param macd_signal: Signal EMA period.
    :return: DataFrame with added 'buy_signal' and 'sell_signal' columns.
    """
    df = df.copy()
    
    # Validate required columns
    if 'Close' not in df.columns:
        raise ValueError("DataFrame must contain 'Close' column")

    close = df['Close']

    # Calculate MACD
    ema_fast = ema(close, macd_fast)
    ema_slow = ema(close, macd_slow)
    macd_line = ema_fast - ema_slow
    signal_line = ema(macd_line, macd_signal)
    
    df['macd_line'] = macd_line
    df['signal_line'] = signal_line
    df['hist'] = macd_line - signal_line

    # Generate Signals (Crossover)
    # Buy: MACD crosses above Signal
    # Sell: MACD crosses below Signal
    
    # Shift to avoid look-ahead bias: signal is generated when the crossover happens at the close of the bar
    prev_macd = macd_line.shift(1)
    prev_signal = signal_line.shift(1)
    
    buy_condition = (prev_macd <= prev_signal) & (macd_line > signal_line)
    sell_condition = (prev_macd >= prev_signal) & (macd_line < signal_line)
    
    df['buy_signal'] = buy_condition
    df['sell_signal'] = sell_condition
    
    return df
