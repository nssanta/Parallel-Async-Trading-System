import os

# Server Settings
SERVER_HOST = "0.0.0.0"
SERVER_PORT = 8760

# Queue Settings
BUFFER_SIZE = 1000

# New ROI Hedging Configuration
HEDGE_CONFIG = {
    "enabled": True,                # Enable new system
    "price_safe_zone": 0.05,        # % PRICE CHANGE to exit safe-zone
    "under_hedge_usd": 0.01,        # $ buffer for paired closing
    "anti_spam_seconds": 2,         # Protection against frequent operations
    "retry_attempts": 3,            # Attempts on insufficient balance
    "retry_delays": [10, 30, 60],   # Delays between attempts (sec)

    # UNLIMITED number of levels
    "levels": [
        {
            "roi_pct": -0.5,             # ROI change threshold in %
            "coef": 0.5,                 # Hedge size multiplier
            "sl_pct": 1.0,               # Initial Stop Loss (%)
            "move_trigger_pct": 0.3,     # Stop move trigger (%)
            "move_sl_pct": 0.15          # Where to move Stop (%)
        },
        {
            "roi_pct": -1.5,             # ROI change threshold in %
            "coef": 0.5,                 # Hedge size multiplier
            "sl_pct": 1.5,               # Initial Stop Loss (%)
            "move_trigger_pct": 0.5,     # Stop move trigger (%)
            "move_sl_pct": 0.25          # Where to move Stop (%)
        },
        {
            "roi_pct": -3.0,             # ROI change threshold in %
            "coef": 1.0,                 # Hedge size multiplier
            "sl_pct": 2.0,               # Initial Stop Loss (%)
            "move_trigger_pct": 1.0,     # Stop move trigger (%)
            "move_sl_pct": 0.5           # Where to move Stop (%)
        }
    ],
}


# Unified Stage Structure: Step 0 - Entry, Steps 1+ - Averaging
TRADING_STAGES = [
    # Step 0: Entry (not averaging)
    {
        "step": 0,
        "tf": "3",                       # Timeframe for indicators
        "tp": 0.5,                       # Final TP after adjustment
        "po_percent": {                  # Percentage for pending order
            "long": 2.0,
            "short": 2.0
        },
        "multiplier": None,              # Volume multiplier (none for entry)
        "required_price_change": 0.0     # Required price change for averaging
    },
    # Step 1: First Averaging
    {
        "step": 1,
        "tf": "5",                       # Timeframe
        "tp": 0.4,                       # Take Profit
        "multiplier": 2.0,               # Averaging volume multiplier
        "required_price_change": 0.0001  # Required price change for averaging
    },
    # Step 2: Second Averaging
    {
        "step": 2,
        "tf": "10",                      # Timeframe
        "tp": 0.5,                       # Take Profit
        "multiplier": 4.0,               # Averaging volume multiplier
        "required_price_change": 0.02    # Required price change for averaging
    },
    # Step 3: Third Averaging
    {
        "step": 3,
        "tf": "120",                     # Timeframe
        "tp": 0.6,                       # Take Profit
        "multiplier": 10.0,              # Averaging volume multiplier
        "required_price_change": 0.1     # Required price change for averaging
    }
]

# Max Open Positions Limit
MAX_OPEN_POSITIONS = 5
# Leverage
LEVERAGE = 10

# Placeholder
STOP_LOSS = 3.0
# For Simulations
MIN_BALANCE = 0.01
COMMISSION = 0.001

# OKX Settings
API_KEY = "YOUR_API_KEY"
API_SECRET = "YOUR_SECRET_KEY"
PASSPHRASE = "YOUR_PASSPHRASE"  # Added generally needed for OKX

# TP Reschedule Multiplier on Failure
TP_MULTIPLIER = 2
# Pause during Reschedule
TP_RETRY_DELAY = 0.2
# Number of TP Set Attempts
TP_MAX_ATTEMPTS = 20

# Number of SL Set Attempts
SL_MAX_ATTEMPTS = 20
# Multiplier, same as for Take Profit
SL_MULTIPLIER = TP_MULTIPLIER
# Reschedule Pause
SL_RETRY_DELAY = 0.2

# Telegram Settings
TELEGRAM_TOKEN = "YOUR_TELEGRAM_TOKEN"
TELEGRAM_CHAT_ID = "YOUR_CHAT_ID"

# Logging
LOG_LEVEL = "INFO"
# Additional Settings
SYMBOLS_FILE = "symbols.txt"

# Max concurrent tasks for coin info requests
MAX_CONCURRENT_TASKS = 5

# Balance update interval (5 sec)
BALANCE_UPDATE_INTERVAL = 5