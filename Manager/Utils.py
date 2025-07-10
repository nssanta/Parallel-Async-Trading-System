import logging
import os
from datetime import datetime
from pydantic import BaseModel
from config import LOG_LEVEL

# Logger Setup
log_file = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'okx.log')  # Relative path to root
logger = logging.getLogger('UtilsLogger')  # Unique logger name
logger.setLevel(getattr(logging, LOG_LEVEL))  # Level from config.py (INFO)

# Create file if it doesn't exist
os.makedirs(os.path.dirname(log_file), exist_ok=True)
open(log_file, 'a', encoding='utf-8').close()

# Check to avoid adding handlers repeatedly
if not logger.handlers:
    # File handler
    file_handler = logging.FileHandler(log_file, 'a', 'utf-8')
    file_handler.setLevel(getattr(logging, LOG_LEVEL))
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    # Stream handler for console
    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(getattr(logging, LOG_LEVEL))
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)


class Signal(BaseModel):
    symbol: str
    timeframe: str
    signal: str
    timestamp: int

    def is_expired(self, max_age: int) -> bool:
        current_time = int(datetime.now().timestamp() * 1000)
        age_ms = current_time - self.timestamp
        return age_ms > max_age * 60 * 1000
