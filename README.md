# Parallel-Async-Trading-System

<p align="center">
  <strong>Enterprise-Grade Multi-Process Trading System for OKX Futures</strong>
</p>

<p align="center">
  <code>Python 3.10+</code> · <code>asyncio</code> · <code>multiprocessing</code> · <code>uvloop</code> · <code>FastAPI</code> · <code>WebSockets</code>
</p>

---

## Architecture Overview

This is a **production-ready, multi-process trading cluster** designed for sub-second execution on OKX Futures. The system employs a distributed architecture where each critical component runs in an **isolated process**, communicating via IPC (Inter-Process Communication) queues and HTTP.

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              TRADING CLUSTER                                │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  ┌─────────────────┐    Queue     ┌──────────────────────────────────────┐ │
│  │   GATEWAY       │─────────────▶│          BOT MASTER                  │ │
│  │   (FastAPI)     │              │     (asyncio Event Loop)             │ │
│  │                 │              │                                      │ │
│  │  - Webhook API  │              │  ┌────────────┐  ┌────────────────┐  │ │
│  │  - uvloop       │              │  │  Signal    │  │  Hedge         │  │ │
│  │  - Signal Queue │              │  │  Manager   │  │  Manager       │  │ │
│  └─────────────────┘              │  └────────────┘  └────────────────┘  │ │
│                                   │         │               │            │ │
│  ┌─────────────────┐              │  ┌──────▼───────────────▼──────────┐ │ │
│  │   SCANNER       │              │  │       STATE MANAGER             │ │ │
│  │   (Pandas/Numpy)│──────HTTP───▶│  │   (state.json persistence)     │ │ │
│  │                 │              │  └──────────────┬──────────────────┘ │ │
│  │  - Indicators   │              │                 │                    │ │
│  │  - Binance WS   │              │  ┌──────────────▼──────────────────┐ │ │
│  └─────────────────┘              │  │           CACHE                 │ │ │
│                                   │  │  (async in-memory storage)      │ │ │
│  ┌─────────────────┐              │  └──────────────┬──────────────────┘ │ │
│  │   TELEGRAM      │◀─────────────│                 │                    │ │
│  │   (Notifications)│             │  ┌──────────────▼──────────────────┐ │ │
│  └─────────────────┘              │  │       TRADER LAYER              │ │ │
│                                   │  │  - OkxWebSocketClient (RT)      │ │ │
│                                   │  │  - OkxTradingBot (Execution)    │ │ │
│                                   │  │  - OKX REST API                 │ │ │
│                                   │  └─────────────────────────────────┘ │ │
│                                   └──────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Process Isolation

| Process | Role | Technology |
|---------|------|------------|
| **Gateway** | Webhook receiver, signal ingestion | FastAPI, uvicorn, uvloop |
| **Bot Master** | Trading orchestration, position management | asyncio, aiohttp, WebSockets |
| **Scanner** | Heavy indicator calculations | Pandas, NumPy, Binance WebSocket |
| **Telegram** | User notifications | python-telegram-bot |

The separation ensures that **CPU-intensive indicator calculations** in Scanner never block the **latency-critical trading logic** in Bot Master.

---

## Deep Dive: Concurrency Model

### Event Loop Architecture

The Bot Master runs a single **asyncio event loop** (accelerated by `uvloop` on Linux). All I/O operations are non-blocking, enabling the system to handle hundreds of concurrent operations:

```python
# Simplified flow
async def main():
    ws_task = asyncio.create_task(ws_client.start())       # WebSocket streams
    signals_task = asyncio.create_task(manager.process())  # Signal processing
    balance_task = asyncio.create_task(update_balance())   # Background updates
    await asyncio.gather(ws_task, signals_task, balance_task)
```

### Per-Symbol Task Isolation

For **each active position**, the system spawns **independent async tasks**:

```
┌─────────────────────────────────────────────────────────┐
│                    SYMBOL: BTC                          │
│  ┌─────────────────┐  ┌─────────────────────────────┐  │
│  │  monitor_task   │  │     hedge_supervisor        │  │
│  │  (Position WS)  │  │  (ROI checks, SL/TP mgmt)   │  │
│  └─────────────────┘  └─────────────────────────────┘  │
└─────────────────────────────────────────────────────────┘
┌─────────────────────────────────────────────────────────┐
│                    SYMBOL: ETH                          │
│  ┌─────────────────┐  ┌─────────────────────────────┐  │
│  │  monitor_task   │  │     hedge_supervisor        │  │
│  └─────────────────┘  └─────────────────────────────┘  │
└─────────────────────────────────────────────────────────┘
```

Each task operates with its own **asyncio.Lock** (`symbol_locks[symbol]`), preventing race conditions when multiple signals arrive for the same instrument.

### Hedge Manager: Parallel Position Control

The **HedgeManager** runs a persistent supervisor coroutine for each main position. It:

1. Monitors main position ROI via WebSocket data pushed to Cache
2. Opens hedge positions when ROI crosses configured thresholds
3. Manages hedge lifecycle independently (Stop Loss trailing, paired closure)
4. Implements **Safe Zone Logic** to prevent re-entry whipsaws

```
Main Position (LONG BTC)
    │
    ├── ROI drops below -0.5% → Open Hedge (SHORT, 0.5x)
    │                               │
    │                               ├── SL hit → Record close price
    │                               │           Wait for safe zone exit
    │                               │
    │                               └── Price exits safe zone → Reopen eligible
    │
    └── ROI drops below -1.5% → Scale hedge (add 0.5x)
```

---

## Unique Trading Mechanisms

### 1. REST/WebSocket Synchronization

The system addresses exchange-level timing inconsistencies between REST API responses and WebSocket data streams:

**Challenge**: When placing an order via REST API, the exchange confirms immediately. However, the WebSocket position stream updates asynchronously, typically within 50-500ms.

**Solution**: The system implements synchronization delays to ensure Cache consistency before executing dependent operations. This architecture prevents:
- Duplicate position entries
- Incorrect averaging calculations  
- Missed Take Profit assignments

This pattern is critical for maintaining state integrity in high-frequency trading environments where multiple data sources must converge.

### 2. Optimistic Locking (Race Condition Prevention)

The system implements **state mismatch checks** to prevent duplicate orders during concurrent signal processing:

```python
# Before opening a position
if (symbol, sig.signal) in open_positions:
    logger.info("Position already exists, rejecting signal")
    return
```

**Flow**:
1. Signal arrives for BTC LONG
2. Lock acquired: `async with symbol_locks["BTC"]`
3. Check Cache for existing BTC position
4. If none → Execute entry
5. If exists → Reject signal (or process as averaging)

This prevents the classic HFT bug where two signals for the same symbol race to open duplicate positions.

### 3. Signal Priority & Queue Management

The Gateway (`Agregator/Server.py`) implements a **bounded queue with displacement**:

```python
try:
    signal_queue.put_nowait(sig)
except queue.Full:
    old = signal_queue.get()  # Displace oldest signal
    signal_queue.put_nowait(sig)
```

This ensures:
- The system never blocks on signal ingestion
- Newer signals take priority over stale ones during overload
- Memory remains bounded regardless of signal flood

---

## Tech Stack

| Component | Technology | Purpose |
|-----------|------------|---------|
| **Core** | Python 3.10+ | Type hints, asyncio improvements |
| **Async Engine** | asyncio + uvloop | High-performance event loop |
| **Process Management** | multiprocessing | Process isolation |
| **Web Framework** | FastAPI + uvicorn | Async webhook server |
| **HTTP Client** | httpx (async) | Non-blocking REST calls |
| **WebSockets** | websockets library | Real-time exchange data |
| **Exchange API** | Custom OKX wrapper | REST + WS integration |
| **Data Processing** | Pandas, NumPy | Indicator calculations |
| **State Persistence** | JSON files | Crash recovery |
| **Notifications** | python-telegram-bot | User alerts |

---

## Configuration

All configuration is centralized in `config.py`:

### Trading Stages (DCA Strategy)

```python
TRADING_STAGES = [
    {"step": 0, "tf": "3",   "tp": 0.5, "multiplier": None},      # Entry
    {"step": 1, "tf": "5",   "tp": 0.4, "multiplier": 2.0},       # 1st Average
    {"step": 2, "tf": "10",  "tp": 0.5, "multiplier": 4.0},       # 2nd Average
    {"step": 3, "tf": "120", "tp": 0.6, "multiplier": 10.0},      # 3rd Average
]
```

### Hedge Configuration

```python
HEDGE_CONFIG = {
    "enabled": True,
    "price_safe_zone": 0.05,        # % to exit before re-hedge
    "levels": [
        {"roi_pct": -0.5, "coef": 0.5, "sl_pct": 1.0},   # First hedge level
        {"roi_pct": -1.5, "coef": 0.5, "sl_pct": 1.5},   # Second level
        {"roi_pct": -3.0, "coef": 1.0, "sl_pct": 2.0},   # Third level
    ]
}
```

### API Credentials

```python
API_KEY = "YOUR_API_KEY"
API_SECRET = "YOUR_SECRET_KEY"
PASSPHRASE = "YOUR_PASSPHRASE"
TELEGRAM_TOKEN = "YOUR_TELEGRAM_TOKEN"
```

> ⚠️ **Security**: Never commit real credentials. Use environment variables in production.

---

## Installation & Deployment

### Local Development

```bash
# Clone repository
git clone <repository-url>
cd FomoDen_v1

# Create virtual environment
python -m venv venv
source venv/bin/activate  # Linux/Mac
# or: venv\Scripts\activate  # Windows

# Install dependencies
pip install -r requirements.txt

# Configure
cp config.py.example config.py
# Edit config.py with your credentials

# Run
python main.py
```

### Docker Deployment

```dockerfile
# Dockerfile
FROM python:3.10-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["python", "main.py"]
```

```yaml
# docker-compose.yml
version: '3.8'

services:
  trading-cluster:
    build: .
    restart: unless-stopped
    ports:
      - "8760:8760"  # Webhook port
    volumes:
      - ./state.json:/app/state.json  # Persist state
      - ./logs:/app/logs
    environment:
      - API_KEY=${API_KEY}
      - API_SECRET=${API_SECRET}
      - PASSPHRASE=${PASSPHRASE}
      - TELEGRAM_TOKEN=${TELEGRAM_TOKEN}
```

```bash
# Start with Docker Compose
docker-compose up -d

# View logs
docker-compose logs -f

# Stop
docker-compose down
```

---

## File Structure

```
FomoDen_v1/
├── main.py                    # Entry point, process orchestration
├── config.py                  # All configuration parameters
├── requirements.txt           # Python dependencies
├── state.json                 # Persistent trade state (auto-generated)
├── subscribers.json           # Telegram subscriber list
│
├── Agregator/
│   └── Server.py              # FastAPI webhook gateway
│
├── Manager/
│   ├── BotMaster.py           # Main orchestrator
│   ├── SignalManager.py       # Signal processing logic
│   ├── HedgeManager.py        # Hedging strategy
│   ├── StateManager.py        # State persistence
│   ├── Cache.py               # In-memory async cache
│   └── Utils.py               # Signal model, logger
│
├── Trader/
│   ├── OKX.py                 # OKX REST API wrapper
│   ├── OkxTradingBot.py       # Trade execution logic
│   ├── OkxWebSocketClient.py  # Real-time WebSocket handler
│   └── Decimal_Numb.py        # Precision utilities
│
├── Scanner/
│   ├── scanner_app.py         # Indicator scanner
│   ├── kline_fetcher.py       # Binance kline fetcher
│   └── info/
│       ├── indicator.py       # Technical indicator logic
│       └── binance_pairs.json # Symbol list
│
└── Telegram/
    └── UtilsTG.py             # Notification system
```

---

## Monitoring & Logs

Each component writes to its own log file:

| File | Content |
|------|---------|
| `Manager/BotMaster.log` | Process lifecycle, WebSocket status |
| `Manager/Signal.log` | Signal processing events |
| `Manager/HedgeManager.log` | Hedge operations |
| `Manager/StateManager.log` | State save/load events |
| `Manager/Cache.log` | Cache operations |
| `Trader/Trader.log` | Order execution |
| `Trader/okx.log` | API calls |
| `Scanner/logs/signals.log` | Generated signals |
| `Scanner/logs/errors.log` | Scanner errors |

---

## Risk Disclaimer

This software is provided for **educational and research purposes only**. Cryptocurrency trading involves substantial risk of loss. The authors are not responsible for any financial losses incurred through the use of this software.

---

<p align="center">
  <sub>Built with precision for high-frequency trading environments</sub>
</p>
