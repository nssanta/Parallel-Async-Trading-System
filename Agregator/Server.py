import sys
import os
import multiprocessing
import asyncio
import uvicorn
from fastapi import FastAPI, HTTPException
from Manager.Utils import Signal, logger
from config import SERVER_HOST, SERVER_PORT
import queue

# Add root directory to sys.path for correct imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def run_server(signal_queue: multiprocessing.Queue):
    """
    Starts the FastAPI server to process webhooks and pass signals to the queue.
    :param signal_queue: Multiprocessing queue for signals
    """
    # Create FastAPI application
    app = FastAPI()

    # Endpoint for receiving signals via POST requests
    @app.post("/public/webhook")
    async def webhook(sig: Signal):
        # Verify signal timestamp is valid (> 0)
        if sig.timestamp <= 0:
            raise HTTPException(status_code=400, detail="timestamp must be > 0")
        try:
            # Try to add signal to the queue without blocking
            signal_queue.put_nowait(sig)
            # Log successful signal receipt (symbol, timeframe, signal type)
            logger.info(f"Received signal: {sig.symbol} {sig.timeframe} {sig.signal}")
        except queue.Full:
            # If queue is full, remove the old signal
            old = signal_queue.get()
            # Add the new signal
            signal_queue.put_nowait(sig)
            # Log displacement of the old signal
            logger.warning(f"Queue full — displaced old signal: {old.symbol}")

        # Return status and current queue size
        return {"status": "accepted", "queued": signal_queue.qsize()}

    try:
        # If not Windows, try to use uvloop for asyncio acceleration
        if sys.platform != "win32":
            try:
                import uvloop
                asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
                logger.info("Using uvloop event loop")
            except ImportError:
                logger.warning("uvloop not installed, using standard asyncio loop")

        # Start server on specified host and port (from config.py)
        uvicorn.run(app, host=SERVER_HOST, port=SERVER_PORT, workers=1)
    except Exception as e:
        # Log error and exit process if server fails to start
        logger.error(f"Server start error: {e}")
        sys.exit(1)
