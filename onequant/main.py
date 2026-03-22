import asyncio
import signal
import sys
from database.db import init_db
from feeds import coinbase, kalshi, news


async def main():
    init_db()
    print("oneQuant data pipeline started")
    print("  - Coinbase WebSocket: BTC-USD ticker")
    print("  - Kalshi poller: every 60s")
    print("  - News + Fear & Greed: every 15m")
    print("Press CTRL+C to stop.\n")

    tasks = [
        asyncio.create_task(coinbase.run(), name="coinbase"),
        asyncio.create_task(kalshi.run(), name="kalshi"),
        asyncio.create_task(news.run(), name="news"),
    ]

    try:
        await asyncio.gather(*tasks)
    except asyncio.CancelledError:
        pass


def shutdown(loop: asyncio.AbstractEventLoop):
    print("\nShutting down...")
    for task in asyncio.all_tasks(loop):
        task.cancel()


if __name__ == "__main__":
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    if sys.platform != "win32":
        loop.add_signal_handler(signal.SIGINT, lambda: shutdown(loop))
        loop.add_signal_handler(signal.SIGTERM, lambda: shutdown(loop))

    try:
        loop.run_until_complete(main())
    except KeyboardInterrupt:
        shutdown(loop)
        loop.run_until_complete(asyncio.gather(*asyncio.all_tasks(loop), return_exceptions=True))
    finally:
        loop.close()
        print("oneQuant stopped.")
