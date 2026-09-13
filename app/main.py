"""Точка входа приложения WOLTRON Voice AI."""

import asyncio
import logging
import signal
import sys

from app.sip_worker import SIPWorker
from app.voice import VoiceEngine
from app.integrations import Integrations
from app.database import Database
from app.agent import Agent
from app.bot import TelegramBot
from config import validate_config, TELEGRAM_BOT_TOKEN, DELIVERY_RETRY_INTERVAL

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("main")


async def delivery_retry_worker(database: Database, integrations: Integrations):
    """Periodic retry для failed deliveries."""
    while True:
        try:
            pending = await database.get_pending_deliveries(limit=10)
            for item in pending:
                call_id = item["call_id"]
                try:
                    from app.models import CallResult
                    result = CallResult.from_json(item["result"])
                    
                    if item["albato_status"] == "pending":
                        albato_ok = await integrations.send_to_albato(result.to_dict())
                        await database.update_delivery_status(
                            call_id,
                            albato_status="sent" if albato_ok else "pending",
                        )
                    
                    if item["telegram_status"] == "pending":
                        telegram_ok = await integrations.send_telegram_result(result)
                        await database.update_delivery_status(
                            call_id,
                            telegram_status="sent" if telegram_ok else "pending",
                        )
                except Exception as e:
                    logger.error("Delivery retry error call_id=%s: %s", call_id, e)
        except Exception as e:
            logger.error("Delivery retry worker error: %s", e)
        await asyncio.sleep(DELIVERY_RETRY_INTERVAL)


async def main():
    logger.info("Starting WOLTRON Voice AI...")

    if not validate_config():
        logger.error("Configuration validation failed")
        sys.exit(1)

    database = Database()
    try:
        await database.init()
    except Exception as e:
        logger.error("Database init failed: %s", e)
        sys.exit(1)

    integrations = Integrations()
    agent = Agent(integrations, database)
    voice_engine = VoiceEngine(integrations, database, agent)
    sip_worker = SIPWorker(voice_engine, database, integrations)

    telegram_bot = None
    if TELEGRAM_BOT_TOKEN:
        telegram_bot = TelegramBot(sip_worker)
    else:
        logger.warning("TELEGRAM_BOT_TOKEN not set, Telegram bot disabled")

    loop = asyncio.get_event_loop()
    shutdown_event = asyncio.Event()

    def handle_signal(sig):
        logger.info("Received signal %s, initiating shutdown...", sig.name)
        shutdown_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, handle_signal, sig)

    tasks = []
    sip_task = asyncio.create_task(sip_worker.start())
    tasks.append(sip_task)
    
    if telegram_bot:
        telegram_task = asyncio.create_task(telegram_bot.start())
        tasks.append(telegram_task)
    
    delivery_task = asyncio.create_task(delivery_retry_worker(database, integrations))
    tasks.append(delivery_task)

    async def monitor_tasks():
        while not shutdown_event.is_set():
            for i, task in enumerate(tasks):
                if task.done() and not shutdown_event.is_set():
                    try:
                        task.result()
                    except Exception as e:
                        logger.error("Critical task %d failed: %s, initiating shutdown", i, e)
                        shutdown_event.set()
                        return
            await asyncio.sleep(1)
    
    monitor_task = asyncio.create_task(monitor_tasks())
    tasks.append(monitor_task)

    await shutdown_event.wait()
    logger.info("Initiating graceful shutdown...")

    await sip_worker.graceful_stop()
    if telegram_bot:
        await telegram_bot.stop()
    await integrations.close()
    await database.close()

    for task in tasks:
        if not task.done():
            task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)

    logger.info("Shutdown complete")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Shutdown by KeyboardInterrupt")
