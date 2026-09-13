"""Точка входа приложения WOLTRON Voice AI."""

"""Точка входа приложения WOLTRON Voice AI."""

import asyncio
import logging
import signal
import sys

from app.agent import Agent
from app.bot import TelegramBot
from app.database import Database
from app.integrations import Integrations
from app.sip_worker import SIPWorker
from app.voice import VoiceEngine
from config import (
    DELIVERY_RETRY_INTERVAL,
    TELEGRAM_BOT_TOKEN,
    validate_config,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("main")

async def safe_telegram_runner(
    telegram_bot: TelegramBot, shutdown_event: asyncio.Event
):
    """Фоновый ранер для Телеграм-бота с авто-переподключением без падения всего сервера."""
    while not shutdown_event.is_set():
        try:
            await telegram_bot.start()
        except Exception as e:
            if shutdown_event.is_set():
                break
            logger.error(
                "Telegram bot connection failed: %s. Retrying in 5 seconds...",
                e,
            )
            await asyncio.sleep(5)

async def delivery_retry_worker(
    database: Database, integrations: Integrations, shutdown_event: asyncio.Event
):
    """Periodic retry для failed deliveries."""
    while not shutdown_event.is_set():
        try:
            pending = await database.get_pending_deliveries(limit=10)
            for item in pending:
                call_id = item["call_id"]
                try:
                    from app.models import CallResult

                    result = CallResult.from_json(item["result"])

                    if item["albato_status"] == "pending":
                        albato_ok = await integrations.send_to_albato(
                            result.to_dict()
                        )
                        await database.update_delivery_status(
                            call_id,
                            albato_status="sent" if albato_ok else "pending",
                        )

                    if item["telegram_status"] == "pending":
                        telegram_ok = await integrations.send_telegram_result(
                            result
                        )
                        await database.update_delivery_status(
                            call_id,
                            telegram_status="sent"
                            if telegram_ok else "pending",
                        )
                except Exception as e:
                    logger.error(
                        "Delivery retry error call_id=%s: %s", call_id, e
                    )
        except Exception as e:
            logger.error("Delivery retry worker error: %s", e)

        try:
            await asyncio.wait_for(
                shutdown_event.wait(), timeout=DELIVERY_RETRY_INTERVAL
            )
        except asyncio.TimeoutError:
            pass

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

    # 1. Запускаем SIP Worker
    sip_task = asyncio.create_task(sip_worker.start())

    # 2. Фоновые процессы (не валят сервер при ошибках)
    if telegram_bot:
        asyncio.create_task(
            safe_telegram_runner(telegram_bot, shutdown_event)
        )

    asyncio.create_task(
        delivery_retry_worker(database, integrations, shutdown_event)
    )

    # Мониторим ТОЛЬКО критичный SIP Worker
    while not shutdown_event.is_set():
        if sip_task.done():
            try:
                sip_task.result()
            except Exception as e:
                logger.error("Critical SIPWorker task failed: %s", e)
            shutdown_event.set()
            break
        await asyncio.sleep(1)

    logger.info("Initiating graceful shutdown...")

    await sip_worker.graceful_stop()
    if telegram_bot:
        await telegram_bot.stop()
    await integrations.close()
    await database.close()

    logger.info("Shutdown complete")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Shutdown by KeyboardInterrupt")
