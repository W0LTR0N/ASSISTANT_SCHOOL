import sys
from pathlib import Path

# Гарантируем, что Python видит все модули в текущей папке и подпапках
BASE_DIR = Path(__file__).resolve().parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

import asyncio
import logging
from fastapi import FastAPI
import uvicorn

import config
from database import Database
from bot import TelegramBot
from sip_worker import SIPWorker

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger(__name__)

app = FastAPI(title="WOLTRON Voice AI Service")

@app.get("/health")
async def health_check():
    return {"status": "ok", "service": "WOLTRON Voice AI"}

async def run_fastapi():
    # Безопасно получаем PORT из любого вида config.py
    config_obj = getattr(config, 'config', getattr(config, 'Config', config))
    port = getattr(config_obj, 'PORT', 8000)
    server_config = uvicorn.Config(app=app, host="0.0.0.0", port=port, log_level="info")
    server = uvicorn.Server(server_config)
    await server.serve()

async def main():
    logger.info("Starting WOLTRON Voice AI System...")
   
    db = Database()
   
    # Автоматически вызываем нужный метод инициализации БД, если он есть
    for method_name in ['init_db', 'init', 'create_tables', 'connect']:
        if hasattr(db, method_name):
            method = getattr(db, method_name)
            if asyncio.iscoroutinefunction(method):
                await method()
            else:
                method()
            break

    sip_worker = SIPWorker()
    bot = TelegramBot(sip_worker)

    await asyncio.gather(
        run_fastapi(),
        bot.start(),
        sip_worker.start(),
    )

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logger.info("System stopped gracefully.")
