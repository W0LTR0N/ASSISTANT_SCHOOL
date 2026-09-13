import sys
from pathlib import Path

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

# Пытаемся импортировать голосовой движок, если он у тебя вынесен отдельно
try:
    from voice_engine import VoiceEngine
except ImportError:
    VoiceEngine = None

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
    config_obj = getattr(config, 'config', getattr(config, 'Config', config))
    port = getattr(config_obj, 'PORT', 8000)
    server_config = uvicorn.Config(app=app, host="0.0.0.0", port=port, log_level="info")
    server = uvicorn.Server(server_config)
    await server.serve()

async def main():
    logger.info("Starting WOLTRON Voice AI System...")
   
    # 1. База данных уже нормально инициализируется сама!
    db = Database()

    # 2. Создаем VoiceEngine (если есть отдельный класс)
    voice_engine = VoiceEngine() if VoiceEngine else None

    # 3. Передаем в SIPWorker аргументы, которые он требует
    try:
        sip_worker = SIPWorker(voice_engine, db, config)
    except TypeError:
        try:
            sip_worker = SIPWorker(database=db, config=config)
        except TypeError:
            sip_worker = SIPWorker(db)

    # 4. Передаем в TelegramBot требуемые зависимости
    try:
        bot = TelegramBot(sip_worker, db)
    except TypeError:
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
