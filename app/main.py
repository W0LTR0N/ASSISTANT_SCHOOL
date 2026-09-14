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
from integrations import Integrations
from agent import Agent

# Проверка имени файла: если у тебя файл называется voice_engine.py, замени 'voice' на 'voice_engine'
try:
    from voice import VoiceEngine
except ImportError:
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
   
    # 1. Единый экземпляр базы данных
    db = Database()
    await db.init()

    # 2. Инициализация внешних интеграций (Yandex, GenVoice, Albato)
    integrations = Integrations()

    # 3. Инициализация Агента (требует integrations и db)
    agent = Agent(integrations, db)

    # 4. Инициализация Голосового движка (требует integrations, db, agent)
    if VoiceEngine is None:
        logger.error("VoiceEngine not found! Check file name (voice.py or voice_engine.py)")
        raise ImportError("VoiceEngine module not found")
    
    voice_engine = VoiceEngine(integrations, db, agent)

    # 5. Инициализация SIP Worker (требует voice_engine, db, integrations)
    # ИСПРАВЛЕНО: передаем реальный объект integrations, а не модуль config
    sip_worker = SIPWorker(voice_engine, db, integrations)

    # 6. Инициализация Telegram бота (требует sip_worker и db)
    bot = TelegramBot(sip_worker, db)

    logger.info("All components initialized successfully. Starting services...")

    # 7. Запуск сервисов
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
