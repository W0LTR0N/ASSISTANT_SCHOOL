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

try:
    from voice import VoiceEngine
except ImportError:
    try:
        from voice_engine import VoiceEngine
    except ImportError:
        logging.error("CRITICAL: Cannot import VoiceEngine")
        sys.exit(1)

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
    port = getattr(config, 'PORT', 8000)
    server_config = uvicorn.Config(app=app, host="0.0.0.0", port=port, log_level="info")
    server = uvicorn.Server(server_config)
    await server.serve()

async def main():
    logger.info("[MAIN] Starting WOLTRON Voice AI System...")
   
    db = Database()
    await db.init()

    integrations = Integrations()
    agent = Agent(integrations, db)
    voice_engine = VoiceEngine(integrations, db, agent)
    
    sip_worker = SIPWorker(voice_engine, db, integrations)
    bot = TelegramBot(sip_worker, db)

    logger.info("[MAIN] All components initialized. Starting services...")

    await asyncio.gather(
        run_fastapi(),
        bot.start(),
        sip_worker.start(),
    )

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logger.info("[MAIN] System stopped gracefully.")
