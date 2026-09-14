import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

import logging
import re
from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aiogram.enums import ParseMode
from aiogram.client.session.aiohttp import AiohttpSession

import config
from database import Database

logger = logging.getLogger(__name__)

def clean_phone(phone: str) -> str:
    """Приводит номер к формату 79XXXXXXXXX."""
    digits = re.sub(r'\D', '', phone)
    if digits.startswith('8') and len(digits) == 11:
        digits = '7' + digits[1:]
    return digits

class TelegramBot:
    def __init__(self, sip_worker, db: Database):
        self.sip_worker = sip_worker
        self.db = db
       
        config_obj = getattr(config, 'config', getattr(config, 'Config', config))
        bot_token = getattr(config_obj, 'TELEGRAM_BOT_TOKEN', None)
        proxy_url = getattr(config_obj, 'TELEGRAM_PROXY_URL', None) or getattr(config_obj, 'PROXY_URL', None)

        session = None
        if proxy_url:
            session = AiohttpSession(proxy=proxy_url)
            logger.info(f"Initializing Telegram bot with Proxy: {proxy_url}")
        else:
            logger.info("Initializing Telegram bot without Proxy")
           
        self.bot = Bot(token=bot_token, session=session)
        self.dp = Dispatcher()
        self._register_handlers()

    def _register_handlers(self):
        self.dp.message.register(self.cmd_start, Command("start"))
        self.dp.message.register(self.cmd_call, Command("call"))
        self.dp.message.register(self.cmd_call_after, Command("call_after"))
        self.dp.message.register(self.cmd_status, Command("status"))
        self.dp.message.register(self.cmd_terminate, Command("terminate"))

    async def cmd_start(self, message: types.Message):
        text = (
            "<b>WOLTRON Voice AI — Панель управления</b>\n\n"
            "<b>Команды запуска:</b>\n"
            "• <code>/call &lt;номер&gt;</code> — Звонок ДО урока\n"
            "• <code>/call_after &lt;номер&gt;</code> — Звонок ПОСЛЕ урока\n\n"
            "<b>Управление:</b>\n"
            "• <code>/status</code> — Активные звонки\n"
            "• <code>/terminate &lt;call_id&gt;</code> — Сбросить звонок"
        )
        await message.answer(text, parse_mode=ParseMode.HTML)

    async def _make_call(self, message: types.Message, scenario: str, label: str):
        args = message.text.split(maxsplit=1)
        if len(args) < 2:
            cmd = message.text.split()[0]
            await message.answer(f"Использование: <code>{cmd} &lt;номер&gt;</code>", parse_mode=ParseMode.HTML)
            return

        raw_phone = args[1].strip()
        phone = clean_phone(raw_phone)

        if not phone or len(phone) < 10:
            await message.answer("Некорректный номер телефона.")
            return

        await message.answer(f"Инициирую звонок ({label}) на <b>{phone}</b>...", parse_mode=ParseMode.HTML)
       
        try:
            # originate_call УЖЕ создаёт запись в БД внутри себя!
            # Не нужно вызывать self.db.create_call() повторно
            call_id = await self.sip_worker.originate_call(phone=phone, scenario=scenario)
            
            if not call_id:
                await message.answer("❌ Не удалось инициировать звонок (возможно, достигнут лимит или ошибка SIP).")
                return

            # ИСПРАВЛЕНО: убран повторный вызов create_call — он уже вызван внутри originate_call
            await message.answer(f"✅ Звонок пошел!\nCall ID: <code>{call_id}</code>", parse_mode=ParseMode.HTML)
        except Exception as e:
            logger.error(f"Ошибка вызова: {e}", exc_info=True)
            await message.answer(f"Ошибка при совершении звонка: {e}")

    async def cmd_call(self, message: types.Message):
        await self._make_call(message, scenario="BEFORE_LESSON", label="До урока")

    async def cmd_call_after(self, message: types.Message):
        await self._make_call(message, scenario="AFTER_LESSON", label="После урока")

    async def cmd_status(self, message: types.Message):
        try:
            calls = await self.db.get_pending_deliveries()
            if not calls:
                await message.answer("Активных или ожидающих звонков нет.")
                return

            text = "<b>Статус звонков:</b>\n"
            for c in calls:
                text += f"• <code>{c['call_id']}</code> | Status: {c['albato_status']}\n"
            await message.answer(text, parse_mode=ParseMode.HTML)
        except Exception as e:
            logger.error(f"Ошибка выполнения status: {e}", exc_info=True)
            await message.answer(f"Ошибка получения статуса: {e}")

    async def cmd_terminate(self, message: types.Message):
        args = message.text.split(maxsplit=1)
        if len(args) < 2:
            await message.answer("Используй: <code>/terminate &lt;call_id&gt;</code>", parse_mode=ParseMode.HTML)
            return

        call_id = args[1].strip()
        try:
            if hasattr(self.sip_worker, 'terminate_call'):
                success = await self.sip_worker.terminate_call(call_id)
                if success:
                    await self.db.update_call_status(call_id, "TERMINATED", "telegram_terminate")
                    await message.answer(f"Звонок <code>{call_id}</code> успешно завершен.", parse_mode=ParseMode.HTML)
                else:
                    await message.answer(f"Звонок <code>{call_id}</code> не найден или уже завершен.", parse_mode=ParseMode.HTML)
            else:
                await message.answer("Функция завершения звонка недоступна.", parse_mode=ParseMode.HTML)
        except Exception as e:
            logger.error(f"Ошибка сброса звонка: {e}", exc_info=True)
            await message.answer(f"Не удалось сбросить звонок: {e}")

    async def start(self):
        logger.info("Starting Telegram bot polling...")
        await self.dp.start_polling(self.bot)

    async def stop(self):
        logger.info("Stopping Telegram bot...")
        if self.bot:
            await self.bot.session.close()
