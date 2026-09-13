"""Telegram-бот с поддержкой SOCKS5 proxy."""

import logging
import re
from typing import Optional

from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aiogram.client.default import DefaultBotProperties
from aiogram.client.session.aiohttp import AiohttpSession

from config import (
    TELEGRAM_BOT_TOKEN, 
    TELEGRAM_ADMIN_IDS,
    TELEGRAM_PROXY_URL,
    TELEGRAM_PROXY_LOGIN,
    TELEGRAM_PROXY_PASSWORD,
)

logger = logging.getLogger("bot")


def normalize_phone(phone: str) -> str | None:
    """Нормализация номера телефона к формату +7XXXXXXXXXX. SIP-safe."""
    if not phone:
        return None
    phone = phone.strip()
    phone = re.sub(r'[^\d+]', '', phone)
    if phone.count('+') > 1:
        phone = phone.replace('+', '', phone.count('+') - 1)
    digits = re.sub(r'[^\d]', '', phone)
    
    if len(digits) == 11:
        if digits.startswith('8'):
            digits = '7' + digits[1:]
        elif digits.startswith('7'):
            pass
        else:
            return None
    elif len(digits) == 10:
        digits = '7' + digits
    else:
        return None
    
    if len(digits) != 11 or not digits.startswith('7'):
        return None
    return '+' + digits


class TelegramBot:
    def __init__(self, sip_worker):
        self.sip_worker = sip_worker
        self.bot: Optional[Bot] = None
        self.dp: Optional[Dispatcher] = None
        self._setup_bot()

    def _setup_bot(self):
        try:
            if TELEGRAM_PROXY_URL:
                logger.info("Using Telegram proxy...")
                
                # Собираем proxy URL
                proxy_str = TELEGRAM_PROXY_URL.strip()
                if not proxy_str.startswith("socks5://"):
                    if TELEGRAM_PROXY_LOGIN and TELEGRAM_PROXY_PASSWORD:
                        proxy_str = f"socks5://{TELEGRAM_PROXY_LOGIN}:{TELEGRAM_PROXY_PASSWORD}@{proxy_str}"
                    else:
                        proxy_str = f"socks5://{proxy_str}"
                
                logger.info("Proxy string: %s", proxy_str.replace(TELEGRAM_PROXY_PASSWORD, '***') if TELEGRAM_PROXY_PASSWORD else proxy_str)
                
                # Создаём сессию с proxy
                session = AiohttpSession(proxy=proxy_str)
                
                self.bot = Bot(
                    token=TELEGRAM_BOT_TOKEN,
                    session=session,
                    default=DefaultBotProperties(parse_mode="HTML")
                )
            else:
                logger.info("Using Telegram without proxy")
                self.bot = Bot(
                    token=TELEGRAM_BOT_TOKEN,
                    default=DefaultBotProperties(parse_mode="HTML")
                )
            
            self.dp = Dispatcher()
            self._setup_handlers()
            
        except Exception as e:
            logger.error("Failed to setup Telegram bot: %s", e)
            raise

    def _setup_handlers(self):
        self.dp.message.register(self._cmd_start, Command("start"))
        self.dp.message.register(self._cmd_call, Command("call"))
        self.dp.message.register(self._cmd_terminate, Command("terminate"))
        self.dp.message.register(self._cmd_status, Command("status"))

    def _is_admin(self, user_id: int) -> bool:
        return str(user_id) in TELEGRAM_ADMIN_IDS

    async def _cmd_start(self, message: types.Message):
        await message.answer(
            "🤖 <b>WOLTRON Voice AI</b>\n\n"
            "Команды:\n"
            "/call <номер> — initiate outbound call\n"
            "/terminate <call_id> — terminate active call\n"
            "/status — show active calls"
        )

    async def _cmd_call(self, message: types.Message):
        if not self._is_admin(message.from_user.id):
            await message.answer("⛔ У вас нет доступа к этому боту.")
            return

        parts = message.text.split(maxsplit=1)
        if len(parts) < 2:
            await message.answer("Используй: /call <номер>\nПример: /call +79991234567")
            return

        raw_phone = parts[1].strip()
        phone = normalize_phone(raw_phone)
        
        if not phone:
            await message.answer(f"❌ <b>Неправильный номер:</b> {raw_phone}")
            return

        await message.answer(f"☎️ <b>Инициирую звонок на {phone}...</b>")

        try:
            call_id = await self.sip_worker.originate_call(
                phone, 
                scenario="BEFORE_LESSON", 
                metadata={"source": "telegram", "user_id": message.from_user.id}
            )
            if call_id:
                await message.answer(f"✅ <b>Звонок инициирован</b>\nCall ID: <code>{call_id}</code>")
            else:
                await message.answer("❌ <b>Не удалось инициировать звонок</b>")
        except Exception as e:
            logger.error("Error initiating call: %s", e)
            await message.answer("❌ Произошла ошибка")

    async def _cmd_terminate(self, message: types.Message):
        if not self._is_admin(message.from_user.id):
            await message.answer("⛔ У вас нет доступа к этому боту.")
            return

        parts = message.text.split(maxsplit=1)
        if len(parts) < 2:
            await message.answer("Используй: /terminate <call_id>")
            return

        call_id = parts[1].strip()
        try:
            success = await self.sip_worker.terminate_call(call_id)
            if success:
                await message.answer(f"✅ <b>Звонок {call_id} завершается</b>")
            else:
                await message.answer(f" <b>Не удалось завершить звонок {call_id}</b>")
        except Exception as e:
            logger.error("Error terminating call: %s", e)
            await message.answer("❌ Произошла ошибка")

    async def _cmd_status(self, message: types.Message):
        if not self._is_admin(message.from_user.id):
            await message.answer(" У вас нет доступа к этому боту.")
            return

        active_calls = self.sip_worker.get_active_calls()
        if not active_calls:
            await message.answer("📊 <b>Нет активных звонков</b>")
            return

        lines = ["📊 <b>Активные звонки:</b>\n"]
        for call_id, session in active_calls.items():
            phone = session.get("phone", "Unknown")
            state = session.get("state", "Unknown")
            lines.append(f"• <code>{call_id}</code>\n  Phone: {phone}\n  State: {state}\n")
        
        await message.answer("\n".join(lines))

    async def start(self):
        logger.info("Starting Telegram bot...")
        if not self.bot or not self.dp:
            logger.error("Telegram bot not initialized")
            return
        await self.dp.start_polling(self.bot, handle_signals=False)

    async def stop(self):
        logger.info("Stopping Telegram bot...")
        if self.bot:
            await self.bot.session.close()
