"""Telegram-бот для управления звонками."""

import logging
import re

from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command

from config import TELEGRAM_BOT_TOKEN, TELEGRAM_ADMIN_IDS

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
        self.bot = Bot(token=TELEGRAM_BOT_TOKEN)
        self.dp = Dispatcher()
        self.sip_worker = sip_worker
        self._setup_handlers()

    def _setup_handlers(self):
        self.dp.message.register(self._cmd_start, Command("start"))
        self.dp.message.register(self._cmd_call, Command("call"))
        self.dp.message.register(self._cmd_terminate, Command("terminate"))
        self.dp.message.register(self._cmd_status, Command("status"))

    def _is_admin(self, user_id: int) -> bool:
        return str(user_id) in TELEGRAM_ADMIN_IDS

    async def _cmd_start(self, message: types.Message):
        await message.answer(
            "🤖 WOLTRON Voice AI\n\n"
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
            await message.answer(
                f"❌ Неправильный номер: {raw_phone}\n"
                "Поддерживаемые форматы:\n"
                "+79991234567\n"
                "89991234567\n"
                "+7 999 123-45-67"
            )
            return

        await message.answer(f"☎️ Инициирую звонок на {phone}...")

        try:
            call_id = await self.sip_worker.originate_call(
                phone, 
                scenario="BEFORE_LESSON", 
                metadata={"source": "telegram", "user_id": message.from_user.id}
            )
            if call_id:
                await message.answer(f"✅ Звонок инициирован\nCall ID: {call_id}")
            else:
                await message.answer("❌ Не удалось инициировать звонок")
        except Exception as e:
            logger.error("Error initiating call: %s", e)
            await message.answer("❌ Произошла ошибка при инициировании звонка")

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
                await message.answer(f"✅ Звонок {call_id} завершается")
            else:
                await message.answer(f"❌ Не удалось завершить звонок {call_id}")
        except Exception as e:
            logger.error("Error terminating call: %s", e)
            await message.answer("❌ Произошла ошибка при завершении звонка")

    async def _cmd_status(self, message: types.Message):
        if not self._is_admin(message.from_user.id):
            await message.answer("⛔ У вас нет доступа к этому боту.")
            return

        active_calls = self.sip_worker.get_active_calls()
        if not active_calls:
            await message.answer("📊 Нет активных звонков")
            return

        lines = ["📊 Активные звонки:\n"]
        for call_id, session in active_calls.items():
            phone = session.get("phone", "Unknown")
            state = session.get("state", "Unknown")
            scenario = session.get("scenario", "Unknown")
            lines.append(f"• {call_id}\n  Phone: {phone}\n  State: {state}\n  Scenario: {scenario}\n")
        await message.answer("\n".join(lines))

    async def start(self):
        logger.info("Starting Telegram bot...")
        await self.dp.start_polling(self.bot, handle_signals=False)

    async def stop(self):
        logger.info("Stopping Telegram bot...")
        await self.bot.session.close()
