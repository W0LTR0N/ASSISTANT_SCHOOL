import logging
import re
from aiogram import Bot, Dispatcher, types
from aiogram.client.session.aiohttp import AiohttpSession
from aiogram.enums import ParseMode
from aiogram.filters import Command

# Импортируем сам модуль config и класс Database
import config
from database import Database

logger = logging.getLogger(__name__)

def clean_phone(phone: str) -> str:
  """Приводит номер к чистому цифровому формату без '+' для Плюсофона."""
  digits = re.sub(r'\D', '', phone)
  if digits.startswith('8') and len(digits) == 11:
    digits = '7' + digits[1:]
  return digits

class TelegramBot:

  def __init__(self, sip_worker):
    self.sip_worker = sip_worker
    self.db = Database()

    # Получаем токен и прокси вне зависимости от того, класс Config или переменные
    bot_token = getattr(
        config,
        'TELEGRAM_BOT_TOKEN',
        getattr(getattr(config, 'Config', None), 'TELEGRAM_BOT_TOKEN', None),
    )
    proxy_url = getattr(
        config,
        'TELEGRAM_PROXY_URL',
        getattr(getattr(config, 'Config', None), 'TELEGRAM_PROXY_URL', None),
    )

    session = None
    if proxy_url:
      session = AiohttpSession(proxy=proxy_url)

    self.bot = Bot(token=bot_token, session=session)
    self.dp = Dispatcher()
    self._register_handlers()

  def _register_handlers(self):
    self.dp.message.register(self.cmd_start, Command('start'))
    self.dp.message.register(self.cmd_call, Command('call'))
    self.dp.message.register(self.cmd_call_after, Command('call_after'))
    self.dp.message.register(self.cmd_status, Command('status'))
    self.dp.message.register(self.cmd_terminate, Command('terminate'))

  async def cmd_start(self, message: types.Message):
    text = (
        '<b>WOLTRON Voice AI — Панель управления</b>\n\n'
        '<b>Команды запуска:</b>\n'
        '• <code>/call &lt;номер&gt;</code> — Звонок ДО урока (Квалификация)\n'
        '• <code>/call_after &lt;номер&gt;</code> — Звонок ПОСЛЕ урока\n\n'
        '<b>Управление:</b>\n'
        '• <code>/status</code> — Активные звонки\n'
        '• <code>/terminate &lt;call_id&gt;</code> — Сбросить звонок'
    )
    await message.answer(text, parse_mode=ParseMode.HTML)

  async def _make_call(self, message: types.Message, scenario: str, label: str):
    args = message.text.split(maxsplit=1)
    if len(args) < 2:
      cmd = message.text.split()[0]
      await message.answer(
          f'Использование: <code>{cmd} &lt;номер&gt;</code>',
          parse_mode=ParseMode.HTML,
      )
      return

    raw_phone = args[1].strip()
    phone = clean_phone(raw_phone)

    if not phone or len(phone) < 10:
      await message.answer('Некорректный номер телефона.')
      return

    await message.answer(
        f'Инициирую звонок ({label}) на <b>{phone}</b>...',
        parse_mode=ParseMode.HTML,
    )

    try:
      call_id = await self.sip_worker.make_call(phone=phone, scenario=scenario)
      await self.db.create_call(
          call_id=call_id, phone=phone, scenario=scenario
      )
      await message.answer(
          f'Звонок пошел!\nCall ID: <code>{call_id}</code>',
          parse_mode=ParseMode.HTML,
      )
    except Exception as e:
      logger.error(f'Ошибка вызова: {e}', exc_info=True)
      await message.answer(f'Ошибка при совершении звонка: {e}')

  async def cmd_call(self, message: types.Message):
    await self._make_call(message, scenario='BEFORE_LESSON', label='До урока')

  async def cmd_call_after(self, message: types.Message):
    await self._make_call(
        message, scenario='AFTER_LESSON', label='После урока'
    )

  async def cmd_status(self, message: types.Message):
    calls = await self.db.get_active_calls()
    if not calls:
      await message.answer('Активных звонков нет.')
      return

    text = '<b>Активные звонки:</b>\n'
    for c in calls:
      text += f"• <code>{c['call_id']}</code> | {c['phone']} | {c['status']}\n"
    await message.answer(text, parse_mode=ParseMode.HTML)

  async def cmd_terminate(self, message: types.Message):
    args = message.text.split(maxsplit=1)
    if len(args) < 2:
      await message.answer(
          'Используй: <code>/terminate &lt;call_id&gt;</code>',
          parse_mode=ParseMode.HTML,
      )
      return

    call_id = args[1].strip()
    try:
      await self.sip_worker.terminate_call(call_id)
      await self.db.update_call_status(call_id, 'TERMINATED')
      await message.answer(
          f'Звонок <code>{call_id}</code> успешно завершен.',
          parse_mode=ParseMode.HTML,
      )
    except Exception as e:
      logger.error(f'Ошибка сброса звонка: {e}', exc_info=True)
      await message.answer(f'Не удалось сбросить звонок: {e}')

  async def start(self):
    logger.info('Starting Telegram bot polling...')
    await self.dp.start_polling(self.bot)
