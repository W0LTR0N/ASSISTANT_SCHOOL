import asyncio
import sys
from pathlib import Path
from html import escape
from typing import Union

BASE_DIR = Path(__file__).resolve().parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

import logging
import re
import json

from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.enums import ParseMode
from aiogram.client.session.aiohttp import AiohttpSession
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton

import config
from database import Database

logger = logging.getLogger(__name__)

def clean_phone(phone: str) -> str:
    """Нормализует номер к формату 79XXXXXXXXX."""
    digits = re.sub(r'\D', '', phone)
    if digits.startswith('8') and len(digits) == 11:
        digits = '7' + digits[1:]
    elif len(digits) == 10 and digits.startswith('9'):
        digits = '7' + digits
    if len(digits) == 11 and digits.startswith('7'):
        return digits
    return ""

def is_admin(user_id: int) -> bool:
    """Проверка прав администратора. config.TELEGRAM_ADMIN_IDS - это Set[str]."""
    admin_ids = config.TELEGRAM_ADMIN_IDS
    if not admin_ids:
        return True
    return str(user_id) in admin_ids

def _get_edit_keyboard(data: dict) -> InlineKeyboardMarkup:
    """Динамическое создание клавиатуры редактирования (без сохранения в FSM)."""
    edit_buttons = [
        [InlineKeyboardButton(text="📱 Телефон", callback_data="edit_phone")],
        [InlineKeyboardButton(text="👤 Имя", callback_data="edit_name")],
    ]
    if data.get('contact_type') == 'parent':
        edit_buttons.append([InlineKeyboardButton(text="🧑‍🎓 Ученик", callback_data="edit_student_name")])
    
    edit_buttons.extend([
        [InlineKeyboardButton(text="📚 Предмет", callback_data="edit_subject")],
        [InlineKeyboardButton(text="🎒 Класс", callback_data="edit_class_level")],
        [InlineKeyboardButton(text="🎯 Цель", callback_data="edit_goal")],
    ])
    
    if data.get('scenario') == 'BEFORE_LESSON':
        edit_buttons.append([InlineKeyboardButton(text="📝 Контекст", callback_data="edit_extra_context")])
    else:
        edit_buttons.append([InlineKeyboardButton(text="📝 Результат урока", callback_data="edit_lesson_result")])
        
    edit_buttons.append([InlineKeyboardButton(text="🔙 Назад к карточке", callback_data="back_to_confirm")])
    return InlineKeyboardMarkup(inline_keyboard=edit_buttons)

class CallSetupStates(StatesGroup):
    scenario = State()
    contact_type = State()
    phone = State()
    name = State()
    student_name = State()
    subject = State()
    class_level = State()
    goal = State()
    extra_context = State()
    lesson_result = State()
    confirmation = State()
    editing = State()

class TelegramBot:
    def __init__(self, sip_worker, db: Database):
        self.sip_worker = sip_worker
        self.db = db
        
        bot_token = config.TELEGRAM_BOT_TOKEN
        proxy_url = config.TELEGRAM_PROXY_URL or config.PROXY_URL
        
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
        self.dp.message.register(self.cmd_history, Command("history"))
        self.dp.message.register(self.cmd_status, Command("status"))
        self.dp.message.register(self.cmd_terminate, Command("terminate"))
        self.dp.message.register(self.cmd_cancel, Command("cancel"))
        
        self.dp.callback_query.register(self.cb_scenario, F.data.in_(["scenario_before", "scenario_after"]))
        self.dp.callback_query.register(self.cb_contact_type, F.data.in_(["contact_parent", "contact_student"]))
        self.dp.callback_query.register(self.cb_show_history, F.data == "show_history")
        self.dp.callback_query.register(self.cb_back_to_menu_from_history, F.data == "back_to_menu_from_history")
        self.dp.callback_query.register(self.cb_back_to_menu, F.data == "back_to_menu")
        self.dp.callback_query.register(self.cb_confirm_start, F.data == "confirm_start")
        self.dp.callback_query.register(self.cb_confirm_edit, F.data == "confirm_edit")
        self.dp.callback_query.register(self.cb_confirm_cancel, F.data == "confirm_cancel")
        self.dp.callback_query.register(self.cb_skip_field, F.data.startswith("skip_"))
        self.dp.callback_query.register(self.cb_edit_field, F.data.startswith("edit_"))
        self.dp.callback_query.register(self.cb_back_to_confirm, F.data == "back_to_confirm")
        
        self.dp.message.register(self.msg_phone, CallSetupStates.phone)
        self.dp.message.register(self.msg_name, CallSetupStates.name)
        self.dp.message.register(self.msg_student_name, CallSetupStates.student_name)
        self.dp.message.register(self.msg_subject, CallSetupStates.subject)
        self.dp.message.register(self.msg_class_level, CallSetupStates.class_level)
        self.dp.message.register(self.msg_goal, CallSetupStates.goal)
        self.dp.message.register(self.msg_extra_context, CallSetupStates.extra_context)
        self.dp.message.register(self.msg_lesson_result, CallSetupStates.lesson_result)
        self.dp.message.register(self.msg_edit_field, CallSetupStates.editing)

    def _check_admin(self, message: types.Message) -> bool:
        if not is_admin(message.from_user.id):
            asyncio.create_task(message.answer("🔒 Доступ запрещён."))
            return False
        return True

    async def cmd_start(self, message: types.Message, state: FSMContext):
        if not self._check_admin(message):
            return
        await state.clear()
        text = (
            "🎙 <b>WOLTRON Voice AI</b>\n\n"
            "Управление исходящими звонками ученикам.\n"
            "Что хотите сделать?"
        )
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📚 До пробного урока", callback_data="scenario_before")],
            [InlineKeyboardButton(text="🎓 После пробного урока", callback_data="scenario_after")],
            [InlineKeyboardButton(text="📋 История звонков", callback_data="show_history")]
        ])
        await message.answer(text, parse_mode=ParseMode.HTML, reply_markup=kb)

    async def cmd_cancel(self, message: types.Message, state: FSMContext):
        await state.clear()
        await message.answer("❌ Действие отменено.")
        await self.cmd_start(message, state)

    async def cb_scenario(self, callback: types.CallbackQuery, state: FSMContext):
        if not is_admin(callback.from_user.id):
            await callback.answer("Доступ запрещён", show_alert=True)
            return
            
        scenario = "BEFORE_LESSON" if callback.data == "scenario_before" else "AFTER_LESSON"
        await state.update_data(scenario=scenario)
        
        text = "👤 <b>Кому звоним?</b>"
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="👨‍👩‍👧 Родителю", callback_data="contact_parent")],
            [InlineKeyboardButton(text="🧑‍🎓 Ученику", callback_data="contact_student")],
            [InlineKeyboardButton(text="❌ Отмена", callback_data="confirm_cancel")]
        ])
        await callback.message.edit_text(text, parse_mode=ParseMode.HTML, reply_markup=kb)
        await state.set_state(CallSetupStates.contact_type)
        await callback.answer()

    async def cb_contact_type(self, callback: types.CallbackQuery, state: FSMContext):
        if not is_admin(callback.from_user.id):
            await callback.answer("Доступ запрещён", show_alert=True)
            return
            
        contact_type = "parent" if callback.data == "contact_parent" else "student"
        await state.update_data(contact_type=contact_type)
        
        await callback.message.edit_text("📱 <b>Введите номер телефона:</b>\n(например, +79991234567 или 89991234567)")
        await state.set_state(CallSetupStates.phone)
        await callback.answer()

    async def msg_phone(self, message: types.Message, state: FSMContext):
        phone = clean_phone(message.text)
        if not phone:
            await message.answer("❌ Не удалось распознать номер. Введите номер ещё раз (например, +79991234567).")
            return
        await state.update_data(phone=phone)
        
        data = await state.get_data()
        if data['contact_type'] == 'parent':
            await message.answer("👤 <b>Введите имя родителя:</b>")
            await state.set_state(CallSetupStates.name)
        else:
            await message.answer("👤 <b>Введите имя ученика:</b>")
            await state.set_state(CallSetupStates.name)

    async def msg_name(self, message: types.Message, state: FSMContext):
        await state.update_data(name=message.text.strip())
        data = await state.get_data()
        if data['contact_type'] == 'parent':
            await message.answer("🧑‍🎓 <b>Введите имя ученика:</b>")
            await state.set_state(CallSetupStates.student_name)
        else:
            await message.answer("📚 <b>Введите предмет:</b>\n(например, Русский язык)")
            await state.set_state(CallSetupStates.subject)

    async def msg_student_name(self, message: types.Message, state: FSMContext):
        await state.update_data(student_name=message.text.strip())
        await message.answer("📚 <b>Введите предмет:</b>\n(например, Русский язык)")
        await state.set_state(CallSetupStates.subject)

    async def msg_subject(self, message: types.Message, state: FSMContext):
        await state.update_data(subject=message.text.strip())
        await message.answer("🎒 <b>Введите класс / уровень:</b>\n(например, 11 или ЕГЭ)")
        await state.set_state(CallSetupStates.class_level)

    async def msg_class_level(self, message: types.Message, state: FSMContext):
        await state.update_data(class_level=message.text.strip())
        await message.answer("🎯 <b>Введите цель занятий:</b>\n(например, ЕГЭ 90+)")
        await state.set_state(CallSetupStates.goal)

    async def msg_goal(self, message: types.Message, state: FSMContext):
        await state.update_data(goal=message.text.strip())
        data = await state.get_data()
        
        kb_skip = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="⏭ Пропустить", callback_data="skip_extra_context" if data['scenario'] == 'BEFORE_LESSON' else "skip_lesson_result")]
        ])
        
        if data['scenario'] == 'AFTER_LESSON':
            await message.answer(
                "📝 <b>Как прошёл пробный урок?</b>\n"
                "(Например: Арина активно работала, но слабая пунктуация.)\n\n"
                "Или нажмите ⏭ Пропустить",
                parse_mode=ParseMode.HTML,
                reply_markup=kb_skip
            )
            await state.set_state(CallSetupStates.lesson_result)
        else:
            await message.answer(
                "📝 <b>Дополнительная информация:</b>\n"
                "(Например: Слабая пунктуация, хочет поступить в МГУ)\n\n"
                "Или нажмите ⏭ Пропустить",
                parse_mode=ParseMode.HTML,
                reply_markup=kb_skip
            )
            await state.set_state(CallSetupStates.extra_context)

    async def msg_lesson_result(self, message: types.Message, state: FSMContext):
        await state.update_data(lesson_result=message.text.strip())
        await state.update_data(extra_context="Не указано")
        await self._show_confirmation(message, state)

    async def msg_extra_context(self, message: types.Message, state: FSMContext):
        await state.update_data(extra_context=message.text.strip())
        await self._show_confirmation(message, state)

    async def cb_skip_field(self, callback: types.CallbackQuery, state: FSMContext):
        field = callback.data.replace("skip_", "")
        if field == 'extra_context':
            await state.update_data(extra_context="Не указано", lesson_result="Не указано")
        elif field == 'lesson_result':
            await state.update_data(lesson_result="Не указано", extra_context="Не указано")
        
        await self._show_confirmation(callback, state)
        await callback.answer()

    async def _show_confirmation(self, obj: Union[types.Message, types.CallbackQuery], state: FSMContext):
        """Универсальный показ карточки: редактирует сообщение при callback, или шлёт новое при message."""
        data = await state.get_data()
        
        phone = data.get('phone', '')
        phone_masked = f"+7 {phone[1:4]} *** **{phone[-2:]}" if len(phone) >= 11 else "Не указан"
        contact_label = "👨‍👩‍👧 Родителю" if data.get('contact_type') == 'parent' else "🧑‍🎓 Ученику"
        scenario_label = "📚 До пробного урока" if data.get('scenario') == 'BEFORE_LESSON' else "🎓 После пробного урока"
        
        kb_edit = _get_edit_keyboard(data)
        
        text = (
            f"📞 <b>Готово к звонку</b>\n\n"
            f"Сценарий:\n{scenario_label}\n\n"
            f"Кому:\n{contact_label}\n\n"
            f"{'Родитель: ' + escape(data.get('name', 'Не указано')) if data.get('contact_type') == 'parent' else 'Ученик: ' + escape(data.get('name', 'Не указано'))}\n"
            f"{('Ученик: ' + escape(data.get('student_name', 'Не указано'))) if data.get('contact_type') == 'parent' else ''}\n"
            f"Телефон:\n{phone_masked}\n"
            f"Предмет:\n{escape(data.get('subject', 'Не указано'))}\n"
            f"Класс:\n{escape(data.get('class_level', 'Не указано'))}\n"
            f"Цель:\n{escape(data.get('goal', 'Не указано'))}\n"
            f"Дополнительная информация:\n{escape(data.get('extra_context', 'Не указано'))}\n"
            f"{'Контекст пробного урока:\n' + escape(data.get('lesson_result', 'Не указано')) if data.get('scenario') == 'AFTER_LESSON' else ''}\n"
            f"────────────────\n"
            f"Всё верно?"
        )
        
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📞 Начать звонок", callback_data="confirm_start")],
            [InlineKeyboardButton(text="✏️ Изменить данные", callback_data="confirm_edit")],
            [InlineKeyboardButton(text="❌ Отмена", callback_data="confirm_cancel")]
        ])
        
        if isinstance(obj, types.CallbackQuery):
            try:
                await obj.message.edit_text(text, parse_mode=ParseMode.HTML, reply_markup=kb)
            except Exception:
                await obj.message.answer(text, parse_mode=ParseMode.HTML, reply_markup=kb)
        else:
            await obj.answer(text, parse_mode=ParseMode.HTML, reply_markup=kb)
            
        await state.set_state(CallSetupStates.confirmation)

    async def cb_confirm_edit(self, callback: types.CallbackQuery, state: FSMContext):
        data = await state.get_data()
        kb_edit = _get_edit_keyboard(data)
        await callback.message.edit_text("✏️ <b>Выберите поле для изменения:</b>", parse_mode=ParseMode.HTML, reply_markup=kb_edit)
        await state.set_state(CallSetupStates.editing)
        await callback.answer()

    async def cb_edit_field(self, callback: types.CallbackQuery, state: FSMContext):
        field = callback.data.replace("edit_", "")
        await state.update_data(editing_field=field)
        
        prompts = {
            "phone": "📱 Введите новый номер телефона:",
            "name": "👤 Введите новое имя:",
            "student_name": "🧑‍🎓 Введите новое имя ученика:",
            "subject": "📚 Введите новый предмет:",
            "class_level": "🎒 Введите новый класс/уровень:",
            "goal": "🎯 Введите новую цель:",
            "extra_context": "📝 Введите новую дополнительную информацию:",
            "lesson_result": "📝 Введите новый контекст пробного урока:"
        }
        await callback.message.edit_text(prompts.get(field, "Введите новое значение:"))
        await callback.answer()

    async def msg_edit_field(self, message: types.Message, state: FSMContext):
        data = await state.get_data()
        field = data.get('editing_field')
        
        if not field:
            await message.answer("Ошибка состояния. Используйте /start.")
            await state.clear()
            return

        if field == 'phone':
            phone = clean_phone(message.text)
            if not phone:
                await message.answer("❌ Неверный формат номера. Попробуйте ещё раз.")
                return
            await state.update_data(phone=phone)
        else:
            await state.update_data(**{field: message.text.strip()})
            
        await self._show_confirmation(message, state)

    async def cb_back_to_confirm(self, callback: types.CallbackQuery, state: FSMContext):
        await self._show_confirmation(callback, state)
        await callback.answer()

    async def cb_confirm_start(self, callback: types.CallbackQuery, state: FSMContext):
        data = await state.get_data()
        
        await callback.message.edit_text(
            f"⏳ <b>Запускаю звонок...</b>\n\n"
            f"👤 {escape(data.get('name', 'Не указано'))}\n"
            f"📱 +7 {data['phone'][1:4]} *** **{data['phone'][-2:]}\n",
            parse_mode=ParseMode.HTML
        )
        await callback.answer()
        
        metadata = {
            "contact_type": data.get('contact_type'),
            "name": data.get('name'),
            "student_name": data.get('student_name'),
            "subject": data.get('subject'),
            "class_level": data.get('class_level'),
            "goal": data.get('goal'),
            "extra_context": data.get('extra_context'),
            "lesson_result": data.get('lesson_result')
        }
        
        try:
            call_id = await self.sip_worker.originate_call(
                phone=data['phone'], 
                scenario=data['scenario'], 
                metadata=metadata
            )
            
            if not call_id:
                await callback.message.edit_text(
                    "❌ <b>Не удалось начать звонок.</b>\n\nПричина: Достигнут лимит звонков или ошибка SIP.\n\nИспользуйте /start для нового звонка.",
                    parse_mode=ParseMode.HTML
                )
            else:
                await callback.message.edit_text(
                    f"✅ <b>Звонок успешно инициирован!</b>\n\n"
                    f"Call ID: <code>{call_id}</code>\n"
                    f"👤 {escape(data.get('name', 'Не указано'))}\n"
                    f"📱 +7 {data['phone'][1:4]} *** **{data['phone'][-2:]}\n\n"
                    f"Ожидайте ответа абонента.",
                    parse_mode=ParseMode.HTML
                )
        except Exception as e:
            logger.error(f"Ошибка вызова: {e}", exc_info=True)
            await callback.message.edit_text(
                f"❌ <b>Ошибка при совершении звонка:</b>\n{escape(str(e))}\n\nИспользуйте /start для нового звонка.",
                parse_mode=ParseMode.HTML
            )
            
        await state.clear()

    async def cb_confirm_cancel(self, callback: types.CallbackQuery, state: FSMContext):
        await state.clear()
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🏠 В главное меню", callback_data="back_to_menu")]
        ])
        await callback.message.edit_text("❌ <b>Создание звонка отменено.</b>", parse_mode=ParseMode.HTML, reply_markup=kb)
        await callback.answer()

    async def cb_back_to_menu(self, callback: types.CallbackQuery, state: FSMContext):
        await state.clear()
        await callback.message.delete()
        await self.cmd_start(callback.message, state)
        await callback.answer()

    async def cb_show_history(self, callback: types.CallbackQuery):
        if not is_admin(callback.from_user.id):
            await callback.answer("Доступ запрещён", show_alert=True)
            return
        await self._show_history_logic(callback.message)
        await callback.answer()

    async def cb_back_to_menu_from_history(self, callback: types.CallbackQuery, state: FSMContext):
        await state.clear()
        await callback.message.delete()
        await self.cmd_start(callback.message, state)
        await callback.answer()

    async def cmd_history(self, message: types.Message):
        if not self._check_admin(message):
            return
        await self._show_history_logic(message)

    async def _show_history_logic(self, target_message: types.Message):
        try:
            calls = await self.db.get_recent_calls(limit=10)
            if not calls:
                await target_message.answer("📋 <b>История звонков пуста.</b>", parse_mode=ParseMode.HTML)
                return
            
            text = "📞 <b>Последние звонки:</b>\n\n"
            for c in calls:
                status_emoji = "🟢" if c['status'] == 'ENDED' else "🟡" if c['status'] in ('DIALING', 'RINGING', 'IN_PROGRESS') else "🔴"
                scenario_short = "до" if c['scenario'] == 'BEFORE_LESSON' else "после"
                meta = {}
                if c['metadata']:
                    try: 
                        meta = json.loads(c['metadata'])
                    except (json.JSONDecodeError, TypeError): 
                        pass
                name = meta.get('name', 'Не указано')
                phone_raw = c.get('phone', '')
                phone_masked = f"+7 {phone_raw[1:4]} *** **{phone_raw[-2:]}" if len(phone_raw) >= 11 else phone_raw
                
                text += f"{status_emoji} <b>{escape(name)}</b> — {scenario_short} урока\n"
                text += f"📱 {phone_masked}\n"
                text += f"🕒 Статус: {c['status']}\n\n"
                
            await target_message.answer(text, parse_mode=ParseMode.HTML)
        except Exception as e:
            logger.error(f"Ошибка истории: {e}", exc_info=True)
            await target_message.answer("Ошибка получения истории.")

    async def cmd_status(self, message: types.Message):
        if not self._check_admin(message):
            return
        try:
            calls = await self.db.get_pending_deliveries()
            if not calls:
                await message.answer("✅ Активных или ожидающих обработки звонков нет.")
                return
            text = "<b>Статус обработки звонков:</b>\n"
            for c in calls:
                text += f"• <code>{c['call_id']}</code> | Albato: {c['albato_status']} | TG: {c['telegram_status']}\n"
            await message.answer(text, parse_mode=ParseMode.HTML)
        except Exception as e:
            logger.error(f"Ошибка status: {e}", exc_info=True)

    async def cmd_terminate(self, message: types.Message):
        if not self._check_admin(message):
            return
        args = message.text.split(maxsplit=1)
        if len(args) < 2:
            await message.answer("Используй: <code>/terminate &lt;call_id&gt;</code>", parse_mode=ParseMode.HTML)
            return
        call_id = args[1].strip()
        try:
            success = await self.sip_worker.terminate_call(call_id)
            if success:
                await self.db.update_call_status(call_id, "TERMINATED", "telegram_terminate")
                await message.answer(f"✅ Звонок <code>{call_id}</code> успешно завершен.", parse_mode=ParseMode.HTML)
            else:
                await message.answer(f"⚠️ Звонок <code>{call_id}</code> не найден или уже завершен.", parse_mode=ParseMode.HTML)
        except Exception as e:
            logger.error(f"Ошибка сброса: {e}", exc_info=True)
            await message.answer(f"Не удалось сбросить звонок: {e}")

    async def start(self):
        logger.info("Starting Telegram bot polling...")
        await self.dp.start_polling(self.bot)

    async def stop(self):
        logger.info("Stopping Telegram bot...")
        if self.bot:
            await self.bot.session.close()
