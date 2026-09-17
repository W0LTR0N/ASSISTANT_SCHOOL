"""Агент — логика диалога, сценарии, промпты, conversation memory."""

import json
import logging
import os

logger = logging.getLogger("agent")

MAX_HISTORY_MESSAGES = 20
MAX_RESPONSE_LENGTH = 500  # Аварийное ограничение, основная краткость через промты


class Agent:
    def __init__(self, integrations, database):
        self.integrations = integrations
        self.database = database

    async def _get_context_info(self, call_id: str) -> str:
        """Загружает metadata из БД и формирует контекст для промпта."""
        context_info = ""
        try:
            call_record = await self.database.get_call(call_id)
            if call_record and call_record.metadata:
                meta = json.loads(call_record.metadata)
                context_info = "\n\nКОНТЕКСТ ЗВОНКА:\n"
                if meta.get('contact_type') == 'parent':
                    context_info += f"- Собеседник: Родитель ({meta.get('name', 'Не указано')})\n"
                    context_info += f"- Ученик: {meta.get('student_name', 'Не указано')}\n"
                else:
                    context_info += f"- Собеседник: Ученик ({meta.get('name', 'Не указано')})\n"
                if meta.get('subject'):
                    context_info += f"- Предмет: {meta.get('subject')}\n"
                if meta.get('class_level'):
                    context_info += f"- Класс: {meta.get('class_level')}\n"
                if meta.get('goal'):
                    context_info += f"- Цель: {meta.get('goal')}\n"
                if meta.get('extra_context'):
                    context_info += f"- Дополнительно: {meta.get('extra_context')}\n"
                if meta.get('lesson_result'):
                    context_info += f"- Результат пробного урока: {meta.get('lesson_result')}\n"
        except (json.JSONDecodeError, TypeError) as e:
            logger.error("JSON decode error loading context for call_id=%s: %s", call_id, e)
        except Exception as e:
            logger.error("Error loading context for call_id=%s: %s", call_id, e)
        return context_info

    def _truncate_response(self, response: str) -> str:
        """Безопасная обрезка ответа с учётом краевых случаев."""
        if not response:
            return response
        if len(response) <= MAX_RESPONSE_LENGTH:
            return response

        truncated = response[:MAX_RESPONSE_LENGTH]

        # Пытаемся обрезать на последнем слове
        if ' ' in truncated:
            truncated = truncated.rsplit(' ', 1)[0]

        # Если после обрезки получилось пусто — берём как есть
        if not truncated.strip():
            truncated = response[:MAX_RESPONSE_LENGTH]

        return truncated + "..."

    async def process_user_message(self, call_id: str, user_text: str, scenario: str) -> str:
        try:
            if scenario not in ("BEFORE_LESSON", "AFTER_LESSON"):
                return "Извините, произошла ошибка."

            system_prompt = await self._load_prompt("system")
            scenario_prompt = await self._load_prompt(scenario.lower())
            context_info = await self._get_context_info(call_id)

            full_system_prompt = system_prompt + context_info + ("\n\n" + scenario_prompt if scenario_prompt else "")

            history = await self.database.get_conversation_history(call_id, max_messages=MAX_HISTORY_MESSAGES)

            messages = [{"role": "system", "content": full_system_prompt}]
            for msg in history:
                messages.append({"role": msg["role"], "content": msg["content"]})
            messages.append({"role": "user", "content": user_text})

            response = await self.integrations.agent_chat(call_id, messages, max_tokens=300)
            if not response:
                return "Извините, не могу ответить."

            return self._truncate_response(response)
        except Exception as e:
            logger.error("process_user_message error call_id=%s: %s", call_id, e)
            return "Произошла техническая ошибка."

    async def get_greeting(self, call_id: str, scenario: str) -> str:
        try:
            if scenario not in ("BEFORE_LESSON", "AFTER_LESSON"):
                return "Здравствуйте!"

            system_prompt = await self._load_prompt("system")
            scenario_prompt = await self._load_prompt(scenario.lower())
            context_info = await self._get_context_info(call_id)

            full_system_prompt = system_prompt + context_info + ("\n\n" + scenario_prompt if scenario_prompt else "")

            messages = [
                {"role": "system", "content": full_system_prompt},
                {"role": "user", "content": "Сгенерируй короткое приветствие (1-2 предложения)."},
            ]
            response = await self.integrations.agent_chat(call_id, messages, max_tokens=100)
            greeting = response or "Здравствуйте! Чем могу помочь?"

            # ВАЖНО: НЕ сохраняем greeting здесь!
            # Вызывающий код (voice.py) уже сохраняет его через add_transcript()
            # Это предотвращает дублирование записи.

            return greeting
        except Exception as e:
            logger.error("get_greeting error call_id=%s: %s", call_id, e)
            return "Здравствуйте! Чем могу помочь?"

    # ──────────────────────────────────────────────
    # НОРМАЛИЗАЦИЯ SUMMARY
    # ──────────────────────────────────────────────

    def _normalize_summary(self, result: dict, metadata: dict) -> dict:
        """Приводит ответ модели к безопасной стандартной структуре."""

        if not isinstance(result, dict):
            result = {}

        # ── contact_type ──
        contact_type = result.get("contact_type")
        if contact_type not in ("parent", "student"):
            # Пытаемся взять из metadata
            meta_ct = metadata.get("contact_type")
            contact_type = meta_ct if meta_ct in ("parent", "student") else None

        # ── name / student_name ──
        name = result.get("name")
        if not name or not isinstance(name, str):
            name = metadata.get("name")

        student_name = result.get("student_name")
        if not student_name or not isinstance(student_name, str):
            student_name = metadata.get("student_name")

        # ── subject / class_level / goal ──
        subject = result.get("subject")
        if not subject or not isinstance(subject, str):
            subject = metadata.get("subject")

        class_level = result.get("class_level")
        if class_level is not None:
            class_level = str(class_level)
        if not class_level:
            class_level = metadata.get("class_level")
            if class_level is not None:
                class_level = str(class_level)

        goal = result.get("goal")
        if not goal or not isinstance(goal, str):
            goal = metadata.get("goal")

        # ── interest ──
        interest = result.get("interest")
        valid_interest = ("high", "medium", "low", "none")
        if interest not in valid_interest:
            # Пытаемся определить по другим данным
            next_step = result.get("next_step", "")
            if isinstance(next_step, str) and any(
                kw in next_step.lower() for kw in
                ("оформл", "оплат", "соглас", "запис", "давайте")
            ):
                interest = "high"
            else:
                # Если определить нельзя — возвращаем None
                interest = None

        # ── lesson_status ──
        lesson_status = result.get("lesson_status")
        valid_lesson_status = ("completed", "scheduled", "cancelled", "unknown")
        if lesson_status not in valid_lesson_status:
            lesson_status = "unknown"

        # ── objections ──
        objections = result.get("objections")
        if isinstance(objections, str):
            objections = [objections] if objections.strip() else None
        elif isinstance(objections, list):
            objections = [str(o) for o in objections if o]
            if not objections:
                objections = None
        else:
            objections = None

        # ── next_step ──
        next_step = result.get("next_step")
        if not next_step or not isinstance(next_step, str):
            next_step = None

        # ── follow_up ──
        follow_up = result.get("follow_up")
        if isinstance(follow_up, str):
            follow_up = follow_up.strip().lower() in ("true", "1", "yes", "да")
        elif not isinstance(follow_up, bool):
            follow_up = False

        # ── summary ──
        summary = result.get("summary")
        if not summary or not isinstance(summary, str):
            summary = "Не удалось сформировать резюме."
        if len(summary) > 200:
            summary = summary[:200].rsplit(' ', 1)[0] + "..."

        return {
            "contact_type": contact_type,
            "name": name,
            "student_name": student_name,
            "subject": subject,
            "class_level": class_level,
            "goal": goal,
            "interest": interest,
            "lesson_status": lesson_status,
            "objections": objections,
            "next_step": next_step,
            "follow_up": follow_up,
            "summary": summary,
        }

    async def generate_summary(self, call_id: str, transcripts: list) -> dict:
        try:
            if not transcripts:
                return self._normalize_summary({}, {})

            summary_prompt = await self._load_prompt("summary")
            context_info = await self._get_context_info(call_id)

            # Берём последнее 30 реплик
            relevant_transcripts = transcripts[-30:] if len(transcripts) > 30 else transcripts
            transcript_text = "\n".join(f"{t['role']}: {t['text']}" for t in relevant_transcripts)

            # Если транскрипт длинный — сохраняем начало и конец
            if len(transcript_text) > 5000:
                head = transcript_text[:1000]
                tail = transcript_text[-4000:]
                transcript_text = head + "\n... [середина разговора сокращена] ...\n" + tail

            user_content = f"Базовый контекст звонка:{context_info}\n\nТранскрипт разговора:\n\n{transcript_text}"

            messages = [
                {"role": "system", "content": summary_prompt},
                {"role": "user", "content": user_content},
            ]
            response = await self.integrations.agent_chat(call_id, messages, max_tokens=500)

            # Парсим ответ модели
            parsed = {}
            try:
                clean_response = response.strip() if response else ""
                if clean_response.startswith("```"):
                    parts = clean_response.split("```")
                    if len(parts) >= 2:
                        clean_response = parts[1]
                        if clean_response.startswith("json"):
                            clean_response = clean_response[4:]
                parsed = json.loads(clean_response)
            except (json.JSONDecodeError, AttributeError, IndexError):
                logger.warning("Failed to parse summary JSON for call_id=%s", call_id)

            # Загружаем metadata для нормализации
            metadata = {}
            try:
                call_record = await self.database.get_call(call_id)
                if call_record and call_record.metadata:
                    metadata = json.loads(call_record.metadata)
            except Exception:
                pass

            return self._normalize_summary(parsed, metadata)

        except Exception as e:
            logger.error("generate_summary error call_id=%s: %s", call_id, e)
            return self._normalize_summary({}, {})

    async def _load_prompt(self, prompt_name: str) -> str:
        try:
            prompt_path = os.path.join(os.path.dirname(__file__), "..", "prompts", f"{prompt_name}.txt")
            with open(prompt_path, "r", encoding="utf-8") as f:
                content = f.read().strip()
                if not content:
                    logger.warning("Empty prompt file: %s", prompt_path)
                    return ""
                return content
        except FileNotFoundError:
            logger.error("Prompt file not found: %s", prompt_name)
            return ""
        except Exception as e:
            logger.error("Failed to load prompt %s: %s", prompt_name, e)
            return ""
