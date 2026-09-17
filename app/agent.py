"""Агент — логика диалога, сценарии, промпты, conversation memory."""

import json
import logging
import os

logger = logging.getLogger("agent")

MAX_HISTORY_MESSAGES = 20
MAX_RESPONSE_LENGTH = 200

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
            if len(response) > MAX_RESPONSE_LENGTH:
                response = response[:MAX_RESPONSE_LENGTH].rsplit(' ', 1)[0] + "..."
            return response
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
            return response or "Здравствуйте! Чем могу помочь?"
        except Exception as e:
            logger.error("get_greeting error call_id=%s: %s", call_id, e)
            return "Здравствуйте! Чем могу помочь?"

    async def generate_summary(self, call_id: str, transcripts: list) -> dict:
        try:
            if not transcripts:
                return {
                    "name": None, "interest": "low", "lesson_status": "unknown",
                    "next_step": None, "objections": None, "summary": "Звонок без содержания.",
                }

            summary_prompt = await self._load_prompt("summary")
            relevant_transcripts = transcripts[-30:] if len(transcripts) > 30 else transcripts
            
            transcript_text = "\n".join(f"{t['role']}: {t['text']}" for t in relevant_transcripts)
            if len(transcript_text) > 5000:
                transcript_text = transcript_text[-5000:]

            messages = [
                {"role": "system", "content": summary_prompt},
                {"role": "user", "content": f"Транскрипт звонка:\n\n{transcript_text}"},
            ]
            response = await self.integrations.agent_chat(call_id, messages, max_tokens=500)

            try:
                clean_response = response.strip() if response else ""
                if clean_response.startswith("```"):
                    parts = clean_response.split("```")
                    if len(parts) >= 2:
                        clean_response = parts[1]
                        if clean_response.startswith("json"):
                            clean_response = clean_response[4:]
                
                result = json.loads(clean_response)
                for field in ["name", "interest", "lesson_status", "next_step", "objections", "summary"]:
                    if field not in result:
                        result[field] = None
                
                if result.get("summary") and len(result["summary"]) > 200:
                    result["summary"] = result["summary"][:200].rsplit(' ', 1)[0] + "..."
                return result
            except (json.JSONDecodeError, AttributeError):
                return {
                    "name": None, "interest": "unknown", "lesson_status": "unknown",
                    "next_step": None, "objections": None,
                    "summary": (response[:200] if response else "Не удалось сгенерировать резюме."),
                }
        except Exception as e:
            logger.error("generate_summary error call_id=%s: %s", call_id, e)
            return {
                "name": None, "interest": "unknown", "lesson_status": "unknown",
                "next_step": None, "objections": None, "summary": "Ошибка генерации резюме.",
            }

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
