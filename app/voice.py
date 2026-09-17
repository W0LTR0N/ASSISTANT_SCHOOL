"""Voice Engine — VAD + STT + Agent + TTS. PCM16 8kHz mono."""

import asyncio
import logging
import time

from config import (
    SPEECH_RMS_THRESHOLD, SILENCE_TO_END_MS, MAX_UTTERANCE_MS,
    MAX_AUDIO_BUFFER_BYTES, VAD_PREROLL_MS,
)

logger = logging.getLogger("voice_engine")


class VoiceEngine:
    def __init__(self, integrations, database, agent):
        self.integrations = integrations
        self.database = database
        self.agent = agent

    async def start(self, call_id: str, session) -> None:
        logger.info("VoiceEngine started call_id=%s scenario=%s", call_id, session.get("scenario"))

        session["audio_buffer"] = bytearray()
        session["speech_buffer"] = bytearray()
        session["preroll_buffer"] = bytearray()
        session["speech_start"] = None
        session["silence_start"] = None
        session["processing"] = False
        session["active"] = True
        session["tracked_tasks"] = []

        try:
            await self._send_greeting(call_id, session)
            await self._audio_loop(call_id, session)
        except asyncio.CancelledError:
            logger.info("VoiceEngine cancelled call_id=%s", call_id)
        except Exception as e:
            logger.error("VoiceEngine error call_id=%s: %s", call_id, e)
        finally:
            session["active"] = False
            for task in session.get("tracked_tasks", []):
                if not task.done():
                    task.cancel()
            logger.info("VoiceEngine stopped call_id=%s", call_id)

    async def _send_greeting(self, call_id: str, session) -> None:
        try:
            scenario = session.get("scenario", "BEFORE_LESSON")
            # 🔥 ПЕРЕДАЕМ METADATA В AGENT ДЛЯ ПЕРСОНАЛИЗИРОВАННОГО ПРИВЕТСТВИЯ
            greeting = await self.agent.get_greeting(call_id, scenario, metadata=session.get("metadata"))
            if not greeting:
                return

            await self.database.add_transcript(call_id, "assistant", greeting)
            tts_task = asyncio.create_task(self._play_tts(call_id, session, greeting))
            session.setdefault("tracked_tasks", []).append(tts_task)
            await self.integrations.register_tts_task(call_id, tts_task)
            await tts_task
        except Exception as e:
            logger.error("Greeting failed call_id=%s: %s", call_id, e)

    async def _play_tts(self, call_id: str, session, text: str) -> None:
        try:
            pcm = await self.integrations.tts_synthesize(text)
            if pcm:
                proto = session.get("proto")
                if proto:
                    await proto.send_pcm(pcm)
        except asyncio.CancelledError:
            logger.info("TTS interrupted call_id=%s", call_id)
            raise
        except Exception as e:
            logger.error("TTS error call_id=%s: %s", call_id, e)
        finally:
            await self.integrations.unregister_tts_task(call_id)

    async def _audio_loop(self, call_id: str, session) -> None:
        proto = session.get("proto")
        if not proto:
            return

        silence_to_end_s = SILENCE_TO_END_MS / 1000.0
        max_utterance_s = MAX_UTTERANCE_MS / 1000.0
        preroll_bytes = int(8000 * 2 * VAD_PREROLL_MS / 1000)

        while session.get("active", True):
            try:
                await asyncio.sleep(0.01)
                if not proto.active:
                    break

                pcm_data = bytes(proto.pcm_buffer)
                proto.pcm_buffer.clear()

                audio_buffer = session["audio_buffer"]
                if len(audio_buffer) > MAX_AUDIO_BUFFER_BYTES:
                    excess = len(audio_buffer) - MAX_AUDIO_BUFFER_BYTES
                    del audio_buffer[:excess]

                if not pcm_data:
                    if session["speech_start"] is not None and session["silence_start"] is None:
                        session["silence_start"] = time.monotonic()
                    continue

                audio_buffer.extend(pcm_data)
                rms = self._calc_rms(pcm_data)

                if rms > SPEECH_RMS_THRESHOLD:
                    if len(session["preroll_buffer"]) < preroll_bytes:
                        session["preroll_buffer"].extend(pcm_data)
                    else:
                        session["preroll_buffer"] = session["preroll_buffer"][-preroll_bytes:]

                    session["speech_buffer"].extend(pcm_data)
                    session["silence_start"] = None

                    if session["speech_start"] is None:
                        session["speech_start"] = time.monotonic()

                    if proto.speaking:
                        await self.integrations.tts_interrupt(call_id)
                else:
                    if session["speech_start"] is not None:
                        if session["silence_start"] is None:
                            session["silence_start"] = time.monotonic()
                        else:
                            silence_duration = time.monotonic() - session["silence_start"]
                            if silence_duration > silence_to_end_s and not session["processing"]:
                                speech_data = bytes(session["preroll_buffer"]) + bytes(session["speech_buffer"])
                                session["speech_buffer"].clear()
                                session["preroll_buffer"].clear()
                                session["speech_start"] = None
                                session["silence_start"] = None
                                task = asyncio.create_task(self._process_speech_async(call_id, session, speech_data))
                                session.setdefault("tracked_tasks", []).append(task)

                if session["speech_start"] is not None:
                    speech_duration = time.monotonic() - session["speech_start"]
                    if speech_duration > max_utterance_s and not session["processing"]:
                        speech_data = bytes(session["preroll_buffer"]) + bytes(session["speech_buffer"])
                        session["speech_buffer"].clear()
                        session["preroll_buffer"].clear()
                        session["speech_start"] = None
                        session["silence_start"] = None
                        task = asyncio.create_task(self._process_speech_async(call_id, session, speech_data))
                        session.setdefault("tracked_tasks", []).append(task)

            except Exception as e:
                logger.error("Audio loop error call_id=%s: %s", call_id, e)
                break

    async def _process_speech_async(self, call_id: str, session, pcm_data: bytes) -> None:
        if not pcm_data:
            return

        session["processing"] = True
        try:
            text = await self.integrations.stt_recognize(pcm_data)
            if not text:
                fallback = "Не расслышал, повторите, пожалуйста."
                await self.database.add_transcript(call_id, "assistant", fallback)
                tts_task = asyncio.create_task(self._play_tts(call_id, session, fallback))
                session.setdefault("tracked_tasks", []).append(tts_task)
                await self.integrations.register_tts_task(call_id, tts_task)
                await tts_task
                return

            await self.database.add_transcript(call_id, "user", text)

            scenario = session.get("scenario", "BEFORE_LESSON")
            # 🔥 ПЕРЕДАЕМ METADATA В AGENT ДЛЯ УЧЁТА КОНТЕКСТА (ИМЯ, ПРЕДМЕТ, КЛАСС И Т.Д.)
            agent_response = await self.agent.process_user_message(call_id, text, scenario, metadata=session.get("metadata"))

            if not agent_response:
                fallback = "Секунду, я уточню информацию."
                await self.database.add_transcript(call_id, "assistant", fallback)
                tts_task = asyncio.create_task(self._play_tts(call_id, session, fallback))
                session.setdefault("tracked_tasks", []).append(tts_task)
                await self.integrations.register_tts_task(call_id, tts_task)
                await tts_task
                return

            await self.database.add_transcript(call_id, "assistant", agent_response)
            tts_task = asyncio.create_task(self._play_tts(call_id, session, agent_response))
            session.setdefault("tracked_tasks", []).append(tts_task)
            await self.integrations.register_tts_task(call_id, tts_task)
            await tts_task

        except Exception as e:
            logger.error("Process speech error call_id=%s: %s", call_id, e)
        finally:
            session["processing"] = False

    def _calc_rms(self, pcm_data: bytes) -> float:
        if len(pcm_data) < 2:
            return 0.0
        total = 0.0
        samples = len(pcm_data) // 2
        for i in range(samples):
            sample = int.from_bytes(pcm_data[i * 2:i * 2 + 2], byteorder="little", signed=True)
            total += sample * sample
        return (total / samples) ** 0.5

    async def finish_call(self, call_id: str, session) -> None:
        logger.info("Finishing call call_id=%s", call_id)
        try:
            transcripts = await self.database.get_transcripts(call_id)
            call_record = await self.database.get_call(call_id)
            if not call_record:
                return

            summary_result = await self.agent.generate_summary(call_id, transcripts)

            from app.models import CallResult
            result = CallResult(
                call_id=call_id,
                direction=call_record.direction,
                scenario=call_record.scenario,
                phone=call_record.phone,
                name=summary_result.get("name"),
                interest=summary_result.get("interest"),
                lesson_status=summary_result.get("lesson_status"),
                next_step=summary_result.get("next_step"),
                objections=summary_result.get("objections"),
                summary=summary_result.get("summary"),
                transcript=self._format_transcript(transcripts),
            )

            saved = await self.database.save_call_result(call_id, result.to_json())
            if saved:
                albato_ok = await self.integrations.send_to_albato(result.to_dict())
                telegram_ok = await self.integrations.send_telegram_result(result)
                await self.database.update_delivery_status(
                    call_id,
                    albato_status="sent" if albato_ok else "failed",
                    telegram_status="sent" if telegram_ok else "failed",
                )
                logger.info("Call result sent call_id=%s albato=%s telegram=%s", call_id, albato_ok, telegram_ok)
        except Exception as e:
            logger.error("Finish call error call_id=%s: %s", call_id, e)

    def _format_transcript(self, transcripts: list) -> str:
        lines = []
        for t in transcripts:
            role = "Клиент" if t["role"] == "user" else "Ассистент"
            lines.append(f"{role}: {t['text']}")
        return "\n".join(lines)
