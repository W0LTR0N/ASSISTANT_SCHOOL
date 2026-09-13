"""Интеграции: Yandex (GPT, STT), GenVoice (TTS), Albato, Telegram."""

import asyncio
import base64
import json
import logging
import os
from typing import Optional

import httpx
import websockets

from config import (
    YANDEX_API_KEY, YANDEX_FOLDER_ID, YANDEX_GPT_MODEL,
    GENVOICE_API_KEY, GENVOICE_REALTIME_URL, GENVOICE_VOICE,
    ALBATO_WEBHOOK_URL, ALBATO_TOKEN,
    TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID,
    STT_TIMEOUT, GPT_TIMEOUT, TTS_TIMEOUT, DELIVERY_TIMEOUT,
)

logger = logging.getLogger("integrations")


class Integrations:
    def __init__(self):
        self._http_client: Optional[httpx.AsyncClient] = None
        self._http_lock = asyncio.Lock()
        self._tts_tasks: dict[str, asyncio.Task] = {}
        self._tts_lock = asyncio.Lock()

    async def _get_http_client(self) -> httpx.AsyncClient:
        async with self._http_lock:
            if self._http_client is None or self._http_client.is_closed:
                self._http_client = httpx.AsyncClient(timeout=30.0)
            return self._http_client

    async def close(self) -> None:
        async with self._http_lock:
            if self._http_client and not self._http_client.is_closed:
                await self._http_client.aclose()
                self._http_client = None

    async def stt_recognize(self, pcm_data: bytes) -> str:
        if not YANDEX_API_KEY or not YANDEX_FOLDER_ID or not pcm_data:
            return ""

        url = "https://stt.api.cloud.yandex.net/speech/v1/stt:recognize"
        headers = {
            "Authorization": f"Api-Key {YANDEX_API_KEY}",
            "Content-Type": "application/octet-stream",
        }
        params = {
            "folderId": YANDEX_FOLDER_ID,
            "lang": "ru-RU",
            "format": "lpcm",
            "sampleRateHertz": "8000",
        }

        for attempt in range(3):
            try:
                client = await self._get_http_client()
                resp = await asyncio.wait_for(
                    client.post(url, headers=headers, content=pcm_data, params=params),
                    timeout=STT_TIMEOUT,
                )
                if resp.status_code == 200:
                    return resp.json().get("result", "")
                if resp.status_code in (429, 500, 502, 503, 504):
                    if attempt < 2:
                        await asyncio.sleep(2 ** attempt)
                        continue
                logger.error("Yandex STT error: %d", resp.status_code)
                return ""
            except asyncio.TimeoutError:
                if attempt < 2:
                    await asyncio.sleep(2 ** attempt)
                    continue
                return ""
            except Exception as e:
                logger.error("Yandex STT error: %s", e)
                return ""
        return ""

    async def agent_chat(self, call_id: str, messages: list[dict], max_tokens: int = 500) -> str:
        if not YANDEX_API_KEY or not YANDEX_FOLDER_ID:
            return ""

        url = "https://llm.api.cloud.yandex.net/foundationModels/v1/completion"
        headers = {
            "Authorization": f"Api-Key {YANDEX_API_KEY}",
            "x-folder-id": YANDEX_FOLDER_ID,
            "Content-Type": "application/json",
        }
        
        yandex_messages = [
            {"role": msg.get("role", "user"), "text": msg.get("content", msg.get("text", ""))}
            for msg in messages
        ]

        body = {
            "modelUri": f"gpt://{YANDEX_FOLDER_ID}/{YANDEX_GPT_MODEL}",
            "completionOptions": {"stream": False, "temperature": 0.6, "maxTokens": str(max_tokens)},
            "messages": yandex_messages,
        }

        for attempt in range(3):
            try:
                client = await self._get_http_client()
                resp = await asyncio.wait_for(
                    client.post(url, headers=headers, json=body),
                    timeout=GPT_TIMEOUT,
                )
                if resp.status_code == 200:
                    data = resp.json()
                    alternatives = data.get("result", {}).get("alternatives", [])
                    if alternatives:
                        return alternatives[0].get("message", {}).get("text", "")
                    return ""
                if resp.status_code in (429, 500, 502, 503, 504):
                    if attempt < 2:
                        await asyncio.sleep(2 ** attempt)
                        continue
                logger.error("Yandex GPT error: %d", resp.status_code)
                return ""
            except asyncio.TimeoutError:
                if attempt < 2:
                    await asyncio.sleep(2 ** attempt)
                    continue
                return ""
            except Exception as e:
                logger.error("Yandex GPT error: %s", e)
                return ""
        return ""

    async def tts_synthesize(self, text: str) -> bytes:
        if not GENVOICE_API_KEY or not text or not GENVOICE_REALTIME_URL:
            return b""

        try:
            async with asyncio.wait_for(
                websockets.connect(
                    GENVOICE_REALTIME_URL,
                    additional_headers={"Authorization": f"Bearer {GENVOICE_API_KEY}"},
                    ping_interval=20,
                    ping_timeout=10,
                ),
                timeout=TTS_TIMEOUT,
            ) as ws:
                await ws.send(json.dumps({
                    "type": "session.begin",
                    "voice_id": GENVOICE_VOICE,
                    "sample_rate": 8000,
                    "format": "pcm",
                }))

                try:
                    response = await asyncio.wait_for(ws.recv(), timeout=10)
                    if json.loads(response).get("type") != "session.ready":
                        return b""
                except asyncio.TimeoutError:
                    return b""

                await ws.send(json.dumps({"type": "text.chunk", "text": text}))
                await ws.send(json.dumps({"type": "text.end"}))

                audio_chunks = []
                while True:
                    try:
                        response = await asyncio.wait_for(ws.recv(), timeout=30)
                        data = json.loads(response)
                        if data.get("type") == "audio.chunk":
                            audio_b64 = data.get("audio")
                            if audio_b64:
                                audio_chunks.append(base64.b64decode(audio_b64))
                        elif data.get("type") in ("session.complete", "session.error"):
                            break
                    except asyncio.TimeoutError:
                        break

                return b"".join(audio_chunks)

        except asyncio.TimeoutError:
            logger.error("GenVoice connection timeout")
            return b""
        except Exception as e:
            logger.error("GenVoice TTS error: %s", e)
            return b""

    async def tts_interrupt(self, call_id: str) -> None:
        async with self._tts_lock:
            task = self._tts_tasks.get(call_id)
            if task and not task.done():
                task.cancel()
                try:
                    await asyncio.wait_for(task, timeout=1.0)
                except (asyncio.CancelledError, asyncio.TimeoutError):
                    pass
                self._tts_tasks.pop(call_id, None)

    async def register_tts_task(self, call_id: str, task: asyncio.Task) -> None:
        async with self._tts_lock:
            self._tts_tasks[call_id] = task

    async def unregister_tts_task(self, call_id: str) -> None:
        async with self._tts_lock:
            self._tts_tasks.pop(call_id, None)

    async def send_to_albato(self, data: dict) -> bool:
        if not ALBATO_WEBHOOK_URL:
            return False

        headers = {"Content-Type": "application/json"}
        if ALBATO_TOKEN:
            headers["Authorization"] = f"Bearer {ALBATO_TOKEN}"
        if "call_id" in data:
            headers["X-Idempotency-Key"] = data["call_id"]

        for attempt in range(3):
            try:
                client = await self._get_http_client()
                resp = await asyncio.wait_for(
                    client.post(ALBATO_WEBHOOK_URL, json=data, headers=headers),
                    timeout=DELIVERY_TIMEOUT,
                )
                if 200 <= resp.status_code < 300:
                    return True
                if resp.status_code in (429, 500, 502, 503, 504):
                    if attempt < 2:
                        await asyncio.sleep(2 ** attempt)
                        continue
                return False
            except Exception:
                if attempt < 2:
                    await asyncio.sleep(2 ** attempt)
                    continue
                return False
        return False

    async def send_telegram_message(self, text: str) -> bool:
        if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
            return False

        if len(text) > 4000:
            text = text[:4000] + "..."

        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        body = {"chat_id": TELEGRAM_CHAT_ID, "text": text}

        for attempt in range(3):
            try:
                client = await self._get_http_client()
                resp = await asyncio.wait_for(
                    client.post(url, json=body),
                    timeout=DELIVERY_TIMEOUT,
                )
                if resp.status_code == 200:
                    return True
                if resp.status_code in (429, 500, 502, 503, 504):
                    if attempt < 2:
                        await asyncio.sleep(2 ** attempt)
                        continue
                return False
            except Exception:
                if attempt < 2:
                    await asyncio.sleep(2 ** attempt)
                    continue
                return False
        return False

    async def send_telegram_result(self, result) -> bool:
        try:
            text = (
                f"📞 Завершён звонок\n\n"
                f"Сценарий: {result.scenario}\n"
                f"Телефон: {result.phone}\n"
                f"Имя: {result.name or '—'}\n"
                f"Интерес: {result.interest or '—'}\n"
                f"Следующий шаг: {result.next_step or '—'}\n\n"
                f"📝 Кратко:\n{result.summary or '—'}"
            )
            return await self.send_telegram_message(text)
        except Exception as e:
            logger.error("send_telegram_result error: %s", e)
            return False
