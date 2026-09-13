"""
Конфигурация проекта WOLTRON Voice AI.
Единый источник конфигурации для всех компонентов.
"""

import os
import logging
from typing import Set, Optional

from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger("config")


def _parse_set(env_name: str, default: str = "") -> Set[str]:
    raw = os.getenv(env_name, default) or ""
    if not raw.strip():
        return set()
    items = {item.strip() for item in raw.split(",")}
    return {item for item in items if item}


def _parse_int(env_name: str, default: int, min_value: Optional[int] = None, max_value: Optional[int] = None) -> int:
    raw = os.getenv(env_name)
    if raw is None or not raw.strip():
        return default
    try:
        value = int(raw.strip())
    except ValueError:
        logger.warning("Invalid int value for %s='%s', using default=%d", env_name, raw, default)
        return default
    if min_value is not None and value < min_value:
        logger.warning("Value %s=%d below min=%d, using default=%d", env_name, value, min_value, default)
        return default
    if max_value is not None and value > max_value:
        logger.warning("Value %s=%d above max=%d, using default=%d", env_name, value, max_value, default)
        return default
    return value


def _parse_float(env_name: str, default: float, min_value: Optional[float] = None, max_value: Optional[float] = None) -> float:
    raw = os.getenv(env_name)
    if raw is None or not raw.strip():
        return default
    try:
        value = float(raw.strip())
    except ValueError:
        logger.warning("Invalid float for %s='%s', using default=%f", env_name, raw, default)
        return default
    if min_value is not None and value < min_value:
        return min_value
    if max_value is not None and value > max_value:
        return max_value
    return value


# === Plusofon SIP ===
PLUSOFON_SIP_HOST = os.getenv("PLUSOFON_SIP_HOST", "")
PLUSOFON_SIP_PORT = _parse_int("PLUSOFON_SIP_PORT", 5060, min_value=1, max_value=65535)
PLUSOFON_SIP_USER = os.getenv("PLUSOFON_SIP_USER", "")
PLUSOFON_SIP_PASSWORD = os.getenv("PLUSOFON_SIP_PASSWORD", "")

# LIVE-TEST REQUIRED: формат R-URI и From для Plusofon
OUTBOUND_RURI_TEMPLATE = os.getenv("OUTBOUND_RURI_TEMPLATE", "sip:{phone}@{host}")
OUTBOUND_FROM_TEMPLATE = os.getenv("OUTBOUND_FROM_TEMPLATE", "<sip:{user}@{host}>")

# === RTP ===
RTP_PORT_MIN = _parse_int("RTP_PORT_MIN", 10000, min_value=1024, max_value=65535)
RTP_PORT_MAX = _parse_int("RTP_PORT_MAX", 10100, min_value=1024, max_value=65535)

if RTP_PORT_MAX < RTP_PORT_MIN:
    logger.warning("RTP_PORT_MAX (%d) < RTP_PORT_MIN (%d), swapping", RTP_PORT_MAX, RTP_PORT_MIN)
    RTP_PORT_MIN, RTP_PORT_MAX = RTP_PORT_MAX, RTP_PORT_MIN

# === Network ===
PUBLIC_IP = os.getenv("PUBLIC_IP", "")

# === Security ===
TRUSTED_SIP_IPS = _parse_set("TRUSTED_SIP_IPS")
BLACKLIST_SIP_IPS = _parse_set("BLACKLIST_SIP_IPS")
SIP_RATE_LIMIT_PER_IP = _parse_int("SIP_RATE_LIMIT_PER_IP", 10, min_value=1, max_value=1000)

# === Yandex ===
YANDEX_API_KEY = os.getenv("YANDEX_API_KEY", "")
YANDEX_FOLDER_ID = os.getenv("YANDEX_FOLDER_ID", "")
YANDEX_GPT_MODEL = os.getenv("YANDEX_GPT_MODEL", "yandexgpt/latest")

# === GenVoice TTS ===
# LIVE-TEST REQUIRED: точный endpoint GenVoice
GENVOICE_API_KEY = os.getenv("GENVOICE_API_KEY", "")
GENVOICE_REALTIME_URL = os.getenv("GENVOICE_REALTIME_URL", "")
GENVOICE_VOICE = os.getenv("GENVOICE_VOICE", "filipp")

# === Albato ===
# LIVE-TEST REQUIRED: точный формат авторизации
ALBATO_WEBHOOK_URL = os.getenv("ALBATO_WEBHOOK_URL", "")
ALBATO_TOKEN = os.getenv("ALBATO_TOKEN", "")

# === Telegram ===
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
TELEGRAM_ADMIN_IDS = _parse_set("TELEGRAM_ADMIN_IDS")

# Telegram Proxy (SOCKS5)
TELEGRAM_PROXY_URL = os.getenv("TELEGRAM_PROXY_URL", "")

# === Database ===
DATABASE_PATH = os.getenv("DATABASE_PATH", "/app/data/woltron.db")

# === VAD ===
SPEECH_RMS_THRESHOLD = _parse_int("SPEECH_RMS_THRESHOLD", 300, min_value=0, max_value=10000)
SILENCE_TO_END_MS = _parse_int("SILENCE_TO_END_MS", 800, min_value=100, max_value=30000)
MAX_UTTERANCE_MS = _parse_int("MAX_UTTERANCE_MS", 15000, min_value=1000, max_value=120000)
VAD_PREROLL_MS = _parse_int("VAD_PREROLL_MS", 200, min_value=0, max_value=2000)

# === Limits ===
MAX_CONCURRENT_CALLS = _parse_int("MAX_CONCURRENT_CALLS", 10, min_value=1, max_value=100)
MAX_CALL_DURATION = _parse_int("MAX_CALL_DURATION", 1800, min_value=60, max_value=7200)
MEDIA_IDLE_TIMEOUT = _parse_int("MEDIA_IDLE_TIMEOUT", 60, min_value=10, max_value=300)
MAX_AUDIO_BUFFER_BYTES = _parse_int("MAX_AUDIO_BUFFER_BYTES", 160000, min_value=10000, max_value=1000000)
MAX_RTP_BUFFER_BYTES = _parse_int("MAX_RTP_BUFFER_BYTES", 320000, min_value=10000, max_value=2000000)

# === SIP Timeouts (RFC 3261 compatible) ===
SIP_T1 = _parse_float("SIP_T1", 0.5, min_value=0.1, max_value=5.0)
SIP_T2 = _parse_float("SIP_T2", 4.0, min_value=1.0, max_value=10.0)
SIP_TIMER_B = _parse_float("SIP_TIMER_B", 32.0, min_value=10.0, max_value=120.0)
SIP_TIMER_F = _parse_float("SIP_TIMER_F", 32.0, min_value=10.0, max_value=120.0)
SIP_MAX_RETRANSMITS = _parse_int("SIP_MAX_RETRANSMITS", 7, min_value=1, max_value=15)

# === API Timeouts ===
STT_TIMEOUT = _parse_float("STT_TIMEOUT", 10.0, min_value=1.0, max_value=60.0)
GPT_TIMEOUT = _parse_float("GPT_TIMEOUT", 15.0, min_value=1.0, max_value=60.0)
TTS_TIMEOUT = _parse_float("TTS_TIMEOUT", 30.0, min_value=1.0, max_value=120.0)
DELIVERY_TIMEOUT = _parse_float("DELIVERY_TIMEOUT", 10.0, min_value=1.0, max_value=60.0)

# === Delivery Retry ===
DELIVERY_RETRY_INTERVAL = _parse_int("DELIVERY_RETRY_INTERVAL", 60, min_value=10, max_value=3600)
DELIVERY_MAX_RETRIES = _parse_int("DELIVERY_MAX_RETRIES", 5, min_value=1, max_value=20)

# === Heartbeat ===
HEARTBEAT_INTERVAL = _parse_int("HEARTBEAT_INTERVAL", 30, min_value=5, max_value=600)
HEARTBEAT_FILE = os.getenv("HEARTBEAT_FILE", "/app/data/heartbeat.txt")

# === Development ===
DEVELOPMENT_MODE = os.getenv("DEVELOPMENT_MODE", "false").strip().lower() == "true"
STRICT_ENV = os.getenv("STRICT_ENV", "true").strip().lower() == "true"
SIP_CAN_START = bool(PUBLIC_IP and PUBLIC_IP != "127.0.0.1")


def validate_config() -> bool:
    """Валидация конфигурации. При STRICT_ENV=true проверяет критичные параметры."""
    if not STRICT_ENV:
        logger.info("STRICT_ENV=false, skipping strict validation")
        return True

    errors = []
    critical = [
        ("PUBLIC_IP", PUBLIC_IP, "публичный IP сервера"),
        ("PLUSOFON_SIP_HOST", PLUSOFON_SIP_HOST, "адрес SIP-сервера Plusofon"),
        ("PLUSOFON_SIP_USER", PLUSOFON_SIP_USER, "SIP-пользователь Plusofon"),
        ("PLUSOFON_SIP_PASSWORD", PLUSOFON_SIP_PASSWORD, "SIP-пароль Plusofon"),
    ]

    for name, value, desc in critical:
        if not value:
            errors.append(f"CRITICAL: Отсутствует {name} ({desc})")

    warnings = []
    if not YANDEX_API_KEY or not YANDEX_FOLDER_ID:
        warnings.append("YANDEX_API_KEY / YANDEX_FOLDER_ID не настроены — STT/GPT не будут работать")
    if not GENVOICE_API_KEY or not GENVOICE_REALTIME_URL:
        warnings.append("GENVOICE_API_KEY / GENVOICE_REALTIME_URL не настроены — TTS не будет работать")
    if not TELEGRAM_BOT_TOKEN:
        warnings.append("TELEGRAM_BOT_TOKEN не настроен — Telegram отключён")
    if not ALBATO_WEBHOOK_URL:
        warnings.append("ALBATO_WEBHOOK_URL не настроен — Albato отключён")
    if not TRUSTED_SIP_IPS and not DEVELOPMENT_MODE:
        errors.append("CRITICAL: TRUSTED_SIP_IPS пуст в production — небезопасно")
    if not TELEGRAM_ADMIN_IDS:
        warnings.append("TELEGRAM_ADMIN_IDS пуст — команда /call будет недоступна")

    for w in warnings:
        logger.warning(w)

    if errors:
        for e in errors:
            logger.error(e)
        return False

    return True
