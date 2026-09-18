"""
Модели данных проекта WOLTRON Voice AI.
"""

import json
from dataclasses import dataclass, asdict, field
from typing import Optional, List


@dataclass
class CallRecord:
    """Запись о звонке."""
    id: str
    direction: str  # "inbound" или "outbound"
    scenario: str
    phone: str
    status: str = "CREATED"
    metadata: Optional[str] = None
    created_at: Optional[str] = None
    started_at: Optional[str] = None
    answered_at: Optional[str] = None
    ended_at: Optional[str] = None
    hangup_reason: Optional[str] = None


@dataclass
class TranscriptRecord:
    """Запись транскрипта."""
    id: Optional[int] = None
    call_id: str = ""
    role: str = ""  # "user" или "assistant"
    text: str = ""
    created_at: Optional[str] = None


@dataclass
class CallResultRecord:
    """Запись результата звонка в БД."""
    id: Optional[int] = None
    call_id: str = ""
    result: str = ""  # JSON-строка
    albato_status: str = "pending"
    telegram_status: str = "pending"
    delivery_retry_count: int = 0
    last_delivery_error: Optional[str] = None
    next_delivery_retry_at: Optional[str] = None
    created_at: Optional[str] = None


@dataclass
class CallResult:
    """
    Структурированный результат звонка.
    Используется для отправки в Albato и Telegram.
    """
    call_id: str
    direction: str
    scenario: str
    phone: str
    name: Optional[str] = None
    interest: Optional[str] = None
    lesson_status: Optional[str] = None
    next_step: Optional[str] = None
    objections: Optional[List[str]] = None
    summary: Optional[str] = None
    transcript: Optional[str] = None

    def to_dict(self, include_none: bool = False) -> dict:
        """Конвертация в словарь для JSON."""
        data = asdict(self)
        if not include_none:
            data = {k: v for k, v in data.items() if v is not None}
        return data

    def to_json(self, include_none: bool = False) -> str:
        """Сериализация в JSON-строку для сохранения в БД."""
        return json.dumps(self.to_dict(include_none=include_none), ensure_ascii=False)

    @classmethod
    def from_json(cls, json_str: str) -> "CallResult":
        """Десериализация из JSON-строки."""
        data = json.loads(json_str)
        return cls(**{k: v for k, v in data.items() if k in cls.__dataclass_fields__})
