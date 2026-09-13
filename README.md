# ASSISTANT_SCHOOL
Ассистент для обзвона учеников в онлайн школе


# WOLTRON Voice AI

Голосовой ассистент для онлайн-школы. Ведёт телефонные разговоры через Plusofon.

## Статус

**READY FOR LIVE TESTING** — но **NOT PRODUCTION READY** без реального звонка через Plusofon.

## Архитектура

Telegram → SIPWorker → Plusofon SIP → RTP → VoiceEngine
↓
Yandex STT → Agent/GPT → GenVoice TTS
↓
Albato + Telegram (результат)
12345678910
Команды Telegram
/start — справка
/call <номер> — исходящий звонок
/terminate <call_id> — завершить звонок
/status — активные звонки
LIVE TEST PROCEDURE
Запуск: docker compose up -d && docker compose logs -f
Проверка REGISTER: в логах REGISTER sent → REGISTERED
Telegram /status: "Нет активных звонков"
Telegram /call +79991234567: "✅ Звонок инициирован"
Ожидание ответа: INVITE sent, 100 Trying, 180 Ringing, 200 OK
После ACK: RTP established, VoiceEngine started
Говорите в трубку: STT recognized: <text>
Ответ ассистента: GPT response: <text>, TTS synthesized
Перебейте ассистента: TTS interrupted
Повесьте трубку: BYE received, Call ended, Result saved
Проверка Albato: Albato sent или Albato failed
Проверка Telegram: сообщение с результатом
Проверка DB: sqlite3 data/woltron.db "SELECT * FROM calls;"
Известные ограничения
SIP retransmission — минимальная реализация (RFC 3261 timers)
Нет jitter buffer (простая обработка out-of-order)
GenVoice protocol не верифицирован
Albato idempotency зависит от endpoint
Порты
SIP: UDP 5060
RTP: UDP 10000-10100
Environment
См. .env.example для полного списка.
Критичные для production:
PLUSOFON_SIP_HOST/USER/PASSWORD
PUBLIC_IP
TRUSTED_SIP_IPS (обязательно в production!)
YANDEX_API_KEY/FOLDER_ID
GENVOICE_API_KEY
TELEGRAM_BOT_TOKEN/ADMIN_IDS
