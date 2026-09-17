"""
Работа с базой данных проекта WOLTRON Voice AI.

SQLite через aiosqlite.

Исправления:
- Delivery retry support
- Migration mechanism
- Atomic operations
"""

import logging
import os
from typing import Optional, List

import aiosqlite

from app.models import CallRecord, CallResult
from config import DATABASE_PATH

logger = logging.getLogger("database")

DB_SCHEMA_VERSION = 2


class Database:
    """Работа с SQLite базой данных."""

    def __init__(self):
        self.db_path = DATABASE_PATH
        self.conn: Optional[aiosqlite.Connection] = None

    def _require_connected(self) -> aiosqlite.Connection:
        if self.conn is None:
            raise RuntimeError("Database is not initialized")
        return self.conn

    async def init(self) -> None:
        if self.conn is not None:
            logger.warning("Database.init() called but connection already exists")
            return

        directory = os.path.dirname(self.db_path)
        if directory:
            try:
                os.makedirs(directory, exist_ok=True)
            except Exception as e:
                logger.error("Cannot create database directory %s: %s", directory, e)
                raise

        try:
            self.conn = await aiosqlite.connect(self.db_path)
            await self.conn.execute("PRAGMA journal_mode=WAL")
            await self.conn.execute("PRAGMA foreign_keys = ON")
            await self.conn.execute("PRAGMA busy_timeout=5000")
        except Exception as e:
            logger.error("Cannot connect to database %s: %s", self.db_path, e)
            raise

        await self._migrate()
        logger.info("Database initialized at %s", self.db_path)
        await self._reconcile_orphan_calls()

    async def _migrate(self) -> None:
        """Простая migration mechanism."""
        conn = self._require_connected()

        try:
            async with conn.execute("SELECT value FROM metadata WHERE key='schema_version'") as cursor:
                row = await cursor.fetchone()
                current_version = int(row[0]) if row else 0
        except aiosqlite.OperationalError:
            current_version = 0

        if current_version < 1:
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS calls (
                    id TEXT PRIMARY KEY,
                    direction TEXT NOT NULL,
                    scenario TEXT NOT NULL DEFAULT 'BEFORE_LESSON',
                    phone TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'CREATED',
                    metadata TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    started_at TIMESTAMP,
                    answered_at TIMESTAMP,
                    ended_at TIMESTAMP,
                    hangup_reason TEXT
                )
            """)

            await conn.execute("""
                CREATE TABLE IF NOT EXISTS transcripts (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    call_id TEXT NOT NULL,
                    role TEXT NOT NULL,
                    text TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (call_id) REFERENCES calls(id)
                )
            """)

            await conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_transcripts_call_id
                ON transcripts(call_id)
            """)

            await conn.execute("""
                CREATE TABLE IF NOT EXISTS call_results (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    call_id TEXT NOT NULL UNIQUE,
                    result TEXT,
                    albato_status TEXT DEFAULT 'pending',
                    telegram_status TEXT DEFAULT 'pending',
                    delivery_retry_count INTEGER DEFAULT 0,
                    last_delivery_error TEXT,
                    next_delivery_retry_at TIMESTAMP,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (call_id) REFERENCES calls(id)
                )
            """)

            await conn.execute("""
                CREATE TABLE IF NOT EXISTS metadata (
                    key TEXT PRIMARY KEY,
                    value TEXT
                )
            """)

            await conn.execute("INSERT OR REPLACE INTO metadata (key, value) VALUES ('schema_version', '1')")
            await conn.commit()
            logger.info("Database migrated to version 1")

        if current_version < 2:
            await conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_call_results_delivery
                ON call_results(albato_status, telegram_status, next_delivery_retry_at)
            """)

            await conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_calls_status
                ON calls(status)
            """)

            await conn.execute("INSERT OR REPLACE INTO metadata (key, value) VALUES ('schema_version', '2')")
            await conn.commit()
            logger.info("Database migrated to version 2")

    async def _reconcile_orphan_calls(self) -> None:
        conn = self._require_connected()
        orphan_statuses = ("DIALING", "RINGING", "ANSWERED", "IN_PROGRESS")
        placeholders = ",".join("?" * len(orphan_statuses))

        try:
            await conn.execute(
                f"""
                UPDATE calls
                SET status = 'CRASHED', hangup_reason = 'server_restart', ended_at = CURRENT_TIMESTAMP
                WHERE status IN ({placeholders})
                """,
                orphan_statuses
            )
            await conn.commit()
            logger.info("Orphan calls reconciled")
        except Exception as e:
            logger.error("Failed to reconcile orphan calls: %s", e)

    async def close(self) -> None:
        if self.conn is not None:
            try:
                await self.conn.close()
            except Exception as e:
                logger.error("Error closing database: %s", e)
            finally:
                self.conn = None
                logger.info("Database connection closed")

    async def create_call(
        self,
        call_id: str,
        direction: str,
        scenario: str,
        phone: str,
        metadata: Optional[str] = None,
    ) -> None:
        conn = self._require_connected()
        if scenario not in ("BEFORE_LESSON", "AFTER_LESSON"):
            logger.warning("Invalid scenario %s, defaulting to BEFORE_LESSON", scenario)
            scenario = "BEFORE_LESSON"

        await conn.execute(
            """
            INSERT INTO calls (id, direction, scenario, phone, metadata)
            VALUES (?, ?, ?, ?, ?)
            """,
            (call_id, direction, scenario, phone, metadata),
        )
        await conn.commit()
        logger.info("Call created: %s (direction=%s, scenario=%s, phone=%s)",
                     call_id, direction, scenario, phone[:8] + "***")

    async def update_call_status(
        self,
        call_id: str,
        status: str,
        hangup_reason: Optional[str] = None,
    ) -> None:
        conn = self._require_connected()
        valid_statuses = (
            "CREATED", "DIALING", "RINGING", "ANSWERED", "IN_PROGRESS",
            "ENDED", "FAILED", "NO_ANSWER", "BUSY", "REJECTED",
            "CANCELLED", "REMOTE_HANGUP", "LOCAL_HANGUP", "CRASHED", "TIMEOUT"
        )
        if status not in valid_statuses:
            logger.warning("Invalid status %s for call %s", status, call_id)

        if hangup_reason:
            await conn.execute(
                "UPDATE calls SET status = ?, hangup_reason = ? WHERE id = ?",
                (status, hangup_reason, call_id),
            )
        else:
            await conn.execute(
                "UPDATE calls SET status = ? WHERE id = ?",
                (status, call_id),
            )
        await conn.commit()

    async def set_call_started(self, call_id: str) -> None:
        conn = self._require_connected()
        await conn.execute(
            "UPDATE calls SET started_at = CURRENT_TIMESTAMP WHERE id = ?",
            (call_id,),
        )
        await conn.commit()

    async def set_call_answered(self, call_id: str) -> None:
        conn = self._require_connected()
        await conn.execute(
            "UPDATE calls SET answered_at = CURRENT_TIMESTAMP, status = 'IN_PROGRESS' WHERE id = ?",
            (call_id,),
        )
        await conn.commit()

    async def set_call_ended(self, call_id: str, hangup_reason: str = "unknown") -> None:
        conn = self._require_connected()
        await conn.execute(
            """
            UPDATE calls
            SET ended_at = CURRENT_TIMESTAMP, status = 'ENDED', hangup_reason = ?
            WHERE id = ?
            """,
            (hangup_reason, call_id),
        )
        await conn.commit()

    async def get_call(self, call_id: str) -> Optional[CallRecord]:
        conn = self._require_connected()
        async with conn.execute("SELECT * FROM calls WHERE id = ?", (call_id,)) as cursor:
            row = await cursor.fetchone()
            if not row:
                return None
            return CallRecord(
                id=row[0],
                direction=row[1],
                scenario=row[2],
                phone=row[3],
                status=row[4],
                metadata=row[5],
                created_at=row[6],
                started_at=row[7],
                answered_at=row[8] if len(row) > 8 else None,
                ended_at=row[9] if len(row) > 9 else None,
            )

    async def add_transcript(self, call_id: str, role: str, text: str) -> None:
        conn = self._require_connected()
        await conn.execute(
            "INSERT INTO transcripts (call_id, role, text) VALUES (?, ?, ?)",
            (call_id, role, text),
        )
        await conn.commit()

    async def get_transcripts(self, call_id: str) -> List[dict]:
        conn = self._require_connected()
        async with conn.execute(
            "SELECT role, text, created_at FROM transcripts WHERE call_id = ? ORDER BY id ASC",
            (call_id,),
        ) as cursor:
            rows = await cursor.fetchall()
            return [{"role": r[0], "text": r[1], "created_at": r[2]} for r in rows]

    async def get_conversation_history(self, call_id: str, max_messages: int = 20) -> List[dict]:
        if max_messages < 1:
            return []

        conn = self._require_connected()
        async with conn.execute(
            "SELECT role, text FROM transcripts WHERE call_id = ? ORDER BY id DESC LIMIT ?",
            (call_id, max_messages),
        ) as cursor:
            rows = await cursor.fetchall()
            return [{"role": r[0], "content": r[1]} for r in reversed(rows)]

    async def save_call_result(self, call_id: str, result: str) -> bool:
        if not isinstance(result, str):
            raise TypeError(f"save_call_result expects serialized str, got {type(result).__name__}")

        conn = self._require_connected()
        try:
            await conn.execute(
                """
                INSERT INTO call_results (call_id, result, albato_status, telegram_status)
                VALUES (?, ?, 'pending', 'pending')
                """,
                (call_id, result),
            )
            await conn.commit()
            logger.info("Call result saved for %s", call_id)
            return True
        except aiosqlite.IntegrityError:
            logger.warning("Call result already exists for %s, skipping", call_id)
            return False

    async def update_delivery_status(
        self,
        call_id: str,
        albato_status: Optional[str] = None,
        telegram_status: Optional[str] = None,
        delivery_error: Optional[str] = None,
    ) -> None:
        conn = self._require_connected()
        updates = []
        params = []

        if albato_status:
            updates.append("albato_status = ?")
            params.append(albato_status)

        if telegram_status:
            updates.append("telegram_status = ?")
            params.append(telegram_status)

        if delivery_error:
            updates.append("last_delivery_error = ?")
            updates.append("delivery_retry_count = delivery_retry_count + 1")
            updates.append("next_delivery_retry_at = datetime('now', '+1 minute')")
            params.append(delivery_error)

        if not updates:
            return

        params.append(call_id)
        await conn.execute(
            f"UPDATE call_results SET {', '.join(updates)} WHERE call_id = ?",
            params,
        )
        await conn.commit()

    async def get_call_result(self, call_id: str) -> Optional[str]:
        conn = self._require_connected()
        async with conn.execute(
            "SELECT result FROM call_results WHERE call_id = ?",
            (call_id,),
        ) as cursor:
            row = await cursor.fetchone()
            return row[0] if row else None

    async def get_pending_deliveries(self, limit: int = 10) -> List[dict]:
        conn = self._require_connected()
        async with conn.execute(
            """
            SELECT cr.call_id, cr.result, cr.albato_status, cr.telegram_status,
                   cr.delivery_retry_count
            FROM call_results cr
            WHERE (cr.albato_status = 'pending' OR cr.telegram_status = 'pending')
              AND (cr.next_delivery_retry_at IS NULL
                   OR cr.next_delivery_retry_at <= datetime('now'))
              AND cr.delivery_retry_count < 5
            LIMIT ?
            """,
            (limit,),
        ) as cursor:
            rows = await cursor.fetchall()
            return [
                {
                    "call_id": r[0],
                    "result": r[1],
                    "albato_status": r[2],
                    "telegram_status": r[3],
                    "retry_count": r[4],
                }
                for r in rows
            ]

    async def get_recent_calls(self, limit: int = 10) -> List[dict]:
        """Получение последних звонков для истории."""
        conn = self._require_connected()
        async with conn.execute(
            "SELECT id, scenario, phone, status, metadata, started_at, ended_at FROM calls ORDER BY created_at DESC LIMIT ?",
            (limit,),
        ) as cursor:
            rows = await cursor.fetchall()
            return [{
                "id": r[0],
                "scenario": r[1],
                "phone": r[2],
                "status": r[3],
                "metadata": r[4],
                "started_at": r[5],
                "ended_at": r[6]
            } for r in rows]
