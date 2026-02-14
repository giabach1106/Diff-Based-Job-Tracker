"""SQLite persistence layer for deduplication and state tracking."""

from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from pathlib import Path


class Database:
    """Simple SQLite wrapper used by the job tracker."""

    def __init__(self, database_path: str) -> None:
        db_path = Path(database_path)
        db_path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(str(db_path))
        self.conn.row_factory = sqlite3.Row

    def init_schema(self) -> None:
        """Create tables if they do not exist."""

        with self.conn:
            self.conn.execute(
                """
                CREATE TABLE IF NOT EXISTS processed_jobs (
                    link_hash TEXT PRIMARY KEY,
                    company TEXT,
                    role TEXT,
                    score INTEGER,
                    notified INTEGER,
                    created_at TEXT NOT NULL
                )
                """
            )
            self.conn.execute(
                """
                CREATE TABLE IF NOT EXISTS state (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                )
                """
            )

    def get_last_commit_sha(self) -> str | None:
        """Return the saved last processed commit SHA, if any."""

        cursor = self.conn.execute(
            "SELECT value FROM state WHERE key = ?",
            ("last_commit_sha",),
        )
        row = cursor.fetchone()
        return None if row is None else str(row["value"])

    def set_last_commit_sha(self, sha: str) -> None:
        """Persist the last processed commit SHA."""

        with self.conn:
            self.conn.execute(
                """
                INSERT INTO state(key, value)
                VALUES(?, ?)
                ON CONFLICT(key) DO UPDATE SET value = excluded.value
                """,
                ("last_commit_sha", sha),
            )

    def exists(self, link_hash: str) -> bool:
        """Return True when the hashed apply link was already processed."""

        cursor = self.conn.execute(
            "SELECT 1 FROM processed_jobs WHERE link_hash = ? LIMIT 1",
            (link_hash,),
        )
        return cursor.fetchone() is not None

    def insert_processed_job(
        self,
        link_hash: str,
        company: str,
        role: str,
        score: int,
        notified: bool,
    ) -> None:
        """Insert a processed job row."""

        created_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
        with self.conn:
            self.conn.execute(
                """
                INSERT INTO processed_jobs(link_hash, company, role, score, notified, created_at)
                VALUES(?, ?, ?, ?, ?, ?)
                """,
                (link_hash, company, role, score, int(notified), created_at),
            )

    def close(self) -> None:
        """Close the underlying sqlite connection."""

        self.conn.close()
