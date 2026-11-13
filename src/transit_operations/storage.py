from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from typing import Any

from .models import DOMAIN_RECORD_TYPE
from .quality import LEGACY_REJECTION_CODE, RULE_ENGINE_VERSION, truncate_text


class DataStore:
    """Owns structured tables, idempotent writes, rejection records and batch state."""

    def __init__(self, database: str) -> None:
        self.conn = sqlite3.connect(database)
        self.conn.execute("PRAGMA foreign_keys = ON")
        self.conn.executescript("""
        CREATE TABLE IF NOT EXISTS batches (
            batch_id INTEGER PRIMARY KEY AUTOINCREMENT,
            source TEXT NOT NULL,
            status TEXT NOT NULL,
            processed INTEGER NOT NULL DEFAULT 0,
            accepted INTEGER NOT NULL DEFAULT 0,
            rejected INTEGER NOT NULL DEFAULT 0,
            error TEXT,
            started_at TEXT NOT NULL,
            finished_at TEXT
        );
        CREATE TABLE IF NOT EXISTS raw_manifests (
            digest TEXT PRIMARY KEY,
            source TEXT NOT NULL,
            content_type TEXT NOT NULL,
            byte_count INTEGER NOT NULL,
            path TEXT,
            captured_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS records (
            record_key TEXT PRIMARY KEY,
            source TEXT NOT NULL,
            observed_at TEXT NOT NULL,
            payload_json TEXT NOT NULL,
            raw_hash TEXT NOT NULL,
            first_seen_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS rejected_records (
            rejection_id INTEGER PRIMARY KEY AUTOINCREMENT,
            batch_id INTEGER NOT NULL,
            source TEXT,
            error TEXT NOT NULL,
            raw_json TEXT NOT NULL,
            created_at TEXT NOT NULL,
            rule_code TEXT,
            rule_codes_json TEXT,
            severity TEXT,
            outcome TEXT,
            rule_engine_version TEXT,
            FOREIGN KEY(batch_id) REFERENCES batches(batch_id)
        );
        """)
        self._migrate_rejection_columns()
        self.conn.commit()

    def _migrate_rejection_columns(self) -> None:
        """Add rule-engine columns to databases created by older versions.

        Rows written before the engine existed keep their free-form ``error``
        text and are stamped with the synthetic ``LEGACY_REJECTION`` code, so
        they remain distinguishable from rule-code rejections and are still
        reproducible by rule code in queries.
        """

        existing = {
            row[1]
            for row in self.conn.execute("PRAGMA table_info(rejected_records)").fetchall()
        }
        additions = (
            ("rule_code", "TEXT"),
            ("rule_codes_json", "TEXT"),
            ("severity", "TEXT"),
            ("outcome", "TEXT"),
            ("rule_engine_version", "TEXT"),
        )
        for column, declaration in additions:
            if column not in existing:
                self.conn.execute(
                    f"ALTER TABLE rejected_records ADD COLUMN {column} {declaration}"
                )
        self.conn.execute(
            "UPDATE rejected_records SET rule_code = ? "
            "WHERE rule_code IS NULL OR rule_code = ''",
            (LEGACY_REJECTION_CODE,),
        )
        self.conn.execute(
            "UPDATE rejected_records SET rule_codes_json = ? "
            "WHERE rule_codes_json IS NULL OR rule_codes_json = ''",
            (json.dumps([LEGACY_REJECTION_CODE]),),
        )
        self.conn.execute(
            "UPDATE rejected_records SET outcome = 'reject' "
            "WHERE outcome IS NULL OR outcome = ''"
        )

    @staticmethod
    def _now() -> str:
        return datetime.now(timezone.utc).isoformat()

    def start_batch(self, source: str) -> int:
        cursor = self.conn.execute(
            "INSERT INTO batches(source, status, started_at) VALUES (?, 'running', ?)",
            (source, self._now()),
        )
        self.conn.commit()
        return int(cursor.lastrowid)

    def save_manifest(self, digest: str, source: str, content_type: str, byte_count: int, path: str | None) -> None:
        self.conn.execute(
            "INSERT OR IGNORE INTO raw_manifests(digest, source, content_type, byte_count, path, captured_at) VALUES (?, ?, ?, ?, ?, ?)",
            (digest, source, content_type, byte_count, path, self._now()),
        )
        self.conn.commit()

    def previous_raw_hash(self, record_key: str) -> str | None:
        """Return the stored hash for a record key, if a version already exists."""

        row = self.conn.execute(
            "SELECT raw_hash FROM records WHERE record_key = ?", (record_key,)
        ).fetchone()
        return None if row is None else str(row[0])

    def upsert(self, record: DOMAIN_RECORD_TYPE) -> bool:
        now = self._now()
        cursor = self.conn.execute(
            """INSERT INTO records(record_key, source, observed_at, payload_json, raw_hash, first_seen_at, updated_at)
               VALUES (?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(record_key) DO UPDATE SET source=excluded.source, observed_at=excluded.observed_at,
                 payload_json=excluded.payload_json, raw_hash=excluded.raw_hash, updated_at=excluded.updated_at
               WHERE records.raw_hash <> excluded.raw_hash""",
            (record.record_key, record.source, record.observed_at, json.dumps(record.payload, ensure_ascii=False, sort_keys=True), record.raw_hash, now, now),
        )
        self.conn.commit()
        return cursor.rowcount > 0

    def reject(
        self,
        batch_id: int,
        raw: Any,
        source: str | None,
        error: str,
        rule_code: str | None = None,
        rule_codes: tuple[str, ...] | list[str] | None = None,
        severity: str | None = None,
        outcome: str = "reject",
        rule_engine_version: str | None = RULE_ENGINE_VERSION,
    ) -> None:
        """Persist a rejection.

        New callers pass stable rule code(s); old callers that only supply an
        error string get the ``LEGACY_REJECTION`` code automatically. When
        several rules fire, ``rule_code`` holds the first (deterministic,
        catalog-ordered) code and ``rule_codes_json`` holds the full list.
        """

        if not rule_code:
            rule_code = LEGACY_REJECTION_CODE
        if not rule_codes:
            rule_codes = [rule_code]
        self.conn.execute(
            """INSERT INTO rejected_records
                 (batch_id, source, error, raw_json, created_at, rule_code,
                  rule_codes_json, severity, outcome, rule_engine_version)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                batch_id,
                source,
                truncate_text(str(error)),
                json.dumps(raw, ensure_ascii=False, sort_keys=True),
                self._now(),
                rule_code,
                json.dumps(list(rule_codes), ensure_ascii=False),
                severity,
                outcome,
                rule_engine_version,
            ),
        )
        self.conn.commit()

    def rejection_rule_codes(self) -> list[tuple[str, int]]:
        """Deterministic (rule_code, count) summary ordered by code then count."""

        rows = self.conn.execute(
            "SELECT rule_code, COUNT(*) FROM rejected_records GROUP BY rule_code"
        ).fetchall()
        return sorted(((str(code), int(count)) for code, count in rows), key=lambda item: item[0])

    def rejections_for_rule(self, rule_code: str) -> list[dict[str, Any]]:
        """Reproduce every rejection row carrying a given stable rule code."""

        rows = self.conn.execute(
            """SELECT rejection_id, batch_id, source, error, rule_code, rule_codes_json,
                      severity, outcome, rule_engine_version, created_at
                 FROM rejected_records WHERE rule_code = ?
                 ORDER BY rejection_id""",
            (rule_code,),
        ).fetchall()
        return [
            {
                "rejection_id": row[0],
                "batch_id": row[1],
                "source": row[2],
                "error": row[3],
                "rule_code": row[4],
                "rule_codes": json.loads(row[5]) if row[5] else [row[4]],
                "severity": row[6],
                "outcome": row[7],
                "rule_engine_version": row[8],
                "created_at": row[9],
            }
            for row in rows
        ]

    def finish_batch(self, batch_id: int, status: str, processed: int, accepted: int, rejected: int, error: str | None = None) -> None:
        self.conn.execute(
            "UPDATE batches SET status=?, processed=?, accepted=?, rejected=?, error=?, finished_at=? WHERE batch_id=?",
            (status, processed, accepted, rejected, error, self._now(), batch_id),
        )
        self.conn.commit()

    def counts(self) -> dict[str, int]:
        return {
            "records": int(self.conn.execute("SELECT COUNT(*) FROM records").fetchone()[0]),
            "rejected": int(self.conn.execute("SELECT COUNT(*) FROM rejected_records").fetchone()[0]),
            "batches": int(self.conn.execute("SELECT COUNT(*) FROM batches").fetchone()[0]),
            "raw_manifests": int(self.conn.execute("SELECT COUNT(*) FROM raw_manifests").fetchone()[0]),
        }

    def close(self) -> None:
        self.conn.close()
