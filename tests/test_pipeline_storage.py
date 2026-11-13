"""End-to-end tests: rule engine + pipeline + SQLite persistence."""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest

from transit_operations.application import run_fixture
from transit_operations.normalize import normalize
from transit_operations.pipeline import IngestionPipeline
from transit_operations.quality import (
    FINDINGS_TRUNCATED,
    LEGACY_REJECTION_CODE,
    NORMALIZE_ERROR,
    UNEXPECTED_ERROR,
    RULE_ENGINE_VERSION,
    Outcome,
    RuleConfig,
    evaluate,
)
from transit_operations.storage import DataStore

FIXTURE = Path(__file__).parents[1] / "fixtures" / "sample.json"


def raw_record(
    *,
    status: str = "delayed",
    delay: float | None = 180,
    route: str = "R1",
    stop: str = "S1",
    vehicle: str = "V1",
    observed_at: str = "2026-11-01T08:00:00Z",
    source: str = "operator-a",
) -> dict:
    payload: dict = {
        "route_id": route,
        "stop_id": stop,
        "status": status,
        "delay_seconds": delay,
    }
    # Only include vehicle_id when provided, mirroring real partial payloads.
    if vehicle:
        payload["vehicle_id"] = vehicle
    return {"source": source, "observed_at": observed_at, "payload": payload}


def run_pipeline(database: str, records: list[dict], source: str = "test://unit") -> dict:
    store = DataStore(database)
    try:
        batch_id = store.start_batch(source)
        pipeline = IngestionPipeline(records, source, store, batch_id, normalize)
        return pipeline.run()
    finally:
        store.close()


# ---------------------------------------------------------------------------
# Fixture-level integration (backward-compatible counters)
# ---------------------------------------------------------------------------

def test_fixture_run_keeps_legacy_counters_and_adds_new_ones(tmp_path):
    result = run_fixture(str(FIXTURE), str(tmp_path / "data.sqlite3"), str(tmp_path / "raw"))
    assert result["processed"] == 4
    assert result["accepted"] == 3
    assert result["rejected"] == 1
    # New counters: the fixture's -30s early arrival is now an explicit warning.
    assert result["warnings"] == 1
    assert result["inconclusive"] == 0


def test_fixture_rejection_is_persisted_with_rule_code(tmp_path):
    run_fixture(str(FIXTURE), str(tmp_path / "data.sqlite3"), str(tmp_path / "raw"))
    store = DataStore(str(tmp_path / "data.sqlite3"))
    try:
        summary = dict(store.rejection_rule_codes())
        assert summary.get(NORMALIZE_ERROR) == 1
        rows = store.rejections_for_rule(NORMALIZE_ERROR)
        assert len(rows) == 1
        assert rows[0]["rule_codes"] == [NORMALIZE_ERROR]
        assert rows[0]["outcome"] == "reject"
        assert rows[0]["rule_engine_version"] is None  # never reached the engine
    finally:
        store.close()


# ---------------------------------------------------------------------------
# Every status branch through the full pipeline
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("status", ["scheduled", "boarding", "delayed", "arrived", "departed"])
def test_all_known_statuses_are_persisted(tmp_path, status):
    db = str(tmp_path / "db.sqlite3")
    result = run_pipeline(db, [raw_record(status=status, delay=0)])
    assert result["accepted"] == 1
    assert result["rejected"] == 0
    store = DataStore(db)
    try:
        assert store.counts()["records"] == 1
    finally:
        store.close()


def test_cancelled_with_delay_is_accepted_and_counted_as_warning(tmp_path):
    db = str(tmp_path / "db.sqlite3")
    result = run_pipeline(db, [raw_record(status="cancelled", delay=42)])
    assert result["accepted"] == 1
    assert result["warnings"] == 1
    assert result["rejected"] == 0


def test_early_arrival_is_accepted_with_warning_not_rejected(tmp_path):
    db = str(tmp_path / "db.sqlite3")
    result = run_pipeline(db, [raw_record(status="arrived", delay=-30)])
    assert result["accepted"] == 1
    assert result["warnings"] == 1
    store = DataStore(db)
    try:
        stored = json.loads(
            store.conn.execute("SELECT payload_json FROM records").fetchone()[0]
        )
        assert stored["delay_seconds"] == -30
    finally:
        store.close()


def test_extreme_early_delay_is_rejected_under_reproducible_rule_code(tmp_path):
    db = str(tmp_path / "db.sqlite3")
    result = run_pipeline(db, [raw_record(status="arrived", delay=-7200)])
    assert result["accepted"] == 0
    assert result["rejected"] == 1
    store = DataStore(db)
    try:
        rows = store.rejections_for_rule("DELAY_EXTREME_EARLY")
        assert len(rows) == 1
        assert rows[0]["rule_codes"] == ["DELAY_EXTREME_EARLY"]
        assert rows[0]["severity"] == "error"
        assert rows[0]["rule_engine_version"] == RULE_ENGINE_VERSION
    finally:
        store.close()


def test_unknown_status_is_quarantined_as_inconclusive(tmp_path):
    db = str(tmp_path / "db.sqlite3")
    result = run_pipeline(db, [raw_record(status="detoured")])
    assert result["accepted"] == 0
    assert result["rejected"] == 1
    assert result["inconclusive"] == 1
    store = DataStore(db)
    try:
        rows = store.rejections_for_rule("STATUS_UNKNOWN")
        assert rows[0]["outcome"] == "inconclusive"
        assert rows[0]["severity"] == "unknown"
        # The raw payload is retained for replay after the registry is extended.
        raw_json = store.conn.execute(
            "SELECT raw_json FROM rejected_records WHERE rejection_id = ?",
            (rows[0]["rejection_id"],),
        ).fetchone()[0]
        assert json.loads(raw_json)["payload"]["status"] == "detoured"
    finally:
        store.close()


# ---------------------------------------------------------------------------
# Multi-issue aggregation, ordering and bounded messages in persistence
# ---------------------------------------------------------------------------

def test_multi_issue_rejection_stores_ordered_rule_codes(tmp_path):
    from transit_operations.models import TransitRecord

    db = str(tmp_path / "db.sqlite3")
    # Construct directly: normalization's presence checks are a separate legacy
    # contract; here we exercise engine-level multi-issue aggregation.
    broken = TransitRecord(
        source="operator-a",
        record_key="rk-broken",
        observed_at="2026-11-01T08:00:00Z",
        route_id="  ",          # whitespace -> FIELD_MISSING (error)
        stop_id="S7",
        vehicle_id="",          # -> VEHICLE_ID_MISSING (warning)
        status="detoured",      # -> STATUS_UNKNOWN (inconclusive)
        delay_seconds=-9000.0,  # -> DELAY_EXTREME_EARLY (error)
        payload={},
        raw_hash="h1",
    )
    store = DataStore(db)
    try:
        batch_id = store.start_batch("test://unit")
        pipeline = IngestionPipeline([], "test://unit", store, batch_id, normalize)
        # Drive one record through the same persistence path the pipeline uses.
        from transit_operations.quality import EvaluationContext, evaluate

        result = evaluate(broken, context=EvaluationContext())
        blocking = result.blocking
        pipeline._persist_rejection(
            {"payload": {"status": "detoured"}},
            "operator-a",
            [item.message for item in blocking],
            [item.rule_code for item in blocking],
            blocking[0].severity.value,
            result.outcome.value,
            result.rule_engine_version,
        )
        row = store.conn.execute(
            "SELECT rule_code, rule_codes_json, error FROM rejected_records"
        ).fetchone()
        codes = json.loads(row[1])
        assert codes == list(dict.fromkeys(codes))  # deterministic order
        assert row[0] == codes[0]
        assert "FIELD_MISSING" in codes and "STATUS_UNKNOWN" in codes
        assert "VEHICLE_ID_MISSING" not in codes  # warnings are not blocking rejections
        assert "DELAY_EXTREME_EARLY" in codes
        assert len(row[2]) <= 512
    finally:
        store.close()


def test_long_error_messages_are_capped_in_storage(tmp_path):
    db = str(tmp_path / "db.sqlite3")
    run_pipeline(db, [raw_record(status="x" * 5000)])
    store = DataStore(db)
    try:
        error = store.conn.execute("SELECT error FROM rejected_records").fetchone()[0]
        assert len(error) <= 512
    finally:
        store.close()


# ---------------------------------------------------------------------------
# Replay, idempotency and revisions
# ---------------------------------------------------------------------------

def test_identical_replay_changes_nothing_and_creates_no_rejections(tmp_path):
    db = str(tmp_path / "db.sqlite3")
    # Distinct routes give the two records distinct record_keys.
    records = [
        raw_record(status="arrived", delay=-30, route="R1"),
        raw_record(status="delayed", delay=60, route="R2"),
    ]
    first = run_pipeline(db, records)
    second = run_pipeline(db, [dict(item) for item in records])
    assert first["accepted"] == 2 and first["changed"] == 2
    assert second["accepted"] == 2 and second["changed"] == 0
    assert second["rejected"] == 0
    # The early-arrival record re-emits its DELAY_EARLY_ARRIVAL warning on each
    # evaluation; no RECORD_REVISION fires because the content hashes match.
    assert second["warnings"] == 1
    store = DataStore(db)
    try:
        assert store.counts()["records"] == 2
        assert store.counts()["rejected"] == 0
    finally:
        store.close()


def test_rejected_record_replay_reproduces_same_rule_code(tmp_path):
    db = str(tmp_path / "db.sqlite3")
    bad = [raw_record(status="arrived", delay=-7200)]
    run_pipeline(db, bad)
    run_pipeline(db, [dict(bad[0])])
    store = DataStore(db)
    try:
        rows = store.rejections_for_rule("DELAY_EXTREME_EARLY")
        assert len(rows) == 2  # re-evaluated and rejected again, same stable code
        assert all(row["rule_engine_version"] == RULE_ENGINE_VERSION for row in rows)
        assert store.counts()["records"] == 0
    finally:
        store.close()


def test_content_revision_for_same_record_key_is_accepted_and_upserts(tmp_path):
    db = str(tmp_path / "db.sqlite3")
    run_pipeline(db, [raw_record(status="delayed", delay=60)])
    revised = raw_record(status="delayed", delay=120)  # same identity, new content
    second = run_pipeline(db, [revised])
    assert second["accepted"] == 1
    assert second["changed"] == 1
    assert second["warnings"] == 1  # RECORD_REVISION
    store = DataStore(db)
    try:
        assert store.counts()["records"] == 1
        payload = json.loads(
            store.conn.execute("SELECT payload_json FROM records").fetchone()[0]
        )
        assert payload["delay_seconds"] == 120
    finally:
        store.close()


def test_results_are_independent_of_input_order(tmp_path):
    db_a = str(tmp_path / "a.sqlite3")
    db_b = str(tmp_path / "b.sqlite3")
    records = [
        raw_record(status="arrived", delay=-9000, route="R1"),
        raw_record(status="detoured", route="R2"),
        raw_record(status="delayed", delay=10, route="R3"),
        raw_record(status="cancelled", delay=5, route="R4"),
    ]
    run_pipeline(db_a, records)
    run_pipeline(db_b, list(reversed(records)))

    def codes_by_raw(db_path: str) -> dict[str, list[str]]:
        store = DataStore(db_path)
        try:
            rows = store.conn.execute(
                "SELECT raw_json, rule_codes_json FROM rejected_records"
            ).fetchall()
            return {raw: json.loads(codes) for raw, codes in rows}
        finally:
            store.close()

    assert codes_by_raw(db_a) == codes_by_raw(db_b)


# ---------------------------------------------------------------------------
# Failure recovery: one bad record never aborts the batch
# ---------------------------------------------------------------------------

def test_normalizer_failure_is_coded_and_batch_completes(tmp_path):
    db = str(tmp_path / "db.sqlite3")
    good = raw_record()
    malformed = {"observed_at": "2026-11-01T08:00:00Z", "payload": "not-an-object"}
    result = run_pipeline(db, [good, malformed])
    assert result["processed"] == 2
    assert result["accepted"] == 1
    assert result["rejected"] == 1
    store = DataStore(db)
    try:
        rows = store.rejections_for_rule(NORMALIZE_ERROR)
        assert len(rows) == 1
        batch_status = store.conn.execute(
            "SELECT status FROM batches ORDER BY batch_id DESC"
        ).fetchone()[0]
        assert batch_status == "completed"
    finally:
        store.close()


def test_engine_failure_is_caught_coded_and_does_not_abort_batch(tmp_path, monkeypatch):
    import transit_operations.pipeline as pipeline_module

    def flaky_evaluate(record, config=None, context=None):
        raise RuntimeError("boom")

    monkeypatch.setattr(pipeline_module, "evaluate", flaky_evaluate)
    db = str(tmp_path / "db.sqlite3")
    result = run_pipeline(db, [raw_record(), raw_record(status="arrived")])
    assert result["processed"] == 2
    assert result["rejected"] == 2
    assert result["accepted"] == 0
    store = DataStore(db)
    try:
        rows = store.rejections_for_rule(UNEXPECTED_ERROR)
        assert len(rows) == 2
        assert "boom" in rows[0]["error"]
    finally:
        store.close()


def test_finding_cap_keeps_batches_cheap(tmp_path):
    from transit_operations.models import TransitRecord

    config = RuleConfig(max_findings=2)
    # Construct directly so required-field normalization does not short-circuit
    # the engine; the record carries three independent findings.
    record = TransitRecord(
        source="x",
        record_key="rk",
        observed_at="2026-11-01T08:00:00",  # naive -> warning
        route_id="R1",
        stop_id="S1",
        vehicle_id="",  # missing vehicle -> warning
        status="detoured",  # unknown -> inconclusive
        delay_seconds=None,
        payload={},
        raw_hash="h",
    )
    result = evaluate(record, config=config)
    assert len(result.findings) == 3  # 2 kept + truncation marker
    assert result.findings[-1].rule_code == FINDINGS_TRUNCATED
    assert result.outcome is Outcome.INCONCLUSIVE


# ---------------------------------------------------------------------------
# Legacy database compatibility (plain-text rejection rows)
# ---------------------------------------------------------------------------

LEGACY_SCHEMA = """
CREATE TABLE batches (
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
CREATE TABLE raw_manifests (
    digest TEXT PRIMARY KEY, source TEXT NOT NULL, content_type TEXT NOT NULL,
    byte_count INTEGER NOT NULL, path TEXT, captured_at TEXT NOT NULL
);
CREATE TABLE records (
    record_key TEXT PRIMARY KEY, source TEXT NOT NULL, observed_at TEXT NOT NULL,
    payload_json TEXT NOT NULL, raw_hash TEXT NOT NULL,
    first_seen_at TEXT NOT NULL, updated_at TEXT NOT NULL
);
CREATE TABLE rejected_records (
    rejection_id INTEGER PRIMARY KEY AUTOINCREMENT,
    batch_id INTEGER NOT NULL,
    source TEXT,
    error TEXT NOT NULL,
    raw_json TEXT NOT NULL,
    created_at TEXT NOT NULL
);
"""


def test_legacy_database_with_plain_text_rejections_is_migrated(tmp_path):
    db = str(tmp_path / "legacy.sqlite3")
    conn = sqlite3.connect(db)
    conn.executescript(LEGACY_SCHEMA)
    conn.execute(
        "INSERT INTO batches(source, status, started_at) VALUES ('old', 'completed', '2026-01-01T00:00:00+00:00')"
    )
    conn.execute(
        "INSERT INTO rejected_records(batch_id, source, error, raw_json, created_at) "
        "VALUES (1, 'old-source', 'delay_seconds is earlier than one hour', '{}', '2026-01-01T00:00:00+00:00')"
    )
    conn.commit()
    conn.close()

    store = DataStore(db)  # triggers migration
    try:
        columns = {row[1] for row in store.conn.execute("PRAGMA table_info(rejected_records)")}
        assert {"rule_code", "rule_codes_json", "severity", "outcome", "rule_engine_version"} <= columns
        legacy = store.rejections_for_rule(LEGACY_REJECTION_CODE)
        assert len(legacy) == 1
        assert legacy[0]["rule_codes"] == [LEGACY_REJECTION_CODE]
        assert legacy[0]["outcome"] == "reject"
        # Original free-form explanation is preserved verbatim.
        assert "earlier than one hour" in legacy[0]["error"]
        summary = dict(store.rejection_rule_codes())
        assert summary[LEGACY_REJECTION_CODE] == 1

        # New writes coexist with migrated rows and remain reproducible.
        batch_id = store.start_batch("new")
        pipeline = IngestionPipeline(
            [raw_record(status="arrived", stop="S2", route="R2", delay=-7200)],
            "new",
            store,
            batch_id,
            normalize,
        )
        result = pipeline.run()
        assert result["rejected"] == 1
        assert len(store.rejections_for_rule("DELAY_EXTREME_EARLY")) == 1
        assert len(store.rejections_for_rule(LEGACY_REJECTION_CODE)) == 1
    finally:
        store.close()


def test_legacy_reject_call_with_only_error_string_defaults_to_legacy_code(tmp_path):
    store = DataStore(str(tmp_path / "db.sqlite3"))
    try:
        batch_id = store.start_batch("x")
        store.reject(batch_id, {"k": 1}, "x", "some free-form failure")
        rows = store.rejections_for_rule(LEGACY_REJECTION_CODE)
        assert rows[0]["error"] == "some free-form failure"
        assert rows[0]["rule_codes"] == [LEGACY_REJECTION_CODE]
    finally:
        store.close()


def test_application_marks_batch_failed_and_closes_store_on_crash(tmp_path, monkeypatch):
    import transit_operations.application as application_module

    def exploding_pipeline(*args, **kwargs):
        raise RuntimeError("pipeline blew up")

    monkeypatch.setattr(application_module, "IngestionPipeline", exploding_pipeline)
    with pytest.raises(RuntimeError, match="pipeline blew up"):
        application_module.run_fixture(
            str(FIXTURE),
            str(tmp_path / "crash.sqlite3"),
            str(tmp_path / "raw"),
        )
    conn = sqlite3.connect(str(tmp_path / "crash.sqlite3"))
    try:
        status, error = conn.execute(
            "SELECT status, error FROM batches ORDER BY batch_id DESC LIMIT 1"
        ).fetchone()
        assert status == "failed"
        assert "pipeline blew up" in error
    finally:
        conn.close()


def test_rule_config_rejects_message_limit_below_marker_size():
    from transit_operations.quality import MESSAGE_TRUNCATION_MARKER

    with pytest.raises(ValueError):
        RuleConfig(max_message_length=len(MESSAGE_TRUNCATION_MARKER))
