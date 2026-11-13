"""Unit tests for the deterministic quality rule engine and event taxonomy."""

from __future__ import annotations

import math

import pytest

from transit_operations.events import (
    EventCategory,
    EventClassification,
    classify,
    classify_event,
)
from transit_operations.models import TransitRecord
from transit_operations.quality import (
    FINDINGS_TRUNCATED,
    LEGACY_REJECTION_CODE,
    MESSAGE_TRUNCATION_MARKER,
    NORMALIZE_ERROR,
    RULE_CODES,
    RULE_ENGINE_VERSION,
    EvaluationContext,
    EvaluationResult,
    Finding,
    Outcome,
    Rule,
    RuleConfig,
    RuleEngine,
    Severity,
    config_signature,
    evaluate,
    explain_rule,
    parse_observed_at,
    timestamp_is_aware,
    truncate_text,
    validate,
)


def make_record(
    *,
    status: str = "delayed",
    delay: float | None = 180.0,
    route: str = "R1",
    stop: str = "S1",
    vehicle: str = "V1",
    observed_at: str = "2026-11-01T08:00:00Z",
    record_key: str = "r1|s1|v1|2026-11-01t08:00:00z",
    raw_hash: str = "hash-1",
    source: str = "operator-a",
) -> TransitRecord:
    payload = {
        "route_id": route,
        "stop_id": stop,
        "vehicle_id": vehicle,
        "status": status,
        "delay_seconds": delay,
    }
    return TransitRecord(
        source=source,
        record_key=record_key,
        observed_at=observed_at,
        route_id=route,
        stop_id=stop,
        vehicle_id=vehicle,
        status=status,
        delay_seconds=delay,
        payload=payload,
        raw_hash=raw_hash,
    )


# ---------------------------------------------------------------------------
# Event classification: every status branch
# ---------------------------------------------------------------------------

@pytest.mark.parametrize(
    ("status", "legacy", "category"),
    [
        ("delayed", "delay", EventCategory.DELAY),
        ("cancelled", "cancellation", EventCategory.CANCELLATION),
        ("arrived", "movement", EventCategory.MOVEMENT),
        ("departed", "movement", EventCategory.MOVEMENT),
        ("scheduled", "schedule", EventCategory.SCHEDULE),
        ("boarding", "schedule", EventCategory.SCHEDULE),
    ],
)
def test_classify_event_covers_every_known_status(status, legacy, category):
    record = make_record(status=status)
    assert classify_event(record) == legacy
    result = classify(record)
    assert isinstance(result, EventClassification)
    assert result.category is category
    assert result.known_status is True


def test_unknown_status_keeps_legacy_schedule_but_is_machine_readable():
    record = make_record(status="offline")
    assert classify_event(record) == "schedule"  # historical fallback
    result = classify(record)
    assert result.category is EventCategory.UNKNOWN
    assert result.known_status is False


# ---------------------------------------------------------------------------
# Outcome triage across statuses
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("status", ["scheduled", "boarding", "delayed", "arrived", "departed"])
def test_known_statuses_with_sane_data_are_accepted(status):
    result = evaluate(make_record(status=status, delay=0))
    assert result.outcome is Outcome.ACCEPT
    assert result.blocking == ()


def test_cancelled_without_delay_is_clean_accept():
    result = evaluate(make_record(status="cancelled", delay=None))
    assert result.outcome is Outcome.ACCEPT
    assert result.rule_codes == ()


def test_unknown_status_is_inconclusive_not_rejected_or_accepted():
    result = evaluate(make_record(status="teleported"))
    assert result.outcome is Outcome.INCONCLUSIVE
    assert result.rule_codes == ("STATUS_UNKNOWN",)
    assert result.blocking[0].severity is Severity.UNKNOWN
    # Legacy validate() still surfaces it as a blocking message.
    assert validate(make_record(status="teleported"))


def test_error_outranks_unknown_when_both_apply():
    record = make_record(status="teleported", route="", delay=-4000)
    result = evaluate(record)
    assert result.outcome is Outcome.REJECT
    assert "STATUS_UNKNOWN" in result.rule_codes
    assert "FIELD_MISSING" in result.rule_codes
    assert "DELAY_EXTREME_EARLY" in result.rule_codes


# ---------------------------------------------------------------------------
# Delay boundary values
# ---------------------------------------------------------------------------

@pytest.mark.parametrize(
    ("delay", "outcome", "code"),
    [
        (0, Outcome.ACCEPT, None),
        (-1, Outcome.ACCEPT, "DELAY_EARLY_ARRIVAL"),
        (-3599, Outcome.ACCEPT, "DELAY_EARLY_ARRIVAL"),
        (-3600, Outcome.ACCEPT, "DELAY_EARLY_ARRIVAL"),  # boundary itself is a warning
        (-3600.0001, Outcome.REJECT, "DELAY_EXTREME_EARLY"),
        (-7200, Outcome.REJECT, "DELAY_EXTREME_EARLY"),
        (86400, Outcome.ACCEPT, None),  # inclusive boundary
        (86400.0001, Outcome.REJECT, "DELAY_OUT_OF_RANGE"),
        (-86400.0001, Outcome.REJECT, "DELAY_OUT_OF_RANGE"),
    ],
)
def test_delay_boundaries_are_exact(delay, outcome, code):
    result = evaluate(make_record(status="arrived", delay=delay))
    assert result.outcome is outcome
    if code is None:
        assert result.rule_codes == ()
    else:
        assert code in result.rule_codes


def test_non_finite_delay_is_rejected():
    result = evaluate(make_record(delay=math.nan))
    assert result.outcome is Outcome.REJECT
    assert result.rule_codes[0] == "DELAY_NOT_FINITE"
    result = evaluate(make_record(delay=math.inf))
    assert "DELAY_NOT_FINITE" in result.rule_codes


def test_missing_delay_is_acceptable():
    result = evaluate(make_record(delay=None))
    assert result.outcome is Outcome.ACCEPT
    assert result.rule_codes == ()


# ---------------------------------------------------------------------------
# Warnings (non-breaking) vs destructive rejections
# ---------------------------------------------------------------------------

def test_early_arrival_is_a_warning_and_still_validates_legacy_clean():
    record = make_record(status="arrived", delay=-120)
    result = evaluate(record)
    assert result.outcome is Outcome.ACCEPT
    assert result.warnings[0].rule_code == "DELAY_EARLY_ARRIVAL"
    assert validate(record) == []  # warnings never break the legacy contract


def test_cancelled_event_carrying_delay_is_warned_not_rejected():
    record = make_record(status="cancelled", delay=300)
    result = evaluate(record)
    assert result.outcome is Outcome.ACCEPT
    assert result.rule_codes == ("CANCELLED_WITH_DELAY",)
    assert validate(record) == []


def test_delayed_status_with_negative_delay_is_a_contradiction_warning():
    record = make_record(status="delayed", delay=-45)
    result = evaluate(record)
    assert result.outcome is Outcome.ACCEPT
    # Negative delay is itself mild early running; the status/delay
    # contradiction adds a second, specific warning.
    assert result.rule_codes == ("DELAY_EARLY_ARRIVAL", "DELAYED_STATUS_NEGATIVE_DELAY")
    assert validate(record) == []


def test_missing_vehicle_is_warning_but_missing_route_or_stop_rejects():
    assert evaluate(make_record(vehicle="")).outcome is Outcome.ACCEPT
    assert evaluate(make_record(vehicle="   ")).warnings[0].rule_code == "VEHICLE_ID_MISSING"
    assert evaluate(make_record(route="")).outcome is Outcome.REJECT
    assert evaluate(make_record(stop=" ")).outcome is Outcome.REJECT


def test_naive_timestamp_warns_but_aware_timestamps_are_silent():
    naive = evaluate(make_record(observed_at="2026-11-01T08:00:00"))
    assert naive.outcome is Outcome.ACCEPT
    assert naive.rule_codes == ("OBSERVED_AT_NAIVE",)
    assert validate(make_record(observed_at="2026-11-01T08:00:00")) == []

    aware_utc = evaluate(make_record(observed_at="2026-11-01T08:00:00Z"))
    aware_offset = evaluate(make_record(observed_at="2026-11-01T16:00:00+08:00"))
    assert aware_utc.rule_codes == ()
    assert aware_offset.rule_codes == ()


def test_unparseable_timestamp_is_a_rejection():
    result = evaluate(make_record(observed_at="not-a-timestamp"))
    assert result.outcome is Outcome.REJECT
    assert result.rule_codes == ("OBSERVED_AT_UNPARSEABLE",)


def test_timestamp_helpers():
    assert parse_observed_at("2026-11-01T08:00:00Z") is not None
    assert parse_observed_at("garbage") is None
    assert parse_observed_at("") is None
    assert timestamp_is_aware("2026-11-01T08:00:00Z") is True
    assert timestamp_is_aware("2026-11-01T08:00:00") is False


# ---------------------------------------------------------------------------
# Record revisions for the same record_key
# ---------------------------------------------------------------------------

def test_first_seen_record_has_no_revision_finding():
    result = evaluate(make_record(), context=EvaluationContext(previous_raw_hash=None))
    assert "RECORD_REVISION" not in result.rule_codes


def test_identical_replay_is_not_a_revision():
    result = evaluate(
        make_record(raw_hash="h"),
        context=EvaluationContext(previous_raw_hash="h"),
    )
    assert "RECORD_REVISION" not in result.rule_codes


def test_content_revision_is_a_warning_and_still_accepted():
    result = evaluate(
        make_record(raw_hash="new-hash"),
        context=EvaluationContext(previous_raw_hash="old-hash"),
    )
    assert result.outcome is Outcome.ACCEPT
    assert result.rule_codes == ("RECORD_REVISION",)
    assert result.warnings[0].field == "record_key"


# ---------------------------------------------------------------------------
# Determinism, aggregation ordering and bounded cost
# ---------------------------------------------------------------------------

def test_finding_order_follows_the_catalog_independent_of_input_order():
    broken = make_record(status="mystery", route="", stop="", vehicle="", delay=-9999)
    first = evaluate(broken)
    second = evaluate(broken)
    assert first.rule_codes == second.rule_codes
    # Completeness rules precede status, which precedes delay rules.
    assert first.rule_codes.index("FIELD_MISSING") < first.rule_codes.index("STATUS_UNKNOWN")
    assert first.rule_codes.index("STATUS_UNKNOWN") < first.rule_codes.index("DELAY_EXTREME_EARLY")


def test_rule_codes_are_stable_unique_and_documented():
    assert tuple(RULE_CODES) == tuple(dict.fromkeys(RULE_CODES))  # no duplicates
    for code in RULE_CODES:
        entry = explain_rule(code)
        assert entry["rule_code"] == code
        assert entry["severity"] in {"warning", "error", "unknown"}
    assert explain_rule("DOES_NOT_EXIST") == {}


def test_findings_are_capped_with_a_deterministic_truncation_marker():
    config = RuleConfig(max_findings=3)
    result = evaluate(
        make_record(status="mystery", route="", stop="", vehicle="", delay=-9999),
        config=config,
    )
    assert len(result.findings) == 4  # 3 kept + truncation marker
    assert result.findings[-1].rule_code == FINDINGS_TRUNCATED
    assert result.outcome is Outcome.REJECT


def test_messages_are_length_bounded():
    long_status = "x" * 5000
    result = evaluate(make_record(status=long_status))
    for finding in result.findings:
        assert len(finding.message) <= 512
    custom = RuleConfig(max_message_length=80)
    result = evaluate(make_record(status=long_status), config=custom)
    assert all(len(f.message) <= 80 for f in result.findings)
    # Limits smaller than the marker itself degrade to a hard cut.
    assert truncate_text("a" * 100, 10) == "a" * 10
    assert len(truncate_text("a" * 600)) == 512  # default cap
    assert len(truncate_text("a" * 600, 80)) == 80
    assert truncate_text("a" * 100, 40).endswith(MESSAGE_TRUNCATION_MARKER)


# ---------------------------------------------------------------------------
# Rule configuration changes
# ---------------------------------------------------------------------------

def test_config_signature_changes_with_thresholds_and_identifies_engine_version():
    default = RuleConfig()
    stricter = RuleConfig(extreme_early_seconds=-600)
    assert config_signature(default) != config_signature(stricter)
    result = evaluate(make_record(delay=-900, status="arrived"), config=stricter)
    assert result.outcome is Outcome.REJECT
    assert result.config_signature == config_signature(stricter)
    assert result.rule_engine_version == RULE_ENGINE_VERSION
    # Same record under default thresholds is only a warning.
    assert evaluate(make_record(delay=-900, status="arrived")).outcome is Outcome.ACCEPT


def test_invalid_rule_config_is_rejected_upfront():
    with pytest.raises(ValueError):
        RuleConfig(extreme_early_seconds=10)
    with pytest.raises(ValueError):
        RuleConfig(max_abs_delay_seconds=0)
    with pytest.raises(ValueError):
        RuleConfig(max_findings=0)


def test_frozen_config_cannot_be_mutated_mid_batch():
    config = RuleConfig()
    with pytest.raises(Exception):
        config.max_abs_delay_seconds = 1  # type: ignore[misc]


# ---------------------------------------------------------------------------
# Legacy validate() contract
# ---------------------------------------------------------------------------

def test_validate_returns_empty_list_for_clean_and_warning_only_records():
    assert validate(make_record()) == []
    assert validate(make_record(delay=-10, status="arrived")) == []


def test_validate_returns_messages_for_blocking_findings_only():
    messages = validate(make_record(status="mystery", delay=-9999))
    assert messages  # non-empty -> legacy caller rejects
    assert all(isinstance(item, str) and item for item in messages)


def test_finding_dataclass_carries_machine_readable_fields():
    finding = Finding(rule_code="X", severity=Severity.ERROR, message="m", field="f")
    assert (finding.rule_code, finding.severity, finding.field) == ("X", Severity.ERROR, "f")
    result = evaluate(make_record())
    assert isinstance(result, EvaluationResult)
    assert result.message_limit > 0
    # Stable cross-module symbols exist for persistence compatibility.
    assert LEGACY_REJECTION_CODE and NORMALIZE_ERROR


# ---------------------------------------------------------------------------
# Extensibility: custom rules without touching the default catalog
# ---------------------------------------------------------------------------

def _boarding_door_rule(record: TransitRecord, config: RuleConfig) -> Finding | None:
    if record.status == "boarding" and not record.vehicle_id:
        return Finding(
            rule_code="BOARDING_WITHOUT_VEHICLE",
            severity=Severity.ERROR,
            message="boarding event must reference a vehicle",
            field="vehicle_id",
        )
    return None


def test_extra_rules_append_after_builtin_catalog_and_keep_default_stable():
    custom = Rule(
        code="BOARDING_WITHOUT_VEHICLE",
        severity=Severity.ERROR,
        check=_boarding_door_rule,
        description="Boarding requires a vehicle",
    )
    record = make_record(status="boarding", vehicle="")
    baseline = evaluate(record)
    extended = evaluate(record, extra_rules=(custom,))
    # Default engine is untouched.
    assert "BOARDING_WITHOUT_VEHICLE" not in baseline.rule_codes
    assert baseline.rule_codes == ("VEHICLE_ID_MISSING",)
    # Extension fires, after the built-in warning, and escalates to rejection.
    assert extended.rule_codes == ("VEHICLE_ID_MISSING", "BOARDING_WITHOUT_VEHICLE")
    assert extended.outcome is Outcome.REJECT


def test_custom_rule_engine_is_isolated_and_deterministic():
    custom = Rule(
        code="X_ONLY_RULE",
        severity=Severity.WARNING,
        check=lambda record, config: Finding("X_ONLY_RULE", Severity.WARNING, "x"),
        description="test rule",
    )
    # An engine built from an empty catalog evaluates nothing.
    assert RuleEngine(()).evaluate(make_record()).findings == ()
    engine = RuleEngine((custom,))
    result = engine.evaluate(make_record())
    assert result.rule_codes == ("X_ONLY_RULE",)
    # Two evaluations are identical and the global default is unaffected.
    assert engine.evaluate(make_record()) == result
    assert "X_ONLY_RULE" not in evaluate(make_record()).rule_codes


def test_a_raising_custom_rule_is_contained_as_unexpected_error():
    def broken_check(record, config):
        raise RuntimeError("rule exploded")

    custom = Rule("BROKEN", Severity.ERROR, broken_check, "always raises")
    result = evaluate(make_record(), extra_rules=(custom,))
    assert result.outcome is Outcome.REJECT
    assert result.rule_codes[-1] == "UNEXPECTED_ERROR"
    assert "rule exploded" in result.findings[-1].message
