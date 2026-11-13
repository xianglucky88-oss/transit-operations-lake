"""Deterministic, extensible quality rule engine for transit records.

The engine evaluates every :class:`~transit_operations.models.TransitRecord`
against an ordered catalog of rules and produces three distinct outcomes:

* :data:`Outcome.ACCEPT` - the record is usable; it may still carry warnings;
* :data:`Outcome.INCONCLUSIVE` - the record cannot be judged (for example an
  unknown status) and must be quarantined rather than silently accepted;
* :data:`Outcome.REJECT` - the record contradicts domain rules.

Backward compatibility
----------------------

``validate(record) -> list[str]`` keeps its historical contract: it returns a
list of human-readable messages and is empty for acceptable records. Records
that are acceptable but carry warnings therefore still validate cleanly, while
rejected and inconclusive records return one message per blocking finding.
The structured API :func:`evaluate` additionally exposes stable rule codes,
severity levels, the event category and per-rule explainable messages.

Determinism
-----------

Rules live in an ordered tuple and findings are always aggregated in that
order; no rule depends on dictionary iteration order or on the order in which
sources were processed. Aggregation, message truncation and outcome
prioritisation are pure functions of the record and the rule configuration.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Final

from .events import KNOWN_STATUSES, EventCategory, classify
from .models import TransitRecord

# Bumped whenever the default rule catalog or its semantics change, so that
# persisted rejections can be reproduced against the exact engine generation.
RULE_ENGINE_VERSION: Final[str] = "2026-11-13.1"

# Hard safety caps so one pathological record cannot exhaust batch memory or
# create megabyte-sized rejection rows. Both are deliberately generous.
MAX_FINDINGS_PER_RECORD: Final[int] = 16
MAX_MESSAGE_LENGTH: Final[int] = 512
MESSAGE_TRUNCATION_MARKER: Final[str] = " ...[truncated]"

# Sentinel rule code used when a record never reaches the engine (normalizer
# or an unexpected adapter failure). It is part of the stable code surface.
NORMALIZE_ERROR: Final[str] = "NORMALIZE_ERROR"
UNEXPECTED_ERROR: Final[str] = "UNEXPECTED_ERROR"
# Synthetic rule emitted when the safety cap above is hit.
FINDINGS_TRUNCATED: Final[str] = "FINDINGS_TRUNCATED"
# Marker stored next to rejection rows that predate the rule engine and only
# contain free-form text.
LEGACY_REJECTION_CODE: Final[str] = "LEGACY_REJECTION"


class Severity(str, Enum):
    WARNING = "warning"
    ERROR = "error"
    UNKNOWN = "unknown"


class Outcome(str, Enum):
    ACCEPT = "accept"
    REJECT = "reject"
    INCONCLUSIVE = "inconclusive"


@dataclass(frozen=True)
class RuleConfig:
    """Thresholds and toggles for the default rule catalog.

    Instances are frozen so a shared configuration cannot be mutated mid-batch
    and change later results.
    """

    extreme_early_seconds: float = -3600.0
    max_abs_delay_seconds: float = 86400.0
    max_message_length: int = MAX_MESSAGE_LENGTH
    max_findings: int = MAX_FINDINGS_PER_RECORD

    def __post_init__(self) -> None:
        if self.extreme_early_seconds >= 0:
            raise ValueError("extreme_early_seconds must be negative")
        if self.max_abs_delay_seconds <= 0:
            raise ValueError("max_abs_delay_seconds must be positive")
        if self.max_message_length <= len(MESSAGE_TRUNCATION_MARKER):
            raise ValueError("max_message_length is too small")
        if self.max_findings <= 0:
            raise ValueError("max_findings must be positive")


@dataclass(frozen=True)
class Finding:
    """One machine-readable rule finding."""

    rule_code: str
    severity: Severity
    message: str
    field: str | None = None

    def __post_init__(self) -> None:
        # Global safety net; the active RuleConfig may apply a tighter limit.
        object.__setattr__(self, "message", truncate_text(self.message))


@dataclass(frozen=True)
class EvaluationContext:
    """Optional side inputs that cannot be derived from the record itself."""

    previous_raw_hash: str | None = None


@dataclass(frozen=True)
class EvaluationResult:
    outcome: Outcome
    findings: tuple[Finding, ...]
    category: EventCategory
    known_status: bool
    rule_engine_version: str
    config_signature: str
    message_limit: int = MAX_MESSAGE_LENGTH

    @property
    def warnings(self) -> tuple[Finding, ...]:
        return tuple(item for item in self.findings if item.severity is Severity.WARNING)

    @property
    def blocking(self) -> tuple[Finding, ...]:
        """Findings that prevent clean acceptance (error or unknown)."""

        return tuple(item for item in self.findings if item.severity is not Severity.WARNING)

    @property
    def rule_codes(self) -> tuple[str, ...]:
        return tuple(item.rule_code for item in self.findings)

    @property
    def messages(self) -> list[str]:
        return [item.message for item in self.findings]


def truncate_text(message: str, limit: int = MAX_MESSAGE_LENGTH) -> str:
    """Bound the size of any persisted message deterministically."""

    if len(message) <= limit:
        return message
    if limit <= len(MESSAGE_TRUNCATION_MARKER):
        return message[:limit]
    keep = limit - len(MESSAGE_TRUNCATION_MARKER)
    return message[: max(0, keep)] + MESSAGE_TRUNCATION_MARKER


# ---------------------------------------------------------------------------
# Time helpers (naive vs timezone-aware handling)
# ---------------------------------------------------------------------------

def parse_observed_at(value: str) -> datetime | None:
    """Parse ISO-8601 timestamps, accepting a trailing ``Z``.

    Returns ``None`` when the value is blank or cannot be parsed. Naive
    timestamps parse successfully; callers decide whether the missing zone is
    a warning or an error via :func:`timestamp_is_aware`.
    """

    text = (value or "").strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None


def timestamp_is_aware(value: str) -> bool:
    parsed = parse_observed_at(value)
    return parsed is not None and parsed.tzinfo is not None and parsed.utcoffset() is not None


# ---------------------------------------------------------------------------
# Individual rule checks. Each returns at most one Finding or None and never
# raises on malformed data, which keeps per-record cost bounded.
# ---------------------------------------------------------------------------

def _missing(field_name: str) -> Finding:
    return Finding(
        rule_code="FIELD_MISSING",
        severity=Severity.ERROR,
        message=f"{field_name} is required and must be non-empty",
        field=field_name,
    )


def check_route(record: TransitRecord, config: RuleConfig) -> Finding | None:
    if not record.route_id or not record.route_id.strip():
        return _missing("route_id")
    return None


def check_stop(record: TransitRecord, config: RuleConfig) -> Finding | None:
    if not record.stop_id or not record.stop_id.strip():
        return _missing("stop_id")
    return None


def check_vehicle(record: TransitRecord, config: RuleConfig) -> Finding | None:
    # A missing vehicle limits fragment reconstruction but route/stop events
    # remain analyzable, so this is a warning rather than a rejection.
    if not record.vehicle_id or not record.vehicle_id.strip():
        return Finding(
            rule_code="VEHICLE_ID_MISSING",
            severity=Severity.WARNING,
            message="vehicle_id is missing; vehicle-level analysis will be limited",
            field="vehicle_id",
        )
    return None


def check_observed_present(record: TransitRecord, config: RuleConfig) -> Finding | None:
    if not record.observed_at or not record.observed_at.strip():
        return _missing("observed_at")
    return None


def check_observed_parseable(record: TransitRecord, config: RuleConfig) -> Finding | None:
    if not record.observed_at or not record.observed_at.strip():
        return None  # reported by OBSERVED_AT_MISSING
    if parse_observed_at(record.observed_at) is None:
        return Finding(
            rule_code="OBSERVED_AT_UNPARSEABLE",
            severity=Severity.ERROR,
            message=f"observed_at is not a valid ISO-8601 timestamp: {record.observed_at!r}",
            field="observed_at",
        )
    return None


def check_observed_timezone(record: TransitRecord, config: RuleConfig) -> Finding | None:
    if not record.observed_at or not record.observed_at.strip():
        return None
    parsed = parse_observed_at(record.observed_at)
    if parsed is None:
        return None  # reported by OBSERVED_AT_UNPARSEABLE
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        return Finding(
            rule_code="OBSERVED_AT_NAIVE",
            severity=Severity.WARNING,
            message="observed_at has no timezone; assuming source-local time until corrected",
            field="observed_at",
        )
    return None


def check_status_known(record: TransitRecord, config: RuleConfig) -> Finding | None:
    if not record.status:
        return Finding(
            rule_code="STATUS_UNKNOWN",
            severity=Severity.UNKNOWN,
            message="status is empty and cannot be classified",
            field="status",
        )
    if record.status not in KNOWN_STATUSES:
        return Finding(
            rule_code="STATUS_UNKNOWN",
            severity=Severity.UNKNOWN,
            message=f"status {record.status!r} is not in the known status registry",
            field="status",
        )
    return None


def check_delay_finite(record: TransitRecord, config: RuleConfig) -> Finding | None:
    delay = record.delay_seconds
    if delay is None:
        return None
    if not math.isfinite(delay):
        return Finding(
            rule_code="DELAY_NOT_FINITE",
            severity=Severity.ERROR,
            message="delay_seconds must be a finite number",
            field="delay_seconds",
        )
    return None


def check_delay_range(record: TransitRecord, config: RuleConfig) -> Finding | None:
    delay = record.delay_seconds
    if delay is None or not math.isfinite(delay):
        return None
    if abs(delay) > config.max_abs_delay_seconds:
        return Finding(
            rule_code="DELAY_OUT_OF_RANGE",
            severity=Severity.ERROR,
            message=(
                f"delay_seconds={delay} is outside the supported range of "
                f"+/-{config.max_abs_delay_seconds:g} seconds"
            ),
            field="delay_seconds",
        )
    return None


def check_early_arrival(record: TransitRecord, config: RuleConfig) -> Finding | None:
    """Negative delays (early running).

    Mild early running is real, useful data and therefore only a warning.
    Running more than one hour early (strictly beyond the configured
    threshold, matching the historical ``delay < -3600`` rule) is a blocking
    data-quality error; the boundary value itself stays a warning.
    """

    delay = record.delay_seconds
    if delay is None or not math.isfinite(delay) or delay >= 0:
        return None
    if delay < config.extreme_early_seconds:
        return Finding(
            rule_code="DELAY_EXTREME_EARLY",
            severity=Severity.ERROR,
            message=(
                f"delay_seconds={delay:g} means the event is more than one hour early; "
                "this is treated as a clock or source error"
            ),
            field="delay_seconds",
        )
    return Finding(
        rule_code="DELAY_EARLY_ARRIVAL",
        severity=Severity.WARNING,
        message=f"delay_seconds={delay:g} indicates early running; accepted with a warning",
        field="delay_seconds",
    )


def check_cancelled_delay(record: TransitRecord, config: RuleConfig) -> Finding | None:
    if record.status != "cancelled" or record.delay_seconds is None:
        return None
    return Finding(
        rule_code="CANCELLED_WITH_DELAY",
        severity=Severity.WARNING,
        message=(
            f"cancelled event carries delay_seconds={record.delay_seconds:g}; "
            "the delay is ignored for cancellation statistics"
        ),
        field="delay_seconds",
    )


def check_delayed_negative(record: TransitRecord, config: RuleConfig) -> Finding | None:
    if record.status != "delayed":
        return None
    delay = record.delay_seconds
    if delay is not None and math.isfinite(delay) and delay < 0:
        return Finding(
            rule_code="DELAYED_STATUS_NEGATIVE_DELAY",
            severity=Severity.WARNING,
            message=(
                f"status is 'delayed' but delay_seconds={delay:g} is negative; "
                "status and delay contradict each other"
            ),
            field="delay_seconds",
        )
    return None


def check_revision(record: TransitRecord, config: RuleConfig, context: EvaluationContext) -> Finding | None:
    previous = context.previous_raw_hash
    if previous is None or previous == record.raw_hash:
        return None
    return Finding(
        rule_code="RECORD_REVISION",
        severity=Severity.WARNING,
        message=(
            f"record_key {record.record_key!r} was already stored with different content; "
            "the revision supersedes the previous version"
        ),
        field="record_key",
    )


# ---------------------------------------------------------------------------
# Ordered rule catalog. Order is part of the public, deterministic contract.
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class Rule:
    code: str
    severity: Severity
    check: Callable[..., Finding | None]
    description: str
    # Context-aware rules (for example record revision) receive the
    # EvaluationContext; plain field rules do not.
    needs_context: bool = False


# Catalog order: identity/completeness -> time -> status -> delay ->
# status/delay combinations -> revision. New rules are appended (or inserted
# with an explicit version bump) rather than reordered in place.
RULE_CATALOG: Final[tuple[Rule, ...]] = (
    Rule("FIELD_MISSING", Severity.ERROR, check_route, "Required identity field is empty"),
    Rule("FIELD_MISSING", Severity.ERROR, check_stop, "Required identity field is empty"),
    Rule("VEHICLE_ID_MISSING", Severity.WARNING, check_vehicle, "vehicle_id is not populated"),
    Rule("OBSERVED_AT_MISSING", Severity.ERROR, check_observed_present, "observed_at is empty"),
    Rule("OBSERVED_AT_UNPARSEABLE", Severity.ERROR, check_observed_parseable, "Timestamp is not ISO-8601"),
    Rule("OBSERVED_AT_NAIVE", Severity.WARNING, check_observed_timezone, "Timestamp lacks a timezone"),
    Rule("STATUS_UNKNOWN", Severity.UNKNOWN, check_status_known, "Status is absent or unregistered"),
    Rule("DELAY_NOT_FINITE", Severity.ERROR, check_delay_finite, "delay_seconds is not finite"),
    Rule("DELAY_OUT_OF_RANGE", Severity.ERROR, check_delay_range, "delay_seconds exceeds configured bounds"),
    # One check emits either DELAY_EXTREME_EARLY (error) or
    # DELAY_EARLY_ARRIVAL (warning); the catalog row names the blocking code.
    Rule("DELAY_EXTREME_EARLY", Severity.ERROR, check_early_arrival, "Early running: extreme (error) or mild (warning)"),
    Rule("CANCELLED_WITH_DELAY", Severity.WARNING, check_cancelled_delay, "Cancelled event still carries a delay"),
    Rule("DELAYED_STATUS_NEGATIVE_DELAY", Severity.WARNING, check_delayed_negative, "Delayed status with negative delay"),
    Rule("RECORD_REVISION", Severity.WARNING, check_revision, "Content revision for an existing record_key", needs_context=True),
)

# Unique, ordered summary of the catalog for documentation and consumers.
RULE_CODES: Final[tuple[str, ...]] = tuple(dict.fromkeys(rule.code for rule in RULE_CATALOG))


def config_signature(config: RuleConfig) -> str:
    """Stable hash of a configuration so stored results name their thresholds."""

    parts = (
        RULE_ENGINE_VERSION,
        f"early={config.extreme_early_seconds:g}",
        f"range={config.max_abs_delay_seconds:g}",
    )
    return "|".join(parts)


def _aggregate(findings: list[Finding], config: RuleConfig) -> tuple[Finding, ...]:
    if len(findings) <= config.max_findings:
        return tuple(findings)
    kept = findings[: config.max_findings]
    kept.append(
        Finding(
            rule_code=FINDINGS_TRUNCATED,
            severity=Severity.UNKNOWN,
            message=(
                f"more than {config.max_findings} quality findings; "
                "remaining problems were not evaluated"
            ),
        )
    )
    return tuple(kept)


def _outcome_for(findings: tuple[Finding, ...]) -> Outcome:
    # Deterministic priority: error beats unknown beats warning-only.
    severities = {item.severity for item in findings}
    if Severity.ERROR in severities:
        return Outcome.REJECT
    if Severity.UNKNOWN in severities:
        return Outcome.INCONCLUSIVE
    return Outcome.ACCEPT


class RuleEngine:
    """Evaluates records against a fixed, ordered tuple of rules.

    Engines are cheap and immutable in behavior: construct one with extra
    rules to extend the default catalog without monkey-patching module state,
    so different batches can use different rule sets in the same process.
    """

    def __init__(self, rules: tuple[Rule, ...] = RULE_CATALOG) -> None:
        self.rules: tuple[Rule, ...] = tuple(rules)

    def evaluate(
        self,
        record: TransitRecord,
        config: RuleConfig | None = None,
        context: EvaluationContext | None = None,
    ) -> EvaluationResult:
        config = config or RuleConfig()
        context = context or EvaluationContext()
        findings: list[Finding] = []
        for rule in self.rules:
            try:
                if rule.needs_context:
                    finding = rule.check(record, config, context)
                else:
                    finding = rule.check(record, config)
            except Exception as exc:  # defensive: one bad rule must not kill a batch
                finding = Finding(
                    rule_code=UNEXPECTED_ERROR,
                    severity=Severity.ERROR,
                    message=f"rule {rule.code} failed: {type(exc).__name__}: {exc}",
                )
            if finding is not None:
                if len(finding.message) > config.max_message_length:
                    finding = Finding(
                        rule_code=finding.rule_code,
                        severity=finding.severity,
                        message=truncate_text(finding.message, config.max_message_length),
                        field=finding.field,
                    )
                findings.append(finding)
        aggregated = _aggregate(findings, config)
        classification = classify(record)
        return EvaluationResult(
            outcome=_outcome_for(aggregated),
            findings=aggregated,
            category=classification.category,
            known_status=classification.known_status,
            rule_engine_version=RULE_ENGINE_VERSION,
            config_signature=config_signature(config),
            message_limit=config.max_message_length,
        )


DEFAULT_ENGINE: Final[RuleEngine] = RuleEngine()


def evaluate(
    record: TransitRecord,
    config: RuleConfig | None = None,
    context: EvaluationContext | None = None,
    extra_rules: tuple[Rule, ...] | list[Rule] | None = None,
) -> EvaluationResult:
    """Run the catalog and return the structured decision.

    ``extra_rules`` are appended after the built-in catalog in the given
    order, so extension findings always have stable, predictable positions.
    """

    if not extra_rules:
        return DEFAULT_ENGINE.evaluate(record, config=config, context=context)
    return RuleEngine((*RULE_CATALOG, *extra_rules)).evaluate(
        record, config=config, context=context
    )


def validate(
    record: TransitRecord,
    config: RuleConfig | None = None,
    context: EvaluationContext | None = None,
) -> list[str]:
    """Legacy compatibility wrapper.

    Returns human-readable messages for *blocking* findings only (errors and
    unknowns). Warning-only records return an empty list, exactly as callers
    that treat a non-empty result as a rejection expect. Use :func:`evaluate`
    to access warnings, rule codes and severities.
    """

    result = evaluate(record, config=config, context=context)
    return [finding.message for finding in result.blocking]


def explain_rule(rule_code: str) -> dict[str, Any]:
    """Return the stable catalog entry for a rule code (empty dict if unknown)."""

    for rule in RULE_CATALOG:
        if rule.code == rule_code:
            return {
                "rule_code": rule.code,
                "severity": rule.severity.value,
                "description": rule.description,
            }
    return {}
