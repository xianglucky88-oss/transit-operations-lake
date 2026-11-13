"""城市公共交通运行数据湖.

Public quality/event API:

* :func:`transit_operations.quality.validate` - legacy
  ``validate(record) -> list[str]`` contract, preserved unchanged.
* :func:`transit_operations.quality.evaluate` - structured decision with
  stable rule codes, severities, outcomes and explainable messages.
* :func:`transit_operations.events.classify_event` - legacy event labels;
  :func:`transit_operations.events.classify` adds the machine-readable
  ``unknown`` category.
"""

from __future__ import annotations

from .events import (
    EventCategory,
    EventClassification,
    StatusDefinition,
    classify,
    classify_event,
)
from .quality import (
    EvaluationContext,
    EvaluationResult,
    Finding,
    Outcome,
    Rule,
    RuleConfig,
    RuleEngine,
    Severity,
    evaluate,
    validate,
)

__all__ = [
    "EventCategory",
    "EventClassification",
    "EvaluationContext",
    "EvaluationResult",
    "Finding",
    "Outcome",
    "Rule",
    "RuleConfig",
    "RuleEngine",
    "Severity",
    "StatusDefinition",
    "classify",
    "classify_event",
    "evaluate",
    "validate",
]
