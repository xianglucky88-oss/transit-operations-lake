"""Operational event taxonomy shared by classification and quality rules.

The status registry is the single source of truth for:

* which ``status`` values are known to the domain,
* which event category a status belongs to,
* which categories :func:`classify_event` exposes through its legacy contract.

Keeping it here (an ordered tuple, never a dict literal that callers iterate)
guarantees rule ordering and classification results are identical regardless of
Python hash seed or the order in which a source listed its data.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Final

from .models import TransitRecord


class EventCategory(str, Enum):
    """Stable machine-readable event categories."""

    DELAY = "delay"
    CANCELLATION = "cancellation"
    MOVEMENT = "movement"
    SCHEDULE = "schedule"
    UNKNOWN = "unknown"


@dataclass(frozen=True)
class StatusDefinition:
    status: str
    category: EventCategory


# Insertion order is the canonical ordering used by every rule engine scan.
STATUS_REGISTRY: Final[tuple[StatusDefinition, ...]] = (
    StatusDefinition("scheduled", EventCategory.SCHEDULE),
    StatusDefinition("boarding", EventCategory.SCHEDULE),
    StatusDefinition("delayed", EventCategory.DELAY),
    StatusDefinition("cancelled", EventCategory.CANCELLATION),
    StatusDefinition("arrived", EventCategory.MOVEMENT),
    StatusDefinition("departed", EventCategory.MOVEMENT),
)

KNOWN_STATUSES: Final[frozenset[str]] = frozenset(item.status for item in STATUS_REGISTRY)

# Legacy categories returned by ``classify_event`` (``unknown`` is only used by
# the explicit, machine-readable API to preserve the old return contract).
LEGACY_CATEGORIES: Final[frozenset[EventCategory]] = frozenset(
    {
        EventCategory.DELAY,
        EventCategory.CANCELLATION,
        EventCategory.MOVEMENT,
        EventCategory.SCHEDULE,
    }
)

_STATUS_TO_CATEGORY: Final[dict[str, EventCategory]] = {
    item.status: item.category for item in STATUS_REGISTRY
}


@dataclass(frozen=True)
class EventClassification:
    """Classification result including the legacy-compatible fallback."""

    status: str
    category: EventCategory
    known_status: bool


def classify(record: TransitRecord) -> EventClassification:
    """Return the full, machine-readable classification of a record."""

    category = _STATUS_TO_CATEGORY.get(record.status, EventCategory.UNKNOWN)
    return EventClassification(
        status=record.status,
        category=category,
        known_status=record.status in KNOWN_STATUSES,
    )


def classify_event(record: TransitRecord) -> str:
    """Classify an operational event.

    The legacy return values (``delay``, ``cancellation``, ``movement``,
    ``schedule``) are unchanged. Unknown statuses keep the historical
    ``schedule`` fallback here; callers that need to distinguish an unknown
    status should use :func:`classify`, which reports ``unknown``.
    """

    result = classify(record)
    if result.category in LEGACY_CATEGORIES:
        return result.category.value
    return EventCategory.SCHEDULE.value
