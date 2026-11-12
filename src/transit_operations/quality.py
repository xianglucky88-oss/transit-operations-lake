from __future__ import annotations

from .models import TransitRecord


def validate(record: TransitRecord) -> list[str]:
    issues: list[str] = []

    if record.delay_seconds is not None and record.delay_seconds < -3600:
        issues.append("delay_seconds is earlier than one hour")

    return issues
