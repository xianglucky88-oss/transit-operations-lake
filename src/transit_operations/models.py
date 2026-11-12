from __future__ import annotations

from dataclasses import dataclass
from typing import Any, TypeAlias


@dataclass(frozen=True)
class TransitRecord:
    source: str
    record_key: str
    observed_at: str
    route_id: str
    stop_id: str
    vehicle_id: str
    status: str
    delay_seconds: float | None
    payload: dict[str, Any]
    raw_hash: str


DOMAIN_RECORD_TYPE: TypeAlias = TransitRecord
