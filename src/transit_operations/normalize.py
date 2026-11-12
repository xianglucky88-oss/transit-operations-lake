from __future__ import annotations

import hashlib
import json
from typing import Any

from .models import TransitRecord
from .identity import record_key


def _text(value: Any) -> str:
    return " ".join(str(value).split()) if value is not None else ""


def _number(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError("numeric field is invalid") from exc


def normalize(raw: Any, source: str) -> TransitRecord:
    if not isinstance(raw, dict):
        raise ValueError("record must be an object")
    payload = raw.get("payload")
    observed_at = _text(raw.get("observed_at"))
    if not isinstance(payload, dict) or not observed_at:
        raise ValueError("observed_at and object payload are required")

    route_id = _text(payload.get("route_id"))
    stop_id = _text(payload.get("stop_id"))
    vehicle_id = _text(payload.get("vehicle_id"))
    status = _text(payload.get("status")).lower()
    delay = _number(payload.get("delay_seconds"))
    if not route_id or not stop_id or not observed_at or not status:
        raise ValueError("route_id, stop_id, observed_at and status are required")
    if status not in {"scheduled", "boarding", "delayed", "cancelled", "arrived", "departed"}:
        raise ValueError("unsupported transit status")
    if delay is not None and abs(delay) > 86400:
        raise ValueError("delay_seconds is outside the supported range")

    normalized = {"route_id": route_id, "stop_id": stop_id, "vehicle_id": vehicle_id, "status": status, "delay_seconds": delay}
    key = record_key(route_id, stop_id, vehicle_id, observed_at)
    canonical = json.dumps({"source": source, "observed_at": observed_at, "payload": normalized}, ensure_ascii=False, sort_keys=True)
    raw_hash = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
    return TransitRecord(
        source=source,
        record_key=key,
        observed_at=observed_at,
        route_id=normalized["route_id"],
        stop_id=normalized["stop_id"],
        vehicle_id=normalized["vehicle_id"],
        status=normalized["status"],
        delay_seconds=normalized["delay_seconds"],
        payload=normalized,
        raw_hash=raw_hash,
    )
