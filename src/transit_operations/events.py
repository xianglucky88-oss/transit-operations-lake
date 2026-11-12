from .models import TransitRecord


def classify_event(record: TransitRecord) -> str:
    if record.status == "delayed":
        return "delay"
    if record.status == "cancelled":
        return "cancellation"
    if record.status in {"arrived", "departed"}:
        return "movement"
    return "schedule"
