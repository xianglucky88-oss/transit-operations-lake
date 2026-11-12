def record_key(route_id: str, stop_id: str, vehicle_id: str, observed_at: str) -> str:
    return "|".join(value.strip().lower() for value in (route_id, stop_id, vehicle_id, observed_at))
