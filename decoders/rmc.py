from utilities import *


def decode(parts: list[str]) -> dict[str, str | float | int | None]:
    data = {
        "timeUTC": parse_datetime(parts[9], parts[1]),
        "status": parts[2],
        "latitude": parse_latitude(parts[3], parts[4]),
        "longitude": parse_longitude(parts[5], parts[6]),
        "speed_knots": parse_float(parts[7]),
        "track_angle": parse_float(parts[8]),
        "magnetic_variation": parts[10] + " " + parts[11] if len(parts) > 11 else None
    }
    return data
