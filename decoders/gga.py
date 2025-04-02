from utilities import *


def decode(parts: list[str]) -> dict[str, str | float | int | None]:
    data = {
        "timeUTC": parse_time(parts[1][0:6]),
        "latitude": parse_latitude(parts[2], parts[3]),
        "longitude": parse_longitude(parts[4], parts[5]),
        "fix_quality": parts[6],
        "num_satellites": parse_int(parts[7]),
        "hdop": parse_float(parts[8]),
        "altitude_meter": parse_float(parts[9]),
        "altitude_units": parts[10].upper()
    }
    if data["altitude_units"] != 'M':
        raise NMEAParsingError(f"Unknown altitude units '{data['altitude_units']}' (expected 'M')")

    return data
