from utilities import *


def decode(parts: list[str]) -> dict[str, str | float | int | None]:
    data = {
        "wind_angle_apparent": parse_float(parts[1]),
        "wind_speed_apparent_knots": parse_float(parts[3]),
        "wind_speed_apparent_mps": parse_float(parts[5]),
        "wind_speed_apparent_kph": parse_float(parts[7]),
    }
    if parts[2].upper() == 'L':
        data["wind_angle_apparent"] *= -1

    return data
