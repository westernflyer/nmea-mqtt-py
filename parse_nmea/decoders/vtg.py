from parse_nmea.__init__ import *


def decode(parts: list[str]) -> dict[str, str | float | int | None]:
    data = {
        "cog_true": parse_float(parts[1]),
        "cog_magnetic": parse_float(parts[3]),
        "sog_knots": parse_float(parts[5]),
        "sog_kph": parse_float(parts[7]),
    }

    return data
