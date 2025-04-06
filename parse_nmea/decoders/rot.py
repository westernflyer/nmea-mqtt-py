from parse_nmea.__init__ import *


def decode(parts: list[str]) -> dict[str, str | float | int | None]:
    data = {
        "rate_of_turn": parse_float(parts[1]) if parts[2].upper() == 'A' else None,
    }
    return data
