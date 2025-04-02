from utilities import *


def decode(parts: list[str]) -> dict[str, str | float | int | None]:
    data = {
        "cog_true": parse_float(parts[1]),
        "reference_true": parts[2],
        "cog_magnetic": parse_float(parts[3]),
        "reference_magnetic": parts[4],
        "sog_knots": parse_float(parts[5]),
        "sog_kph": parse_float(parts[7]),
    }
    try:
        data["mode"] = parts[9][0]
    except IndexError:
        data["mode"] = None

    return data
