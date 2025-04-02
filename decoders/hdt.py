from utilities import *


def decode(parts: list[str]) -> dict[str, str | float | int | None]:
    data = {
        "heading_true": parse_float(parts[1]),
    }
    if parts[2].upper() != 'T':
        raise NMEAParsingError(f"Unknown HDT reference '{parts[2]}' (expected 'T')")
    return data
