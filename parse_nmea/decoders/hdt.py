"""Parse NMEA sentence HDT - Heading True

For field descriptions: https://gpsd.gitlab.io/gpsd/NMEA.html#_hdt_heading_true
"""
from parse_nmea.__init__ import *


def decode(parts: list[str]) -> dict[str, str | float | int | None]:
    if parts[2].upper() != 'T':
        raise NMEAParsingError(f"Unknown HDT reference '{parts[2]}' (expected 'T')")

    data = {
        # Heading true
        "hdg_true": parse_float(parts[1]),
    }
    return data
