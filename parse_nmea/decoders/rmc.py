"""Parse NMEA sentence RMC - Recommended Minimum Navigation Information

For field descriptions: https://gpsd.gitlab.io/gpsd/NMEA.html#_rmc_recommended_minimum_navigation_information
"""
from parse_nmea.__init__ import *


def decode(parts: list[str]) -> dict[str, str | float | int | None]:

    # Check status
    status = parts[2].upper()
    if status != 'A':
        raise NMEAStatusError(f"Bad RMC status '{status}' (expected 'A')")

    data = {
        "datetimeUTC": parse_datetime(parts[9], parts[1]),
        "status": parts[2],
        "latitude": parse_latitude(parts[3], parts[4]),
        "longitude": parse_longitude(parts[5], parts[6]),
        "sog_knots": parse_float(parts[7]),
        "cog_true": parse_float(parts[8]),
        "magnetic_variation": parts[10],
    }

    if parts[11].upper() == 'W':
        data["magnetic_variation"] *= -1

    return data
