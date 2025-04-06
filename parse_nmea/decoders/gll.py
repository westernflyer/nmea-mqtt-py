"""Parse NMEA sentence GLL - Geographic Position Latitude/Longitude

For field descriptions: https://gpsd.gitlab.io/gpsd/NMEA.html#_gll_geographic_position_latitudelongitude
"""
from parse_nmea.__init__ import *


def decode(parts: list[str]) -> dict[str, str | float | int | None]:

    # Check status
    status = parts[6].upper()
    if status != "A":
        raise NMEAStatusError(f"Bad status ('{status}') for sentence type 'GLL'")

    data = {
        "latitude": parse_latitude(parts[1], parts[2]),
        "longitude": parse_longitude(parts[3], parts[4]),
        "timeUTC": parse_time(parts[5]),
    }
    try:
        data["gll_mode"] = parts[7][0]
    except IndexError:
        data["gll_mode"] = None
    return data
