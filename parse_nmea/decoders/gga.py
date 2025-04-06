"""Parse NMEA sentence GGA - Global Positioning System Fix Data

For field descriptions: https://gpsd.gitlab.io/gpsd/NMEA.html#_gga_global_positioning_system_fix_data
"""
from parse_nmea.__init__ import *


def decode(parts: list[str]) -> dict[str, str | float | int | None]:
    data = {
        "timeUTC": parse_time(parts[1][0:6]),
        "latitude": parse_latitude(parts[2], parts[3]),
        "longitude": parse_longitude(parts[4], parts[5]),
        "fix_quality": parts[6],
        "num_satellites": parse_int(parts[7]),
        "hdop": parse_float(parts[8]),
        "altitude_meter": parse_float(parts[9]),
    }
    altitude_unit = parts[10].upper()
    if altitude_unit != 'M':
        raise NMEAParsingError(f"Unknown altitude units '{altitude_unit}' (expected 'M')")

    return data
