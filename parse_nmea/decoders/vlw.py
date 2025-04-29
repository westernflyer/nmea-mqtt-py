"""Parse NMEA sentence VLW - Distance travelled through water

For field descriptions: https://gpsd.gitlab.io/gpsd/NMEA.html#_vlw_distance_traveled_through_water
"""
from parse_nmea.__init__ import *


def decode(parts: list[str]) -> dict[str, str | float | int | None]:
    data = {
        "water_total_nm": parse_float(parts[1]),
        "water_since_reset_nm": parse_float(parts[3]),
        "ground_total_nm": parse_float(parts[5]),
        "ground_since_reset_nm": parse_float(parts[7]),
    }

    return data
