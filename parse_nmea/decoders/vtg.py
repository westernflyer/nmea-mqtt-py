"""Parse NMEA sentence VTG - Track Made Good and Ground Speed

For field descriptions: https://gpsd.gitlab.io/gpsd/NMEA.html#_vtg_track_made_good_and_ground_speed
"""
from parse_nmea.__init__ import *


def decode(parts: list[str]) -> NmeaDict:
    data = {
        "cog_true": parse_float(parts[1]),
        "cog_magnetic": parse_float(parts[3]),
        "sog_knots": parse_float(parts[5]),
        "sog_kph": parse_float(parts[7]),
    }

    return data
