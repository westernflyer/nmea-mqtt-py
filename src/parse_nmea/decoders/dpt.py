"""Parse NMEA sentence DPT - Depth of Water

For field descriptions: https://gpsd.gitlab.io/gpsd/NMEA.html#_dpt_depth_of_water
"""
from parse_nmea.__init__ import *


def decode(parts: list[str]) -> NmeaDict:
    data = {
        "depth_below_transducer_meters": parse_float(parts[1]),
        "transducer_depth_meters": parse_float(parts[2]),
    }
    try:
        data["water_depth_meters"] = data["depth_below_transducer_meters"] + data["transducer_depth_meters"]
    except TypeError:
        data["water_depth_meters"] = None

    return data
