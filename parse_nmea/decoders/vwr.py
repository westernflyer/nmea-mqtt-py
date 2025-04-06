"""Parse NMEA sentence VWR - Relative Wind Speed and Angle

For field descriptions: https://gpsd.gitlab.io/gpsd/NMEA.html#_vwr_relative_wind_speed_and_angle
"""
from parse_nmea.__init__ import *


def decode(parts: list[str]) -> dict[str, str | float | int | None]:
    data = {
        "awa": parse_float(parts[1]),
        "aws_knots": parse_float(parts[3]),
        "aws_mps": parse_float(parts[5]),
        "aws_kph": parse_float(parts[7]),
    }
    if parts[2].upper() == 'L':
        data["awa"] *= -1

    return data
