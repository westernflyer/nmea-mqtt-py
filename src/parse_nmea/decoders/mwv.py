"""Parse NMEA sentence MWV - Wind Speed and Angle

For field descriptions: https://gpsd.gitlab.io/gpsd/NMEA.html#_mwv_wind_speed_and_angle
"""
from parse_nmea.__init__ import *


def decode(parts: list[str]) -> NmeaDict:

    # Check status
    status = parts[5].upper()
    if status != 'A':
        raise NMEAStatusError(f"Bad status ('{status}') for sentence type 'MWV'")

    # Determine if we have true or apparent wind
    reference = parts[2].upper()
    if reference == 'T':
        key = "twa"
        value_key = "tws_knots"
    elif reference == 'R' or not reference:
        # Assume apparent (relative) if reference is missing.
        key = "awa"
        value_key = "aws_knots"
    else:
        raise NMEAParsingError(f"Unknown MWV reference '{reference}' (expected 'T' or 'R')")

    # Convert to knots
    value_knot = parse_float(parts[3])
    unit = parts[4].upper()
    if unit == 'N':
        pass
    elif unit == 'M':
        # Unit is m/s
        value_knot *= 1.94384
    elif unit == 'K':
        # Unit is kph
        value_knot *= 0.539957
    else:
        raise NMEAParsingError(f"Unknown MWV unit '{unit}' (expected 'M', 'K', or 'N')")

    data = {
        key: parse_float(parts[1]),
        value_key: value_knot,
    }

    return data
