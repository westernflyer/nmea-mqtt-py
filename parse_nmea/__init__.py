#
# Copyright (c) 2025-present Tom Keffer <tkeffer@gmail.com>
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.
#
"""Parse NMEA sentences."""
import datetime
import importlib
import operator
import re
import time
from functools import reduce


class UnknownNMEASentence(ValueError):
    "Raised whe an unknown NMEA sentence is received."
    sentence_type: str


class NMEAParsingError(ValueError):
    """Raised when unable to parse an NMEA sentence."""


class NMEAStatusError(ValueError):
    """Raised when an NMEA sentence has a bad status."""


def parse(sentence: str) -> dict[str, str | float | int | None]:
    """Parses an NMEA 0183 sentence into a dictionary."""

    if not sentence.startswith("$"):
        raise NMEAParsingError(f"Invalid NMEA sentence '{sentence}'")

    # If it's present, check the checksum
    asterick = sentence.find('*')
    if asterick != -1:
        cs = checksum(sentence[1:asterick])
        cs_msg = int(sentence[asterick + 1:], 16)
        if cs != cs_msg:
            raise NMEAParsingError(f"Checksum mismatch for sentence {sentence}")
        # Strip off the checksum:
        sentence = sentence[:asterick]

    parts = sentence.strip().split(',')
    sentence_type = parts[0][3:]

    # Dynamically import the appropriate decoder module
    try:
        m = importlib.import_module(f"parse_nmea.decoders.{sentence_type.lower()}")
    except ModuleNotFoundError:
        e = UnknownNMEASentence(f"Unsupported NMEA sentence type {sentence_type}")
        e.sentence_type = sentence_type
        raise e

    # Using the decoder module, parse the parts of the sentence
    data = m.decode(parts)

    # Add the sentence type and a timestamp
    data["sentence_type"] = sentence_type.upper()
    data["timestamp"] = int(time.time() * 1000 + 0.5)

    return data


def checksum(nmea_str: str) -> int:
    return reduce(operator.xor, map(ord, nmea_str), 0)


def parse_time(time_str: str | None) -> str | None:
    """Parses a time string of the form HHMMSS.SS into hours, minutes, and seconds."""
    try:
        hours = int(time_str[:2])
        minutes = int(time_str[2:4])
        seconds = round(float(time_str[4:]))
    except (TypeError, ValueError):
        return None
    return f"{hours:02}:{minutes:02}:{seconds:02}"


def parse_datetime(date_str: str, time_str: str) -> str:
    """Parses a date string of the form DDMMYY and time string of the form HHMMSS.SS."""
    hours = int(time_str[:2])
    minutes = int(time_str[2:4])
    seconds = round(float(time_str[4:]))
    day = int(date_str[:2])
    month = int(date_str[2:4])
    year = int(date_str[4:])
    if year < 2000:
        year += 2000
    dt = datetime.datetime(year, month, day, hours, minutes, seconds)
    return dt.isoformat()


def dm_to_sd(dm: str | None) -> float:
    """
    Converts a geographic co-ordinate given in "degrees/minutes" dddmm.mmmm
    format (eg, "12319.943281" = 123 degrees, 19.943281 minutes) to a signed
    decimal (python float) format.

    From the library 'pynmea2' by Tom Flanagan.
    https://github.com/Knio/pynmea2https://github.com/Knio/pynmea2
    """
    if dm is None or not is_number(dm):
        return None
    if not dm or dm == '0':
        return 0.
    r = re.match(r'^(\d+)(\d\d\.\d+)$', dm)
    if not r:
        raise ValueError("Geographic coordinate value '{}' is not valid DDDMM.MMM".format(dm))
    d, m = r.groups()
    return float(d) + float(m) / 60


def parse_latitude(latitude: str | None, hemisphere: str = 'N') -> float:
    val = dm_to_sd(latitude)
    if hemisphere == 'S':
        val = -val
    return val


def parse_longitude(longitude: str | None, hemisphere: str = 'E') -> float:
    val = dm_to_sd(longitude)
    if hemisphere == 'W':
        val = -val
    return val


def parse_float(float_str: str) -> float:
    if float_str is None or float_str == '':
        return None
    try:
        return float(float_str)
    except ValueError:
        return None


def parse_int(int_str: str) -> int:
    if int_str is None or int_str == '':
        return None
    try:
        return int(int_str)
    except ValueError:
        return None


def is_number(string: str) -> bool:
    try:
        float(string)
        return True
    except ValueError:
        return False
