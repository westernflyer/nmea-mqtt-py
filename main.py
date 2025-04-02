#
# Copyright (c) 2025-present Tom Keffer <tkeffer@gmail.com>
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.
#
"""Read NMEA sentences from a socket, parse, then publish to MQTT."""
import datetime
import json
import operator
import re
import socket
import time
from collections import defaultdict
from functools import reduce
from typing import Dict, Any

import paho.mqtt.client as mqtt

from config import *


class UnknownNMEASentence(ValueError):
    "Raised whe an unknown NMEA sentence is received."


# Last published timestamps
last_published = defaultdict(lambda: 0)


def publish_nmea(mqtt_client: mqtt.Client, nmea_sentence: str):
    """Publish parsed NMEA data to MQTT."""
    global last_published

    if not nmea_sentence.startswith("$"):
        raise ValueError(f"Invalid NMEA sentence '{nmea_sentence}'")

    # If it's present, check the checksum
    asterick = nmea_sentence.find('*')
    if asterick != -1:
        cs = checksum(nmea_sentence[1:asterick])
        cs_msg = int(nmea_sentence[asterick + 1:], 16)
        if cs != cs_msg:
            print(f"Checksum mismatch for sentence {nmea_sentence}")
        # Strip off the checksum:
        nmea_sentence = nmea_sentence[:asterick]
    sentence_type = nmea_sentence[3:6]  # Extract sentence type (e.g., GGA, RMC)

    if sentence_type not in PUBLISH_INTERVALS:
        return  # Skip sentences that are not in the publish list

    current_time = time.time()
    if current_time - last_published[sentence_type] < PUBLISH_INTERVALS[sentence_type]:
        return  # Skip publishing if within rate limit

    try:
        parsed_data = parse_nmea(nmea_sentence)
    except UnknownNMEASentence as e:
        print("UnknownNMEASentence", e)
    else:
        if parsed_data:
            parsed_data["sentence_type"] = sentence_type.upper()
            parsed_data["timestamp"] = int(time.time() + 0.5)
            topic = f"{MQTT_TOPIC_PREFIX}/{MMSI}/{sentence_type}"
            mqtt_client.publish(topic, json.dumps(parsed_data))
            last_published[sentence_type] = current_time


# Socket listener
def listen_nmea(mqtt_client: mqtt.Client):
    """Listen for NMEA data on a TCP socket."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect((NMEA_HOST, NMEA_PORT))
        with s.makefile('r') as nmea_stream:
            for line in nmea_stream:
                line = line.strip()
                if line.startswith("$"):
                    publish_nmea(mqtt_client, line)


def parse_nmea(sentence: str) -> Dict[str, Any]:
    """Parses an NMEA 0183 sentence into a dictionary."""
    parts = sentence.strip().split(',')
    sentence_type = parts[0][3:]

    if sentence_type == "GGA":  # Global Positioning System Fix Data
        data = {
            "timeUTC": parse_time(parts[1][0:6]),
            "latitude": parse_latitude(parts[2], parts[3]),
            "longitude": parse_longitude(parts[4], parts[5]),
            "fix_quality": parts[6],
            "num_satellites": parse_int(parts[7]),
            "hdop": parse_float(parts[8]),
            "altitude": parse_float(parts[9]),
            "altitude_units": parts[10]
        }

    elif sentence_type == "GLL":  # Geographic Position - Latitude/Longitude
        data = {
            "latitude": parse_latitude(parts[1], parts[2]),
            "longitude": parse_longitude(parts[3], parts[4]),
            "timeUTC": parse_time(parts[5][0:6]),
            "status": parts[6],
        }
        try:
            data["mode"] = parts[7][0]
        except IndexError:
            data["mode"] = None

    elif sentence_type == "GSV":  # Satellites in View
        data = {
            "num_messages": parse_int(parts[1]),
            "message_number": parse_int(parts[2]),
            "num_satellites": parse_int(parts[3]),
            "satellites": []
        }
        for i in range(4, len(parts) - 4, 4):
            if len(parts) >= i + 4:
                satellite_info = {
                    "satellite_prn": parse_int(parts[i]),
                    "elevation": parse_int(parts[i + 1]),
                    "azimuth": parse_int(parts[i + 2]),
                    "snr": parse_int(parts[i + 3])
                }
                data["satellites"].append(satellite_info)

    elif sentence_type == "HDT":  # Heading - True
        data = {
            "heading": parse_float(parts[1]),
            "reference": parts[2]
        }

    elif sentence_type == "MDA":  # Meteorological Composite
        data = {
            "pressure_inches": parse_float(parts[1]),
            "pressure_bars": parse_float(parts[3]),
            "temperature_air": parse_float(parts[5]),
            "temperature_water": parse_float(parts[7]),
            "humidity_relative": parse_float(parts[9]),
            "dew_point": parse_float(parts[11])
        }

    elif sentence_type == "MWV":  # Wind Speed and Angle
        data = {
            "wind_angle": parse_float(parts[1]),
            "reference": parts[2],
            "wind_speed": parse_float(parts[3]),
            "units": parts[4],
            "status": parts[5][0] if len(parts[5]) > 0 else None
        }

    elif sentence_type == "RMC":  # Recommended Minimum Specific GPS/Transit Data
        data = {
            "timeUTC": parse_datetime(parts[9], parts[1]),
            "status": parts[2],
            "latitude": parse_latitude(parts[3], parts[4]),
            "longitude": parse_longitude(parts[5], parts[6]),
            "speed_knots": parse_float(parts[7]),
            "track_angle": parse_float(parts[8]),
            "magnetic_variation": parts[10] + " " + parts[11] if len(parts) > 11 else None
        }

    elif sentence_type == "RSA":
        data = {
            "rudder_angle": parse_float(parts[1]) if parts[2].upper() == 'A' else None,
        }

    elif sentence_type == "VTG":  # Track Made Good and Ground Speed
        data = {
            "course_true": parse_float(parts[1]),
            "reference_true": parts[2],
            "course_magnetic": parse_float(parts[3]),
            "reference_magnetic": parts[4],
            "speed_knots": parse_float(parts[5]),
            "speed_kmh": parse_float(parts[7]),
        }
        try:
            data["mode"] = parts[9][0]
        except IndexError:
            data["mode"] = None

    elif sentence_type == "VWR":
        data = {
            "wind_apparent_angle": parse_float(parts[1]),
            "wind_speed_apparent_knots": parse_float(parts[3]),
            "wind_speed_apparent_mps": parse_float(parts[5]),
            "wind_speed_apparent_kmh": parse_float(parts[7]),
        }
        if parts[2].upper() == 'L':
            data["wind_apparent_angle"] = -data["wind_apparent_angle"]

    else:
        raise UnknownNMEASentence(f"Unsupported NMEA sentence type {sentence_type}")

    return data


def checksum(nmea_str: str):
    return reduce(operator.xor, map(ord, nmea_str), 0)


def parse_time(time_str: str) -> str:
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


def parse_latitude(latitude: str, hemisphere: str = 'N') -> float:
    val = dm_to_sd(latitude)
    if hemisphere == 'S':
        val = -val
    return val


def parse_longitude(longitude: str, hemisphere: str = 'E') -> float:
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


if __name__ == "__main__":
    # MQTT setup
    mqtt_client = mqtt.Client(callback_api_version=2)
    if MQTT_USERNAME and MQTT_PASSWORD:
        mqtt_client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)
    mqtt_client.connect(MQTT_BROKER, MQTT_PORT, 60)
    mqtt_client.loop_start()

    try:
        listen_nmea(mqtt_client)
    except KeyboardInterrupt:
        print("Exiting...")
