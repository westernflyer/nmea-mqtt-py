#
# Copyright (c) 2025-present Tom Keffer <tkeffer@gmail.com>
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.
#
"""Read NMEA sentences from a socket, parse, then publish to MQTT."""
from __future__ import annotations

import importlib
import json
import operator
import socket
import sys
import time
from collections import defaultdict
from contextlib import contextmanager
from functools import reduce

import paho.mqtt.client as mqtt

from config import *
from utilities import *

# Last published timestamps
last_published = defaultdict(lambda: 0.0)

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
    except NMEAParsingError as e:
        print("NMEAParsingError", e)
    else:
        if parsed_data:
            parsed_data["sentence_type"] = sentence_type.upper()
            parsed_data["timestamp"] = int(time.time() * 1000 + 0.5) # Unix epoch time in milliseconds
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


def parse_nmea(sentence: str) -> dict[str, str|float|int|None]:
    """Parses an NMEA 0183 sentence into a dictionary."""

    parts = sentence.strip().split(',')
    sentence_type = parts[0][3:]

    # Dynamically import the appropriate decoder module
    try:
        m = importlib.import_module(f"decoders.{sentence_type.lower()}")
    except ModuleNotFoundError:
        raise UnknownNMEASentence(f"Unsupported NMEA sentence type {sentence_type}")

    data = m.decode(parts)

    return data


def checksum(nmea_str: str):
    return reduce(operator.xor, map(ord, nmea_str), 0)

@contextmanager
def managed_connection():
    """Provides a context manager for a paho MQTT client connection."""
    mqtt_client = mqtt.Client(callback_api_version=2)
    try:
        yield mqtt_client
    finally:
        print("Stopping loop and disconnecting from MQTT broker. Goodbye!")
        mqtt_client.loop_stop()
        mqtt_client.disconnect()

if __name__ == "__main__":
    while True:
        try:
            with managed_connection() as mqtt_client:
                if MQTT_USERNAME and MQTT_PASSWORD:
                    mqtt_client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)
                mqtt_client.connect(MQTT_BROKER, MQTT_PORT, 60)
                mqtt_client.loop_start()

                listen_nmea(mqtt_client)
        except KeyboardInterrupt:
            sys.exit("Keyboard interrupt. Exiting.")
        except ConnectionResetError:
            print("Connection reset. Waiting 5 seconds before retrying.")
            time.sleep(5)
            print("Retrying...")
