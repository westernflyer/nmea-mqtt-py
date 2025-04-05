#
# Copyright (c) 2025-present Tom Keffer <tkeffer@gmail.com>
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.
#
"""Read NMEA sentences from a socket, parse, then publish to MQTT."""
from __future__ import annotations

import json
import logging
import operator
import socket
import sys
import time
from collections import defaultdict
from contextlib import contextmanager
from functools import reduce
from logging.handlers import SysLogHandler

import paho.mqtt.client as mqtt

import parse_nmea
from config import *

# Set up logging using the system logger
log = logging.getLogger("nmea-mqtt")
log.setLevel(logging.DEBUG)  # Set the minimum logging level
handler = SysLogHandler(address='/dev/log')  # Use '/dev/log' for local syslog
formatter = logging.Formatter('%(name)s: %(levelname)s %(message)s')
handler.setFormatter(formatter)
log.addHandler(handler)

# Last published timestamps
last_published = defaultdict(lambda: 0.0)

def main():
    while True:
        try:
            nmea_loop()
        except KeyboardInterrupt:
            sys.exit("Keyboard interrupt. Exiting.")
        except ConnectionResetError as e:
            print(f"Connection reset. Reason: {e}")
            print("*** Waiting 5 seconds before retrying.")
            log.warning(f"Connection reset. Reason: {e}")
            log.warning("*** Waiting 5 seconds before retrying.")
            time.sleep(5)
            print("*** Retrying...")
            log.warning("*** Retrying...")


def nmea_loop():
    with managed_connection() as mqtt_client:
        if MQTT_USERNAME and MQTT_PASSWORD:
            mqtt_client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)
        mqtt_client.on_connect = on_connect
        mqtt_client.connect(MQTT_BROKER, MQTT_PORT, 60)
        mqtt_client.loop_start()
        for line in gen_nmea(NMEA_HOST, NMEA_PORT):
            sentence = check_nmea(line)
            if not sentence:
                continue
            sentence_type, nmea_sentence = sentence
            try:
                parsed_nmea = parse_nmea.parse(nmea_sentence)
            except (parse_nmea.UnknownNMEASentence, parse_nmea.NMEAParsingError, parse_nmea.NMEAStatusError) as e:
                log.warning("NMEA error: %s", e)
                print(f"NMEA error: {e}", file=sys.stderr)
            else:
                publish_nmea(mqtt_client, sentence_type, parsed_nmea)


def on_connect(client, userdata, flags, reason_code, properties):
    """The callback for when the client receives a CONNACK response from the server."""
    print(f"Connected to MQTT broker with result code: '{reason_code}'")
    log.info(f"Connected to MQTT broker with result code: '{reason_code}'")


def gen_nmea(host: str, port: int):
    """Listen for NMEA data on a TCP socket."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect((host, port))
        with s.makefile('r') as nmea_stream:
            for line in nmea_stream:
                yield line.strip()


def check_nmea(nmea_sentence: str) -> tuple[str, str] | None:
    """Check NMEA sentence for validity and rate limit. """
    global last_published

    if not nmea_sentence.startswith("$"):
        raise parse_nmea.NMEAParsingError(f"Invalid NMEA sentence '{nmea_sentence}'")

    # If it's present, check the checksum
    asterick = nmea_sentence.find('*')
    if asterick != -1:
        cs = checksum(nmea_sentence[1:asterick])
        cs_msg = int(nmea_sentence[asterick + 1:], 16)
        if cs != cs_msg:
            raise parse_nmea.NMEAParsingError(f"Checksum mismatch for sentence {nmea_sentence}")
        # Strip off the checksum:
        nmea_sentence = nmea_sentence[:asterick]
    sentence_type = nmea_sentence[3:6]  # Extract sentence type (e.g., GGA, RMC)

    if sentence_type not in PUBLISH_INTERVALS:
        return None

    delta = time.time() * 1000 - last_published[sentence_type]
    if delta < PUBLISH_INTERVALS[sentence_type]:
        return None

    return sentence_type, nmea_sentence


def publish_nmea(mqtt_client: mqtt.Client, sentence_type: str, parsed_data: dict[str, str | float | int | None]):
    """Publish parsed NMEA data to MQTT."""
    now = int(time.time() * 1000 + 0.5)  # Unix epoch time in milliseconds
    parsed_data["sentence_type"] = sentence_type.upper()
    parsed_data["timestamp"] = now
    topic = f"{MQTT_TOPIC_PREFIX}/{MMSI}/{sentence_type}"
    mqtt_client.publish(topic, json.dumps(parsed_data), qos=0)
    last_published[sentence_type] = now


def checksum(nmea_str: str) -> int:
    return reduce(operator.xor, map(ord, nmea_str), 0)


@contextmanager
def managed_connection():
    """Provides a context manager for a paho MQTT client connection."""
    mqtt_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    try:
        yield mqtt_client
    finally:
        print("Stopping loop and disconnecting from MQTT broker. Goodbye!")
        mqtt_client.loop_stop()
        mqtt_client.disconnect()


if __name__ == "__main__":
    main()
