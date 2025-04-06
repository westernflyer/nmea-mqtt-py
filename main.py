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
import socket
import sys
import time
from collections import defaultdict
from contextlib import contextmanager
from logging.handlers import SysLogHandler

import paho.mqtt.client as mqtt

import parse_nmea
from config import *

# Set up logging using the system logger
if sys.platform == "darwin":
    address = '/var/run/syslog'
else:
    address = '/dev/log'
log = logging.getLogger("nmea-mqtt")
log.setLevel(logging.DEBUG if DEBUG else logging.INFO)
handler = SysLogHandler(address=address)
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
            print(f"Connection reset. Reason: {e}", file=sys.stderr)
            print("*** Waiting 5 seconds before retrying.", file=sys.stderr)
            log.warning(f"Connection reset. Reason: {e}")
            log.warning("*** Waiting 5 seconds before retrying.")
            time.sleep(5)
            print("*** Retrying...", file=sys.stderr)
            log.warning("*** Retrying...")
        except ConnectionRefusedError as e:
            print(f"Connection refused. Reason: {e}", file=sys.stderr)
            print("*** Waiting 60 seconds before retrying.", file=sys.stderr)
            log.warning(f"Connection refused. Reason: {e}")
            log.warning("*** Waiting 60 seconds before retrying.")
            time.sleep(60)
            print("*** Retrying...", file=sys.stderr)
            log.warning("*** Retrying...")


def nmea_loop():
    """Read sentences from a socket, parse, then publish to MQTT.

    This is the heart of the program.
    """
    global last_published

    # Open up a connection to the MQTT broker
    with managed_connection() as mqtt_client:
        if MQTT_USERNAME and MQTT_PASSWORD:
            mqtt_client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)
        mqtt_client.on_connect = on_connect
        mqtt_client.on_publish = on_publish
        mqtt_client.connect(MQTT_BROKER, MQTT_PORT, 60)
        mqtt_client.loop_start()

        # Open the socket connection and start reading lines
        for line in gen_nmea(NMEA_HOST, NMEA_PORT):
            try:
                # Parse the line. Be prepared to catch any exceptions.
                parsed_nmea = parse_nmea.parse(line)
            except parse_nmea.UnknownNMEASentence as e:
                if e.sentence_type in PUBLISH_INTERVALS:
                    # The user asked for a sentence type, yet we don't know anything about it.
                    # File a warning.
                    log.warning(f"No decoder for sentence type: {e.sentence_type}")
                    print(f"No decoder for NMEA sentence type: {e.sentence_type}", file=sys.stderr)
                    continue
            except (parse_nmea.NMEAParsingError, parse_nmea.NMEAStatusError) as e:
                log.warning("NMEA error: %s", e)
                print(f"NMEA error: {e}", file=sys.stderr)
                continue
            else:
                # Parsing went ok. Check to see whether this sentence type should be published
                sentence_type = parsed_nmea["sentence_type"]
                if sentence_type in PUBLISH_INTERVALS:
                    # Check whether enough time has elapsed
                    delta = parsed_nmea["timestamp"] - last_published[sentence_type]
                    if delta >= PUBLISH_INTERVALS[sentence_type]:
                        publish_nmea(mqtt_client, parsed_nmea)
                        last_published[sentence_type] = parsed_nmea["timestamp"]


def on_connect(client, userdata, flags, reason_code, properties):
    """The callback for when the client receives a CONNACK response from the server."""
    print(f"Connected to MQTT broker with result code: '{reason_code}'")
    log.info(f"Connected to MQTT broker with result code: '{reason_code}'")


def on_publish(client, userdata, mid, reason_code, properties):
    """Callback for when a PUBLISH message is sent to the server."""
    if DEBUG:
        print(f"Message id {mid} published.")
        log.debug(f"Message id {mid} published.")


def gen_nmea(host: str, port: int):
    """Listen for NMEA data on a TCP socket."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect((host, port))
        with s.makefile('r') as nmea_stream:
            for line in nmea_stream:
                yield line.strip()


def publish_nmea(mqtt_client: mqtt.Client, parsed_data: dict[str, str | float | int | None]):
    """Publish parsed NMEA data to MQTT."""
    topic = f"{MQTT_TOPIC_PREFIX}/{MMSI}/{parsed_data['sentence_type']}"
    mqtt_client.publish(topic, json.dumps(parsed_data), qos=0)


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
