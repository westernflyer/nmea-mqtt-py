#
# Copyright (c) 2025-present Tom Keffer <tkeffer@gmail.com>
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.
#
"""Read NMEA sentences from a socket, parse, then publish to MQTT."""
from __future__ import annotations

import errno
import json
import logging
import socket
import sys
import time
from collections import defaultdict
from contextlib import contextmanager
from logging.handlers import SysLogHandler
from typing import Generator

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


def main():
    log.info("Starting up nmea-mqtt.  ")
    log.info("Debug level: %s", DEBUG)

    while True:
        try:
            nmea_loop()
        except KeyboardInterrupt:
            sys.exit("Keyboard interrupt. Exiting.")
        except ConnectionResetError as e:
            warn_print_sleep(f"Connection reset: {e}")
        except ConnectionRefusedError as e:
            warn_print_sleep(f"Connection refused: {e}")
        except TimeoutError as e:
            warn_print_sleep(f"Socket timeout: {e}")
        except socket.gaierror as e:
            warn_print_sleep(f"GAI error: {e}")
        except OSError as e:
            # Retry if it's a network unreachable error. Otherwise, reraise the exception.
            if e.errno == errno.ENETUNREACH or e.errno == errno.EHOSTUNREACH:
                warn_print_sleep(f"Network unreachable: {e}")
            else:
                raise


def nmea_loop():
    """Read sentences from a socket, parse, then publish to MQTT.

    This is the heart of the program.
    """
    # The time each topic was last published
    last_published = defaultdict(lambda: 0.0)

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
    if DEBUG >= 2:
        print(f"Message id {mid} published.")
        log.debug(f"Message id {mid} published.")


def gen_nmea(host: str, port: int) -> Generator[str]:
    """Listen for NMEA data on a TCP socket."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.settimeout(NMEA_TIMEOUT)
        s.connect((host, port))
        log.info(f"Connected to NMEA socket at {host}:{port}; timeout: {NMEA_TIMEOUT} seconds.")
        with s.makefile('r') as nmea_stream:
            for line in nmea_stream:
                yield line.strip()


def publish_nmea(mqtt_client: mqtt.Client, parsed_nmea: dict[str, str | float | int | None]):
    """Publish parsed NMEA data to MQTT."""
    topic = f"{MQTT_TOPIC_PREFIX}/{MMSI}/{parsed_nmea['sentence_type']}"
    info = mqtt_client.publish(topic, json.dumps(parsed_nmea), qos=0)
    if DEBUG >= 1 and info.mid % 1000 == 0:
        log.debug(f"{info.mid}: {parsed_nmea['sentence_type']} {parsed_nmea['timestamp']}")


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


def warn_print_sleep(msg: str):
    """Print and log a warning message, then sleep for NMEA_RETRY_WAIT seconds."""
    print(msg, file=sys.stderr)
    print(f"*** Waiting {NMEA_RETRY_WAIT} seconds before retrying.", file=sys.stderr)
    log.warning(msg)
    log.warning(f"*** Waiting {NMEA_RETRY_WAIT} seconds before retrying.")
    time.sleep(NMEA_RETRY_WAIT)
    print("*** Retrying...", file=sys.stderr)
    log.warning("*** Retrying...")


if __name__ == "__main__":
    main()
