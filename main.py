#
# Copyright (c) 2025-present Tom Keffer <tkeffer@gmail.com>
#
# This source code is licensed under the MIT license found in the
# LICENSE.txt.txt file in the root directory of this source tree.
#
"""Read NMEA sentences from multiple sockets, parse, then publish to MQTT."""
from __future__ import annotations

import asyncio
import errno
import json
import logging
import socket
import sys
from collections import defaultdict
from contextlib import asynccontextmanager
from logging.handlers import SysLogHandler, TimedRotatingFileHandler
from typing import AsyncGenerator

import paho.mqtt.client as mqtt

import parse_nmea
from config import *

# Set up logging using the system logger
if sys.platform == "darwin":
    log_file = "/var/tmp/nmea-mqtt.log"
    handler = TimedRotatingFileHandler(log_file, when='midnight', backupCount=7)
else:
    handler = SysLogHandler(address='/dev/log')
log = logging.getLogger("nmea-mqtt")
log.setLevel(logging.DEBUG if DEBUG else logging.INFO)
formatter = logging.Formatter('%(name)s: %(levelname)s %(message)s')
handler.setFormatter(formatter)
log.addHandler(handler)


async def main():
    log.info("Starting up nmea-mqtt.  ")
    log.info("Debug level: %s", DEBUG)

    # The time each topic was last published. Shared among all readers.
    last_published = defaultdict(lambda: 0.0)

    while True:
        try:
            async with managed_connection() as mqtt_client:
                if MQTT_USERNAME and MQTT_PASSWORD:
                    mqtt_client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)
                mqtt_client.on_connect = on_connect
                mqtt_client.on_publish = on_publish
                mqtt_client.connect(MQTT_BROKER, MQTT_PORT, 60)

                # Tasks for MQTT background tasks and each NMEA reader
                tasks = [asyncio.create_task(mqtt_misc_loop(mqtt_client))]
                for channel, host, port in NMEA_SOCKETS:
                    tasks.append(asyncio.create_task(nmea_reader_task(channel, host, port, mqtt_client, last_published)))

                # Run until any task fails or we are cancelled
                await asyncio.gather(*tasks)

        except asyncio.CancelledError:
            break
        except (ConnectionResetError, ConnectionRefusedError, TimeoutError, socket.gaierror, OSError) as e:
            # Retry if it's a network unreachable error. Otherwise, reraise the exception.
            if isinstance(e, OSError) and e.errno not in [errno.ENETUNREACH, errno.EHOSTUNREACH]:
                log.exception("Unexpected OS error in main loop")
                raise
            await warn_print_sleep(f"MQTT broker or network error: {e}")
        except Exception as e:
            log.exception("Unexpected error in main loop")
            await warn_print_sleep(f"Unexpected error: {e}")


async def nmea_reader_task(channel, host, port, mqtt_client, last_published):
    """Task for reading from a single NMEA socket and publishing."""
    while True:
        try:
            async for line in gen_nmea(host, port):
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
                            topic = f"{MQTT_TOPIC_PREFIX}/{MMSI}/{channel}/{parsed_nmea['sentence_type']}"
                            publish_nmea(mqtt_client, topic, parsed_nmea)
                            last_published[sentence_type] = parsed_nmea["timestamp"]
        except (ConnectionResetError, ConnectionRefusedError, asyncio.TimeoutError, socket.gaierror, OSError) as e:
            if isinstance(e, OSError) and e.errno not in [errno.ENETUNREACH, errno.EHOSTUNREACH]:
                log.warning(f"Unexpected OS error on {host}:{port}: {e}")
            await warn_print_sleep(f"Error reading from {host}:{port}: {e}")
        except Exception as e:
            log.exception(f"Unexpected error in reader task for {host}:{port}")
            await warn_print_sleep(f"Unexpected error on {host}:{port}: {e}")


def on_connect(client, userdata, flags, reason_code, properties):
    """The callback for when the client receives a CONNACK response from the server."""
    print(f"Connected to MQTT broker with result code: '{reason_code}'")
    log.info(f"Connected to MQTT broker with result code: '{reason_code}'")


def on_publish(client, userdata, mid, reason_code, properties):
    """Callback for when a PUBLISH message is sent to the server."""
    if DEBUG >= 2:
        print(f"Message id {mid} published.")
        log.debug(f"Message id {mid} published.")


async def gen_nmea(host: str, port: int) -> AsyncGenerator[str, None]:
    """Listen for NMEA data on a TCP socket."""
    reader, writer = await asyncio.open_connection(host, port)
    log.info(f"Connected to NMEA socket at {host}:{port}; timeout: {NMEA_TIMEOUT} seconds.")
    print(f"Connected to NMEA socket at {host}:{port}; timeout: {NMEA_TIMEOUT} seconds.")
    try:
        while True:
            # Use asyncio.wait_for to implement the timeout
            line = await asyncio.wait_for(reader.readline(), timeout=NMEA_TIMEOUT)
            if not line:
                log.info(f"Connection closed by {host}:{port}")
                break
            yield line.decode().strip()
    finally:
        writer.close()
        try:
            await writer.wait_closed()
        except Exception:
            pass


def publish_nmea(mqtt_client: mqtt.Client, topic: str,parsed_nmea: parse_nmea.NmeaDict):
    """Publish parsed NMEA data to MQTT."""
    info = mqtt_client.publish(topic, json.dumps(parsed_nmea), qos=0)
    if info.rc != mqtt.MQTT_ERR_SUCCESS:
        log.error(f"Failed to publish to MQTT: {info.rc}")
    if DEBUG >= 1 and info.mid % 1000 == 0:
        log.debug(f"{info.mid}: {parsed_nmea['sentence_type']} {parsed_nmea['timestamp']}")


async def mqtt_misc_loop(mqtt_client):
    """Task to handle MQTT background tasks like keep-alives."""
    while True:
        try:
            mqtt_client.loop_misc()
        except Exception as e:
            log.error(f"Error in MQTT misc loop: {e}")
            break
        await asyncio.sleep(1)


@asynccontextmanager
async def managed_connection():
    """Provides an async context manager for a paho MQTT client connection integrated with asyncio."""
    loop = asyncio.get_running_loop()
    mqtt_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)

    def on_socket_open(client, userdata, sock):
        loop.add_reader(sock, client.loop_read)

    def on_socket_close(client, userdata, sock):
        loop.remove_reader(sock)

    def on_socket_register_write(client, userdata, sock):
        loop.add_writer(sock, client.loop_write)

    def on_socket_unregister_write(client, userdata, sock):
        loop.remove_writer(sock)

    mqtt_client.on_socket_open = on_socket_open
    mqtt_client.on_socket_close = on_socket_close
    mqtt_client.on_socket_register_write = on_socket_register_write
    mqtt_client.on_socket_unregister_write = on_socket_unregister_write

    try:
        yield mqtt_client
    finally:
        log.info("Stopping loop and disconnecting from MQTT broker. Goodbye!")
        mqtt_client.disconnect()


async def warn_print_sleep(msg: str):
    """Print and log a warning message, then sleep for NMEA_RETRY_WAIT seconds."""
    print(msg, file=sys.stderr)
    print(f"*** Waiting {NMEA_RETRY_WAIT} seconds before retrying.", file=sys.stderr)
    log.warning(msg)
    log.warning(f"*** Waiting {NMEA_RETRY_WAIT} seconds before retrying.")
    await asyncio.sleep(NMEA_RETRY_WAIT)
    print("*** Retrying...", file=sys.stderr)
    log.warning("*** Retrying...")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        sys.exit("Keyboard interrupt. Exiting.")
