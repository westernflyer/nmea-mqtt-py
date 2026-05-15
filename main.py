#
# Copyright (c) 2025-present Tom Keffer <tkeffer@gmail.com>
#
# This source code is licensed under the MIT license found in the
# LICENSE.txt.txt file in the root directory of this source tree.
#
"""Read NMEA sentences from multiple sockets, parse, then publish to MQTT and InfluxDB.

Data Flow Summary
1. Read: gen_nmea pulls raw bytes from multiple TCP sockets.
2. Parse: parse_nmea.parse() validates the checksum and converts the raw string into a
   Python dictionary.
3. Queue: Validated data is pushed into two asyncio.Queues, one for InfluxDB and one for MQTT.
4a. Save to InfluxDB. influxdb_publisher_task pulls from the queue, then saves to InfluxDB.
4b. Publish to MQTT: mqtt_publisher_task pulls from the queue and checks whether enough time has
    elapsed. If so, it sends the JSON-encoded payload to the MQTT broker using a topic structure
    like nmea/MMSI/SENTENCE_TYPE.

Example Output
When a GLL (Geographic Position - Latitude/Longitude) sentence is processed, it is published to
a topic like nmea/123456789/GPGLL with a JSON body:
{
    "latitude": 36.805785,
    "longitude": -121.785685,
    "timeUTC": "18:00:15",
    "gll_mode": "D",
    "sentence_type": "GLL",
    "timestamp": 1776794415269
}
"""
from __future__ import annotations

import argparse
import asyncio
import errno
import json
import logging
import socket
import sys
import tomllib
try:
    from influxdb_client_3 import InfluxDBClient3
except ImportError:
    InfluxDBClient3 = None
from collections import defaultdict
from contextlib import asynccontextmanager
from typing import AsyncGenerator

import paho.mqtt.client as mqtt

import parse_nmea

# Global variables
config = {}
publish_intervals = {}
last_published = defaultdict(lambda: 0.0)

# Logger will be initialized in main()
log = logging.getLogger("nmea-mqtt")

async def main():
    global config, publish_intervals

    parser = argparse.ArgumentParser(description="Read NMEA sentences from multiple sockets, parse, then publish to MQTT.")
    parser.add_argument("--config", default="config.toml", help="Path to the TOML configuration file (default: config.toml)")
    args = parser.parse_args()

    try:
        with open(args.config, "rb") as f:
            config = tomllib.load(f)
    except FileNotFoundError:
        print(f"Configuration file {args.config} not found.", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"Error loading configuration file {args.config}: {e}", file=sys.stderr)
        sys.exit(1)

    # Set up logging using the system logger
    if sys.platform == "darwin":
        from logging.handlers import TimedRotatingFileHandler
        log_file = "/var/tmp/nmea-mqtt.log"
        handler = TimedRotatingFileHandler(log_file, when='midnight', backupCount=7)
    else:
        from logging.handlers import SysLogHandler
        handler = SysLogHandler(address='/dev/log')
    log.setLevel(logging.DEBUG if config.get("DEBUG") else logging.INFO)
    formatter = logging.Formatter('%(name)s: %(levelname)s %(message)s')
    handler.setFormatter(formatter)
    log.addHandler(handler)

    log.info("Starting up nmea-mqtt.  ")
    log.info("Debug level: %s", config.get("DEBUG"))

    # Set up the dictionary of last published timestamps.
    publish_intervals = config.get("MQTT_PUBLISH_INTERVALS", {})

    while True:
        try:
            # Shared queues for parsed NMEA sentences
            subscribers = []
            mqtt_queue = asyncio.Queue(maxsize=100)
            subscribers.append(mqtt_queue)

            # InfluxDB subscriber
            influx_config = config.get("INFLUXDB")
            influx_client = None
            if influx_config:
                if InfluxDBClient3:
                    influx_queue = asyncio.Queue(maxsize=100)
                    subscribers.append(influx_queue)
                    try:
                        influx_client = InfluxDBClient3(
                            host=influx_config.get("HOST"),
                            token=influx_config.get("TOKEN"),
                            database=influx_config.get("DATABASE")
                        )
                    except Exception as e:
                        log.error(f"Failed to initialize InfluxDB client: {e}")
                        influx_client = None
                else:
                    log.error("InfluxDB V3 client library not found. Please install influxdb3-python.")

            # Set up the MQTT connection
            async with managed_connection() as mqtt_client:
                # Use an Event to signal when the MQTT connection is dropped
                disconnect_event = asyncio.Event()

                mqtt_config = config.get("MQTT_OPTIONS", {})
                mqtt_username = mqtt_config.get("MQTT_USERNAME")
                mqtt_password = mqtt_config.get("MQTT_PASSWORD")
                if mqtt_username and mqtt_password:
                    mqtt_client.username_pw_set(mqtt_username, mqtt_password)

                # Set up callbacks
                mqtt_client.on_connect = on_connect
                mqtt_client.on_publish = on_publish
                mqtt_client.on_disconnect = lambda client, userdata, flags, rc, properties=None: \
                    on_disconnect(client, userdata, flags, rc, disconnect_event, properties)

                mqtt_client.connect(mqtt_config.get("MQTT_BROKER", "localhost"),
                                    mqtt_config.get("MQTT_PORT", 1883), 60)

                # Tasks for MQTT background tasks, publisher, and each NMEA reader
                tasks = [
                    asyncio.create_task(mqtt_misc_loop(mqtt_client)),
                    asyncio.create_task(mqtt_publisher_task(mqtt_client, mqtt_queue)),
                    asyncio.create_task(wait_for_disconnect(disconnect_event))
                ]
                if influx_client:
                    tasks.append(asyncio.create_task(
                        influxdb_publisher_task(influx_client,
                                                influx_config.get("DATABASE"),
                                                influx_config.get("TABLE", "nmea-data"),
                                                influx_queue)))
                nmea_options = config.get("NMEA_OPTIONS", {})
                for host, port in nmea_options.get("NMEA_SOCKETS", []):
                    tasks.append(asyncio.create_task(
                        nmea_reader_task(host, port, subscribers, last_published)))

                # Run until any task fails, or we are cancelled, or MQTT disconnects.
                # Use return_when=asyncio.FIRST_COMPLETED to catch failures or disconnects.
                done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)

                # If we're here, one of the tasks has finished. The socket reads should never
                # complete, so it must be the wait_for_disconnect() task, which means the MQTT
                # broker disconnected. Tidy up, then restart.
                for task in done:
                    try:
                        task.result()
                    except Exception as e:
                        log.error(f"Task {task.get_coro()} failed with error: {e}")

                # Cancel remaining tasks before restarting
                for task in pending:
                    task.cancel()
                await asyncio.gather(*pending, return_exceptions=True)

        except asyncio.CancelledError:
            break
        except (ConnectionResetError, ConnectionRefusedError, TimeoutError, socket.gaierror,
                OSError) as e:
            # Retry if it's a network unreachable error. Otherwise, reraise the exception.
            if isinstance(e, OSError) and e.errno not in [errno.ENETUNREACH, errno.EHOSTUNREACH]:
                log.exception("Unexpected OS error in main loop")
                raise
            await warn_print_sleep(f"MQTT broker or network error: {e}")
        except Exception as e:
            log.exception("Unexpected error in main loop")
            await warn_print_sleep(f"Unexpected error: {e}")


async def wait_for_disconnect(disconnect_event):
    """Small task that waits for the disconnect event to be set."""
    await disconnect_event.wait()
    log.warning("MQTT disconnect event triggered.")


async def mqtt_publisher_task(mqtt_client, queue):
    """Task that consumes from the queue and publishes to MQTT."""
    global last_published, publish_intervals

    while True:
        address_field, parsed_nmea = await queue.get()
        delta = parsed_nmea["timestamp"] - last_published[address_field]
        if delta >= publish_intervals[address_field]:

            try:
                mqtt_config = config.get("MQTT_OPTIONS", {})
                topic = (f"{mqtt_config.get('MQTT_TOPIC_PREFIX', 'nmea')}/"
                         f"{config['MMSI']}/"
                         f"{address_field}")
                publish_nmea(mqtt_client, topic, parsed_nmea)
            except Exception as e:
                log.error(f"Error in publisher task: {e}")
            finally:
                queue.task_done()
                last_published[address_field] = parsed_nmea["timestamp"]


async def influxdb_publisher_task(client, database, table, queue):
    """Task that consumes from the queue and publishes to InfluxDB V3."""
    blacklist = {"sentence_type", "timeUTC", "gll_mode", "timestamp"}
    while True:
        address_field, parsed_nmea = await queue.get()
        try:
            # Prepare tags and fields according to requirements
            mmsi = config.get("MMSI")
            tag_str = f"mmsi={mmsi},sentence={address_field}"

            field_list = []
            for k, v in parsed_nmea.items():
                if k in blacklist or v is None:
                    continue
                if isinstance(v, str):
                    # Basic string escaping for line protocol
                    v_escaped = v.replace('"', '\\"')
                    v_str = f'"{v_escaped}"'
                elif isinstance(v, int):
                    v_str = f"{v}i"
                else:
                    v_str = str(v)
                field_list.append(f"{k}={v_str}")
            field_str = ",".join(field_list)

            # Construct line protocol
            lp = f"{table},{tag_str} {field_str} {parsed_nmea['timestamp']}"

            await asyncio.to_thread(
                client.write,
                record=lp,
                database=database,
                write_precision="ms"
            )
        except Exception as e:
            log.error(f"Error in InfluxDB publisher task: {e}")
        finally:
            log.debug(f"Published to InfluxDB: {lp}")
            queue.task_done()


async def nmea_reader_task(host, port, subscribers, last_published):
    """Task for reading from a single NMEA socket and putting into the queue.
    Args:
        host (str): The hostname or IP address of the NMEA socket.
        port (int): The port number of the NMEA socket.
        subscribers (list[asyncio.Queue]): List of queues to put parsed NMEA data into.
        last_published (dict): Dictionary to track the last published timestamp for each NMEA
            address field.
    """
    global publish_intervals
    print(f"Starting NMEA reader for {host}:{port}")
    while True:
        try:
            async for line in gen_nmea(host, port):
                try:
                    # Parse the line. Be prepared to catch any exceptions.
                    address_field, parsed_nmea = parse_nmea.parse(line)
                except parse_nmea.UnknownNMEASentence as e:
                    if e.address_field in publish_intervals:
                        # The user asked for an address field type,
                        # yet we don't know anything about it. File a warning.
                        log.warning(f"No decoder for sentence type: {e.sentence_type}")
                        print(f"No decoder for NMEA sentence type: {e.sentence_type}",
                              file=sys.stderr)
                        continue
                except (parse_nmea.NMEAParsingError, parse_nmea.NMEAStatusError) as e:
                    log.warning("NMEA error: %s", e)
                    print(f"NMEA error: {e}", file=sys.stderr)
                    continue
                else:
                    # Hack for dealing with the FT602. Give it a different talker ID, so it doesn't
                    # collide with the Airmar 200WX.
                    if port == 60002 and address_field == "WIMWV":
                        address_field = "FTMWV"
                    # Put the parsed nmea data in the subscriber queues
                    if address_field in publish_intervals:
                        for queue in subscribers:
                            await queue.put((address_field, parsed_nmea))
        except (ConnectionResetError, ConnectionRefusedError, asyncio.TimeoutError,
                socket.gaierror, OSError) as e:
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


def on_disconnect(client, userdata, flags, reason_code, disconnect_event, properties):
    """The callback for when the client disconnects from the MQTT broker."""
    print(f"Disconnected from MQTT broker with result code: '{reason_code}'")
    log.warning(f"Disconnected from MQTT broker with result code: '{reason_code}'")
    disconnect_event.set()


def on_publish(client, userdata, mid, reason_code, properties):
    """Callback for when a PUBLISH message is sent to the server."""
    if config.get("DEBUG", 0) >= 2:
        print(f"Message id {mid} published.")
        log.debug(f"Message id {mid} published.")


async def gen_nmea(host: str, port: int) -> AsyncGenerator[str, None]:
    """Listen for NMEA data on a TCP socket."""
    nmea_options = config.get("NMEA_OPTIONS", {})
    nmea_timeout = nmea_options.get("NMEA_TIMEOUT", 20)
    reader, writer = await asyncio.open_connection(host, port)
    log.info(f"Connected to NMEA socket at {host}:{port}; timeout: {nmea_timeout} seconds.")
    print(f"Connected to NMEA socket at {host}:{port}; timeout: {nmea_timeout} seconds.")
    try:
        while True:
            # Use asyncio.wait_for to implement the timeout
            line = await asyncio.wait_for(reader.readline(), timeout=nmea_timeout)
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


def publish_nmea(mqtt_client: mqtt.Client, topic: str, parsed_nmea: parse_nmea.NmeaDict):
    """Publish parsed NMEA data to MQTT."""
    info = mqtt_client.publish(topic, json.dumps(parsed_nmea), qos=0)
    if info.rc != mqtt.MQTT_ERR_SUCCESS:
        log.error(f"Failed to publish to MQTT: {info.rc}")
    if config.get("DEBUG", 0) >= 1 and info.mid % 1000 == 0:
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
    nmea_options = config.get("NMEA_OPTIONS", {})
    nmea_retry_wait = nmea_options.get("NMEA_RETRY_WAIT", 60)
    print(msg, file=sys.stderr)
    print(f"*** Waiting {nmea_retry_wait} seconds before retrying.", file=sys.stderr)
    log.warning(msg)
    log.warning(f"*** Waiting {nmea_retry_wait} seconds before retrying.")
    await asyncio.sleep(nmea_retry_wait)
    print("*** Retrying...", file=sys.stderr)
    log.warning("*** Retrying...")


def run():
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        sys.exit("Keyboard interrupt. Exiting.")


if __name__ == "__main__":
    run()
