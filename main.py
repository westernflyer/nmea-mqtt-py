#
# Copyright (c) 2025-present Tom Keffer <tkeffer@gmail.com>
#
# This source code is licensed under the MIT license found in the
# LICENSE.txt.txt file in the root directory of this source tree.
#
"""Read NMEA sentences from multiple sockets, parse, then publish to MQTT and a DuckDB database.

Summary of data flow:
1. Read: gen_nmea pulls raw bytes from multiple TCP sockets.
2. Parse: parse_nmea.parse() validates the checksum and converts the raw string into a
   Python dictionary.
3. Queue: Validated data is pushed into two asyncio.Queues, one for DuckDB and one for MQTT.
4a. Save to DuckDB: duckdb_publisher_task pulls from the queue, then saves to DuckDB.
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
import datetime
import errno
import json
import logging
import os
import socket
import sys
import tomllib
from collections import defaultdict
from contextlib import asynccontextmanager
from typing import AsyncGenerator

import duckdb
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

    if os.getenv("NMEA_MQTT_DEBUG") is not None:
        try:
            config["DEBUG"] = int(os.getenv("NMEA_MQTT_DEBUG"))
        except ValueError:
            print("Environment variable NMEA_MQTT_DEBUG must be an integer.", file=sys.stderr)
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

            # DuckDB subscriber
            duckdb_database_path = config['DUCKDB'].get("DATABASE_PATH", "nmea_database.db")
            duckdb_queue = asyncio.Queue(maxsize=1000)
            subscribers.append(duckdb_queue)

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

                tasks = [asyncio.create_task(mqtt_misc_loop(mqtt_client)),
                         asyncio.create_task(mqtt_publisher_task(mqtt_client, mqtt_queue)),
                         asyncio.create_task(wait_for_disconnect(disconnect_event)),
                         asyncio.create_task(duckdb_publisher_task(duckdb_database_path,
                                                                   duckdb_queue))]

                nmea_options = config.get("NMEA_OPTIONS", {})
                for host_url, port in nmea_options.get("NMEA_SOCKETS", []):
                    tasks.append(asyncio.create_task(
                        nmea_reader_task(host_url, port, subscribers, last_published)))

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
            await warn_print_sleep(f"Unexpected error in main loop: {e}")


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


TABLE_SCHEMAS = {
    "DPT": """CREATE TABLE IF NOT EXISTS DPT (
        timestamp TIMESTAMP_MS,
        talker VARCHAR,
        depth_below_transducer_meters DOUBLE,
        transducer_depth_meters DOUBLE,
        water_depth_meters DOUBLE
    );""",
    "GLL": """CREATE TABLE IF NOT EXISTS GLL (
        timestamp TIMESTAMP_MS,
        talker VARCHAR,
        latitude DOUBLE,
        longitude DOUBLE
    );""",
    "HDT": """CREATE TABLE IF NOT EXISTS HDT (
        timestamp TIMESTAMP_MS,
        talker VARCHAR,
        hdg_true DOUBLE
    );""",
    "MDA": """CREATE TABLE IF NOT EXISTS MDA (
        timestamp TIMESTAMP_MS,
        talker VARCHAR,
        pressure_millibars DOUBLE,
        temperature_air_celsius DOUBLE,
        temperature_water_celsius DOUBLE,
        humidity_relative DOUBLE,
        dew_point_celsius DOUBLE,
        twd_true DOUBLE,
        twd_magnetic DOUBLE,
        tws_knots DOUBLE
    );""",
    "MWV": """CREATE TABLE IF NOT EXISTS MWV (
        timestamp TIMESTAMP_MS,
        talker VARCHAR,
        awa DOUBLE,
        aws_knots DOUBLE
    );""",
    "ROT": """CREATE TABLE IF NOT EXISTS ROT (
        timestamp TIMESTAMP_MS,
        talker VARCHAR,
        rate_of_turn DOUBLE
    );""",
    "RSA": """CREATE TABLE IF NOT EXISTS RSA (
        timestamp TIMESTAMP_MS,
        talker VARCHAR,
        rudder_angle DOUBLE
    );""",
    "VTG": """CREATE TABLE IF NOT EXISTS VTG (
        timestamp TIMESTAMP_MS,
        talker VARCHAR,
        cog_true DOUBLE,
        cog_magnetic DOUBLE,
        sog_knots DOUBLE
    );"""
}


def map_fields(sentence_type, talker, parsed_nmea):
# TODO: read the ordering from the database schema
    timestamp_ms = parsed_nmea["timestamp"]
    timestamp = datetime.datetime.fromtimestamp(timestamp_ms / 1000.0, tz=datetime.timezone.utc).replace(tzinfo=None)
    if sentence_type == "DPT":
        return (timestamp, talker, parsed_nmea.get("depth_below_transducer_meters"), parsed_nmea.get("transducer_depth_meters"), parsed_nmea.get("water_depth_meters"))
    elif sentence_type == "GLL":
        return (timestamp, talker, parsed_nmea.get("latitude"), parsed_nmea.get("longitude"))
    elif sentence_type == "HDT":
        return (timestamp, talker, parsed_nmea.get("hdg_true"))
    elif sentence_type == "MDA":
        return (timestamp, talker, parsed_nmea.get("pressure_millibars"), parsed_nmea.get("temperature_air_celsius"), parsed_nmea.get("temperature_water_celsius"), parsed_nmea.get("humidity_relative"), parsed_nmea.get("dew_point_celsius"), parsed_nmea.get("twd_true"), parsed_nmea.get("twd_magnetic"), parsed_nmea.get("tws_knots"))
    elif sentence_type == "MWV":
        return (timestamp, talker, parsed_nmea.get("awa"), parsed_nmea.get("aws_knots"))
    elif sentence_type == "ROT":
        return (timestamp, talker, parsed_nmea.get("rate_of_turn"))
    elif sentence_type == "RSA":
        return (timestamp, talker, parsed_nmea.get("rudder_angle"))
    elif sentence_type == "VTG":
        return (timestamp, talker, parsed_nmea.get("cog_true"), parsed_nmea.get("cog_magnetic"), parsed_nmea.get("sog_knots"))
    return None


def write_batch(conn, batch):
    grouped = defaultdict(list)
    for address_field, parsed_nmea in batch:
        talker = address_field[0:2]
        sentence_type = address_field[2:]
        if sentence_type in TABLE_SCHEMAS:
            row = map_fields(sentence_type, talker, parsed_nmea)
            if row:
                grouped[sentence_type].append(row)
                
    # Don't do anything if there was nothing in the batch:
    if not grouped:
        return

    conn.execute("BEGIN TRANSACTION")
    try:
        for table_name, rows in grouped.items():
            placeholders = ", ".join(["?"] * len(rows[0]))
            conn.executemany(f"INSERT INTO {table_name} VALUES ({placeholders})", rows)
        conn.execute("COMMIT")
        log.debug(f"Inserted {len(batch)} rows into database.")
    except Exception as e:
        conn.execute("ROLLBACK")
        log.error(f"Error inserting batch into DuckDB: {e}")


async def duckdb_publisher_task(database_path, queue):
    """Task that consumes from the queue and publishes to DuckDB."""
    conn = None
    try:
        # Establish connection
        conn = await asyncio.to_thread(duckdb.connect, database_path)

        # Initialize schemas
        for schema_sql in TABLE_SCHEMAS.values():
            await asyncio.to_thread(conn.execute, schema_sql)

        batch_size = config["DUCKDB"].get("BATCH_SIZE", 600)
        batch_interval = config["DUCKDB"].get("BATCH_INTERVAL", 60)
        log.info(f"Using DuckDB batch size {batch_size} and batch interval {batch_interval} seconds.")

        while True:
            batch = []

            # Get items from the queue until we reach the batch size or batch interval, whichever
            # happens first
            start_time = asyncio.get_event_loop().time()
            while len(batch) < batch_size:
                elapsed = asyncio.get_event_loop().time() - start_time
                remaining = batch_interval - elapsed
                if remaining <= 0:
                    break
                try:
                    # In order to honor the batch interval, we need to process the batch
                    # eventually, so set a timeout for the queue get operation.
                    item = await asyncio.wait_for(queue.get(), timeout=remaining)
                    batch.append(item)
                except asyncio.TimeoutError:
                    break

            # Group and insert batch in a single thread-safe transaction
            await asyncio.to_thread(write_batch, conn, batch)

            for _ in range(len(batch)):
                queue.task_done()
    finally:
        if conn is not None:
            log.info("Closing DuckDB connection.")
            await asyncio.to_thread(conn.close)


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
        except (ConnectionResetError, ConnectionRefusedError) as e:
            log.warning(f"Connection error on {host}:{port} ({e})")
            await warn_print_sleep(f"Connection error on {host}:{port} ({e})")
        except asyncio.TimeoutError as e:
            log.warning(f"Timeout reading error from {host}:{port} ({e})")
            await warn_print_sleep(f"Timeout reading error from {host}:{port} ({e})")
        except socket.gaierror as e:
            log.warning(f"DNS error on {host}:{port} ({e})")
            await warn_print_sleep(f"DNS error on {host}:{port} ({e})")
        except OSError as e:
            log.warning(f"Unexpected OS error on {host}:{port} ({e})")
            await warn_print_sleep(f"Unexpected OS error on {host}:{port} ({e})")
        except Exception as e:
            log.exception(f"Unexpected error in reader task for {host}:{port} ({e})")
            await warn_print_sleep(f"Unexpected error in reader task for {host}:{port} ({e})")


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
