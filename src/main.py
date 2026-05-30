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
import logging
import os
import sys
import tomllib
from typing import AsyncGenerator

import mqtt_services
import duckdb_services
import parse_nmea
from service_utils import RETRYABLE_ERRORS, warn_print_sleep

# Global variables
config = {}
publish_intervals = {}

# Logger will be initialized in main()
log = logging.getLogger("nmea-mqtt")


async def main():
    global config, publish_intervals

    parser = argparse.ArgumentParser(
        description="Read NMEA sentences from multiple sockets, parse, then publish to MQTT.")
    parser.add_argument("--config", default="config.toml",
                        help="Path to the TOML configuration file (default: config.toml)")
    args = parser.parse_args()

    try:
        with open(args.config, "rb") as f:
            config = tomllib.load(f)
    except FileNotFoundError:
        sys.exit(f"Configuration file {args.config} not found.")

    if os.getenv("NMEA_MQTT_DEBUG") is not None:
        config["DEBUG"] = int(os.getenv("NMEA_MQTT_DEBUG", 0))

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

    # Shared queues for parsed NMEA sentences. Moving them here makes them
    # persist across failures in any individual service.
    mqtt_queue = asyncio.Queue(maxsize=1000)
    duckdb_queue = asyncio.Queue(maxsize=1000)
    subscribers = [mqtt_queue, duckdb_queue]

    # Start the self-healing services
    tasks = [
        asyncio.create_task(mqtt_services.mqtt_service(mqtt_queue, config)),
        asyncio.create_task(duckdb_services.duckdb_service(duckdb_queue, config))
    ]

    # Start the NMEA readers
    nmea_options = config.get("NMEA_OPTIONS", {})
    for host_url, port in nmea_options.get("NMEA_SOCKETS", []):
        tasks.append(asyncio.create_task(
            nmea_reader_task(host_url, port, subscribers)))

    try:
        # Wait for all tasks to complete (which they shouldn't, as they are self-healing)
        await asyncio.gather(*tasks)
    except asyncio.CancelledError:
        log.info("Main loop cancelled.")
    except Exception as e:
        log.exception(f"Fatal error in main loop: {e}")


async def nmea_reader_task(host, port, subscribers):
    """Task for reading from a single NMEA socket and putting into the queue.
    Args:
        host (str): The hostname or IP address of the NMEA socket.
        port (int): The port number of the NMEA socket.
        subscribers (list[asyncio.Queue]): List of queues to put parsed NMEA data into.
    """
    global publish_intervals
    log.info(f"Starting NMEA reader for {host}:{port}")
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
        except RETRYABLE_ERRORS as e:
            await warn_print_sleep(str(e), config, prefix=f"{host}:{port}")
        except Exception as e:
            log.exception(f"Unexpected error in reader task for {host}:{port}")
            await warn_print_sleep(str(e), config, prefix=f"{host}:{port}")


async def gen_nmea(host: str, port: int) -> AsyncGenerator[str, None]:
    """Listen for NMEA data on a TCP socket."""
    nmea_options = config.get("NMEA_OPTIONS", {})
    nmea_timeout = nmea_options.get("NMEA_TIMEOUT", 30)
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


def run():
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        sys.exit("Keyboard interrupt. Exiting.")


if __name__ == "__main__":
    run()
