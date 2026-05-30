#
# Copyright (c) 2025-present Tom Keffer <tkeffer@gmail.com>
#
# This source code is licensed under the MIT license found in the
# LICENSE.txt.txt file in the root directory of this source tree.
#
import asyncio
import datetime
import logging
from collections import defaultdict
from contextlib import asynccontextmanager

import duckdb

from service_utils import RETRYABLE_ERRORS, warn_print_sleep

log = logging.getLogger("nmea-mqtt.duckdb")

TABLE_SCHEMAS = {
    "DPT": """CREATE TABLE IF NOT EXISTS DPT
              (
                  timestamp
                  TIMESTAMP_MS,
                  talker
                  VARCHAR,
                  depth_below_transducer_meters
                  DOUBLE,
                  transducer_depth_meters
                  DOUBLE,
                  water_depth_meters
                  DOUBLE
              );""",
    "GLL": """CREATE TABLE IF NOT EXISTS GLL
              (
                  timestamp
                  TIMESTAMP_MS,
                  talker
                  VARCHAR,
                  latitude
                  DOUBLE,
                  longitude
                  DOUBLE
              );""",
    "HDT": """CREATE TABLE IF NOT EXISTS HDT
              (
                  timestamp
                  TIMESTAMP_MS,
                  talker
                  VARCHAR,
                  hdg_true
                  DOUBLE
              );""",
    "MDA": """CREATE TABLE IF NOT EXISTS MDA
              (
                  timestamp
                  TIMESTAMP_MS,
                  talker
                  VARCHAR,
                  pressure_millibars
                  DOUBLE,
                  temperature_air_celsius
                  DOUBLE,
                  temperature_water_celsius
                  DOUBLE,
                  humidity_relative
                  DOUBLE,
                  dew_point_celsius
                  DOUBLE,
                  twd_true
                  DOUBLE,
                  twd_magnetic
                  DOUBLE,
                  tws_knots
                  DOUBLE
              );""",
    "MWV": """CREATE TABLE IF NOT EXISTS MWV
              (
                  timestamp
                  TIMESTAMP_MS,
                  talker
                  VARCHAR,
                  awa
                  DOUBLE,
                  aws_knots
                  DOUBLE
              );""",
    "ROT": """CREATE TABLE IF NOT EXISTS ROT
              (
                  timestamp
                  TIMESTAMP_MS,
                  talker
                  VARCHAR,
                  rate_of_turn
                  DOUBLE
              );""",
    "RSA": """CREATE TABLE IF NOT EXISTS RSA
              (
                  timestamp
                  TIMESTAMP_MS,
                  talker
                  VARCHAR,
                  rudder_angle
                  DOUBLE
              );""",
    "VTG": """CREATE TABLE IF NOT EXISTS VTG
              (
                  timestamp
                  TIMESTAMP_MS,
                  talker
                  VARCHAR,
                  cog_true
                  DOUBLE,
                  cog_magnetic
                  DOUBLE,
                  sog_knots
                  DOUBLE
              );"""
}


def map_fields(sentence_type, talker, parsed_nmea):
    # TODO: read the ordering from the database schema
    timestamp_ms = parsed_nmea["timestamp"]
    timestamp = datetime.datetime.fromtimestamp(timestamp_ms / 1000.0,
                                                tz=datetime.timezone.utc).replace(tzinfo=None)
    if sentence_type == "DPT":
        return timestamp, talker, parsed_nmea.get(
            "depth_below_transducer_meters"), parsed_nmea.get(
            "transducer_depth_meters"), parsed_nmea.get("water_depth_meters")
    elif sentence_type == "GLL":
        return timestamp, talker, parsed_nmea.get("latitude"), parsed_nmea.get("longitude")
    elif sentence_type == "HDT":
        return timestamp, talker, parsed_nmea.get("hdg_true")
    elif sentence_type == "MDA":
        return timestamp, talker, parsed_nmea.get("pressure_millibars"), parsed_nmea.get(
            "temperature_air_celsius"), parsed_nmea.get(
            "temperature_water_celsius"), parsed_nmea.get("humidity_relative"), parsed_nmea.get(
            "dew_point_celsius"), parsed_nmea.get("twd_true"), parsed_nmea.get(
            "twd_magnetic"), parsed_nmea.get("tws_knots")
    elif sentence_type == "MWV":
        return timestamp, talker, parsed_nmea.get("awa"), parsed_nmea.get("aws_knots")
    elif sentence_type == "ROT":
        return timestamp, talker, parsed_nmea.get("rate_of_turn")
    elif sentence_type == "RSA":
        return timestamp, talker, parsed_nmea.get("rudder_angle")
    elif sentence_type == "VTG":
        return timestamp, talker, parsed_nmea.get("cog_true"), parsed_nmea.get(
            "cog_magnetic"), parsed_nmea.get("sog_knots")
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
        raise


async def duckdb_publisher_task(db_conn, queue: asyncio.Queue, config: dict):
    """
    Publishes data from an asynchronous queue to a DuckDB database in batches. The batches
    are configurable in size and interval. The function initializes the database schemas on
    startup.

    Args:
        db_conn: DuckDB database connection.
        queue (asyncio.Queue): The asyncio queue containing items to be batched and inserted into
            the database. Each item represents a single row to be processed.
        config (dict): Configuration dictionary.
    """
    # Initialize schemas
    for schema_sql in TABLE_SCHEMAS.values():
        await asyncio.to_thread(db_conn.execute, schema_sql)

    batch_size = config["DUCKDB"].get("BATCH_SIZE", 600)
    batch_interval = config["DUCKDB"].get("BATCH_INTERVAL", 60)
    log.info(f"Using DuckDB batch size {batch_size} and batch interval {batch_interval} seconds.")

    while True:
        batch = []

        # Get items from the queue until we reach the batch size or batch interval, whichever
        # happens first. Measure elapsed time using the event loop clock, which is guaranteed
        # to increase monotonically (unlike time.time()).
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
        await asyncio.to_thread(write_batch, db_conn, batch)

        for _ in range(len(batch)):
            queue.task_done()


async def duckdb_service(queue, config):
    """Service that manages the DuckDB connection and publisher task."""
    duckdb_database_path = config['DUCKDB'].get("DATABASE_PATH", "nmea_database.db")
    while True:
        try:
            async with duckdb_connection(duckdb_database_path) as duckdb_conn:
                await duckdb_publisher_task(duckdb_conn, queue, config)
        except asyncio.CancelledError:
            break
        except RETRYABLE_ERRORS as e:
            await warn_print_sleep(str(e), config, prefix="DuckDB service")
        except Exception as e:
            log.exception("Unexpected error in DuckDB service")
            await warn_print_sleep(str(e), config, prefix="DuckDB service")


@asynccontextmanager
async def duckdb_connection(database_path):
    conn = await asyncio.to_thread(duckdb.connect, database_path)
    try:
        yield conn
    finally:
        log.info("Closing DuckDB connection.")
        await asyncio.to_thread(conn.close)
