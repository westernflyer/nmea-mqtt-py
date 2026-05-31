#
# Copyright (c) 2025-present Tom Keffer <tkeffer@gmail.com>
#
# This source code is licensed under the MIT license found in the
# LICENSE.txt.txt file in the root directory of this source tree.
#
"""
MQTT-based handling and publishing of NMEA messages.

This module provides routines for publishing NMEA data to an MQTT broker. It
handles all aspects of MQTT communication, including establishing connections,
publishing data at specified intervals, and managing disconnects. It leverages
the asyncio framework to support asynchronous operations and ensure the
efficient handling of MQTT communication.

Classes and high-level functions within the module include:
- mqtt_publisher_task: Publishes parsed NMEA messages from an
  asynchronous queue to an MQTT broker.
- mqtt_service: Manages the MQTT client connection and spawns tasks for
  message publishing and other MQTT-related activities.
- mqtt_misc_loop: Executes MQTT background tasks such as keep-alives.
- mqtt_managed_connection: A context manager to handle MQTT connections
  asynchronously.

It relies on the `paho.mqtt.client` library for MQTT protocol handling and
supports error handling for reconnection on retryable errors.

Functions:
- mqtt_wait_for_disconnect: Handles waiting for disconnect events.
- mqtt_on_connect: Handles MQTT client connection events.
- mqtt_on_disconnect: Handles MQTT client disconnection events.
- mqtt_on_publish: Handles events triggered after message publication.

Exceptions raised during operations are logged or handled, ensuring
robust service behavior.
"""
import asyncio
import json
import logging
from asyncio import Queue
from collections import defaultdict
from contextlib import asynccontextmanager

import paho.mqtt.client as mqtt

import parse_nmea
from service_utils import RETRYABLE_ERRORS, warn_print_sleep

log = logging.getLogger("nmea-mqtt.mqtt")

# State for MQTT
last_published = defaultdict(lambda: 0.0)
publish_intervals = {}

async def mqtt_wait_for_disconnect(disconnect_event: asyncio.Event):
    """Small task that waits for the disconnect event to be set."""
    await disconnect_event.wait()
    log.warning("MQTT disconnect event triggered.")
    raise ConnectionError("MQTT broker disconnected")


async def mqtt_publisher_task(mqtt_client: mqtt.Client, queue: Queue, config: dict) -> None:
    """
    Publishes NMEA data to an MQTT broker at specified intervals.

    The function listens for NMEA messages from an asynchronous queue, calculates
    the time interval since the last publication for a specific address, and, if
    the interval exceeds the defined threshold, publishes the NMEA data to the
    configured MQTT topic.

    Parameters:
        mqtt_client: The MQTT client instance used for publishing messages. This
            client should be connected and ready to publish.
        queue: An asyncio.Queue object containing tuples with address fields and
            parsed NMEA data. The parsed NMEA data is expected to include a
            "timestamp" field.
        config: A configuration dictionary that includes the MQTT options and the
            MMSI value. The MQTT_OPTIONS section may contain MQTT_TOPIC_PREFIX to
            customize the base topic.
    """
    global last_published, publish_intervals

    while True:
        address_field, parsed_nmea = await queue.get()
        delta = parsed_nmea["timestamp"] - last_published[address_field]
        if delta >= publish_intervals[address_field]:
            mqtt_config = config.get("MQTT_OPTIONS", {})
            topic = (f"{mqtt_config.get('MQTT_TOPIC_PREFIX', 'nmea')}/"
                     f"{config['MMSI']}/"
                     f"{address_field}")
            mqtt_publish_nmea(mqtt_client, topic, parsed_nmea, config)
            last_published[address_field] = parsed_nmea["timestamp"]
        queue.task_done()

async def mqtt_service(queue: Queue, config: dict):
    """Service that manages the MQTT connection and publisher tasks."""
    global publish_intervals
    publish_intervals = config.get("MQTT_PUBLISH_INTERVALS", {})

    while True:
        try:
            async with mqtt_managed_connection(userdata=config) as mqtt_client:
                # Use an Event to signal when the MQTT connection is dropped
                disconnect_event = asyncio.Event()

                mqtt_config = config.get("MQTT_OPTIONS", {})
                mqtt_username = mqtt_config.get("MQTT_USERNAME")
                mqtt_password = mqtt_config.get("MQTT_PASSWORD")
                if mqtt_username and mqtt_password:
                    mqtt_client.username_pw_set(mqtt_username, mqtt_password)

                # Set up MQTT callbacks
                mqtt_client.on_connect = mqtt_on_connect
                mqtt_client.on_publish = mqtt_on_publish
                mqtt_client.on_disconnect = lambda client, userdata, flags, rc, properties=None: \
                    mqtt_on_disconnect(client, userdata, flags, rc, disconnect_event, properties)

                mqtt_client.connect(mqtt_config.get("MQTT_BROKER", "localhost"),
                                    mqtt_config.get("MQTT_PORT", 1883), 60)

                # Use asyncio.gather to run the publisher and misc tasks.
                # If any of them fails (including wait_for_disconnect), gather will stop.
                tasks = [
                    asyncio.create_task(mqtt_misc_loop(mqtt_client)),
                    asyncio.create_task(mqtt_publisher_task(mqtt_client, queue, config)),
                    asyncio.create_task(mqtt_wait_for_disconnect(disconnect_event))
                ]
                try:
                    await asyncio.gather(*tasks)
                finally:
                    # Cancel all tasks on exit
                    for task in tasks:
                        task.cancel()
                    # Wait for all tasks to finish, but ignore CancelledError
                    await asyncio.gather(*tasks, return_exceptions=True)
        except asyncio.CancelledError:
            break
        except RETRYABLE_ERRORS as e:
            await warn_print_sleep(str(e), config, prefix="MQTT service")
        except Exception as e:
            log.exception("Unexpected error in MQTT service")
            await warn_print_sleep(str(e), config, prefix="MQTT service")

def mqtt_on_connect(client, config, flags, reason_code, properties):
    """The callback for when the client receives a CONNACK response from the server."""
    print(f"Connected to MQTT broker with result code: '{reason_code}'")
    log.info(f"Connected to MQTT broker with result code: '{reason_code}'")

def mqtt_on_disconnect(client, config, flags, reason_code, disconnect_event, properties):
    """The callback for when the client disconnects from the MQTT broker."""
    print(f"Disconnected from MQTT broker with result code: '{reason_code}'")
    log.warning(f"Disconnected from MQTT broker with result code: '{reason_code}'")
    disconnect_event.set()

def mqtt_on_publish(client, config, mid, reason_code, properties):
    """Callback for when a PUBLISH message is sent to the server."""
    if config.get("DEBUG", 0) >= 2:
        print(f"Message id {mid} published.")
        log.debug(f"Message id {mid} published.")

def mqtt_publish_nmea(mqtt_client: mqtt.Client, topic: str, parsed_nmea: parse_nmea.NmeaDict, config: dict):
    """Publish parsed NMEA data to MQTT."""
    info = mqtt_client.publish(topic, json.dumps(parsed_nmea), qos=0)
    if info.rc != mqtt.MQTT_ERR_SUCCESS:
        log.error(f"Failed to publish to MQTT: {info.rc}")
    if config.get("DEBUG", 0) >= 1 and info.mid % 1000 == 0:
        log.debug(f"{info.mid}: {parsed_nmea['sentence_type']} {parsed_nmea['timestamp']}")

async def mqtt_misc_loop(mqtt_client: mqtt.Client):
    """Task to handle MQTT background tasks like keep-alives."""
    while True:
        try:
            mqtt_client.loop_misc()
        except Exception as e:
            log.error(f"Error in MQTT misc loop: {e}")
            raise
        await asyncio.sleep(1)

@asynccontextmanager
async def mqtt_managed_connection(userdata=None):
    """Provides an async context manager for a paho MQTT client connection integrated with asyncio."""
    loop = asyncio.get_running_loop()
    mqtt_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, userdata=userdata)

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
