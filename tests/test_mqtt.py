import asyncio
import pytest
from unittest.mock import MagicMock, AsyncMock
import main
import json

main.publish_intervals['GPGLL'] = 1.0

@pytest.mark.asyncio
async def test_mqtt_publisher_task():
    # Mock config
    main.config = {
        "MMSI": "123456789",
        "MQTT_OPTIONS": {
            "MQTT_TOPIC_PREFIX": "nmea-debug"
        }
    }
    
    # Mock mqtt_client
    mock_mqtt_client = MagicMock()
    mock_mqtt_client.publish.return_value.rc = 0
    mock_mqtt_client.publish.return_value.mid = 1
    
    # Mock queue
    queue = asyncio.Queue()
    
    # Sample data
    address_field = "GPGLL"
    parsed_nmea = {
        "latitude": 44.623,
        "longitude": -124.05156,
        "timeUTC": "12:34:56",
        "sentence_type": "GLL",
        "timestamp": 1778857043108
    }
    
    await queue.put((address_field, parsed_nmea))
    
    # Create the task
    task = asyncio.create_task(main.mqtt_publisher_task(mock_mqtt_client, queue))
    
    # Wait for processing
    try:
        await asyncio.wait_for(queue.join(), timeout=1.0)
    except asyncio.TimeoutError:
        pytest.fail("Queue join timed out")
    
    # Cancel the task
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass
    
    # Verify mqtt_client.publish call
    assert mock_mqtt_client.publish.called
    
    args, kwargs = mock_mqtt_client.publish.call_args
    topic = args[0]
    payload = args[1]
    
    assert topic == "nmea-debug/123456789/GPGLL"
    payload_dict = json.loads(payload)
    assert payload_dict == parsed_nmea


@pytest.mark.asyncio
async def test_mqtt_service_task_cancellation():
    # Setup mocks
    original_config = main.config
    original_conn = main.mqtt_managed_connection
    original_wait = main.mqtt_wait_for_disconnect
    original_pub = main.mqtt_publisher_task
    original_misc = main.mqtt_misc_loop
    original_sleep = main.warn_print_sleep

    try:
        main.config = {
            "MQTT_OPTIONS": {
                "MQTT_BROKER": "localhost",
                "MQTT_PORT": 1883
            },
            "MMSI": "123456789"
        }

        mock_mqtt_client = MagicMock()

        # Mock mqtt_managed_connection
        class MockManagedConnection:
            async def __aenter__(self):
                return mock_mqtt_client
            async def __aexit__(self, exc_type, exc_val, exc_tb):
                pass

        main.mqtt_managed_connection = MagicMock(return_value=MockManagedConnection())

        # Mock tasks
        async def mock_wait_for_disconnect(event):
            await asyncio.sleep(0.1)
            raise ConnectionError("Mock disconnect")

        main.mqtt_wait_for_disconnect = mock_wait_for_disconnect

        publisher_task_running = True
        async def mock_mqtt_publisher_task(client, q):
            nonlocal publisher_task_running
            try:
                while True:
                    await asyncio.sleep(1)
            except asyncio.CancelledError:
                publisher_task_running = False
                raise

        main.mqtt_publisher_task = mock_mqtt_publisher_task

        misc_loop_running = True
        async def mock_mqtt_misc_loop(client):
            nonlocal misc_loop_running
            try:
                while True:
                    await asyncio.sleep(1)
            except asyncio.CancelledError:
                misc_loop_running = False
                raise

        main.mqtt_misc_loop = mock_mqtt_misc_loop

        class StopLoop(Exception): pass

        async def mock_warn_print_sleep(msg, prefix=""):
            raise StopLoop()

        main.warn_print_sleep = mock_warn_print_sleep

        queue = asyncio.Queue()

        # Run mqtt_service
        try:
            await main.mqtt_service(queue)
        except StopLoop:
            pass

        # Verify that sub-tasks were cancelled
        assert publisher_task_running is False, "mqtt_publisher_task was not cancelled"
        assert misc_loop_running is False, "mqtt_misc_loop was not cancelled"

    finally:
        # Restore originals
        main.config = original_config
        main.mqtt_managed_connection = original_conn
        main.mqtt_wait_for_disconnect = original_wait
        main.mqtt_publisher_task = original_pub
        main.mqtt_misc_loop = original_misc
        main.warn_print_sleep = original_sleep
