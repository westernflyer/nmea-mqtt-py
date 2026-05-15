import asyncio
import pytest
from unittest.mock import MagicMock
import main
import json

@pytest.mark.asyncio
async def test_mqtt_publisher_task():
    # Mock config
    main.config = {
        "MMSI": "123456789",
        "MQTT_TOPIC_PREFIX": "nmea"
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
    
    assert topic == "nmea/123456789/GPGLL"
    payload_dict = json.loads(payload)
    assert payload_dict == parsed_nmea
