import asyncio
import pytest
from unittest.mock import MagicMock
import main

@pytest.mark.asyncio
async def test_influxdb_publisher_task():
    # Mock config
    main.config = {
        "MMSI": "123456789",
        "INFLUXDB": {
            "BATCH_SIZE": 1,
            "BATCH_INTERVAL": 10
        }
    }
    
    # Mock InfluxDBClient3
    mock_client = MagicMock()
    
    # Mock queue
    queue = asyncio.Queue()
    
    # Sample data
    address_field = "GPGLL"
    parsed_nmea = {
        "latitude": 44.623,
        "longitude": -124.05156,
        "timeUTC": "12:34:56",
        "gll_mode": "A",
        "sentence_type": "GLL",
        "timestamp": 1778857043108 # ms
    }
    
    await queue.put((address_field, parsed_nmea))
    
    # Create the task
    task = asyncio.create_task(main.influxdb_publisher_task(mock_client, "test_db", queue))
    
    # Wait for the task to process one item
    # Since the task uses asyncio.to_thread, we might need to wait a bit or use a timeout join
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
    
    # Verify client.write call
    mock_client.write.assert_called_once()
    args, kwargs = mock_client.write.call_args
    
    assert kwargs["database"] == "test_db"
    records = kwargs["record"]
    assert isinstance(records, list)
    assert len(records) == 1
    lp = records[0]
    
    # Expected line protocol:
    # GLL,mmsi=123456789,talker=GP latitude=44.623,longitude=-124.05156 1778857043108
    assert lp.startswith("GLL,mmsi=123456789,talker=GP ")
    
    # Check fields
    parts = lp.split(" ")
    measurement_tags = parts[0]
    fields_str = parts[1]
    timestamp = parts[2]
    
    assert measurement_tags == "GLL,mmsi=123456789,talker=GP"
    
    fields = dict(item.split("=") for item in fields_str.split(","))
    assert "latitude" in fields
    assert float(fields["latitude"]) == 44.623
    assert "longitude" in fields
    assert float(fields["longitude"]) == -124.05156
    
    # Blacklisted fields should NOT be in fields
    assert "sentence_type" not in fields
    assert "timeUTC" not in fields
    assert "gll_mode" not in fields
    assert "timestamp" not in fields
    
    # Timestamp should be in ms
    assert float(timestamp) == 1778857043108


@pytest.mark.asyncio
async def test_influxdb_batching():
    # Mock config with small batch size and interval
    main.config = {
        "MMSI": "123456789",
        "INFLUXDB": {
            "BATCH_SIZE": 3,
            "BATCH_INTERVAL": 1
        }
    }

    # Mock InfluxDBClient3
    mock_client = MagicMock()

    # Mock queue
    queue = asyncio.Queue()

    # Sample data
    data = [
        ("GPGLL", {"latitude": 44.623, "longitude": -124.05156, "timestamp": 1778857043108}),
        ("GPRMC", {"latitude": 44.624, "longitude": -124.05157, "timestamp": 1778857043109}),
        ("GPVTG", {"speed": 5.5, "timestamp": 1778857043110}),
    ]

    for item in data:
        await queue.put(item)

    # Create the task
    task = asyncio.create_task(main.influxdb_publisher_task(mock_client, "test_db", queue))

    # Wait for the task to process the batch (size 3)
    try:
        await asyncio.wait_for(queue.join(), timeout=2.0)
    except asyncio.TimeoutError:
        pytest.fail("Queue join timed out")

    # Cancel the task
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass

    # Verify client.write call
    mock_client.write.assert_called_once()
    args, kwargs = mock_client.write.call_args

    assert kwargs["database"] == "test_db"
    records = kwargs["record"]

    assert isinstance(records, list)
    assert len(records) == 3
    assert records[0].startswith("GLL,mmsi=123456789,talker=GP ")
    assert records[1].startswith("RMC,mmsi=123456789,talker=GP ")
    assert records[2].startswith("VTG,mmsi=123456789,talker=GP ")


@pytest.mark.asyncio
async def test_influxdb_timeout_flush():
    # Mock config with large batch size and small interval
    main.config = {
        "MMSI": "123456789",
        "INFLUXDB": {
            "BATCH_SIZE": 10,
            "BATCH_INTERVAL": 0.5
        }
    }

    # Mock InfluxDBClient3
    mock_client = MagicMock()

    # Mock queue
    queue = asyncio.Queue()

    # Only put 2 items
    data = [
        ("GPGLL", {"latitude": 44.623, "longitude": -124.05156, "timestamp": 1778857043108}),
        ("GPRMC", {"latitude": 44.624, "longitude": -124.05157, "timestamp": 1778857043109}),
    ]

    for item in data:
        await queue.put(item)

    # Create the task
    task = asyncio.create_task(main.influxdb_publisher_task(mock_client, "test_db", queue))

    # Wait for the task to process the batch (triggered by timeout)
    try:
        await asyncio.wait_for(queue.join(), timeout=2.0)
    except asyncio.TimeoutError:
        pytest.fail("Queue join timed out")

    # Cancel the task
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass

    # Verify client.write call
    mock_client.write.assert_called_once()
    args, kwargs = mock_client.write.call_args
    records = kwargs["record"]

    assert isinstance(records, list)
    assert len(records) == 2
