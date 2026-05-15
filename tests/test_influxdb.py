import asyncio
import pytest
from unittest.mock import MagicMock
import main

@pytest.mark.asyncio
async def test_influxdb_publisher_task():
    # Mock config
    main.config = {
        "MMSI": "123456789"
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
    task = asyncio.create_task(main.influxdb_publisher_task(mock_client, "test_db", "nmea-data", queue))
    
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
    lp = kwargs["record"]
    
    # Expected line protocol:
    # nmea-data,mmsi=123456789,sentence=GPGLL latitude=44.623,longitude=-124.05156 1778857043.108
    assert lp.startswith("nmea-data,mmsi=123456789,sentence=GPGLL ")
    
    # Check fields
    parts = lp.split(" ")
    measurement_tags = parts[0]
    fields_str = parts[1]
    timestamp = parts[2]
    
    assert measurement_tags == "nmea-data,mmsi=123456789,sentence=GPGLL"
    
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
    
    # Timestamp should be in seconds
    assert float(timestamp) == 1778857043.108
