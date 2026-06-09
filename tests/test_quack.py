import asyncio
import duckdb
import pytest
import os
import duckdb_services

@pytest.mark.asyncio
async def test_quack_protocol(tmp_path):
    db_path = str(tmp_path / "quack_test.db")
    # Use a different port to avoid conflicts
    address = "localhost:12345"
    token = "test_token"
    
    config = {
        "DUCKDB": {
            "DATABASE_PATH": db_path,
            "BATCH_SIZE": 1,
            "BATCH_INTERVAL": 1,
            "QUACK": {
                "ENABLE": True,
                "ADDRESS": address,
                "TOKEN": token
            }
        }
    }
    
    queue = asyncio.Queue()
    # Note: duckdb_publisher_task expects an open connection
    conn = duckdb.connect(db_path)
    
    # Start publisher task
    task = asyncio.create_task(duckdb_services.duckdb_publisher_task(conn, queue, config))
    
    # Wait for initialization and Quack server start
    await asyncio.sleep(1.0)
    
    try:
        # Try to connect from another connection using quack protocol
        remote_conn = duckdb.connect()
        # Load quack extension explicitly in the client
        remote_conn.execute("LOAD quack")
        # The syntax for ATTACH with quack protocol
        remote_conn.execute(f"ATTACH 'quack:{address}' AS remote (TOKEN '{token}')")
        
        # Verify we can query the GLL table (even if it's empty)
        remote_conn.execute("SELECT * FROM remote.GLL LIMIT 0")
        
        # Put some data in the queue
        data = ("GPGLL", {"latitude": 45.0, "longitude": -123.0, "timestamp": 1700000000000})
        await queue.put(data)
        
        # Wait for flush (batch size is 1)
        # We use wait_for because queue.join() will finish when task_done() is called for all items
        await asyncio.wait_for(queue.join(), timeout=5.0)
        
        # Query via quack. Use the alias 'remote'
        rows = remote_conn.execute("SELECT latitude, longitude FROM remote.GLL").fetchall()
        assert len(rows) == 1
        assert rows[0] == (45.0, -123.0)
        
        remote_conn.close()
    finally:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        conn.close()

@pytest.mark.asyncio
async def test_quack_wrong_token(tmp_path):
    db_path = str(tmp_path / "quack_token_test.db")
    address = "localhost:12346"
    token = "correct_token"
    
    config = {
        "DUCKDB": {
            "DATABASE_PATH": db_path,
            "BATCH_SIZE": 1,
            "BATCH_INTERVAL": 1,
            "QUACK": {
                "ENABLE": True,
                "ADDRESS": address,
                "TOKEN": token
            }
        }
    }
    
    queue = asyncio.Queue()
    conn = duckdb.connect(db_path)
    task = asyncio.create_task(duckdb_services.duckdb_publisher_task(conn, queue, config))
    await asyncio.sleep(1.0)
    
    try:
        remote_conn = duckdb.connect()
        remote_conn.execute("LOAD quack")
        # Attempt with wrong token
        with pytest.raises(duckdb.InvalidInputException):
            remote_conn.execute(f"ATTACH 'quack:{address}' AS remote (TOKEN 'wrong_token')")
    finally:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        conn.close()

@pytest.mark.asyncio
async def test_quack_disabled(tmp_path):
    db_path = str(tmp_path / "quack_disabled_test.db")
    address = "localhost:12347"
    
    config = {
        "DUCKDB": {
            "DATABASE_PATH": db_path,
            "BATCH_SIZE": 1,
            "BATCH_INTERVAL": 1,
            "QUACK": {
                "ENABLE": False,
                "ADDRESS": address
            }
        }
    }
    
    queue = asyncio.Queue()
    conn = duckdb.connect(db_path)
    task = asyncio.create_task(duckdb_services.duckdb_publisher_task(conn, queue, config))
    await asyncio.sleep(1.0)
    
    try:
        remote_conn = duckdb.connect()
        remote_conn.execute("LOAD quack")
        # Attempt to connect when disabled - should fail
        with pytest.raises(duckdb.IOException):
            remote_conn.execute(f"ATTACH 'quack:{address}' AS remote (TOKEN 'some_token')")
    finally:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        conn.close()
