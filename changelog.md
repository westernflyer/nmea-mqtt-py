# 1.7.0
Added support for the DuckDB "Quack" protocol.

# 1.6.0
Changed name to `nmea-logger-py`.

# 1.5.0
Wait for the DuckDB queue to drain before exiting.

# 1.4.0
Make recovery more robust and consistent between MQTT and DuckDB.

# 1.3.0
Switch from InfluxDB to DuckDB. InfluxDB had too many limitations.

# 1.2.0
Writes to the InfluxDB database are now batched in order to improve performance.

# 1.1.0
Changed the schema to something less sparse. The sentence type is now used as 
the table name.

# 1.0.0
Added support for InfluxDB V3