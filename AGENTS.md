# Implement DuckDB data storage

Currently, this program uses InfluxDB V3 as a database. I am running into its
limit of 432 parquet files per query. The goal is to rewrite program to use a
DuckDB database (Hot Layer). Later, I will write another program to copy it to
Parquet files (Cold Layer) offline.

As before, the implementation should parse the incoming NMEA data and put it in
consumer queues. One  queue is used to publish to an MQTT broker. It is working
fine. 

The other queue is consumed by DuckDB. The writes should be buffered so that
data is written every so many seconds, or so many rows (set in the configuration
file), whichever comes first. This minimizes transaction costs, maximizes
compression, and maximizes throughput.

There are eight different sentence types. The implementation should use eight
different tables, one for each sentence type. Here are the schemas for each
table:

```SQL
CREATE TABLE IF NOT EXISTS DPT (
    timestamp TIMESTAMP_MS,
    talker VARCHAR,
    depth_below_transducer_meters DOUBLE,
    transducer_depth_meters DOUBLE,
    water_depth_meters DOUBLE
);

CREATE TABLE IF NOT EXISTS GLL (
    timestamp TIMESTAMP_MS,
    talker VARCHAR,
    latitude DOUBLE,
    longitude DOUBLE
);

CREATE TABLE IF NOT EXISTS HDT (
    timestamp TIMESTAMP_MS,
    talker VARCHAR,
    hdg_true DOUBLE
);

CREATE TABLE IF NOT EXISTS MDA (
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
);

CREATE TABLE IF NOT EXISTS MWV (
    timestamp TIMESTAMP_MS,
    talker VARCHAR,
    awa DOUBLE,
    aws_knots DOUBLE
);

CREATE TABLE IF NOT EXISTS ROT (
    timestamp TIMESTAMP_MS,
    talker VARCHAR,
    rate_of_turn DOUBLE
);

CREATE TABLE IF NOT EXISTS RSA (
    timestamp TIMESTAMP_MS,
    talker VARCHAR,
    rudder_angle DOUBLE
);


CREATE TABLE IF NOT EXISTS VTG (
    timestamp TIMESTAMP_MS,
    talker VARCHAR,
    cog_true DOUBLE,
    cog_magnetic DOUBLE,
    sog_knots DOUBLE
);

```

Note that the talker and the sentence type can be extracted from the address
field. For example, an address field `GPGLL` has talker `GP` and sentence type
`GLL`.

The path to the DuckDB database is specified in the configuration file.

Change the configuration files `config.toml` and `config_sample.toml` to
reflect. Change the test modules. Change the `README.md` file to reflect.