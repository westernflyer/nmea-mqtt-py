## nmea-mqtt-py

Read NMEA 0183 sentences from one or more sockets, parse them, then publish to
MQTT as JSON and (optionally) store to a DuckDB database.

## Socket input

One or more sockets can be monitored. See option `NMEA_SOCKETS` under the
`[NMEA_OPTIONS]` section in `config.toml`. 

The input is expected to be standard NMEA sentences, possibly with a checksum.
For example,

```aiignore
$GPGLL,4202.8367,N,12416.0404,W,123408.8,A,D*44
$SDDBT,347.24,f,105.84,M,57.87,F*05
$GPDTM,W84,,0.0000,N,0.0000,E,0,W84*71
$HETHS,327,D*30
$WIMWV,20.4,R,3.19,N,A*2E
$IIVHW,327,T,308.4,M,,N,,K*42
$IIVBW,,,V,-0,-0.01,A,,V,0,A*5C
...
```

## MQTT output

As an example of what gets published to MQTT, let's look at NMEA address field 
`GPGLL`. It will get published as topic `nmea/MMSI/GPGLL`, where `MMSI` is the
MMSI number of the boat. The message will look something like:

    {
    "latitude": 22.929,
    "longitude": -109.755,
    "timeUTC": "23:55:31",
    "gll_mode": "D",
    "sentence_type": "GLL",
    "timestamp": 1743983731183
    }

There is a hack in the code for the FT602. If an address field of `WIMWV` is
received from port 60002, it will be changed to `FTMWV` to disambiguate it from
sentences being sent by the Airmar 200WX.

## DuckDB output

If the `[DUCKDB]` section is present in `config.toml`, parsed NMEA data will also
be written to a DuckDB database.

Example configuration:

```toml
[DUCKDB]
DATABASE_PATH = "nmea_database.db"
BATCH_SIZE = 100
BATCH_INTERVAL = 10
```

The data is grouped by sentence type and written using parameterized batch insertions. The database contains eight distinct tables, one for each supported NMEA sentence type (`DPT`, `GLL`, `HDT`, `MDA`, `MWV`, `ROT`, `RSA`, `VTG`). Naive UTC timestamps are stored under the `timestamp` column.

Here are the SQL schemas used for each of the eight tables:

```sql
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

## Requirements

- DuckDB.
- An MQTT broker.
- Python v3.12 or greater. Earlier versions cannot be used due to how
  parameter types have been specified, and how `asyncio` raises `Timeout` 
  exceptions.
- `git`
- Root privileges to install (but not to run).

## Installation

1. Create the user `nmea` and set a password:

    ```
    sudo useradd -m -c"Owns the nmea-mqtt process" -s /bin/bash nmea
    sudo passwd nmea
   ```

2. Log in as that user, then clone the Git repository. The following will place
the repository at `~nmea/git/nmea-mqtt-py`. Adjust the path to your preference,
but make sure you use it consistently in what follows.

    ```
    cd ~
    mkdir git
    cd git
    git clone https://github.com/westernflyer/nmea-mqtt-py
    ```

3. Create a Python virtual environment, activate it, then install requirements

    ```
    cd ~/git/nmea-mqtt-py
    python3 -m venv .venv
    source .venv/bin/activate
    pip install -e .
    ```
   
4. Copy a configuration file into place, then edit it with your requirements.

   ```
   cd ~/git/nmea-mqtt-py
   cp config_sample.toml config.toml
   nano config.toml
   ```

5. Install a defaults environment file, `/etc/default/nmea-mqtt`, with the
   following contents. Substitute your own values for the `DUCKDB_DATABASE_PATH`
   and `DEBUG` entries.

    ```
    # Environment variables for the nmea-mqtt service

    # For DuckDB (optional override):
    # DUCKDB_DATABASE_PATH=nmea_database.db

    # Set debugging:
    DEBUG=1
    ```
   
6. Time to install a systemd service file. Log into an account that has root
privileges. Copy the provided systemd service file into place, then edit it
appropriately. In particular, make sure the entries for `WorkingDirectory` and
`ExecStart` reflect your choices.

   ```
   cd ~nmea/git/nmea-mqtt-py/systemd
   sudo cp nmea-mqtt.service /etc/systemd/system
   sudo nano /etc/systemd/system/nmea-mqtt.service
   ```
   
7. Reload the systemd manager to reflect your changes, then start the nmea-mqtt
   daemon. Finally, enable the daemon so it will automatically start when the
   system boots.

   ```
   sudo systemctl daemon-reload
   sudo systemctl start nmea-mqtt
   sudo systemctl enable nmea-mqtt
   ```
   
## Copyright

Copyright (c) 2025 Tom Keffer <tkeffer@gmail.com>

See the file LICENSE.txt for your rights.