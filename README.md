## nmea-logger-py

Read NMEA 0183 sentences from one or more sockets, parse them, then publish to
MQTT as JSON, and store to a DuckDB database.

## Socket input

One or more sockets can be monitored. See option `NMEA_SOCKETS` under the
`[NMEA_OPTIONS]` section in `config.toml`. 

The input is expected to be standard NMEA sentences, possibly with a checksum.
For example,

```
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

## DuckDB database

Parsed NMEA data is also written to a DuckDB database.

Example configuration:

```toml
[DUCKDB]
DATABASE_PATH = "nmea_database.db"
BATCH_SIZE = 1200
BATCH_INTERVAL = 120
```

The data is grouped by sentence type and written using parameterized batch
insertions. The database contains eight distinct tables, one for each supported
NMEA sentence type (`DPT`, `GLL`, `HDT`, `MDA`, `MWV`, `ROT`, `RSA`, `VTG`).
Naive UTC timestamps are stored under the `timestamp` column.

Here is the schema for the `GLL` table. Other tables are similar.

```sql
CREATE TABLE IF NOT EXISTS GLL
(
    timestamp TIMESTAMP_MS,
    talker    VARCHAR,
    latitude  DOUBLE,
    longitude DOUBLE
);
```

### DuckDB Quack protocol

To allow multiple processes to access the DuckDB database concurrently, you can
enable the [Quack protocol](https://duckdb.org/docs/current/quack/overview).
This starts a Quack server within the `nmea-logger` process, which maintains
primary ownership of the database file while allowing remote connections.

The Quack protocol requires DuckDB version 1.5.3 or later.

Example configuration in `config.toml`:

```toml
[DUCKDB.QUACK]
ENABLE = true
ADDRESS = "localhost:9494"
TOKEN = "optional_secret_token"
```

You can then connect to the database from another process (e.g., using the DuckDB CLI):

```bash
duckdb -c "ATTACH 'quack:localhost:9494' AS nmea (TOKEN optional_secret_token); SELECT * FROM nmea.GLL LIMIT 10;"```
```

## Requirements

- DuckDB (version 1.5.3 or later required for Quack protocol).
- An MQTT broker.
- Python v3.12 or greater. Earlier versions cannot be used due to how
  parameter types have been specified, and how `asyncio` raises `Timeout` 
  exceptions.
- `git`
- Root privileges to install (but not to run).

## Installation

1. Create the user `nmea` and set a password:

    ```
    sudo useradd -m -c"Owns the nmea-logger process" -s /bin/bash nmea
    sudo passwd nmea
   ```

2. Log in as that user, then clone the Git repository. The following will place
the repository at `~nmea/git/nmea-logger-py`. Adjust the path to your preference,
but make sure you use it consistently in what follows.

    ```
    cd ~
    mkdir git
    cd git
    git clone https://github.com/westernflyer/nmea-logger-py
    ```

3. Create a Python virtual environment, activate it, then install requirements

    ```
    cd ~/git/nmea-logger-py
    python3 -m venv .venv
    source .venv/bin/activate
    pip install -e .
    ```
   
4. Copy a configuration file into place, then edit it with your requirements.

   ```
   cd ~/git/nmea-logger-py
   cp config_sample.toml config.toml
   nano config.toml
   ```

5. Time to install a systemd service file. Log into an account that has root
   privileges. Copy the provided systemd service file into place, then edit it
   appropriately. In particular, make sure the entries for `WorkingDirectory` and
   `ExecStart` reflect your choices.

   ```
   cd ~nmea/git/nmea-logger-py/systemd
   sudo cp nmea-logger.service /etc/systemd/system
   sudo nano /etc/systemd/system/nmea-logger.service
   ```
   
6. Reload the systemd manager to reflect your changes, then start the `nmea-logger`
   daemon. Finally, enable the daemon so it will automatically start when the
   system boots.

   ```
   sudo systemctl daemon-reload
   sudo systemctl start nmea-logger
   sudo systemctl enable nmea-logger
   ```
   
## Copyright

Copyright (c) 2025-present Tom Keffer <tkeffer@gmail.com>

See the file LICENSE.txt for your rights.