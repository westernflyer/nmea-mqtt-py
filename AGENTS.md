# NMEA to MQTT application

This application reads NMEA 0183 sentences from multiple sockets, parses them,
then publish to MQTT as JSON.

## Ingesting NMEA sentences

Data is read from multiple sockets, using an async TCP client.

## Configuration file

The application reads a configuration file, config.py, before starting.


## MQTT output

As an example of what gets published, let's look at NMEA sentence `GLL`. It will
get published as topic `nmea/MMSI/GLL`, where `MMSI` is the MMSI number of the
boat. The message will look something like:

    {
    "latitude": 22.929,
    "longitude": -109.755,
    "timeUTC": "23:55:31",
    "gll_mode": "D",
    "sentence_type": "GLL",
    "timestamp": 1743983731183
    }

