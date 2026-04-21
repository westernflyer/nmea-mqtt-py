# NMEA to MQTT application

This application reads NMEA 0183 sentences from multiple sockets, parses them,
then publish to MQTT as JSON.

## Ingesting NMEA sentences

Data is read from multiple sockets, using an async TCP client.

## Configuration file

The application reads a configuration file, `config.py`, before starting.


## MQTT output

As an example of what gets published, let's look at NMEA sentence `GLL`. It will
get published as topic `nmea/MMSI/ch1/GLL`, where `MMSI` is the MMSI number of the
boat, and `ch` is the channel. The message will look something like this:

    {
        "latitude": 36.805785, 
        "longitude": -121.785685,
        "timeUTC": "18:00:15",
        "gll_mode": "D", 
        "sentence_type": "GLL",
        "timestamp": 1776794415269
    }

