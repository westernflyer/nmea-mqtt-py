# InfluxDB V3

After NMEA data has been read from the connected sockets and parsed, the
contents are written to an InfluxDB V3 database for further analysis and
storage.

The database name, table, and token are defined in the `config.toml` file.

The tag set includes MMSI, and NMEA address field. The field set includes all 
NMEA fields, with the exception of the checksum field. 

For example, using the InfluxDB line format, this would be a typical write:

```
nmea-data,mmsi=123456789,sentence=GPGLL latitude=44.623,longitude=-124.05156 1778857043.108
```

Note that the timestamp field is in seconds.

There are a number of "black listed" field types that are not written to the 
database:

```
sentence_type
timeUTC
gll_mode
```