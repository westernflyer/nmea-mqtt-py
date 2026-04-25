import parse_nmea

def test_parse_latitude():
    assert parse_nmea.parse_latitude(None) is None
    assert parse_nmea.parse_latitude('') is None
    assert parse_nmea.parse_latitude('0') == 0
    assert parse_nmea.parse_latitude('151130.00') == 1511.5
    assert parse_nmea.parse_latitude('151130.00', 'S') == -1511.5

def test_parse_longitude():
    assert parse_nmea.parse_longitude(None) is None
    assert parse_nmea.parse_longitude('') is None
    assert parse_nmea.parse_longitude('0') == 0
    assert parse_nmea.parse_longitude('1501130.00') == 15011.5
    assert parse_nmea.parse_longitude('1501130.00', 'W') == -15011.5

def test_parse_utc_time():
    assert parse_nmea.parse_time(None) is None
    assert parse_nmea.parse_time('') is None
    assert parse_nmea.parse_time('0') is None
    assert parse_nmea.parse_time("151209.00") == "15:12:09"
    assert parse_nmea.parse_time("151209.80") == "15:12:10"
