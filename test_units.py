import main

def test_parse_latitude():
    assert main.parse_latitude(None) is None
    assert main.parse_latitude('') is None
    assert main.parse_latitude('0') == 0
    assert main.parse_latitude('151130.00') == 1511.5
    assert main.parse_latitude('151130.00', 'S') == -1511.5

def test_parse_longitude():
    assert main.parse_longitude(None) is None
    assert main.parse_longitude('') is None
    assert main.parse_longitude('0') == 0
    assert main.parse_longitude('1501130.00') == 15011.5
    assert main.parse_longitude('1501130.00', 'W') == -15011.5

def test_parse_utc_time():
    assert main.parse_time(None) is None
    assert main.parse_time('') is None
    assert main.parse_time('0') is None
    assert main.parse_time("151209.00") == "15:12:09"
    assert main.parse_time("151209.80") == "15:12:10"
