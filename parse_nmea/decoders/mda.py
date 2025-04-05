from parse_nmea.__init__ import *


def decode(parts: list[str]) -> dict[str, str | float | int | None]:
    data = {
        "pressure_inches": parse_float(parts[1]),
        "pressure_bars": parse_float(parts[3]),
        "temperature_air_celsius": parse_float(parts[5]),
        "temperature_water_celsius": parse_float(parts[7]),
        "humidity_relative": parse_float(parts[9]),
        "dew_point_celsius": parse_float(parts[11]),
        "twd_true": parse_float(parts[13]),
        "twd_magnetic": parse_float(parts[15]),
        "tws_knots": parse_float(parts[17]),
        "tws_mps": parse_float(parts[19]),
    }
    if data["pressure_bars"] is not None:
        data["pressure_millibars"] = data["pressure_bars"] * 1000
    else:
        data["pressure_millibars"] = None
    return data
