from utilities import *


def decode(parts: list[str]) -> dict[str, str | float | int | None]:
    data = {
        "pressure_inch": parse_float(parts[1]),
        "pressure_bar": parse_float(parts[3]),
        "temperature_air_celsius": parse_float(parts[5]),
        "temperature_water_celsius": parse_float(parts[7]),
        "humidity_relative": parse_float(parts[9]),
        "dew_point_celsius": parse_float(parts[11]),
        "twd_true": parse_float(parts[13]),
        "twd_mag": parse_float(parts[15]),
        "tws_knot": parse_float(parts[17]),
        "tws_mps": parse_float(parts[19]),
    }
    return data
