from utilities import *


def decode(parts: list[str]) -> dict[str, str | float | int | None]:
    data = {
        "awa": parse_float(parts[1]),
        "aws_knots": parse_float(parts[3]),
        "aws_mps": parse_float(parts[5]),
        "aws_kph": parse_float(parts[7]),
    }
    if parts[2].upper() == 'L':
        data["awa"] *= -1

    return data
