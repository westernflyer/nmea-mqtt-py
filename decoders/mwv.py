from utilities import *


def decode(parts: list[str]) -> dict[str, str | float | int | None]:
    # Only accept sentences with a good status
    status = parts[5].upper()
    if status != 'A':
        raise NMEAStatusError(f"Bad status ('{status}') for sentence type 'MWV'")

    # Determine if we have true or apparent wind
    reference = parts[2].upper()
    if reference == 'T':
        key = "wind_angle_true"
        value_key = "wind_speed_true_knots"
    elif reference == 'R':
        key = "wind_angle_apparent"
        value_key = "wind_speed_apparent_knots"
    else:
        raise NMEAParsingError(f"Unknown MWV reference '{reference}' (expected 'T' or 'R')")

    # Convert to knots
    value_knot = parse_float(parts[3])
    unit = parts[4].upper()
    if unit == 'N':
        pass
    elif unit == 'M':
        # Unit is m/s
        value_knot *= 1.94384
    elif unit == 'K':
        # Unit is kph
        value_knot *= 0.539957
    else:
        raise NMEAParsingError(f"Unknown MWV unit '{unit}' (expected 'M', 'K', or 'N')")

    data = {
        key: parse_float(parts[1]),
        value_key: value_knot,
    }

    return data
