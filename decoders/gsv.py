from utilities import *


def decode(parts: list[str]) -> dict[str, str | float | int | None]:
    data = {
        "gsv_messages": parse_int(parts[1]),
        "message_number": parse_int(parts[2]),
        "satellites_in_view": parse_int(parts[3]),
        "satellites": []
    }
    for i in range(4, len(parts) - 4, 4):
        if len(parts) >= i + 4:
            satellite_info = {
                "satellite_prn": parse_int(parts[i]),
                "elevation_angle": parse_int(parts[i + 1]),
                "azimuth_angle": parse_int(parts[i + 2]),
                "snr": parse_int(parts[i + 3])
            }
            data["satellites"].append(satellite_info)
    return data
