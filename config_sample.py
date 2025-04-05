"""Configuration file for nmea-mqtt-py

    DO NOT EDIT THIS FILE!

Instead, copy it, then edit the copy.
     cp config-sample.py config.py
     nano config.py
"""

# Vessel related
MMSI = 368323170

# NMEA related
NMEA_HOST = "localhost"
NMEA_PORT = 10110  # Adjust based on your setup

# MQTT related
MQTT_BROKER = "localhost"
MQTT_PORT = 1883
MQTT_TOPIC_PREFIX = "nmea"
MQTT_USERNAME = ""
MQTT_PASSWORD = ""

# How often to publish in milliseconds. If a sentence is not listed below, it will not get published.
PUBLISH_INTERVALS = {
    "GGA": 10000,
    "GLL": 10000,
    "HDT": 10000,
    "MDA": 10000,
    "MWV": 10000,
    "RMC": 10000,
    "RSA": 10000,
    "VTG": 10000,
    "VWR": 10000,
}
