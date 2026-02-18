# NMEA Simulator

This directory contains a simulator that generates synthetic NMEA 0183 sentences and publishes them to an MQTT broker. 
It uses the same data structures and configuration as the main `nmea-mqtt` program.

## Usage

To run the simulator:

```bash
python3 simulator/simulate.py
```

The simulator reads its configuration from `config.py` in the parent directory. It will generate data for all sentence types listed in `PUBLISH_INTERVALS` and publish them every 10 seconds.

## Requirements

- Python 3
- `paho-mqtt` library

You can install the requirements using the `requirements.txt` file in the parent directory:

```bash
pip install -r requirements.txt
```
