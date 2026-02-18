#!/usr/bin/env python3
#
# Copyright (c) 2025-present Tom Keffer <tkeffer@gmail.com>
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.
#
"""Simulate NMEA sentences and publish them to MQTT."""
from __future__ import annotations

import json
import random
import sys
import time
from pathlib import Path

import paho.mqtt.client as mqtt

# Add the parent directory to sys.path so we can import config and parse_nmea
parent_dir = str(Path(__file__).resolve().parent.parent)
sys.path.insert(0, parent_dir)

import parse_nmea
from config import *


def main():
    print("Starting NMEA simulator...")
    print(f"MQTT Broker: {MQTT_BROKER}:{MQTT_PORT}")
    print(f"Topic prefix: {MQTT_TOPIC_PREFIX}")

    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    if MQTT_USERNAME and MQTT_PASSWORD:
        client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)

    client.on_connect = lambda c, u, f, r, p: print(
        f"Connected to MQTT broker with result code: {r}")

    client.connect(MQTT_BROKER, MQTT_PORT, 60)
    client.loop_start()

    try:
        while True:
            # Generate and publish data for each sentence type in PUBLISH_INTERVALS
            for sentence_type in PUBLISH_INTERVALS:
                sentence = generate_sentence(sentence_type)
                if sentence:
                    try:
                        parsed_nmea = parse_nmea.parse(sentence)
                        publish_nmea(client, parsed_nmea)
                    except Exception as e:
                        print(f"Error parsing/publishing {sentence_type}: {e}")

            # Wait for a bit before the next round. 
            # In a real simulator we might want different frequencies, 
            # but for now, let's just loop every 10 seconds (default interval in config)
            time.sleep(10)
    except KeyboardInterrupt:
        print("\nStopping simulator...")
    finally:
        client.loop_stop()
        client.disconnect()


def generate_sentence(sentence_type: str) -> str | None:
    """Generate a synthetic NMEA 0183 sentence."""
    now = time.gmtime()
    hhmmss = time.strftime("%H%M%S", now)
    ddmmyy = time.strftime("%d%m%y", now)

    if sentence_type == "GGA":
        # $GPGGA,hhmmss.ss,llll.ll,a,yyyyy.yy,a,x,xx,x.x,x.x,M,x.x,M,x.x,xxxx*hh
        # $GPGGA,173500.00,4530.000,N,12240.000,W,1,08,0.9,10.0,M,-30.0,M,,*
        lat = 4530.0 + random.uniform(-0.1, 0.1)
        lon = 12240.0 + random.uniform(-0.1, 0.1)
        payload = f"GPGGA,{hhmmss}.00,{lat:.3f},N,{lon:.3f},W,1,08,0.9,10.0,M,-30.0,M,,"
    elif sentence_type == "RMC":
        # $GPRMC,hhmmss.ss,A,llll.ll,a,yyyyy.yy,a,x.x,x.x,ddmmyy,x.x,a*hh
        lat = 4530.0 + random.uniform(-0.1, 0.1)
        lon = 12240.0 + random.uniform(-0.1, 0.1)
        sog = random.uniform(0, 15)
        cog = random.uniform(0, 360)
        payload = f"GPRMC,{hhmmss}.00,A,{lat:.3f},N,{lon:.3f},W,{sog:.1f},{cog:.1f},{ddmmyy},15.0,E"
    elif sentence_type == "DPT":
        # $IIDPT,x.x,x.x,x.x*hh
        depth = random.uniform(5, 50)
        offset = 1.5
        payload = f"IIDPT,{depth:.1f},{offset:.1f},100.0"
    elif sentence_type == "MWV":
        # $IIMWV,x.x,a,x.x,a,A*hh
        angle = random.uniform(0, 360)
        speed = random.uniform(0, 30)
        payload = f"IIMWV,{angle:.1f},R,{speed:.1f},N,A"
    elif sentence_type == "HDT":
        # $IIHDT,x.x,T*hh
        heading = random.uniform(0, 360)
        payload = f"IIHDT,{heading:.1f},T"
    elif sentence_type == "GLL":
        # $GPGLL,llll.ll,a,yyyyy.yy,a,hhmmss.ss,A,a*hh
        lat = 4530.0 + random.uniform(-0.1, 0.1)
        lon = 12240.0 + random.uniform(-0.1, 0.1)
        payload = f"GPGLL,{lat:.3f},N,{lon:.3f},W,{hhmmss}.00,A,A"
    elif sentence_type == "VTG":
        # $GPVTG,x.x,T,x.x,M,x.x,N,x.x,K,a*hh
        cog = random.uniform(0, 360)
        sog = random.uniform(0, 15)
        payload = f"GPVTG,{cog:.1f},T,{cog - 15:.1f},M,{sog:.1f},N,{sog * 1.852:.1f},K,A"
    elif sentence_type == "ROT":
        # $IIROT,x.x,A*hh
        rot = random.uniform(-5, 5)
        payload = f"IIROT,{rot:.1f},A"
    elif sentence_type == "RSA":
        # $IIRSA,x.x,A,x.x,A*hh
        rudder = random.uniform(-30, 30)
        payload = f"IIRSA,{rudder:.1f},A,{rudder:.1f},A"
    elif sentence_type == "MDA":
        # $IIMDA,x.x,I,x.x,B,x.x,C,x.x,C,x.x,x.x,x.x,C,x.x,T,x.x,M,x.x,N,x.x,M*hh
        temp = 20.0 + random.uniform(-5, 5)
        press = 1013.0 + random.uniform(-10, 10)
        payload = f"IIMDA,30.0,I,{press / 1000:.3f},B,{temp:.1f},C,,,,,15.0,C,,,,,,"
    elif sentence_type == "VWR":
        # $IIVWR,x.x,a,x.x,N,x.x,M,x.x,K*hh
        angle = random.uniform(0, 180)
        speed = random.uniform(0, 30)
        payload = f"IIVWR,{angle:.1f},L,{speed:.1f},N,{speed * 0.514:.1f},M,{speed * 1.852:.1f},K"
    elif sentence_type == "VLW":
        # $IIVLW,x.x,N,x.x,N,x.x,N,x.x,N*hh
        payload = f"IIVLW,123.4,N,12.3,N,110.0,N,11.0,N"
    else:
        return None

    cs = parse_nmea.checksum(payload)
    return f"${payload}*{cs:02X}"


def publish_nmea(client: mqtt.Client, parsed_nmea: parse_nmea.NmeaDict):
    """Publish parsed NMEA data to MQTT."""
    topic = f"{MQTT_TOPIC_PREFIX}/{MMSI}/{parsed_nmea['sentence_type']}"
    client.publish(topic, json.dumps(parsed_nmea), qos=0)
    print(f"Published {parsed_nmea['sentence_type']} to {topic}")


if __name__ == "__main__":
    main()
