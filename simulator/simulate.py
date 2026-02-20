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
import math
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

# Simulator state
class SimulatorState:
    def __init__(self):
        # Starting position (lat, lon)
        self.lat = 45.5  # 45 degrees 30.0 minutes
        self.lon = -122.666  # 122 degrees 40.0 minutes
        
        # Current Speed Over Ground (knots)
        self.sog = 6.0
        # Current Course Over Ground (degrees true)
        self.cog = 45.0
        # Current Heading (degrees true)
        self.heading = 45.0

        self.depth = 15.0
        
        # Last update time
        self.last_update = time.time()
        
    def update(self):
        now = time.time()
        dt = now - self.last_update
        self.last_update = now
        
        # Add some random fluctuations to SOG and COG
        self.sog += random.uniform(-0.1, 0.1)
        self.sog = max(0, min(self.sog, 20)) # Keep speed within 0-20 knots
        
        self.cog += random.uniform(-1.0, 1.0)
        self.cog %= 360
        
        # Heading follows COG with some deviation
        self.heading = (self.cog + random.uniform(-2.0, 2.0)) % 360
        
        # Update position based on SOG and COG
        # 1 knot = 1 nautical mile per hour
        # 1 nautical mile = 1 minute of latitude
        # 1 minute of latitude = 1/60 degree
        
        # Distance traveled in nautical miles
        distance_nm = (self.sog * dt) / 3600.0
        
        # Heading and course are in degrees true.
        # NMEA 0183 convention: 0 is North, 90 is East, 180 is South, 270 is West.
        # Math convention: 0 is East, 90 is North.
        # Angle in math convention: angle_math = (90 - cog) % 360
        angle_math = math.radians(90.0 - self.cog)

        # Change in latitude (nm * sin(angle_math))
        d_lat_deg = (distance_nm * math.sin(angle_math)) / 60.0
        
        # Change in longitude (nm * cos(angle_math) / cos(lat))
        d_lon_deg = (distance_nm * math.cos(angle_math)) / (60.0 * math.cos(math.radians(self.lat)))
        
        self.lat += d_lat_deg
        self.lon += d_lon_deg

        self.depth += random.uniform(-1.0, 1.0)
        if self.depth < 0:
            self.depth = 15.0

state = SimulatorState()

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
            # Update the simulator state
            state.update()
            
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

    # Convert decimal degrees to NMEA format (DDMM.MMM)
    lat_abs = abs(state.lat)
    lat_deg = int(lat_abs)
    lat_min = (lat_abs - lat_deg) * 60
    lat_dir = 'N' if state.lat >= 0 else 'S'
    
    lon_abs = abs(state.lon)
    lon_deg = int(lon_abs)
    lon_min = (lon_abs - lon_deg) * 60
    lon_dir = 'E' if state.lon >= 0 else 'W'

    if sentence_type == "GGA":
        # $GPGGA,hhmmss.ss,llll.ll,a,yyyyy.yy,a,x,xx,x.x,x.x,M,x.x,M,x.x,xxxx*hh
        payload = f"GPGGA,{hhmmss}.00,{lat_deg:02d}{lat_min:06.3f},{lat_dir},{lon_deg:03d}{lon_min:06.3f},{lon_dir},1,08,0.9,10.0,M,-30.0,M,,"
    elif sentence_type == "RMC":
        # $GPRMC,hhmmss.ss,A,llll.ll,a,yyyyy.yy,a,x.x,x.x,ddmmyy,x.x,a*hh
        payload = f"GPRMC,{hhmmss}.00,A,{lat_deg:02d}{lat_min:06.3f},{lat_dir},{lon_deg:03d}{lon_min:06.3f},{lon_dir},{state.sog:.1f},{state.cog:.1f},{ddmmyy},15.0,E"
    elif sentence_type == "DPT":
        # $IIDPT,x.x,x.x,x.x*hh
        depth = state.depth
        offset = 1.5
        payload = f"IIDPT,{depth:.1f},{offset:.1f},100.0"
    elif sentence_type == "MWV":
        # $IIMWV,x.x,a,x.x,a,A*hh
        # Relative wind angle and speed
        angle = random.uniform(0, 360)
        speed = random.uniform(0, 30)
        payload = f"IIMWV,{angle:.1f},R,{speed:.1f},N,A"
    elif sentence_type == "HDT":
        # $IIHDT,x.x,T*hh
        payload = f"IIHDT,{state.heading:.1f},T"
    elif sentence_type == "GLL":
        # $GPGLL,llll.ll,a,yyyyy.yy,a,hhmmss.ss,A,a*hh
        payload = f"GPGLL,{lat_deg:02d}{lat_min:06.3f},{lat_dir},{lon_deg:03d}{lon_min:06.3f},{lon_dir},{hhmmss}.00,A,A"
    elif sentence_type == "VTG":
        # $GPVTG,x.x,T,x.x,M,x.x,N,x.x,K,a*hh
        payload = f"GPVTG,{state.cog:.1f},T,{state.cog - 15.0:.1f},M,{state.sog:.1f},N,{state.sog * 1.852:.1f},K,A"
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
