## nmea-mqtt-py

Read from NMEA 0183 sentences from a socket, parse them, then publish to MQTT as
JSON.

## Requirements

- Requires Python v3.10 or greater.
- `git`
- Root privileges to install (but not to run).

## Installation

1. Create the user `nmea` and set a password:

    ```
    sudo useradd -m -c"Owns the nmea-mqtt process" -s /bin/bash nmea
    sudo passwd nmea
   ```

2. Log in as that user, then clone the Git repository. The following will place
it at `~nmea/git/nmea-mqtt-py`. Adjust the path to your preference, but make
sure you use it consistently in what follows.

    ```
    cd ~
    mkdir git
    cd git
    git clone https://github.com/westernflyer/nmea-mqtt-py
    ```

3. Create a Python virtual environment, activate it, then install requirements

    ```
    cd ~/git/nmea-mqtt-py
    python3 -m venv .venv
    source .venv/bin/activate
    pip install -r requirements.txt
    ```
   
4. Copy a configuration file into place, then edit it with your requirements.

   ```
   cd ~/git/nmea-mqtt-py
   cp config_sample.py config.py
   nano config.py
   ```

5. Time to install a systemd service file. Copy the provided file into place,
   then edit it appropriately. In particular, make sure the entries for
   `WorkingDirectory` and `ExecStart` reflect your choices.

   ```
   cd ~/git/nmea-mqtt-py/systemd
   sudo cp nmea_mqtt.service /etc/systemd/system
   sudo nano /etc/systemd/system/nmea-mqtt-py 
   ```
   
6. Reload the systemd manager to reflect your changes and start the daemon.
   Finally, enable it so it will automatically start when the system boots.

   ```
   sudo systemctl daemon-reload
   sudo systemctl start nmea-mqtt
   sudo systemctl enable nmea-mqtt
   ```
   