#
# Copyright (c) 2025-present Tom Keffer <tkeffer@gmail.com>
#
# This source code is licensed under the MIT license found in the
# LICENSE.txt.txt file in the root directory of this source tree.
#
import asyncio
import logging
import socket
import sys

RETRYABLE_ERRORS = (OSError, socket.gaierror, TimeoutError, asyncio.TimeoutError)

log = logging.getLogger("nmea-mqtt.utils")

async def warn_print_sleep(msg: str, config: dict, prefix: str = ""):
    """Print and log a warning message, then sleep for NMEA_RETRY_WAIT seconds."""
    full_msg = f"{prefix}: {msg}" if prefix else msg
    nmea_options = config.get("NMEA_OPTIONS", {})
    nmea_retry_wait = nmea_options.get("NMEA_RETRY_WAIT", 60)
    print(full_msg, file=sys.stderr)
    print(f"*** Waiting {nmea_retry_wait} seconds before retrying.", file=sys.stderr)
    log.warning(full_msg)
    log.warning(f"*** Waiting {nmea_retry_wait} seconds before retrying.")
    await asyncio.sleep(nmea_retry_wait)
    print(f"*** {prefix} Retrying..." if prefix else "*** Retrying...", file=sys.stderr)
    log.warning(f"*** {prefix} Retrying..." if prefix else "*** Retrying...")
