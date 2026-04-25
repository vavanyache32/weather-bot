"""WIS 2.0 Global Broker MQTT client (skeleton / TODO).

The project currently focuses on the two HTTP-based observation sources
(OGIMET SYNOP and IEM ASOS) because they provide explicit daily Tmax with
minimal operational complexity.

WIS 2.0 is a *push* channel over MQTT with BUFR payloads.  Implementing it
robustly requires:

1. A long-running MQTT subscriber (``asyncio-mqtt`` / ``paho-mqtt``) that
   auto-reconnects on network loss.
2. Parsing BUFR via ``eccodes`` to extract WIGOS-ID-matched fields:
   ``#1#airTemperature``, ``#1#maximumTemperatureAtHeightAndOverPeriodSpecified``.
3. Downloading linked BUFR files from the WIS 2.0 notification JSON.
4. Handling the binary eccodes dependency on all target platforms (Windows
   wheels exist, but smoke-testing BUFR key names is non-trivial).

Because the scheduler already has an ``asyncio`` loop, the infrastructure
for a long-lived connection exists.  When this TODO is picked up, wire
``WIS2Service.start()`` as a background task in ``__main__.py`` and feed
observations from ``get_recent_observations()`` into the scheduler tick.
"""
from __future__ import annotations

import logging
from typing import Optional

logger = logging.getLogger(__name__)


class WIS2Service:
    """Placeholder for WIS 2.0 MQTT ingestion.

    ``enabled=False`` by default so it is a no-op until fully implemented.
    """

    def __init__(
        self,
        broker: str = "mqtts://globalbroker.meteo.fr:8883",
        topic: str = "origin/a/wis2/ru-roshydromet/data/core/weather/surface-based-observations/synop",
        username: str = "everyone",
        password: str = "everyone",
        wigos_id: str = "0-20000-0-27611",
        enabled: bool = False,
    ) -> None:
        self._broker = broker
        self._topic = topic
        self._username = username
        self._password = password
        self._wigos_id = wigos_id
        self.enabled = enabled

    async def start(self) -> None:
        if not self.enabled:
            logger.debug("WIS 2.0 is disabled; skipping start")
            return
        logger.warning("WIS 2.0 start() is a TODO — MQTT subscriber not implemented yet")

    async def stop(self) -> None:
        if not self.enabled:
            return
        logger.debug("WIS 2.0 stop() is a TODO")

    def get_recent_observations(self) -> list:
        """Return observations accumulated since the last call."""
        return []
