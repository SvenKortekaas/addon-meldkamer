"""Home Assistant and Supervisor REST API client."""

import logging
import os
from typing import Any

import aiohttp

logger = logging.getLogger(__name__)

SUPERVISOR_URL = "http://supervisor"
CORE_API_URL = f"{SUPERVISOR_URL}/core/api"


class HAClientError(Exception):
    """Home Assistant API request failed (unreachable, auth or server error)."""


class HAClient:
    """Talks to Home Assistant Core (through the Supervisor proxy) and to the Supervisor itself."""

    def __init__(self):
        self._token = os.environ.get("SUPERVISOR_TOKEN", "")
        self._session: aiohttp.ClientSession | None = None

    @property
    def available(self) -> bool:
        """Check if Supervisor token is available."""
        return bool(self._token)

    async def _get_session(self) -> aiohttp.ClientSession:
        """Get or create aiohttp session."""
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                headers={
                    "Authorization": f"Bearer {self._token}",
                    "Content-Type": "application/json",
                },
                # tts.speak waits for the speech to be generated
                timeout=aiohttp.ClientTimeout(total=30),
            )
        return self._session

    async def _request(self, method: str, url: str, payload: Any = None) -> tuple[int, Any]:
        """Send a request and return (status, parsed JSON body or text).

        Raises HAClientError when Home Assistant / Supervisor can't be reached.
        """
        try:
            session = await self._get_session()
            async with session.request(method, url, json=payload) as resp:
                if resp.content_type == "application/json":
                    return resp.status, await resp.json()
                return resp.status, await resp.text()
        except Exception as e:
            raise HAClientError(f"{type(e).__name__}: {e}") from e

    async def _core(self, method: str, path: str, payload: Any = None) -> Any:
        """Call the Core REST API, raising HAClientError on anything but a 2xx answer."""
        status, body = await self._request(method, f"{CORE_API_URL}{path}", payload)
        if not 200 <= status < 300:
            raise HAClientError(f"HA API error {status} for {path}: {body}")
        return body

    async def get_state(self, entity_id: str) -> dict[str, Any] | None:
        """Get the current state of an entity.

        Returns the state dict, or None if the entity does not exist (404).
        Raises HAClientError on any other failure, so callers can tell "not
        found" apart from "Home Assistant unreachable".
        """
        status, body = await self._request("GET", f"{CORE_API_URL}/states/{entity_id}")
        if status == 404:
            return None
        if status != 200:
            raise HAClientError(f"HA API error {status} for {entity_id}: {body}")
        return body

    async def set_state(self, entity_id: str, state: str, attributes: dict[str, Any]) -> bool:
        """Create or update an entity's state. Returns False (and logs) on failure."""
        try:
            await self._core("POST", f"/states/{entity_id}", {"state": state, "attributes": attributes})
        except HAClientError as e:
            logger.error(f"Failed to update {entity_id}: {e}")
            return False
        logger.debug(f"Updated {entity_id}")
        return True

    async def fire_event(self, event_type: str, data: dict[str, Any]) -> bool:
        """Fire an event on the Home Assistant event bus. Returns False (and logs) on failure."""
        try:
            await self._core("POST", f"/events/{event_type}", data)
        except HAClientError as e:
            logger.error(f"Failed to fire {event_type}: {e}")
            return False
        return True

    async def call_service(self, domain: str, service: str, data: dict[str, Any]) -> bool:
        """Call a Home Assistant service. Returns False (and logs) on failure."""
        try:
            await self._core("POST", f"/services/{domain}/{service}", data)
        except HAClientError as e:
            logger.error(f"Failed to call {domain}.{service}: {e}")
            return False
        return True

    async def get_addon_options(self) -> dict[str, Any]:
        """Read this add-on's options from the Supervisor."""
        status, body = await self._request("GET", f"{SUPERVISOR_URL}/addons/self/info")
        if status != 200:
            raise HAClientError(f"Supervisor error {status}: {body}")
        return body["data"]["options"]

    async def set_addon_options(self, options: dict[str, Any]) -> None:
        """Replace this add-on's options (takes effect in /data/options.json immediately)."""
        status, body = await self._request("POST", f"{SUPERVISOR_URL}/addons/self/options", {"options": options})
        if status != 200:
            raise HAClientError(f"Supervisor error {status}: {body}")

    async def close(self):
        """Close the HTTP session."""
        if self._session and not self._session.closed:
            await self._session.close()
            self._session = None
