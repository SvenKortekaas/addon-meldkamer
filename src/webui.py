"""Web UI server for Meldkamer dashboard with map and history."""

import asyncio
import contextlib
import json
import logging
import os
import time
from collections import deque
from datetime import UTC, datetime
from pathlib import Path

import aiohttp
from aiohttp import web

from .database import Database
from .decoder import Decoder
from .ha_client import HAClientError
from .message import P2000Message
from .processor import MessageProcessor
from .sensor_manager import SensorManager
from .tts import SpeechRules

logger = logging.getLogger(__name__)

MAX_MESSAGES = 500
# Only per_page is capped tightly; page just needs to keep SQL OFFSET in range
MAX_PAGE = 1_000_000_000
# CSV imports arrive as one JSON body; aiohttp's default limit is 1 MiB
MAX_REQUEST_SIZE = 50 * 1024 * 1024
# Vehicle photo uploads (multipart, which client_max_size does not bound)
MAX_PHOTO_SIZE = 5 * 1024 * 1024
MAX_FORM_FIELD_SIZE = 4 * 1024
PHOTO_MIME_TYPES = ("image/jpeg", "image/png", "image/webp", "image/gif")

ADDON_VERSION = "1.1.1"

# Home Assistant core (2026.9+) proxies OpenStreetMap raster tiles itself via
# its built-in map_tiles integration, identifying the requests the way OSM's
# tile usage policy requires. Browsers loading tiles straight from OSM inside
# the ingress iframe get 403 "Access blocked", so the dashboard asks us for
# tiles and we fetch them from core through the Supervisor API proxy.
HA_MAP_TILES_URL = "http://supervisor/core/api/map_tiles/raster/{z}/{x}/{y}.png"
# Fallback for cores older than 2026.9, which have no map_tiles proxy: fetch
# from OSM directly with an identifying User-Agent, as the policy asks.
OSM_TILES_URL = "https://tile.openstreetmap.org/{z}/{x}/{y}.png"
OSM_USER_AGENT = f"Meldkamer/{ADDON_VERSION} (+https://github.com/cyberjunky/addon-meldkamer)"
MAP_MAX_ZOOM = 19
# OSM asks consumers to cache tiles for at least a week
MAP_TILE_MAX_AGE = 7 * 24 * 60 * 60


def _int_param(request: web.Request, name: str, default: int, minimum: int = 1, maximum: int = 1000) -> int:
    """Parse an integer query parameter, falling back to a clamped default."""
    try:
        value = int(request.query.get(name, default))
    except (TypeError, ValueError):
        return default
    return max(minimum, min(maximum, value))


def _bad_request(message: str) -> web.HTTPBadRequest:
    """Build a 400 with the same {"error": ...} JSON body the handlers return."""
    return web.HTTPBadRequest(text=json.dumps({"error": message}), content_type="application/json")


async def _json_object(request: web.Request) -> dict:
    """Parse the request body as a JSON object, raising a JSON 400 otherwise."""
    try:
        data = await request.json()
    except ValueError:
        raise _bad_request("Invalid JSON body") from None
    if not isinstance(data, dict):
        raise _bad_request("JSON body must be an object")
    return data


def _city_id(value) -> int | None:
    """Convert a street's city_id form value to int, or None when blank."""
    if value is None or (isinstance(value, str) and not value.strip()):
        return None
    if isinstance(value, bool):
        raise _bad_request("city_id must be an integer")
    try:
        return int(value)
    except (TypeError, ValueError):
        raise _bad_request("city_id must be an integer") from None


def _sniff_image_type(data: bytes) -> str | None:
    """Return the image MIME type from the file's magic bytes, or None."""
    if data.startswith(b"\xff\xd8\xff"):
        return "image/jpeg"
    if data.startswith(b"\x89PNG\r\n\x1a\n"):
        return "image/png"
    if data.startswith((b"GIF87a", b"GIF89a")):
        return "image/gif"
    if data[:4] == b"RIFF" and data[8:12] == b"WEBP":
        return "image/webp"
    return None


class _DictMessageView:
    """Lets a plain message dict (from P2000Message.to_dict()) be matched against
    SensorManager's filter logic, which expects P2000Message-style attribute access.
    """

    def __init__(self, data: dict):
        self._data = data

    def __getattr__(self, name):
        return self._data.get(name)


# Statistics kept across restarts in the stats file
PERSISTED_STATS = ("total_messages", "by_region", "by_discipline", "start_time")

# ppm calibration request limits
CALIBRATION_MAX_SPAN = 100
CALIBRATION_MAX_STEPS = 41


def _float_or_none(value) -> float | None:
    """Convert a form value to float, or None when empty/invalid."""
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def get_stats_file() -> Path:
    """Get stats file path."""
    return Path("/data/webui_stats_p2000.json")


class WebUI:
    """Web UI server for Meldkamer addon ingress with map support."""

    def __init__(
        self,
        port: int,
        database: Database,
        speech: SpeechRules,
        processor: MessageProcessor,
        decoder: Decoder,
        sensor_manager: SensorManager,
    ):
        self.port = port
        self.database = database
        self.speech = speech
        self.processor = processor
        self.decoder = decoder
        self.sensor_manager = sensor_manager
        self.app = web.Application(client_max_size=MAX_REQUEST_SIZE)
        self.messages: deque = deque(maxlen=MAX_MESSAGES)
        self.stats_file = get_stats_file()
        self.stats = self._load_stats()
        self._last_stats_save = 0.0
        self._setup_routes()
        self._runner: web.AppRunner | None = None
        self._tile_session: aiohttp.ClientSession | None = None
        self._supervisor_token = os.environ.get("SUPERVISOR_TOKEN", "")
        # Cleared once core answers 404, i.e. it predates the map_tiles proxy
        self._ha_map_tiles = bool(self._supervisor_token)

    def _tts_changed(self) -> None:
        """Apply edited TTS replacement rules right away."""
        self.speech.reload()

    def _ignore_changed(self) -> None:
        """Apply edited global ignore filters right away."""
        self.processor.reload_ignore_filters()

    def _load_stats(self) -> dict:
        """Load stats from file/database and restore message history."""
        db_messages = self.database.get_recent_messages(MAX_MESSAGES)
        self.messages.extend(db_messages)
        logger.info(f"Loaded {len(db_messages)} messages from database")

        stats = {"by_region": {}, "by_discipline": {}, "start_time": datetime.now(UTC).isoformat()}
        if self.stats_file.exists():
            try:
                with open(self.stats_file) as f:
                    saved = json.load(f)
                # Keep the persisted start_time so uptime survives restarts
                stats.update({key: saved[key] for key in PERSISTED_STATS if key in saved})
            except Exception as e:
                logger.warning(f"Failed to load stats: {e}")
        # The database message count is the source of truth
        stats["total_messages"] = self.database.get_message_count()
        return stats

    def _save_stats(self) -> None:
        """Save stats to file."""
        try:
            with open(self.stats_file, "w") as f:
                json.dump({key: self.stats[key] for key in PERSISTED_STATS}, f)
            self._last_stats_save = time.monotonic()
        except Exception as e:
            logger.warning(f"Failed to save stats: {e}")

    def _save_stats_throttled(self) -> None:
        """Save stats at most once per 10 seconds."""
        if time.monotonic() - self._last_stats_save >= 10.0:
            self._save_stats()

    def _setup_routes(self) -> None:
        """Set up web routes."""
        self.app.router.add_get("/", self._handle_index)
        self.app.router.add_get("/api/messages", self._handle_messages)
        self.app.router.add_get("/api/sensors", self._handle_sensors)
        self.app.router.add_get("/api/stats", self._handle_stats)
        self.app.router.add_get("/api/stream", self._handle_stream)
        self.app.router.add_get("/api/history/{location}", self._handle_history)
        self.app.router.add_get("/api/map-tiles/{z:[0-9]+}/{x:[0-9]+}/{y:[0-9]+}.png", self._handle_map_tile)
        self.app.router.add_get("/api/map-history", self._handle_map_history)
        self.app.router.add_get("/api/incident/{incident_id}", self._handle_incident)

        # Receiver quality and ppm calibration
        self.app.router.add_get("/api/receiver", self._handle_receiver)
        self.app.router.add_post("/api/calibration", self._handle_start_calibration)
        self.app.router.add_delete("/api/calibration", self._handle_cancel_calibration)
        self.app.router.add_post("/api/calibration/apply", self._handle_apply_ppm)

        # Database management API
        self.app.router.add_get("/api/db/capcodes", self._handle_list_capcodes)
        self.app.router.add_post("/api/db/capcodes", self._handle_add_capcode)
        self.app.router.add_put("/api/db/capcodes/{capcode}", self._handle_update_capcode)
        self.app.router.add_delete("/api/db/capcodes/{capcode}", self._handle_delete_capcode)

        self.app.router.add_get("/api/db/places", self._handle_list_places)
        self.app.router.add_post("/api/db/places", self._handle_add_place)
        self.app.router.add_put("/api/db/places/{id}", self._handle_update_place)
        self.app.router.add_delete("/api/db/places/{id}", self._handle_delete_place)

        self.app.router.add_get("/api/db/streets", self._handle_list_streets)
        self.app.router.add_post("/api/db/streets", self._handle_add_street)
        self.app.router.add_put("/api/db/streets/{id}", self._handle_update_street)
        self.app.router.add_delete("/api/db/streets/{id}", self._handle_delete_street)

        self.app.router.add_get("/api/db/geocodes", self._handle_list_geocodes)
        self.app.router.add_delete("/api/db/geocodes/{query}", self._handle_delete_geocode)
        self.app.router.add_delete("/api/db/geocodes", self._handle_delete_all_geocodes)

        # CSV Export/Import
        self.app.router.add_get("/api/db/export/{table}", self._handle_export_csv)
        self.app.router.add_post("/api/db/import/{table}", self._handle_import_csv)

        # BAG Data Import (Dutch address data by province)
        self.app.router.add_get("/api/db/bag/provinces", self._handle_list_provinces)
        self.app.router.add_post("/api/db/bag/import/{province}", self._handle_import_province)
        self.app.router.add_post("/api/db/bag/import-all-places", self._handle_import_all_places)
        self.app.router.add_get("/api/db/bag/progress", self._handle_bag_progress)

        # Capcode Import (from bommel.net)
        self.app.router.add_post("/api/db/capcodes/import-bommel", self._handle_import_bommel)
        self.app.router.add_get("/api/db/capcodes/import-progress", self._handle_capcode_progress)

        # Message history management
        self.app.router.add_get("/api/db/messages", self._handle_list_messages)
        self.app.router.add_delete("/api/db/messages/{id}", self._handle_delete_message)
        self.app.router.add_delete("/api/db/messages", self._handle_delete_all_messages)

        # Abbreviations
        self.app.router.add_get("/api/db/abbreviations", self._handle_list_abbreviations)
        self.app.router.add_post("/api/db/abbreviations", self._handle_add_abbreviation)
        self.app.router.add_delete("/api/db/abbreviations/{id}", self._handle_delete_abbreviation)
        self.app.router.add_post("/api/db/abbreviations/import", self._handle_import_abbreviations)
        self.app.router.add_post("/api/db/abbreviations/find", self._handle_find_abbreviations)

        # TTS Replacements
        self.app.router.add_get("/api/db/tts_replacements", self._handle_list_tts)
        self.app.router.add_post("/api/db/tts_replacements", self._handle_add_tts)
        self.app.router.add_put("/api/db/tts_replacements/{id}", self._handle_update_tts)
        self.app.router.add_delete("/api/db/tts_replacements/{id}", self._handle_delete_tts)
        self.app.router.add_delete("/api/db/tts_replacements", self._handle_delete_all_tts)

        # Global message filters
        self.app.router.add_get("/api/db/ignore_text", self._handle_list_ignore_text)
        self.app.router.add_post("/api/db/ignore_text", self._handle_add_ignore_text)
        self.app.router.add_put("/api/db/ignore_text/{id}", self._handle_update_ignore_text)
        self.app.router.add_delete("/api/db/ignore_text/{id}", self._handle_delete_ignore_text)
        self.app.router.add_delete("/api/db/ignore_text", self._handle_delete_all_ignore_text)
        self.app.router.add_get("/api/db/ignore_capcodes", self._handle_list_ignore_capcodes)
        self.app.router.add_post("/api/db/ignore_capcodes", self._handle_add_ignore_capcode)
        self.app.router.add_put("/api/db/ignore_capcodes/{id}", self._handle_update_ignore_capcode)
        self.app.router.add_delete("/api/db/ignore_capcodes/{id}", self._handle_delete_ignore_capcode)
        self.app.router.add_delete("/api/db/ignore_capcodes", self._handle_delete_all_ignore_capcodes)

        # Vehicle photos
        self.app.router.add_get("/api/vehicle-photo/{key_type}/{key_value}", self._handle_get_vehicle_photo)
        self.app.router.add_post("/api/vehicle-photo", self._handle_set_vehicle_photo)
        self.app.router.add_delete("/api/vehicle-photo/{key_type}/{key_value}", self._handle_delete_vehicle_photo)
        self.app.router.add_get("/api/db/vehicle_photos", self._handle_list_vehicle_photos)
        self.app.router.add_get("/api/vehicle-categories", self._handle_list_vehicle_categories)

        # Database reset
        self.app.router.add_delete("/api/db/reset", self._handle_reset_database)
        # Clear a single table
        self.app.router.add_delete("/api/db/clear/{table}", self._handle_clear_table)

    async def start(self) -> None:
        """Start the web server."""
        self._runner = web.AppRunner(self.app)
        await self._runner.setup()
        site = web.TCPSite(self._runner, "0.0.0.0", self.port)
        await site.start()
        logger.info(f"Web UI listening on port {self.port}")

    async def stop(self) -> None:
        """Stop the web server."""
        self._save_stats()
        if self._tile_session and not self._tile_session.closed:
            await self._tile_session.close()
        if self._runner:
            with contextlib.suppress(Exception):
                await self._runner.cleanup()
            self._runner = None

    def add_message(self, msg: P2000Message) -> None:
        """Add a new (already stored) P2000 message and update stats."""
        self.messages.appendleft(msg.to_dict())
        self.stats["total_messages"] += 1

        # Track by region and discipline (split multi-discipline combos so each
        # discipline is counted individually instead of as a combined key)
        region = msg.region or "Unknown"
        self.stats["by_region"][region] = self.stats["by_region"].get(region, 0) + 1
        disciplines = [d.strip() for d in (msg.discipline or "").split(",") if d.strip()] or ["Unknown"]
        for discipline in disciplines:
            self.stats["by_discipline"][discipline] = self.stats["by_discipline"].get(discipline, 0) + 1

        self._save_stats_throttled()

    def mark_repeat(self, msg: P2000Message) -> None:
        """The pager network resent a message: update its repeat count."""
        for item in self.messages:
            if item.get("id") == msg.id:
                item["repeats"] = msg.repeats
                return

    def _stats_payload(self) -> dict:
        """Stats plus the live receiver/decoder state, for the dashboard."""
        decoder = self.decoder
        geocoder = self.processor.geocoder
        config = decoder.config
        return {
            **self.stats,
            "network_name": config.network_name,
            "frequency": config.frequency,
            "sample_rate": config.sample_rate,
            "decoder": config.decoder,
            "decoder_running": decoder.is_running,
            "decoder_error": decoder.last_error,
            "receiver_online": decoder.stats.online(config.offline_after_minutes),
            "receiver_source": decoder.source.label,
            "device_type": decoder.device_type,
            "device_driver": decoder.device_driver,
            "geocoding_configured": config.opencage_enabled,
            "geocoding_enabled": geocoder.enabled,
            "geocode_rate_remaining": geocoder.rate_remaining,
            "geocode_rate_limited": geocoder.rate_limited,
            "sensor_zones": [
                {
                    "name": s.name,
                    "radius_km": s.zone.radius_km,
                    "center_lat": s.zone.latitude,
                    "center_lon": s.zone.longitude,
                }
                for s in self.sensor_manager.sensors
                if s.zone
            ],
        }

    def update_db_stats(self, places: int, streets: int, capcodes: int, texts: int, geocodes: int) -> None:
        """Update database statistics for Advanced tab."""
        self.stats["db_places"] = places
        self.stats["db_streets"] = streets
        self.stats["db_capcodes"] = capcodes
        self.stats["db_texts"] = texts
        self.stats["db_geocodes"] = geocodes

    async def _handle_index(self, request: web.Request) -> web.Response:
        html = DASHBOARD_HTML.replace("__ADDON_VERSION__", ADDON_VERSION)
        return web.Response(
            text=html,
            content_type="text/html",
            # Never let the browser serve a stale cached page after an addon update
            headers={"Cache-Control": "no-cache, must-revalidate"},
        )

    def _with_fresh_abbreviations(self, messages: list[dict]) -> list[dict]:
        """Re-derive each message's abbreviation tags from its body against the
        current Texts glossary, instead of the frozen text captured at parse
        time - so editing an abbreviation's meaning (e.g. "A2" from "geen
        spoed" to "zonder spoed") is reflected immediately on already-received
        messages, not just newly-decoded ones.
        """
        result = []
        for m in messages:
            body = m.get("body")
            if body:
                m = {**m, "abbreviations": self.database.find_abbreviations_in_text(body, m.get("discipline", ""))}
            result.append(m)
        return result

    def _filter_by_sensor(self, messages: list[dict], sensor_name: str) -> list[dict]:
        """Filter message dicts down to those matching a configured sensor's criteria.

        Reuses SensorManager's actual filter-matching logic (discipline, radius/
        haversine, priority, etc.) via a thin attribute-access adapter, rather than
        re-implementing that logic in JS - keeps "what counts as a match" identical
        to what the HA/MQTT sensor publishing path uses.
        """
        manager = self.sensor_manager
        if not sensor_name:
            return messages
        if sensor_name == "__any__":
            return [m for m in messages if manager.get_matching_sensors(_DictMessageView(m))]
        sensor = next((s for s in manager.sensors if s.name == sensor_name), None)
        if not sensor:
            return messages
        return [m for m in messages if manager.matches(_DictMessageView(m), sensor)]

    async def _handle_sensors(self, request: web.Request) -> web.Response:
        """List configured sensors, for the dashboard's filter toggle."""
        return web.json_response([{"name": s.name, "icon": s.icon} for s in self.sensor_manager.sensors])

    async def _handle_messages(self, request: web.Request) -> web.Response:
        messages = self._filter_by_sensor(list(self.messages), request.query.get("sensor", ""))
        return web.json_response(self._with_fresh_abbreviations(messages))

    async def _handle_stats(self, request: web.Request) -> web.Response:
        return web.json_response(self._stats_payload())

    async def _handle_stream(self, request: web.Request) -> web.StreamResponse:
        response = web.StreamResponse()
        response.headers["Content-Type"] = "text/event-stream"
        response.headers["Cache-Control"] = "no-cache"
        await response.prepare(request)
        sensor_name = request.query.get("sensor", "")
        last_sent = None
        try:
            while True:
                latest = self._filter_by_sensor(list(self.messages), sensor_name)[:50]
                data = json.dumps({"stats": self._stats_payload(), "latest": self._with_fresh_abbreviations(latest)})
                # Only push when something changed; the client recomputes
                # uptime locally so idle periods need no traffic
                if data != last_sent:
                    await response.write(f"data: {data}\n\n".encode())
                    last_sent = data
                await asyncio.sleep(2)
        except (ConnectionResetError, TimeoutError, asyncio.CancelledError):
            pass
        return response

    async def _handle_history(self, request: web.Request) -> web.Response:
        """Get message history for a location (address or city)."""
        location = request.match_info.get("location", "")
        limit = _int_param(request, "limit", 20, maximum=100)
        history = self.database.get_history_by_address(location, limit=limit)
        if not history:
            # Nothing at this exact address: search body/street/city instead
            history = self.database.list_messages(page=1, per_page=limit, search=location)["items"]
        return web.json_response(history[:limit])

    async def _handle_incident(self, request: web.Request) -> web.Response:
        """All messages of one incident, oldest first."""
        return web.json_response(self.database.get_incident_messages(request.match_info["incident_id"]))

    async def _handle_map_history(self, request: web.Request) -> web.Response:
        """Located messages of the last `hours` hours, for the map's history layer."""
        hours = _int_param(request, "hours", 24, maximum=24 * 365)
        return web.json_response(self.database.get_map_points(hours))

    async def _handle_receiver(self, request: web.Request) -> web.Response:
        """Receiver decode quality per minute and the ppm calibration state."""
        decoder = self.decoder
        return web.json_response(
            {
                **decoder.stats.snapshot(),
                "offline_after_minutes": decoder.config.offline_after_minutes,
                "sources": [
                    {"label": s.label, "type": s.receiver_type, "ppm": s.ppm, "active": s is decoder.source}
                    for s in decoder.sources
                ],
                "ppm": decoder.ppm,
                "calibration": decoder.calibration,
                "can_apply": bool(self._supervisor_token),
            }
        )

    async def _handle_start_calibration(self, request: web.Request) -> web.Response:
        data = await _json_object(request)
        try:
            span = int(data.get("span", 30))
            step = int(data.get("step", 5))
            dwell = int(data.get("dwell", 30))
        except (TypeError, ValueError):
            raise _bad_request("span, step and dwell must be integers") from None
        if not (1 <= step <= span <= CALIBRATION_MAX_SPAN) or 2 * span // step + 1 > CALIBRATION_MAX_STEPS:
            raise _bad_request(f"Use a step of at least 1 and at most {CALIBRATION_MAX_STEPS} ppm values to try")
        if not 10 <= dwell <= 600:
            raise _bad_request("dwell must be 10-600 seconds")
        try:
            self.decoder.start_calibration(span, step, dwell)
        except RuntimeError as e:
            raise _bad_request(str(e)) from None
        return web.json_response({"success": True})

    async def _handle_cancel_calibration(self, request: web.Request) -> web.Response:
        await self.decoder.cancel_calibration()
        return web.json_response({"success": True})

    async def _handle_apply_ppm(self, request: web.Request) -> web.Response:
        data = await _json_object(request)
        ppm = data.get("ppm")
        if isinstance(ppm, bool) or not isinstance(ppm, int):
            raise _bad_request("ppm must be an integer")
        try:
            await self.decoder.apply_ppm(ppm)
        except (RuntimeError, HAClientError) as e:
            return web.json_response({"error": f"Could not save ppm_correction: {e}"}, status=500)
        return web.json_response({"success": True})

    # =========================================================================
    # Database Management API Handlers
    # =========================================================================

    async def _handle_list_capcodes(self, request: web.Request) -> web.Response:
        """List capcodes with pagination and search."""
        page = _int_param(request, "page", 1, maximum=MAX_PAGE)
        per_page = _int_param(request, "per_page", 50)
        search = request.query.get("search", "")
        result = self.database.list_capcodes(page, per_page, search)
        return web.json_response(result)

    async def _handle_add_capcode(self, request: web.Request) -> web.Response:
        """Add a new capcode."""
        data = await _json_object(request)
        success = self.database.add_capcode(
            data.get("capcode", ""),
            data.get("discipline", ""),
            data.get("region", ""),
            data.get("location", ""),
            data.get("description", ""),
            data.get("remark", ""),
        )
        if success:
            return web.json_response({"success": True})
        return web.json_response({"error": "Failed to add capcode (may already exist)"}, status=400)

    async def _handle_update_capcode(self, request: web.Request) -> web.Response:
        """Update a capcode."""
        capcode = request.match_info.get("capcode", "")
        data = await _json_object(request)
        success = self.database.update_capcode(
            capcode,
            data.get("discipline", ""),
            data.get("region", ""),
            data.get("location", ""),
            data.get("description", ""),
            data.get("remark", ""),
        )
        return web.json_response({"success": success})

    async def _handle_delete_capcode(self, request: web.Request) -> web.Response:
        """Delete a capcode."""
        capcode = request.match_info.get("capcode", "")
        success = self.database.delete_capcode(capcode)
        return web.json_response({"success": success})

    async def _handle_list_places(self, request: web.Request) -> web.Response:
        """List places with pagination and search."""
        page = _int_param(request, "page", 1, maximum=MAX_PAGE)
        per_page = _int_param(request, "per_page", 50)
        search = request.query.get("search", "")
        result = self.database.list_places(page, per_page, search)
        return web.json_response(result)

    async def _handle_add_place(self, request: web.Request) -> web.Response:
        """Add a new place."""
        data = await _json_object(request)
        success = self.database.add_place(
            data.get("city", ""),
            data.get("abbreviation", ""),
            data.get("province", ""),
            _float_or_none(data.get("latitude")),
            _float_or_none(data.get("longitude")),
        )
        if success:
            return web.json_response({"success": True})
        return web.json_response({"error": "Failed to add place (may already exist)"}, status=400)

    async def _handle_update_place(self, request: web.Request) -> web.Response:
        """Update a place."""
        try:
            place_id = int(request.match_info.get("id", 0))
        except (TypeError, ValueError):
            place_id = 0
        data = await _json_object(request)
        success = self.database.update_place(
            place_id,
            data.get("city", ""),
            data.get("abbreviation", ""),
            data.get("province", ""),
            _float_or_none(data.get("latitude")),
            _float_or_none(data.get("longitude")),
        )
        return web.json_response({"success": success})

    async def _handle_delete_place(self, request: web.Request) -> web.Response:
        """Delete a place."""
        try:
            place_id = int(request.match_info.get("id", 0))
        except (TypeError, ValueError):
            place_id = 0
        success = self.database.delete_place(place_id)
        return web.json_response({"success": success})

    async def _handle_list_streets(self, request: web.Request) -> web.Response:
        """List streets with pagination and search."""
        page = _int_param(request, "page", 1, maximum=MAX_PAGE)
        per_page = _int_param(request, "per_page", 50)
        search = request.query.get("search", "")
        city_id = request.query.get("city_id")
        try:
            city_id = int(city_id) if city_id else None
        except (TypeError, ValueError):
            city_id = None
        result = self.database.list_streets(page, per_page, search, city_id)
        return web.json_response(result)

    async def _handle_add_street(self, request: web.Request) -> web.Response:
        """Add a new street."""
        data = await _json_object(request)
        success = self.database.add_street(
            data.get("street", ""), _city_id(data.get("city_id")), data.get("postalcode", "")
        )
        if success:
            return web.json_response({"success": True})
        return web.json_response({"error": "Failed to add street"}, status=400)

    async def _handle_update_street(self, request: web.Request) -> web.Response:
        """Update a street."""
        try:
            street_id = int(request.match_info.get("id", 0))
        except (TypeError, ValueError):
            street_id = 0
        data = await _json_object(request)
        success = self.database.update_street(
            street_id, data.get("street", ""), _city_id(data.get("city_id")), data.get("postalcode", "")
        )
        return web.json_response({"success": success})

    async def _handle_delete_street(self, request: web.Request) -> web.Response:
        """Delete a street."""
        try:
            street_id = int(request.match_info.get("id", 0))
        except (TypeError, ValueError):
            street_id = 0
        success = self.database.delete_street(street_id)
        return web.json_response({"success": success})

    async def _handle_list_geocodes(self, request: web.Request) -> web.Response:
        """List cached geocodes with pagination and search."""
        page = _int_param(request, "page", 1, maximum=MAX_PAGE)
        per_page = _int_param(request, "per_page", 50)
        search = request.query.get("search", "")
        result = self.database.list_geocodes(page, per_page, search)
        return web.json_response(result)

    async def _handle_delete_geocode(self, request: web.Request) -> web.Response:
        """Delete a cached geocode."""
        query = request.match_info.get("query", "")
        success = self.database.delete_geocode(query)
        return web.json_response({"success": success})

    async def _handle_delete_all_geocodes(self, request: web.Request) -> web.Response:
        """Delete all cached geocodes."""
        count = self.database.delete_all_geocodes()
        return web.json_response({"success": True, "deleted": count})

    async def _handle_export_csv(self, request: web.Request) -> web.Response:
        """Export a table to CSV."""
        table = request.match_info.get("table", "")

        exporters = {
            "capcodes": self.database.export_capcodes_csv,
            "places": self.database.export_places_csv,
            "streets": self.database.export_streets_csv,
            "messages": self.database.export_messages_csv,
            "abbreviations": self.database.export_abbreviations_csv,
            "tts_replacements": self.database.export_tts_csv,
            "ignore_text": self.database.export_ignore_text_csv,
            "ignore_capcodes": self.database.export_ignore_capcodes_csv,
        }
        exporter = exporters.get(table)
        if exporter is None:
            return web.json_response({"error": "Invalid table"}, status=400)
        # Off the event loop so SSE and the decoder feed don't stall on big tables
        csv_data = await asyncio.to_thread(exporter)

        return web.Response(
            text=csv_data, content_type="text/csv", headers={"Content-Disposition": f"attachment; filename={table}.csv"}
        )

    async def _handle_import_csv(self, request: web.Request) -> web.Response:
        """Import CSV data to a table."""
        table = request.match_info.get("table", "")

        data = await _json_object(request)
        csv_content = data.get("csv", "")
        replace = bool(data.get("replace", False))
        if not isinstance(csv_content, str):
            return web.json_response({"error": "csv must be a string"}, status=400)

        importers = {
            "capcodes": self.database.import_capcodes_csv,
            "places": self.database.import_places_csv,
            "streets": self.database.import_streets_csv,
        }
        if table == "messages":
            return web.json_response(
                {"error": "Messages cannot be imported. They come from the P2000 decoder."}, status=400
            )
        importer = importers.get(table)
        if importer is None:
            return web.json_response({"error": "Invalid table"}, status=400)
        if busy := self._import_running():
            return web.json_response({"error": busy}, status=409)

        try:
            # Off the event loop so SSE and the decoder feed don't stall on big files
            result = await asyncio.to_thread(importer, csv_content, replace)
        except ValueError as e:
            return web.json_response({"error": str(e)}, status=400)
        return web.json_response(result)

    async def _handle_list_provinces(self, request: web.Request) -> web.Response:
        """List available Dutch provinces for BAG import."""
        from .bag_import import get_provinces

        provinces = get_provinces()
        return web.json_response({"provinces": provinces})

    async def _handle_import_province(self, request: web.Request) -> web.Response:
        """Start BAG import for a specific Dutch province in background thread."""

        province = request.match_info.get("province", "")

        import threading

        from .bag_import import PROVINCES, get_import_progress, import_province_data, reset_progress

        if province not in PROVINCES:
            return web.json_response({"error": f"Unknown province '{province}'"}, status=400)

        # Check if already running
        progress = get_import_progress()
        if progress.get("running"):
            return web.json_response({"error": "Import already in progress"}, status=400)

        # Reset progress BEFORE starting thread to avoid race condition
        reset_progress(province)

        # Start import in background thread
        def run_import():
            import_province_data(province, self.database)

        thread = threading.Thread(target=run_import, daemon=True)
        thread.start()

        # Return immediately - client will poll for progress
        return web.json_response({"started": True, "province": province})

    async def _handle_import_all_places(self, request: web.Request) -> web.Response:
        """Import ALL Dutch woonplaatsen (places) into database."""

        import threading

        from .bag_import import get_import_progress, import_all_places, reset_progress

        # Check if already running
        progress = get_import_progress()
        if progress.get("running"):
            return web.json_response({"error": "Import already in progress"}, status=400)

        # Reset progress BEFORE starting thread so the first poll can't see the previous run's result
        reset_progress("all places")

        # Start import in background thread
        def run_import():
            import_all_places(self.database)
            # Update stats after import
            stats = self.database.get_stats()
            self.update_db_stats(
                stats["places"], stats["streets"], stats["capcodes"], stats["texts"], stats["geocodes"]
            )

        thread = threading.Thread(target=run_import, daemon=True)
        thread.start()

        return web.json_response({"started": True, "message": "Importing all Dutch places..."})

    async def _handle_bag_progress(self, request: web.Request) -> web.Response:
        """Get current BAG import progress."""
        from .bag_import import get_import_progress

        progress = get_import_progress()
        return web.json_response(progress)

    async def _handle_import_bommel(self, request: web.Request) -> web.Response:
        """Start capcode import from bommel.net in background thread."""

        import threading

        from .capcode_import import get_import_progress, import_all_capcodes, reset_progress

        # Check if already running
        progress = get_import_progress()
        if progress.get("running"):
            return web.json_response({"error": "Import already in progress"}, status=400)

        # Reset progress BEFORE starting thread to avoid race condition
        reset_progress()

        # Start import in background thread
        def run_import():
            import_all_capcodes(self.database)
            # Refresh DB counts so the Advanced tab updates right away
            stats = self.database.get_stats()
            self.update_db_stats(
                stats["places"], stats["streets"], stats["capcodes"], stats["texts"], stats["geocodes"]
            )

        thread = threading.Thread(target=run_import, daemon=True)
        thread.start()

        # Return immediately - client will poll for progress
        return web.json_response({"started": True, "message": "Import started"})

    async def _handle_capcode_progress(self, request: web.Request) -> web.Response:
        """Get current capcode import progress."""
        from .capcode_import import get_import_progress

        progress = get_import_progress()
        return web.json_response(progress)

    # =========================================================================
    # MESSAGE HISTORY HANDLERS
    # =========================================================================

    async def _handle_list_messages(self, request: web.Request) -> web.Response:
        """List messages with pagination and search."""

        page = _int_param(request, "page", 1, maximum=MAX_PAGE)
        per_page = _int_param(request, "per_page", 50)
        search = request.query.get("search", "")
        discipline = request.query.get("discipline", "")
        city = request.query.get("city", "")

        result = self.database.list_messages(
            page=page, per_page=per_page, search=search, discipline=discipline, city=city
        )
        return web.json_response(result)

    async def _handle_delete_message(self, request: web.Request) -> web.Response:
        """Delete a single message."""

        try:
            message_id = int(request.match_info["id"])
        except (KeyError, TypeError, ValueError):
            return web.json_response({"error": "Invalid id"}, status=400)
        success = self.database.delete_message(message_id)

        if success:
            return web.json_response({"success": True})
        return web.json_response({"error": "Message not found"}, status=404)

    async def _handle_delete_all_messages(self, request: web.Request) -> web.Response:
        """Delete all messages."""

        count = self.database.delete_all_messages()
        # Clear in-memory cache and counters so the dashboard reflects the wipe
        self.messages.clear()
        self.stats["total_messages"] = 0
        self.stats["by_region"] = {}
        self.stats["by_discipline"] = {}
        self._save_stats()
        return web.json_response({"success": True, "deleted": count})

    @staticmethod
    def _import_running() -> str | None:
        """Return an error message while a BAG or capcode import thread is running."""
        from . import bag_import, capcode_import

        if bag_import.get_import_progress().get("running"):
            return "A BAG import is running; wait for it to finish"
        if capcode_import.get_import_progress().get("running"):
            return "A capcode import is running; wait for it to finish"
        return None

    async def _handle_clear_table(self, request: web.Request) -> web.Response:
        """Delete all rows from a single table (whitelisted in database.clear_table)."""
        table = request.match_info.get("table", "")
        if busy := self._import_running():
            return web.json_response({"error": busy}, status=409)
        count = self.database.clear_table(table)
        if count < 0:
            return web.json_response({"error": f"Cannot clear table '{table}'"}, status=400)
        if table == "messages":
            # Keep in-memory cache and counters in sync
            self.messages.clear()
            self.stats["total_messages"] = 0
            self.stats["by_region"] = {}
            self.stats["by_discipline"] = {}
            self._save_stats()
        elif table == "tts_replacements":
            self._tts_changed()
        elif table in ("ignore_text", "ignore_capcodes"):
            self._ignore_changed()
        return web.json_response({"success": True, "deleted": count})

    async def _handle_reset_database(self, request: web.Request) -> web.Response:
        """Reset the database - delete all data and recreate schema."""
        if busy := self._import_running():
            return web.json_response({"error": busy}, status=409)

        try:
            # Drop all tables
            tables = [
                "capcodes",
                "places",
                "streets",
                "geocodes",
                "messages",
                "stats",
                "abbreviations",
                "tts_replacements",
                "ignore_text",
                "ignore_capcodes",
                "vehicle_photos",
            ]
            for table in tables:
                with contextlib.suppress(Exception):
                    self.database.cursor.execute(f"DROP TABLE IF EXISTS {table}")
            self.database.conn.commit()

            # Recreate schema and drop cached text-matching data
            self.database._migrate_schema()
            self.database.invalidate_text_caches()
            self._tts_changed()
            self._ignore_changed()

            # Clear in-memory message cache and counters
            self.messages.clear()
            self.stats["total_messages"] = 0
            self.stats["by_region"] = {}
            self.stats["by_discipline"] = {}
            self._save_stats()

            # Update stats
            stats = self.database.get_stats()
            self.update_db_stats(
                stats["places"], stats["streets"], stats["capcodes"], stats["texts"], stats["geocodes"]
            )

            return web.json_response({"success": True, "message": "Database reset successfully"})
        except Exception as e:
            return web.json_response({"error": str(e)}, status=500)

    async def _handle_list_abbreviations(self, request: web.Request) -> web.Response:
        """List abbreviations with pagination and search."""
        page = _int_param(request, "page", 1, maximum=MAX_PAGE)
        per_page = _int_param(request, "per_page", 50)
        search = request.query.get("search", "")
        result = self.database.list_abbreviations(page, per_page, search)
        return web.json_response(result)

    async def _handle_add_abbreviation(self, request: web.Request) -> web.Response:
        """Add a new abbreviation."""
        data = await _json_object(request)
        abbrev = data.get("abbreviation", "")
        full_text = data.get("full_text", "")
        if not abbrev or not full_text:
            return web.json_response({"error": "abbreviation and full_text are required"}, status=400)
        success = self.database.add_abbreviation(abbrev, full_text)
        return web.json_response({"success": success})

    async def _handle_delete_abbreviation(self, request: web.Request) -> web.Response:
        """Delete an abbreviation by id."""
        try:
            abbrev_id = int(request.match_info.get("id", 0))
        except (TypeError, ValueError):
            return web.json_response({"error": "Invalid id"}, status=400)
        success = self.database.delete_abbreviation(abbrev_id)
        return web.json_response({"success": success})

    async def _handle_import_abbreviations(self, request: web.Request) -> web.Response:
        """Import abbreviations from Bommel."""
        try:
            from .abbreviation_import import import_abbreviations

            count = import_abbreviations(self.database)
            return web.json_response({"success": True, "count": count, "message": f"Imported {count} abbreviations"})
        except Exception as e:
            logger.error(f"Failed to import abbreviations: {e}")
            return web.json_response({"error": str(e)}, status=500)

    async def _handle_find_abbreviations(self, request: web.Request) -> web.Response:
        """Find abbreviations in text and return their full meanings."""
        data = await _json_object(request)
        try:
            text = data.get("text", "")
            found = self.database.find_abbreviations_in_text(text)
            return web.json_response({"abbreviations": found})
        except Exception as e:
            logger.error(f"Failed to find abbreviations: {e}")
            return web.json_response({"error": str(e)}, status=500)

    async def _handle_list_tts(self, request: web.Request) -> web.Response:
        """List TTS replacements with pagination and search."""
        page = _int_param(request, "page", 1, maximum=MAX_PAGE)
        per_page = _int_param(request, "per_page", 50)
        search = request.query.get("search", "")
        result = self.database.list_tts_replacements(page, per_page, search)
        return web.json_response(result)

    async def _handle_add_tts(self, request: web.Request) -> web.Response:
        """Add a TTS replacement."""
        data = await _json_object(request)
        pattern = data.get("pattern", "")
        replacement = data.get("replacement", "")
        if not pattern:
            return web.json_response({"error": "Pattern is required"}, status=400)
        success = self.database.add_tts_replacement(pattern, replacement)
        if success:
            self._tts_changed()
        return web.json_response({"success": success})

    async def _handle_update_tts(self, request: web.Request) -> web.Response:
        """Update a TTS replacement."""
        try:
            tts_id = int(request.match_info.get("id", 0))
        except (TypeError, ValueError):
            return web.json_response({"error": "Invalid id"}, status=400)
        data = await _json_object(request)
        pattern = data.get("pattern", "")
        if not pattern:
            return web.json_response({"error": "Pattern is required"}, status=400)
        enabled = data.get("enabled")  # None = keep current value
        success = self.database.update_tts_replacement(tts_id, pattern, data.get("replacement", ""), enabled)
        if success:
            self._tts_changed()
        return web.json_response({"success": success})

    async def _handle_delete_tts(self, request: web.Request) -> web.Response:
        """Delete a TTS replacement."""
        try:
            tts_id = int(request.match_info.get("id", 0))
        except (TypeError, ValueError):
            tts_id = 0
        success = self.database.delete_tts_replacement(tts_id)
        if success:
            self._tts_changed()
        return web.json_response({"success": success})

    async def _handle_delete_all_tts(self, request: web.Request) -> web.Response:
        """Delete all TTS replacements."""
        count = self.database.delete_all_tts_replacements()
        if count:
            self._tts_changed()
        return web.json_response({"success": True, "deleted": count})

    async def _handle_list_ignore_text(self, request: web.Request) -> web.Response:
        """List ignore-text patterns with pagination and search."""
        page = _int_param(request, "page", 1, maximum=MAX_PAGE)
        per_page = _int_param(request, "per_page", 50)
        search = request.query.get("search", "")
        result = self.database.list_ignore_text(page, per_page, search)
        return web.json_response(result)

    async def _handle_add_ignore_text(self, request: web.Request) -> web.Response:
        """Add an ignore-text pattern."""
        data = await _json_object(request)
        pattern = data.get("pattern", "")
        if not pattern:
            return web.json_response({"error": "Pattern is required"}, status=400)
        success = self.database.add_ignore_text(pattern)
        if success:
            self._ignore_changed()
        return web.json_response({"success": success})

    async def _handle_update_ignore_text(self, request: web.Request) -> web.Response:
        """Update an ignore-text pattern."""
        try:
            rule_id = int(request.match_info.get("id", 0))
        except (TypeError, ValueError):
            return web.json_response({"error": "Invalid id"}, status=400)
        data = await _json_object(request)
        pattern = data.get("pattern", "")
        if not pattern:
            return web.json_response({"error": "Pattern is required"}, status=400)
        enabled = data.get("enabled")  # None = keep current value
        success = self.database.update_ignore_text(rule_id, pattern, enabled)
        if success:
            self._ignore_changed()
        return web.json_response({"success": success})

    async def _handle_delete_ignore_text(self, request: web.Request) -> web.Response:
        """Delete an ignore-text pattern."""
        try:
            rule_id = int(request.match_info.get("id", 0))
        except (TypeError, ValueError):
            rule_id = 0
        success = self.database.delete_ignore_text(rule_id)
        if success:
            self._ignore_changed()
        return web.json_response({"success": success})

    async def _handle_delete_all_ignore_text(self, request: web.Request) -> web.Response:
        """Delete all ignore-text patterns."""
        count = self.database.delete_all_ignore_text()
        if count:
            self._ignore_changed()
        return web.json_response({"success": True, "deleted": count})

    async def _handle_list_ignore_capcodes(self, request: web.Request) -> web.Response:
        """List ignored capcodes with pagination and search."""
        page = _int_param(request, "page", 1, maximum=MAX_PAGE)
        per_page = _int_param(request, "per_page", 50)
        search = request.query.get("search", "")
        result = self.database.list_ignore_capcodes(page, per_page, search)
        return web.json_response(result)

    async def _handle_add_ignore_capcode(self, request: web.Request) -> web.Response:
        """Add an ignored capcode."""
        data = await _json_object(request)
        capcode = data.get("capcode", "")
        if not capcode:
            return web.json_response({"error": "Capcode is required"}, status=400)
        success = self.database.add_ignore_capcode(capcode)
        if success:
            self._ignore_changed()
        return web.json_response({"success": success})

    async def _handle_update_ignore_capcode(self, request: web.Request) -> web.Response:
        """Update an ignored capcode."""
        try:
            rule_id = int(request.match_info.get("id", 0))
        except (TypeError, ValueError):
            return web.json_response({"error": "Invalid id"}, status=400)
        data = await _json_object(request)
        capcode = data.get("capcode", "")
        if not capcode:
            return web.json_response({"error": "Capcode is required"}, status=400)
        enabled = data.get("enabled")  # None = keep current value
        success = self.database.update_ignore_capcode(rule_id, capcode, enabled)
        if success:
            self._ignore_changed()
        return web.json_response({"success": success})

    async def _handle_delete_ignore_capcode(self, request: web.Request) -> web.Response:
        """Delete an ignored capcode."""
        try:
            rule_id = int(request.match_info.get("id", 0))
        except (TypeError, ValueError):
            rule_id = 0
        success = self.database.delete_ignore_capcode(rule_id)
        if success:
            self._ignore_changed()
        return web.json_response({"success": success})

    async def _handle_delete_all_ignore_capcodes(self, request: web.Request) -> web.Response:
        """Delete all ignored capcodes."""
        count = self.database.delete_all_ignore_capcodes()
        if count:
            self._ignore_changed()
        return web.json_response({"success": True, "deleted": count})

    async def _fetch_tile(self, url: str, headers: dict) -> tuple[int, bytes]:
        """Fetch one map tile, returning (status, body)."""
        if self._tile_session is None or self._tile_session.closed:
            self._tile_session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10))
        async with self._tile_session.get(url, headers=headers) as resp:
            return resp.status, await resp.read() if resp.status == 200 else b""

    async def _handle_map_tile(self, request: web.Request) -> web.Response:
        """Serve an OpenStreetMap raster tile via Home Assistant's map tile proxy."""
        z, x, y = (int(request.match_info[k]) for k in ("z", "x", "y"))
        if z > MAP_MAX_ZOOM or x >= 2**z or y >= 2**z:
            return web.Response(status=404)

        try:
            status = 0
            if self._ha_map_tiles:
                status, body = await self._fetch_tile(
                    HA_MAP_TILES_URL.format(z=z, x=x, y=y),
                    {"Authorization": f"Bearer {self._supervisor_token}"},
                )
                if status == 404:
                    logger.info("Home Assistant has no map_tiles proxy (core < 2026.9), using OpenStreetMap directly")
                    self._ha_map_tiles = False
            if not self._ha_map_tiles:
                status, body = await self._fetch_tile(
                    OSM_TILES_URL.format(z=z, x=x, y=y), {"User-Agent": OSM_USER_AGENT}
                )
        except (aiohttp.ClientError, TimeoutError) as e:
            logger.debug(f"Map tile {z}/{x}/{y} failed: {e}")
            return web.Response(status=502)

        if status != 200:
            logger.debug(f"Map tile {z}/{x}/{y} returned {status}")
            return web.Response(status=502)
        return web.Response(
            body=body,
            content_type="image/png",
            headers={"Cache-Control": f"private, max-age={MAP_TILE_MAX_AGE}"},
        )

    async def _handle_get_vehicle_photo(self, request: web.Request) -> web.Response:
        """Serve a stored vehicle photo's raw image bytes."""
        key_type = request.match_info.get("key_type", "")
        key_value = request.match_info.get("key_value", "")
        photo = self.database.get_vehicle_photo(key_type, key_value)
        if not photo:
            return web.json_response({"error": "Not found"}, status=404)
        # Rows stored before upload validation may hold an SVG or a parameterised type
        mime_type = photo["mime_type"].split(";", 1)[0].strip().lower()
        if mime_type not in PHOTO_MIME_TYPES:
            mime_type = "application/octet-stream"
        return web.Response(
            body=photo["image"],
            content_type=mime_type,
            headers={"Cache-Control": "no-cache, must-revalidate", "X-Content-Type-Options": "nosniff"},
        )

    async def _handle_set_vehicle_photo(self, request: web.Request) -> web.Response:
        """Upload/replace a vehicle photo (multipart form: key_type, key_value, label, file)."""
        try:
            reader = await request.multipart()
            fields: dict[str, str] = {}
            image_bytes = b""
            async for part in reader:
                if not isinstance(part, aiohttp.BodyPartReader):
                    continue
                if part.name == "file":
                    image_bytes = await self._read_part_limited(part, MAX_PHOTO_SIZE)
                    if image_bytes is None:
                        return web.json_response({"error": "Image too large (max 5MB)"}, status=400)
                elif part.name:
                    value = await self._read_part_limited(part, MAX_FORM_FIELD_SIZE)
                    if value is None:
                        return web.json_response({"error": f"Field '{part.name}' too large"}, status=400)
                    fields[part.name] = value.decode("utf-8", errors="replace")
        except Exception as e:
            return web.json_response({"error": f"Invalid upload: {e}"}, status=400)

        key_type = fields.get("key_type", "")
        key_value = fields.get("key_value", "").strip()
        label = fields.get("label", "").strip()

        if key_type not in ("number", "category"):
            return web.json_response({"error": "key_type must be 'number' or 'category'"}, status=400)
        if not key_value:
            return web.json_response({"error": "key_value is required"}, status=400)
        if not image_bytes:
            return web.json_response({"error": "file is required"}, status=400)
        # Trust the file's magic bytes, not the client's Content-Type: SVG (script)
        # or a parameterised type must never be stored and served back same-origin
        mime_type = _sniff_image_type(image_bytes)
        if mime_type not in PHOTO_MIME_TYPES:
            return web.json_response({"error": "file must be a JPEG, PNG, WebP or GIF image"}, status=400)

        success = self.database.set_vehicle_photo(key_type, key_value, mime_type, image_bytes, label)
        return web.json_response({"success": success})

    @staticmethod
    async def _read_part_limited(part: aiohttp.BodyPartReader, limit: int) -> bytes | None:
        """Read a multipart part in chunks, returning None once it exceeds limit bytes."""
        chunks = []
        size = 0
        while chunk := await part.read_chunk():
            size += len(chunk)
            if size > limit:
                return None
            chunks.append(chunk)
        return b"".join(chunks)

    async def _handle_delete_vehicle_photo(self, request: web.Request) -> web.Response:
        """Delete a vehicle photo."""
        key_type = request.match_info.get("key_type", "")
        key_value = request.match_info.get("key_value", "")
        success = self.database.delete_vehicle_photo(key_type, key_value)
        return web.json_response({"success": success})

    async def _handle_list_vehicle_categories(self, request: web.Request) -> web.Response:
        """List known vehicle categories (for the standalone photo-upload dropdown)."""
        from .vehicle_types import CATEGORY_ICONS

        categories = [{"category": k, "icon": v} for k, v in CATEGORY_ICONS.items()]
        return web.json_response({"categories": categories})

    async def _handle_list_vehicle_photos(self, request: web.Request) -> web.Response:
        """List vehicle photos with pagination and search, for the Database tab."""
        page = _int_param(request, "page", 1, maximum=MAX_PAGE)
        per_page = _int_param(request, "per_page", 50)
        search = request.query.get("search", "")
        result = self.database.list_vehicle_photos(page, per_page, search)
        return web.json_response(result)


DASHBOARD_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Meldkamer</title>
    <link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css"/>
    <script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
    <script src="https://unpkg.com/leaflet.heat@0.2.0/dist/leaflet-heat.js"></script>
    <style>
        :root {
            --ha-card-background: #1a1d21;
            --ha-background: #101214;
            --primary-text-color: #e4e7ea;
            --secondary-text-color: #98a0a8;
            --primary-color: #03a9f4;
            --accent-color: #ff9800;
            --divider-color: rgba(255,255,255,0.09);
            --card-radius: 12px;
            --card-border: 1px solid var(--divider-color);
            --prio-1: #e5484d;
            --prio-2: #f57c00;
            --prio-3: #fdd835;
            --success-color: #43a047;
            --danger-color: #e5484d;
            --shadow-sm: 0 1px 2px rgba(0,0,0,0.25);
            --shadow-lg: 0 8px 30px rgba(0,0,0,0.35);
            --hover-bg: rgba(140,150,160,0.10);
        }

        @media (prefers-color-scheme: light) {
            :root {
                --ha-card-background: #ffffff;
                --ha-background: #f3f5f7;
                --primary-text-color: #1f2328;
                --secondary-text-color: #6a737c;
                --divider-color: rgba(16,24,40,0.10);
                --shadow-sm: 0 1px 2px rgba(16,24,40,0.06);
                --shadow-lg: 0 8px 30px rgba(16,24,40,0.16);
                --hover-bg: rgba(16,24,40,0.045);
            }
        }

        * { box-sizing: border-box; margin: 0; padding: 0; }
        body {
            font-family: Roboto, -apple-system, 'Segoe UI', 'Noto Sans', sans-serif;
            background: var(--ha-background);
            color: var(--primary-text-color);
            min-height: 100vh;
            line-height: 1.45;
            font-size: 14px;
            -webkit-font-smoothing: antialiased;
        }
        .container { max-width: 1500px; margin: 0 auto; padding: 16px 20px 32px; }

        ::-webkit-scrollbar { width: 8px; height: 8px; }
        ::-webkit-scrollbar-thumb { background: rgba(140,150,160,0.35); border-radius: 4px; }
        ::-webkit-scrollbar-track { background: transparent; }

        /* ---------- Header ---------- */
        .app-header {
            display: flex; justify-content: space-between; align-items: center;
            gap: 16px; flex-wrap: wrap; padding: 4px 0 14px;
        }
        .brand { display: flex; align-items: center; gap: 12px; }
        .brand-icon {
            width: 38px; height: 38px; border-radius: 10px; flex-shrink: 0;
            background: rgba(3,169,244,0.12); color: var(--primary-color);
            display: flex; align-items: center; justify-content: center;
        }
        .brand-icon svg { width: 22px; height: 22px; }
        .brand h1 { font-size: 17px; font-weight: 600; letter-spacing: 0.01em; }
        .brand-sub { font-size: 12px; color: var(--secondary-text-color); }
        .brand-sub span + span::before { content: ' · '; }

        .header-actions { display: flex; align-items: center; gap: 8px; }
        .conn {
            display: inline-flex; align-items: center; gap: 7px;
            font-size: 12px; color: var(--secondary-text-color); padding: 0 4px;
        }
        .conn .dot { width: 8px; height: 8px; border-radius: 50%; background: var(--secondary-text-color); }
        .conn.live .dot { background: var(--success-color); box-shadow: 0 0 0 3px rgba(67,160,71,0.22); }
        .conn.reconnecting .dot { background: var(--prio-2); }
        .conn.live { color: var(--success-color); }
        .conn.reconnecting { color: var(--prio-2); }

        .icon-btn {
            width: 32px; height: 32px; border-radius: 8px; border: var(--card-border);
            background: var(--ha-card-background); color: var(--primary-text-color);
            display: inline-flex; align-items: center; justify-content: center;
            cursor: pointer; font-size: 15px; transition: background 0.15s, opacity 0.15s;
        }
        .icon-btn:hover { background: var(--hover-bg); }
        .icon-btn.off { opacity: 0.4; }
        .icon-btn.paused { background: rgba(255,152,0,0.18); color: #ff9800; }

        .pill { padding: 5px 12px; border-radius: 999px; font-size: 12px; font-weight: 600; }
        .pill-success { background: rgba(67,160,71,0.15); color: #4caf50; }
        .pill-danger { background: rgba(229,72,77,0.14); color: #ef5350; }

        /* ---------- Tabs ---------- */
        .tabs {
            display: flex; gap: 2px; margin-bottom: 18px;
            border-bottom: 1px solid var(--divider-color);
        }
        .tab {
            padding: 10px 16px; cursor: pointer; border: none; background: transparent;
            color: var(--secondary-text-color); font-size: 13px; font-weight: 500;
            border-bottom: 2px solid transparent; margin-bottom: -1px;
            display: inline-flex; align-items: center; gap: 7px;
            border-radius: 8px 8px 0 0; transition: color 0.15s, background 0.15s;
        }
        .tab svg { width: 17px; height: 17px; }
        .tab:hover { color: var(--primary-text-color); background: var(--hover-bg); }
        .tab.active { color: var(--primary-color); border-bottom-color: var(--primary-color); }
        .tab-content { display: none; }
        .tab-content.active { display: block; }

        /* ---------- Cards & stats ---------- */
        .stats-grid { display: grid; grid-template-columns: repeat(4, 1fr); gap: 12px; margin-bottom: 14px; }
        .stat-card {
            background: var(--ha-card-background); border: var(--card-border);
            border-radius: var(--card-radius); box-shadow: var(--shadow-sm);
            padding: 16px 18px; display: flex; align-items: center; gap: 14px;
        }
        .stat-icon {
            width: 40px; height: 40px; border-radius: 10px; flex-shrink: 0;
            background: rgba(3,169,244,0.10); color: var(--primary-color);
            display: flex; align-items: center; justify-content: center;
        }
        .stat-icon svg { width: 20px; height: 20px; }
        .stat-value { font-size: 24px; font-weight: 600; line-height: 1.15; color: var(--primary-text-color); }
        .stat-label {
            color: var(--secondary-text-color); font-size: 11px; margin-top: 2px;
            text-transform: uppercase; letter-spacing: 0.06em; font-weight: 500;
        }

        /* ---------- Map ---------- */
        .map-wrapper {
            position: relative; border-radius: var(--card-radius);
            border: var(--card-border); overflow: hidden; box-shadow: var(--shadow-sm);
            margin-bottom: 14px;
        }
        #map { height: 400px; min-height: 200px; max-height: 80vh; }
        .map-history {
            position: absolute; top: 10px; right: 10px; z-index: 1000;
            display: flex; align-items: center; gap: 8px; flex-wrap: wrap; justify-content: flex-end;
            padding: 6px 10px; border-radius: 8px; max-width: calc(100% - 70px);
            background: var(--ha-card-background); border: var(--card-border); box-shadow: var(--shadow-sm);
            font-size: 12px;
        }
        .map-history select { padding: 3px 6px; font-size: 12px; }
        .map-history input[type=range] { width: 180px; }
        .map-history .until { color: var(--secondary-text-color); white-space: nowrap; }
        .map-resize-handle {
            position: absolute; bottom: 0; left: 0; right: 0; height: 10px;
            cursor: ns-resize; z-index: 1000;
            display: flex; align-items: center; justify-content: center;
            background: rgba(0,0,0,0.08);
        }
        .map-resize-handle:hover, .map-resize-handle.active { background: rgba(3,169,244,0.25); }
        .map-resize-handle::after {
            content: ''; width: 40px; height: 3px; border-radius: 2px;
            background: rgba(128,128,128,0.6);
        }

        /* ---------- Dashboard content ---------- */
        .content-grid { display: grid; grid-template-columns: 340px 1fr; gap: 14px; align-items: start; }

        .panel {
            background: var(--ha-card-background); border: var(--card-border);
            border-radius: var(--card-radius); box-shadow: var(--shadow-sm);
        }
        .panel-head {
            display: flex; justify-content: space-between; align-items: center;
            padding: 12px 14px; border-bottom: 1px solid var(--divider-color);
        }
        .panel-title { font-size: 13px; font-weight: 600; }
        .panel-sub { color: var(--secondary-text-color); font-size: 11px; }
        .panel-body { padding: 16px; }

        .sensor-filter-bar {
            display: flex; flex-wrap: wrap; gap: 6px;
            padding: 0 10px 10px;
        }
        .sensor-filter-pill {
            padding: 4px 11px; border-radius: 999px; font-size: 12px; font-weight: 500;
            border: 1px solid var(--divider-color); background: var(--ha-card-background);
            color: var(--secondary-text-color); cursor: pointer; transition: all 0.12s;
        }
        .sensor-filter-pill:hover { border-color: var(--primary-color); color: var(--primary-text-color); }
        .sensor-filter-pill.active {
            background: var(--primary-color); border-color: var(--primary-color); color: white;
        }
        .msg-list {
            display: flex; flex-direction: column; gap: 8px;
            max-height: 460px; overflow-y: auto; padding: 10px;
        }
        .empty-state {
            color: var(--secondary-text-color); text-align: center;
            padding: 40px 20px; font-size: 12px;
        }

        .message {
            background: var(--ha-card-background); border: var(--card-border);
            border-left: 3px solid var(--secondary-text-color);
            border-radius: 8px; padding: 9px 11px; cursor: pointer;
            transition: background 0.12s, border-color 0.12s;
        }
        .message:hover { background: var(--hover-bg); }
        .message.selected { border-color: var(--primary-color); background: rgba(3,169,244,0.08); }
        .message.prio-1 { border-left-color: var(--prio-1); }
        .message.prio-2 { border-left-color: var(--prio-2); }
        .message.prio-3 { border-left-color: var(--prio-3); }
        .message-header { display: flex; justify-content: space-between; gap: 8px; margin-bottom: 4px; }
        .message-time { color: var(--secondary-text-color); font-size: 11px; white-space: nowrap; }
        .message-badges { display: flex; gap: 4px; flex-wrap: wrap; }
        .message-body { font-size: 12px; line-height: 1.4; color: var(--primary-text-color); }
        .message-location { color: var(--secondary-text-color); font-size: 11px; margin-top: 4px; }

        .badge {
            padding: 2px 8px; border-radius: 999px; font-size: 10px; font-weight: 600;
            background: rgba(3,169,244,0.12); color: var(--primary-color);
        }
        .badge.discipline { background: rgba(255,152,0,0.14); color: var(--accent-color); }
        .badge.prio { background: rgba(229,72,77,0.14); color: #ef5350; }
        .badge.incident { background: rgba(229,72,77,0.22); color: #ef5350; }
        .pill-warning { background: rgba(245,124,0,0.16); color: var(--prio-2); }

        /* ---------- Detail panel ---------- */
        .detail-panel { position: sticky; top: 0; }
        .detail-wrap { display: flex; gap: 16px; align-items: flex-start; }
        .detail-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 4px 16px; flex: 1; min-width: 0; }
        .detail-row {
            display: flex; justify-content: space-between; gap: 12px;
            padding: 7px 0; border-bottom: 1px solid var(--divider-color);
        }
        .detail-label { color: var(--secondary-text-color); font-size: 12px; white-space: nowrap; }
        .detail-value { color: var(--primary-text-color); font-size: 12px; font-weight: 500; text-align: right; word-break: break-word; }
        .detail-body {
            grid-column: 1 / -1; padding: 9px 11px; margin-bottom: 6px;
            background: var(--hover-bg); border-radius: 8px;
            font-size: 12px; word-break: break-word;
        }
        .abbrev-tag {
            display: inline-block; padding: 2px 7px; margin: 2px 2px 2px 0;
            background: rgba(3,169,244,0.10); border-radius: 999px; font-size: 10px;
        }
        .raw-block { grid-column: 1 / -1; margin-top: 4px; }
        .raw-block summary { cursor: pointer; color: var(--secondary-text-color); font-size: 11px; }
        .raw-block pre {
            margin-top: 6px; padding: 8px 10px; background: var(--hover-bg);
            border-radius: 8px; font-size: 11px; white-space: pre-wrap; word-break: break-all;
        }

        .vehicle-side {
            flex-shrink: 0;
            display: flex; flex-direction: column; align-items: center; gap: 6px;
            width: 140px; padding: 9px 11px;
            background: var(--hover-bg); border-radius: 8px; text-align: center;
        }
        .vehicle-visual {
            width: 120px; height: 120px; border-radius: 10px; flex-shrink: 0; cursor: pointer;
            background: var(--ha-card-background); border: var(--card-border);
            display: flex; align-items: center; justify-content: center; overflow: hidden;
            font-size: 48px; transition: opacity 0.12s;
        }
        .vehicle-visual:hover { opacity: 0.8; }
        .vehicle-visual img { width: 100%; height: 100%; object-fit: cover; }
        .vehicle-category-label { font-size: 12.5px; font-weight: 500; color: var(--primary-text-color); }
        .vehicle-number-label { font-size: 11px; color: var(--secondary-text-color); margin-top: 1px; }
        .vehicle-side .btn { margin-top: 4px; padding: 4px 10px; font-size: 11.5px; width: 100%; }

        .history-section { padding: 0 14px 12px; }
        .history-section h4 { color: var(--accent-color); font-size: 11px; font-weight: 600; margin-bottom: 8px; text-transform: uppercase; letter-spacing: 0.05em; }
        .history-item {
            padding: 8px 10px; background: var(--hover-bg); border-radius: 8px;
            margin-bottom: 6px; font-size: 12px;
        }
        .history-item .time { color: var(--secondary-text-color); font-size: 11px; }

        /* ---------- Buttons & inputs ---------- */
        .btn {
            display: inline-flex; align-items: center; gap: 6px;
            padding: 7px 14px; border-radius: 8px; border: var(--card-border);
            background: var(--ha-card-background); color: var(--primary-text-color);
            font-size: 12.5px; font-weight: 500; cursor: pointer;
            transition: background 0.12s, opacity 0.12s;
        }
        .btn:hover { background: var(--hover-bg); }
        .btn:disabled { opacity: 0.45; cursor: default; }
        .btn-primary { background: var(--primary-color); border-color: var(--primary-color); color: #fff; }
        .btn-primary:hover { background: #039be5; }
        .btn-success { background: var(--success-color); border-color: var(--success-color); color: #fff; }
        .btn-success:hover { background: #3d9142; }
        .btn-danger { background: transparent; color: #ef5350; border-color: rgba(229,72,77,0.45); }
        .btn-danger:hover { background: rgba(229,72,77,0.12); }
        .btn-danger-solid { background: var(--danger-color); border-color: var(--danger-color); color: #fff; }
        .btn-danger-solid:hover { background: #d32f2f; }

        .input {
            padding: 7px 12px; border-radius: 8px; border: var(--card-border);
            background: var(--ha-background); color: var(--primary-text-color);
            font-size: 13px; outline: none; font-family: inherit;
            transition: border-color 0.12s;
        }
        .input:focus { border-color: var(--primary-color); }
        textarea.input { resize: vertical; line-height: 1.4; }

        /* ---------- Advanced tab ---------- */
        .info-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(300px, 1fr)); gap: 14px; }
        .info-card h3 {
            font-size: 11px; font-weight: 600; color: var(--secondary-text-color);
            text-transform: uppercase; letter-spacing: 0.06em; margin-bottom: 6px;
            display: flex; align-items: center; gap: 8px;
        }
        .info-row {
            display: flex; justify-content: space-between; gap: 12px;
            padding: 8px 0; border-bottom: 1px solid var(--divider-color); font-size: 13px;
        }
        .info-row:last-child { border-bottom: none; }
        .info-label { color: var(--secondary-text-color); }
        .info-value { color: var(--primary-text-color); font-weight: 500; text-align: right; }

        .stats-panel { margin-top: 16px; }
        .stats-charts-grid {
            display: grid; grid-template-columns: repeat(auto-fit, minmax(280px, 1fr));
            gap: 28px; margin-top: 16px; padding-top: 16px; border-top: 1px solid var(--divider-color);
        }
        .chart-title {
            display: flex; align-items: center; gap: 8px; margin-bottom: 12px;
            font-size: 11px; font-weight: 600; color: var(--secondary-text-color);
            text-transform: uppercase; letter-spacing: 0.06em;
        }
        .chart-row { display: flex; align-items: center; gap: 20px; }
        .chart-legend { flex: 1; min-width: 0; display: flex; flex-direction: column; gap: 6px; }
        .chart-legend-item {
            display: flex; align-items: center; gap: 8px; font-size: 12.5px;
            color: var(--primary-text-color); overflow: hidden;
        }
        .chart-legend-item .swatch { width: 10px; height: 10px; border-radius: 3px; flex-shrink: 0; }
        .chart-legend-item .label { overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
        .chart-legend-item .count { color: var(--secondary-text-color); margin-left: auto; padding-left: 8px; flex-shrink: 0; }

        /* ---------- Database tab ---------- */
        .db-toolbar { display: flex; justify-content: space-between; align-items: center; gap: 12px; flex-wrap: wrap; margin-bottom: 14px; }
        .db-toolbar-left { display: flex; align-items: center; gap: 10px; }
        .db-toolbar-right { display: flex; align-items: center; gap: 8px; flex-wrap: wrap; }
        .db-table-title { font-size: 15px; font-weight: 600; }

        .db-table-wrap { max-height: 480px; overflow-y: auto; }
        .db-table { width: 100%; border-collapse: collapse; font-size: 13px; }
        .db-table th {
            text-align: left; padding: 10px 12px; font-size: 11px; font-weight: 600;
            text-transform: uppercase; letter-spacing: 0.05em;
            color: var(--secondary-text-color); border-bottom: 1px solid var(--divider-color);
            position: sticky; top: 0; background: var(--ha-card-background); z-index: 1;
        }
        .db-table td { padding: 9px 12px; border-bottom: 1px solid var(--divider-color); color: var(--primary-text-color); }
        .db-table tbody tr:hover { background: var(--hover-bg); }
        .row-del {
            border: none; background: transparent; color: var(--secondary-text-color);
            cursor: pointer; padding: 4px 7px; border-radius: 6px; font-size: 13px; line-height: 1;
        }
        .row-del:hover { color: #ef5350; background: rgba(229,72,77,0.12); }

        .db-pagination {
            display: flex; justify-content: space-between; align-items: center;
            padding: 12px 14px; border-top: 1px solid var(--divider-color);
            font-size: 12px; color: var(--secondary-text-color);
        }

        .progress { height: 8px; border-radius: 999px; background: var(--divider-color); overflow: hidden; }
        .progress > div { height: 100%; background: var(--primary-color); border-radius: 999px; transition: width 0.3s; }

        /* ---------- Modal ---------- */
        .modal-overlay {
            position: fixed; inset: 0; background: rgba(0,0,0,0.55);
            display: none; align-items: center; justify-content: center;
            z-index: 3000; padding: 20px;
        }
        .modal-overlay.open { display: flex; }
        .modal {
            background: var(--ha-card-background); border: var(--card-border);
            border-radius: 12px; width: min(520px, 100%); max-height: 85vh;
            overflow-y: auto; box-shadow: var(--shadow-lg);
        }
        .modal-head {
            display: flex; justify-content: space-between; align-items: center;
            padding: 14px 18px; border-bottom: 1px solid var(--divider-color);
        }
        .modal-title { font-size: 15px; font-weight: 600; }
        .modal-close {
            border: none; background: transparent; color: var(--secondary-text-color);
            font-size: 18px; cursor: pointer; line-height: 1; padding: 2px 6px; border-radius: 6px;
        }
        .modal-close:hover { background: var(--hover-bg); color: var(--primary-text-color); }
        .modal-body { padding: 16px 18px; display: flex; flex-direction: column; gap: 12px; font-size: 13px; }
        .modal-foot {
            padding: 14px 18px; border-top: 1px solid var(--divider-color);
            display: flex; justify-content: flex-end; gap: 8px;
        }
        .form-field label {
            display: block; font-size: 11px; font-weight: 600; text-transform: uppercase;
            letter-spacing: 0.05em; color: var(--secondary-text-color); margin-bottom: 5px;
        }
        .form-field .input { width: 100%; }
        .form-check { display: flex; align-items: center; gap: 8px; font-size: 13px; cursor: pointer; }

        /* ---------- Toasts ---------- */
        #toast-root {
            position: fixed; right: 18px; bottom: 18px; z-index: 4000;
            display: flex; flex-direction: column; gap: 8px;
        }
        .toast {
            padding: 10px 14px; border-radius: 8px; max-width: 340px;
            background: var(--ha-card-background); border: var(--card-border);
            border-left: 3px solid var(--primary-color); box-shadow: var(--shadow-lg);
            font-size: 13px; animation: toast-in 0.18s ease-out;
        }
        .toast.success { border-left-color: var(--success-color); }
        .toast.error { border-left-color: var(--danger-color); }
        @keyframes toast-in { from { opacity: 0; transform: translateY(6px); } to { opacity: 1; transform: none; } }

        /* ---------- Database landing cards ---------- */
        .db-cards { display: grid; grid-template-columns: repeat(auto-fit, minmax(270px, 1fr)); gap: 12px; }
        .db-card {
            border: var(--card-border); border-radius: var(--card-radius);
            background: var(--ha-background); padding: 14px 16px;
            display: flex; flex-direction: column; gap: 8px;
        }
        .db-card-head { display: flex; align-items: center; gap: 8px; }
        .db-step {
            width: 20px; height: 20px; border-radius: 50%; flex-shrink: 0;
            background: rgba(3,169,244,0.12); color: var(--primary-color);
            font-size: 11px; font-weight: 700;
            display: inline-flex; align-items: center; justify-content: center;
        }
        .db-card-icon { font-size: 16px; }
        .db-card-title { font-size: 13px; font-weight: 600; flex: 1; }
        .db-card-status { font-size: 10px; font-weight: 600; padding: 2px 8px; border-radius: 999px; }
        .db-card-status.ready { background: rgba(67,160,71,0.15); color: #4caf50; }
        .db-card-status.empty { background: rgba(245,124,0,0.15); color: var(--prio-2); }
        .db-card-desc { font-size: 12px; color: var(--secondary-text-color); line-height: 1.45; }
        .db-card-count { font-size: 12px; font-weight: 600; color: var(--secondary-text-color); }
        .db-card-count span { color: var(--primary-color); font-size: 14px; }
        .db-card-actions { display: flex; gap: 6px; flex-wrap: wrap; margin-top: auto; }

        @media (max-width: 1100px) {
            .content-grid { grid-template-columns: 1fr; }
            .stats-grid { grid-template-columns: repeat(2, 1fr); }
            .detail-panel { position: static; }
        }
        @media (max-width: 560px) {
            .stats-grid { grid-template-columns: 1fr; }
            .detail-grid { grid-template-columns: 1fr; }
        }
    </style>
</head>
<body>
    <div class="container">
        <header class="app-header">
            <div class="brand">
                <span class="brand-icon">
                    <svg viewBox="0 0 24 24"><path fill="currentColor" d="M4.93,4.93C3.12,6.74 2,9.24 2,12C2,14.76 3.12,17.26 4.93,19.07L6.34,17.66C4.89,16.22 4,14.22 4,12C4,9.79 4.89,7.78 6.34,6.34L4.93,4.93M19.07,4.93L17.66,6.34C19.11,7.78 20,9.79 20,12C20,14.22 19.11,16.22 17.66,17.66L19.07,19.07C20.88,17.26 22,14.76 22,12C22,9.24 20.88,6.74 19.07,4.93M7.76,7.76C6.67,8.85 6,10.35 6,12C6,13.65 6.67,15.15 7.76,16.24L9.17,14.83C8.45,14.11 8,13.11 8,12C8,10.89 8.45,9.89 9.17,9.17L7.76,7.76M16.24,7.76L14.83,9.17C15.55,9.89 16,10.89 16,12C16,13.11 15.55,14.11 14.83,14.83L16.24,16.24C17.33,15.15 18,13.65 18,12C18,10.35 17.33,8.85 16.24,7.76M12,10A2,2 0 0,0 10,12A2,2 0 0,0 12,14A2,2 0 0,0 14,12A2,2 0 0,0 12,10Z"/></svg>
                </span>
                <div>
                    <h1>Meldkamer</h1>
                    <div class="brand-sub"><span id="network-name">P2000 FLEX</span><span id="receiver-type"></span></div>
                </div>
            </div>
            <div class="header-actions">
                <span id="stream-status" class="conn" title="Live data connection"><span class="dot"></span><span id="stream-status-text">Connecting…</span></span>
                <button id="sound-toggle" class="icon-btn" onclick="toggleSound()" title="Toggle notification sound">
                    <span id="sound-icon">🔔</span>
                </button>
                <button id="tts-toggle" class="icon-btn off" onclick="toggleTts()" title="Toggle speech output">
                    <span id="tts-icon">🔇</span>
                </button>
                <button id="automap-toggle" class="icon-btn" onclick="toggleAutoMap()" title="Auto-show new messages on map">
                    <span id="automap-icon">📍</span>
                </button>
                <span id="decoder-status" class="pill pill-danger">Decoder stopped</span>
            </div>
        </header>

        <div class="tabs">
            <button class="tab active" data-tab="dashboard" onclick="switchTab('dashboard')">
                <svg viewBox="0 0 24 24"><path fill="currentColor" d="M13,9H11V7H13M13,17H11V11H13M12,2A10,10 0 0,0 2,12A10,10 0 0,0 12,22A10,10 0 0,0 22,12A10,10 0 0,0 12,2Z"/></svg>
                Dashboard
            </button>
            <button class="tab" data-tab="advanced" onclick="switchTab('advanced')">
                <svg viewBox="0 0 24 24"><path fill="currentColor" d="M12,15.5A3.5,3.5 0 0,1 8.5,12A3.5,3.5 0 0,1 12,8.5A3.5,3.5 0 0,1 15.5,12A3.5,3.5 0 0,1 12,15.5M19.43,12.97C19.47,12.65 19.5,12.33 19.5,12C19.5,11.67 19.47,11.34 19.43,11L21.54,9.37C21.73,9.22 21.78,8.95 21.66,8.73L19.66,5.27C19.54,5.05 19.27,4.96 19.05,5.05L16.56,6.05C16.04,5.66 15.5,5.32 14.87,5.07L14.5,2.42C14.46,2.18 14.25,2 14,2H10C9.75,2 9.54,2.18 9.5,2.42L9.13,5.07C8.5,5.32 7.96,5.66 7.44,6.05L4.95,5.05C4.73,4.96 4.46,5.05 4.34,5.27L2.34,8.73C2.21,8.95 2.27,9.22 2.46,9.37L4.57,11C4.53,11.34 4.5,11.67 4.5,12C4.5,12.33 4.53,12.65 4.57,12.97L2.46,14.63C2.27,14.78 2.21,15.05 2.34,15.27L4.34,18.73C4.46,18.95 4.73,19.03 4.95,18.95L7.44,17.94C7.96,18.34 8.5,18.68 9.13,18.93L9.5,21.58C9.54,21.82 9.75,22 10,22H14C14.25,22 14.46,21.82 14.5,21.58L14.87,18.93C15.5,18.67 16.04,18.34 16.56,17.94L19.05,18.95C19.27,19.03 19.54,18.95 19.66,18.73L21.66,15.27C21.78,15.05 21.73,14.78 21.54,14.63L19.43,12.97Z"/></svg>
                Advanced
            </button>
            <button class="tab" data-tab="database" onclick="switchTab('database')">
                <svg viewBox="0 0 24 24"><path fill="currentColor" d="M12,3C7.58,3 4,4.79 4,7C4,9.21 7.58,11 12,11C16.42,11 20,9.21 20,7C20,4.79 16.42,3 12,3M4,9V12C4,14.21 7.58,16 12,16C16.42,16 20,14.21 20,12V9C20,11.21 16.42,13 12,13C7.58,13 4,11.21 4,9M4,14V17C4,19.21 7.58,21 12,21C16.42,21 20,19.21 20,17V14C20,16.21 16.42,18 12,18C7.58,18 4,16.21 4,14Z"/></svg>
                Database
            </button>
        </div>

        <div id="tab-dashboard" class="tab-content active">
            <div class="stats-grid">
                <div class="stat-card">
                    <span class="stat-icon"><svg viewBox="0 0 24 24"><path fill="currentColor" d="M20,2H4C2.9,2 2,2.9 2,4V22L6,18H20C21.1,18 22,17.1 22,16V4C22,2.9 21.1,2 20,2Z"/></svg></span>
                    <div><div class="stat-value" id="total-messages">0</div><div class="stat-label">Total messages</div></div>
                </div>
                <div class="stat-card">
                    <span class="stat-icon"><svg viewBox="0 0 24 24"><path fill="currentColor" d="M12,20A8,8 0 0,0 20,12A8,8 0 0,0 12,4A8,8 0 0,0 4,12A8,8 0 0,0 12,20M12,2A10,10 0 0,1 22,12A10,10 0 0,1 12,22C6.47,22 2,17.5 2,12A10,10 0 0,1 12,2M12.5,7V12.25L17,14.92L16.25,16.15L11,13V7H12.5Z"/></svg></span>
                    <div><div class="stat-value" id="uptime">0m</div><div class="stat-label">Uptime</div></div>
                </div>
                <div class="stat-card">
                    <span class="stat-icon"><svg viewBox="0 0 24 24"><path fill="currentColor" d="M17.9,17.39C17.64,16.59 16.89,16 16,16H15V13A1,1 0 0,0 14,12H8V10H10A1,1 0 0,0 11,9V7H13A2,2 0 0,0 15,5V4.59C17.93,5.77 20,8.65 20,12C20,14.08 19.2,15.97 17.9,17.39M11,19.93C7.05,19.44 4,16.08 4,12C4,11.38 4.08,10.78 4.21,10.21L9,15V16A2,2 0 0,0 11,18M12,2A10,10 0 0,0 2,12A10,10 0 0,0 12,22A10,10 0 0,0 22,12A10,10 0 0,0 12,2Z"/></svg></span>
                    <div><div class="stat-value" id="regions-count">0</div><div class="stat-label">Regions</div></div>
                </div>
                <div class="stat-card">
                    <span class="stat-icon"><svg viewBox="0 0 24 24"><path fill="currentColor" d="M11,13.5V21.5H3V13.5H11M12,2L17.5,11H6.5L12,2M17.5,13A4.5,4.5 0 0,1 22,17.5A4.5,4.5 0 0,1 17.5,22A4.5,4.5 0 0,1 13,17.5A4.5,4.5 0 0,1 17.5,13Z"/></svg></span>
                    <div><div class="stat-value" id="disciplines-count">0</div><div class="stat-label">Disciplines</div></div>
                </div>
            </div>

            <div class="map-wrapper">
                <div id="map"></div>
                <div class="map-history">
                    <label for="map-history-hours">History</label>
                    <select id="map-history-hours" class="input" onchange="loadMapHistory()">
                        <option value="">Off</option>
                        <option value="24">24 hours</option>
                        <option value="168">7 days</option>
                        <option value="720">30 days</option>
                        <option value="8760">1 year</option>
                    </select>
                    <input id="map-history-until" type="range" style="display:none;" oninput="renderMapHistory()" title="Show incidents up to this moment">
                    <span id="map-history-label" class="until"></span>
                </div>
                <div class="map-resize-handle" id="map-resize-handle" title="Drag to resize map"></div>
            </div>

            <div class="content-grid">
                <div class="panel messages-section">
                    <div id="sensor-filter-section" style="display:none;">
                        <div class="panel-head"><span class="panel-title">Show</span></div>
                        <div id="sensor-filter-bar" class="sensor-filter-bar"></div>
                    </div>
                    <div class="panel-head">
                        <span class="panel-title">Recent messages</span>
                        <span id="msg-count" class="panel-sub"></span>
                    </div>
                    <div id="messages" class="msg-list"><div class="empty-state">No messages yet</div></div>
                </div>

                <div id="detail-panel" class="panel detail-panel">
                    <div class="panel-head"><span class="panel-title">Message details</span></div>
                    <div class="panel-body">
                        <div id="detail-content" class="detail-wrap"><div class="empty-state">Click a message to view details</div></div>
                    </div>
                    <div id="incident-section" class="history-section" style="display:none;">
                        <h4>Incident</h4>
                        <div id="incident-list"></div>
                    </div>
                    <div id="history-section" class="history-section" style="display:none;">
                        <h4>Location history</h4>
                        <div id="history-list"></div>
                    </div>
                </div>
            </div>
        </div>

        <div id="tab-advanced" class="tab-content">
            <div class="info-grid">
                <div class="panel info-card">
                    <div class="panel-body">
                        <h3>📡 Receiver</h3>
                        <div class="info-row"><span class="info-label">Network</span><span class="info-value" id="adv-network">-</span></div>
                        <div class="info-row"><span class="info-label">Frequency</span><span class="info-value" id="adv-frequency">-</span></div>
                        <div class="info-row"><span class="info-label">Sample rate</span><span class="info-value" id="adv-samplerate">-</span></div>
                        <div class="info-row"><span class="info-label">Decoder</span><span class="info-value" id="adv-decoder">-</span></div>
                    </div>
                </div>

                <div class="panel info-card">
                    <div class="panel-body">
                        <h3>🔌 Device</h3>
                        <div class="info-row"><span class="info-label">Type</span><span class="info-value" id="adv-device">-</span></div>
                        <div class="info-row"><span class="info-label">Driver</span><span class="info-value" id="adv-driver">-</span></div>
                        <div class="info-row"><span class="info-label">Status</span><span class="info-value" id="adv-status">-</span></div>
                        <div class="info-row"><span class="info-label">Geocoding</span><span class="info-value" id="adv-geocoding">-</span></div>
                        <div class="info-row"><span class="info-label">OpenCage quota</span><span class="info-value" id="adv-geocode-quota">-</span></div>
                    </div>
                </div>

                <div class="panel info-card">
                    <div class="panel-body">
                        <h3>🗄️ Database</h3>
                        <div class="info-row"><span class="info-label">Places</span><span class="info-value" id="adv-places">0</span></div>
                        <div class="info-row"><span class="info-label">Streets</span><span class="info-value" id="adv-streets">0</span></div>
                        <div class="info-row"><span class="info-label">Capcodes</span><span class="info-value" id="adv-capcodes">0</span></div>
                        <div class="info-row"><span class="info-label">Texts</span><span class="info-value" id="adv-texts">0</span></div>
                        <div class="info-row"><span class="info-label">Geocodes</span><span class="info-value" id="adv-geocodes">0</span></div>
                    </div>
                </div>
            </div>

            <div class="info-grid" style="margin-top: 14px;">
                <div class="panel info-card">
                    <div class="panel-body">
                        <h3>📶 Reception</h3>
                        <div class="info-row"><span class="info-label">Active receiver</span><span class="info-value" id="rx-source">-</span></div>
                        <div class="info-row"><span class="info-label">Last FLEX frame</span><span class="info-value" id="rx-last">-</span></div>
                        <div class="info-row"><span class="info-label">Frames last minute</span><span class="info-value" id="rx-frames-min">-</span></div>
                        <div class="info-row"><span class="info-label">Messages last hour</span><span class="info-value" id="rx-msgs-hour">-</span></div>
                        <div class="info-row"><span class="info-label">Corrected bit errors per frame</span><span class="info-value" id="rx-corrected">-</span></div>
                        <div class="info-row"><span class="info-label">Failed decodes last hour</span><span class="info-value" id="rx-failed">-</span></div>
                        <div class="info-row"><span class="info-label">Sync lost last hour</span><span class="info-value" id="rx-sync">-</span></div>
                        <div class="chart-title" style="margin-top: 14px;">FLEX frames per minute, last hour</div>
                        <div id="frames-chart"></div>
                        <div class="panel-sub" style="margin-top: 8px;">Fewer corrected bit errors and failed decodes mean a cleaner signal - use them when tuning gain and ppm. rtl_fm and rx_fm don't report an RF signal level.</div>
                    </div>
                </div>

                <div class="panel info-card">
                    <div class="panel-body">
                        <h3>🎯 ppm calibration</h3>
                        <div class="panel-sub" style="margin-bottom: 10px;">Tries ppm values around the current one on the active receiver and keeps the one that decodes the most FLEX frames. Decoding continues while it runs.</div>
                        <div style="display: flex; gap: 10px; flex-wrap: wrap; align-items: flex-end;">
                            <div class="form-field"><label>± range (ppm)</label><input id="cal-span" class="input" type="number" value="30" min="1" max="100" style="width: 90px;"></div>
                            <div class="form-field"><label>Step</label><input id="cal-step" class="input" type="number" value="5" min="1" max="50" style="width: 70px;"></div>
                            <div class="form-field"><label>Seconds each</label><input id="cal-dwell" class="input" type="number" value="30" min="10" max="600" style="width: 90px;"></div>
                            <button id="cal-start" class="btn btn-primary" onclick="startCalibration()">Start</button>
                            <button id="cal-cancel" class="btn btn-danger" onclick="cancelCalibration()" style="display: none;">Cancel</button>
                        </div>
                        <div id="cal-status" class="panel-sub" style="margin-top: 12px;"></div>
                        <div id="cal-results" style="margin-top: 8px;"></div>
                    </div>
                </div>
            </div>

            <div class="panel info-card stats-panel">
                <div class="panel-body">
                    <h3>📊 Statistics</h3>
                    <div class="info-row"><span class="info-label">Total messages</span><span class="info-value" id="adv-total">0</span></div>
                    <div class="info-row"><span class="info-label">Session uptime</span><span class="info-value" id="adv-uptime">-</span></div>
                    <div class="stats-charts-grid">
                        <div>
                            <div class="chart-title">By region
                                <button id="charts-pause-toggle" class="icon-btn" style="width:24px;height:24px;font-size:12px;margin-left:auto;" onclick="toggleChartsPause()" title="Pause chart updates to investigate without refreshes">
                                    <span id="charts-pause-icon">⏸️</span>
                                </button>
                            </div>
                            <div id="region-chart"></div>
                        </div>
                        <div>
                            <div class="chart-title">By discipline</div>
                            <div id="discipline-chart"></div>
                        </div>
                    </div>
                </div>
            </div>
        </div>

        <div id="tab-database" class="tab-content">
            <div class="panel">
                <div class="panel-body">
                    <!-- Landing: guided setup flow -->
                    <div id="db-landing">
                        <div class="db-toolbar">
                            <span class="db-table-title">Database setup</span>
                            <button class="btn btn-danger" onclick="dbResetDatabase()">🗑️ Reset database</button>
                        </div>
                        <div class="panel-sub" style="margin-bottom: 14px;">Load the reference data in order - each step improves message parsing, badges and map pins.</div>

                        <div class="db-cards">
                            <div class="db-card">
                                <div class="db-card-head">
                                    <span class="db-step">1</span>
                                    <span class="db-card-icon">📟</span>
                                    <span class="db-card-title">Capcodes</span>
                                    <span class="db-card-status empty" id="db-status-capcodes">-</span>
                                </div>
                                <div class="db-card-desc">Pager addresses with discipline, region and receiver info. Powers the badges and receiver details on every message.</div>
                                <div class="db-card-count"><span id="db-count-capcodes">0</span> rows</div>
                                <div class="db-card-actions">
                                    <button class="btn btn-success" onclick="dbImportBommel(this)">Import Bommel</button>
                                    <button class="btn" onclick="dbOpenTable('capcodes')">Manage</button>
                                </div>
                            </div>

                            <div class="db-card">
                                <div class="db-card-head">
                                    <span class="db-step">2</span>
                                    <span class="db-card-icon">🏙️</span>
                                    <span class="db-card-title">Places</span>
                                    <span class="db-card-status empty" id="db-status-places">-</span>
                                </div>
                                <div class="db-card-desc">All ~2600 Dutch places with coordinates. Enables city detection in messages and city-level map pins.</div>
                                <div class="db-card-count"><span id="db-count-places">0</span> rows</div>
                                <div class="db-card-actions">
                                    <button class="btn btn-success" onclick="importAllPlaces(this)">Import All Places</button>
                                    <button class="btn" onclick="dbOpenTable('places')">Manage</button>
                                </div>
                            </div>

                            <div class="db-card">
                                <div class="db-card-head">
                                    <span class="db-step">3</span>
                                    <span class="db-card-icon">🛤️</span>
                                    <span class="db-card-title">Streets</span>
                                    <span class="db-card-status empty" id="db-status-streets">-</span>
                                </div>
                                <div class="db-card-desc">Official BAG street names per province. Enables full street-address extraction for precise geocoding.</div>
                                <div class="db-card-count"><span id="db-count-streets">0</span> rows</div>
                                <div class="db-card-actions">
                                    <button class="btn btn-success" onclick="dbImportBAG(this)">Import BAG</button>
                                    <button class="btn" onclick="dbOpenTable('streets')">Manage</button>
                                </div>
                            </div>

                        </div>

                        <div class="panel-sub" style="margin: 18px 0 10px;">No import needed - built-in, auto-filled, or managed directly from the dashboard</div>
                        <div class="db-cards">
                            <div class="db-card">
                                <div class="db-card-head">
                                    <span class="db-card-icon">📖</span>
                                    <span class="db-card-title">Texts</span>
                                    <span class="db-card-status ready" id="db-status-texts">-</span>
                                </div>
                                <div class="db-card-desc">300+ P2000 abbreviations with their full meanings, shown as tags in message details. Seeded automatically on startup - your own edits are never overwritten by that.</div>
                                <div class="db-card-count"><span id="db-count-texts">0</span> rows</div>
                                <div class="db-card-actions">
                                    <button class="btn" onclick="dbOpenTable('texts')">Manage</button>
                                </div>
                            </div>

                            <div class="db-card">
                                <div class="db-card-head">
                                    <span class="db-card-icon">📨</span>
                                    <span class="db-card-title">Messages</span>
                                    <span class="db-card-status ready" id="db-status-messages">-</span>
                                </div>
                                <div class="db-card-desc">Decoded P2000 messages with parsed location and receiver info. Filled automatically by the decoder.</div>
                                <div class="db-card-count"><span id="db-count-messages">0</span> rows</div>
                                <div class="db-card-actions">
                                    <button class="btn" onclick="dbOpenTable('messages')">Manage</button>
                                </div>
                            </div>

                            <div class="db-card">
                                <div class="db-card-head">
                                    <span class="db-card-icon">📍</span>
                                    <span class="db-card-title">Geocodes</span>
                                    <span class="db-card-status ready" id="db-status-geocodes">-</span>
                                </div>
                                <div class="db-card-desc">OpenCage address lookup cache. Fills automatically when geocoding is enabled; saves API quota.</div>
                                <div class="db-card-count"><span id="db-count-geocodes">0</span> rows</div>
                                <div class="db-card-actions">
                                    <button class="btn" onclick="dbOpenTable('geocodes')">Manage</button>
                                </div>
                            </div>

                            <div class="db-card">
                                <div class="db-card-head">
                                    <span class="db-card-icon">🗣️</span>
                                    <span class="db-card-title">TTS Replacements</span>
                                    <span class="db-card-status ready" id="db-status-tts">-</span>
                                </div>
                                <div class="db-card-desc">Regex rules that make messages speakable, used by browser speech and Home Assistant sensors.</div>
                                <div class="db-card-count"><span id="db-count-tts">0</span> rows</div>
                                <div class="db-card-actions">
                                    <button class="btn" onclick="dbOpenTable('tts')">Manage</button>
                                </div>
                            </div>

                            <div class="db-card">
                                <div class="db-card-head">
                                    <span class="db-card-icon">🙈</span>
                                    <span class="db-card-title">Ignore Text</span>
                                    <span class="db-card-status ready" id="db-status-ignore_text">-</span>
                                </div>
                                <div class="db-card-desc">Wildcard patterns (e.g. *TESTOPROEP*) - messages matching any of these are dropped entirely.</div>
                                <div class="db-card-count"><span id="db-count-ignore_text">0</span> rows</div>
                                <div class="db-card-actions">
                                    <button class="btn" onclick="dbOpenTable('ignore_text')">Manage</button>
                                </div>
                            </div>

                            <div class="db-card">
                                <div class="db-card-head">
                                    <span class="db-card-icon">🚫</span>
                                    <span class="db-card-title">Ignore Capcodes</span>
                                    <span class="db-card-status ready" id="db-status-ignore_capcodes">-</span>
                                </div>
                                <div class="db-card-desc">Capcodes to drop. A message is only dropped when every one of its capcodes is on this list.</div>
                                <div class="db-card-count"><span id="db-count-ignore_capcodes">0</span> rows</div>
                                <div class="db-card-actions">
                                    <button class="btn" onclick="dbOpenTable('ignore_capcodes')">Manage</button>
                                </div>
                            </div>

                            <div class="db-card">
                                <div class="db-card-head">
                                    <span class="db-card-icon">📷</span>
                                    <span class="db-card-title">Vehicle Photos</span>
                                    <span class="db-card-status empty" id="db-status-vehicle_photos">-</span>
                                </div>
                                <div class="db-card-desc">Photos you've uploaded per voertuignummer (e.g. "07-1782", "18-187") or vehicle category (e.g. "ambulance", "politie"), shown on matching messages.</div>
                                <div class="db-card-count"><span id="db-count-vehicle_photos">0</span> rows</div>
                                <div class="db-card-actions">
                                    <button class="btn" onclick="dbOpenTable('vehicle_photos')">Manage</button>
                                </div>
                            </div>
                        </div>
                    </div>
                    <!-- Table view -->
                    <div id="db-table-view" style="display: none;">
                        <div class="db-toolbar">
                            <div class="db-toolbar-left">
                                <button class="btn" onclick="dbBackToLanding()">← Back</button>
                                <span id="db-table-title" class="db-table-title">Table</span>
                            </div>
                            <div class="db-toolbar-right">
                                <input type="text" id="db-search" class="input" placeholder="Search…" onkeyup="dbSearch()">
                                <div id="db-buttons" style="display: flex; gap: 6px; flex-wrap: wrap;"></div>
                            </div>
                        </div>

                        <div class="db-table-wrap">
                            <table class="db-table">
                                <thead id="db-thead"></thead>
                                <tbody id="db-tbody"></tbody>
                            </table>
                        </div>

                        <div class="db-pagination">
                            <span id="db-count">0 items</span>
                            <div style="display: flex; align-items: center; gap: 10px;">
                                <button id="db-prev" class="btn" onclick="dbPrev()">← Prev</button>
                                <span id="db-page">Page 1</span>
                                <button id="db-next" class="btn" onclick="dbNext()">Next →</button>
                            </div>
                        </div>
                    </div>
                </div>
            </div>
        </div>
    </div>

    <div id="modal-root"></div>
    <datalist id="capcode-suggestions"></datalist>
    <div id="toast-root"></div>
    <div style="position: fixed; left: 12px; bottom: 8px; font-size: 10px; color: var(--secondary-text-color); opacity: 0.6; z-index: 100;">v__ADDON_VERSION__</div>

    <script>
        const basePath = window.location.pathname.endsWith('/') ? window.location.pathname : window.location.pathname + '/';

        // =====================================================================
        // Map
        // =====================================================================
        const savedMapHeight = localStorage.getItem('p2000_map_height');
        if (savedMapHeight) {
            document.getElementById('map').style.height = savedMapHeight + 'px';
        }

        const map = L.map('map').setView([52.2, 5.3], 8);
        // Tiles come through the add-on (and Home Assistant's built-in map tile
        // proxy); OSM blocks browsers requesting them directly from here.
        L.tileLayer(basePath + 'api/map-tiles/{z}/{x}/{y}.png', {
            attribution: '© <a href="https://www.openstreetmap.org/copyright" target="_blank">OpenStreetMap</a> contributors',
            maxZoom: 19
        }).addTo(map);
        setTimeout(() => map.invalidateSize(), 100);

        (function() {
            const handle = document.getElementById('map-resize-handle');
            const mapEl = document.getElementById('map');
            let startY, startH;

            handle.addEventListener('mousedown', function(e) {
                e.preventDefault();
                startY = e.clientY;
                startH = mapEl.offsetHeight;
                handle.classList.add('active');
                document.addEventListener('mousemove', onDrag);
                document.addEventListener('mouseup', onRelease);
            });

            handle.addEventListener('touchstart', function(e) {
                startY = e.touches[0].clientY;
                startH = mapEl.offsetHeight;
                handle.classList.add('active');
                document.addEventListener('touchmove', onDragTouch);
                document.addEventListener('touchend', onRelease);
            });

            function onDrag(e) {
                const newH = Math.max(200, Math.min(window.innerHeight * 0.8, startH + e.clientY - startY));
                mapEl.style.height = newH + 'px';
                map.invalidateSize();
            }
            function onDragTouch(e) {
                const newH = Math.max(200, Math.min(window.innerHeight * 0.8, startH + e.touches[0].clientY - startY));
                mapEl.style.height = newH + 'px';
                map.invalidateSize();
            }
            function onRelease() {
                handle.classList.remove('active');
                document.removeEventListener('mousemove', onDrag);
                document.removeEventListener('mouseup', onRelease);
                document.removeEventListener('touchmove', onDragTouch);
                document.removeEventListener('touchend', onRelease);
                localStorage.setItem('p2000_map_height', mapEl.offsetHeight);
            }
        })();

        // Heatmap of past incidents from the message history; the slider
        // replays them up to the chosen moment
        let historyLayer = null;
        let historyPoints = [];

        function loadMapHistory() {
            const hours = document.getElementById('map-history-hours').value;
            const slider = document.getElementById('map-history-until');
            const label = document.getElementById('map-history-label');
            if (!hours) {
                if (historyLayer) { map.removeLayer(historyLayer); historyLayer = null; }
                slider.style.display = 'none';
                label.textContent = '';
                return;
            }
            fetch(basePath + `api/map-history?hours=${hours}`)
                .then(r => r.json())
                .then(points => {
                    historyPoints = points;
                    if (!points.length) {
                        slider.style.display = 'none';
                        label.textContent = 'No located messages';
                    } else {
                        slider.min = points[0].time;
                        slider.max = Math.max(points[points.length - 1].time, points[0].time + 1);
                        slider.value = slider.max;
                        slider.style.display = '';
                    }
                    renderMapHistory();
                })
                .catch(() => toast('Failed to load map history', 'error'));
        }

        function renderMapHistory() {
            const slider = document.getElementById('map-history-until');
            const until = Number(slider.value);
            const shown = historyPoints.filter(p => p.time <= until);
            // Urgent (P1) incidents weigh more in the heatmap
            const latlngs = shown.map(p => [p.latitude, p.longitude, p.priority === 1 ? 1 : 0.5]);
            if (historyLayer) {
                historyLayer.setLatLngs(latlngs);
            } else {
                historyLayer = L.heatLayer(latlngs, { radius: 20, blur: 15, maxZoom: 12 }).addTo(map);
            }
            if (historyPoints.length) {
                document.getElementById('map-history-label').textContent =
                    `${shown.length} until ${new Date(until * 1000).toLocaleString([], {dateStyle: 'short', timeStyle: 'short'})}`;
            }
        }

        let currentMarker = null;
        let selectedMsg = null;
        let selectedMsgKey = null;
        // Stable identity for a message: list positions shift as new ones arrive
        function msgKey(msg) {
            return msg.id !== undefined && msg.id !== null ? 'id:' + msg.id : msg.timestamp + '|' + (msg.body || '');
        }
        function highlightSelectedMessage() {
            document.querySelectorAll('.message').forEach(el => el.classList.remove('selected'));
            if (selectedMsgKey === null) return;
            const idx = currentMessages.findIndex(m => msgKey(m) === selectedMsgKey);
            if (idx < 0) return;
            const card = document.querySelector(`.message[data-msg-index="${idx}"]`);
            if (card) card.classList.add('selected');
        }
        const markers = [];

        // Home Assistant zones that sensors filter on. They are read from Home
        // Assistant after startup, so redraw only when the set changes.
        const sensorZoneLayer = L.layerGroup().addTo(map);
        let sensorZonesSig = null;
        function renderSensorZones(zones) {
            const sig = JSON.stringify(zones || []);
            if (sig === sensorZonesSig) return;
            sensorZonesSig = sig;
            sensorZoneLayer.clearLayers();
            (zones || []).forEach(z => {
                L.circle([z.center_lat, z.center_lon], {
                    radius: z.radius_km * 1000,
                    color: '#03a9f4', weight: 1.5, fillColor: '#03a9f4', fillOpacity: 0.06, dashArray: '4,4'
                }).addTo(sensorZoneLayer).bindTooltip(`${escapeHtml(z.name)} (${z.radius_km.toFixed(1)} km)`, { sticky: true });
            });
        }

        const messagesEl = document.getElementById('messages');
        const totalEl = document.getElementById('total-messages');
        const uptimeEl = document.getElementById('uptime');
        const regionsEl = document.getElementById('regions-count');
        const disciplinesEl = document.getElementById('disciplines-count');
        const decoderEl = document.getElementById('decoder-status');
        const regionChartEl = document.getElementById('region-chart');
        const disciplineChartEl = document.getElementById('discipline-chart');
        const detailContent = document.getElementById('detail-content');
        const historyList = document.getElementById('history-list');
        const streamStatusEl = document.getElementById('stream-status');
        const streamStatusText = document.getElementById('stream-status-text');

        // =====================================================================
        // Toasts & modals
        // =====================================================================
        function toast(message, type) {
            const root = document.getElementById('toast-root');
            const el = document.createElement('div');
            el.className = 'toast' + (type ? ' ' + type : '');
            el.textContent = message;
            root.appendChild(el);
            setTimeout(() => { el.style.opacity = '0'; el.style.transition = 'opacity 0.3s'; }, 3600);
            setTimeout(() => el.remove(), 4000);
        }

        function openModal(opts) {
            closeModal();
            const overlay = document.createElement('div');
            overlay.className = 'modal-overlay open';
            overlay.innerHTML = `
                <div class="modal">
                    <div class="modal-head">
                        <span class="modal-title">${escapeHtml(opts.title || '')}</span>
                        <button class="modal-close" data-close>✕</button>
                    </div>
                    <div class="modal-body">${opts.bodyHTML || ''}</div>
                    <div class="modal-foot">${opts.footHTML || ''}</div>
                </div>`;
            overlay.addEventListener('click', (e) => {
                if (e.target === overlay || e.target.hasAttribute('data-close')) closeModal();
            });
            document.getElementById('modal-root').appendChild(overlay);
            return overlay;
        }

        function closeModal() {
            const root = document.getElementById('modal-root');
            root.innerHTML = '';
        }

        function confirmDialog(opts) {
            return new Promise((resolve) => {
                const overlay = openModal({
                    title: opts.title || 'Are you sure?',
                    bodyHTML: `<div>${opts.message || ''}</div>`,
                    footHTML: `
                        <button class="btn" data-close>Cancel</button>
                        <button class="btn ${opts.danger ? 'btn-danger-solid' : 'btn-primary'}" data-confirm>${escapeHtml(opts.confirmLabel || 'Confirm')}</button>`
                });
                overlay.querySelector('[data-confirm]').addEventListener('click', () => {
                    closeModal();
                    resolve(true);
                });
                overlay.addEventListener('click', (e) => {
                    if (e.target === overlay || e.target.hasAttribute('data-close')) resolve(false);
                });
            });
        }

        // =====================================================================
        // Sound notifications
        // =====================================================================
        let soundEnabled = localStorage.getItem('p2000_sound_enabled') !== 'false';
        let audioContext = null;

        function initSoundToggle() {
            const btn = document.getElementById('sound-toggle');
            const icon = document.getElementById('sound-icon');
            btn.classList.toggle('off', !soundEnabled);
            icon.textContent = soundEnabled ? '🔔' : '🔕';
            btn.title = soundEnabled ? 'Sound notifications ON - click to disable' : 'Sound notifications OFF - click to enable';
        }

        function toggleSound() {
            soundEnabled = !soundEnabled;
            localStorage.setItem('p2000_sound_enabled', soundEnabled);
            initSoundToggle();
            if (soundEnabled) {
                unlockAudio();
                playNotificationSound();
            }
        }

        function unlockAudio() {
            if (!audioContext) {
                audioContext = new (window.AudioContext || window.webkitAudioContext)();
            }
            if (audioContext.state === 'suspended') {
                audioContext.resume();
            }
        }

        document.addEventListener('click', unlockAudio, { once: true });
        document.addEventListener('touchstart', unlockAudio, { once: true });

        function playNotificationSound() {
            if (!soundEnabled) return;
            try {
                if (!audioContext) {
                    audioContext = new (window.AudioContext || window.webkitAudioContext)();
                }
                if (audioContext.state === 'suspended') {
                    audioContext.resume();
                }
                const oscillator = audioContext.createOscillator();
                const gainNode = audioContext.createGain();
                oscillator.connect(gainNode);
                gainNode.connect(audioContext.destination);
                oscillator.frequency.setValueAtTime(880, audioContext.currentTime);
                oscillator.frequency.setValueAtTime(1100, audioContext.currentTime + 0.1);
                gainNode.gain.setValueAtTime(0.3, audioContext.currentTime);
                gainNode.gain.exponentialRampToValueAtTime(0.01, audioContext.currentTime + 0.3);
                oscillator.start(audioContext.currentTime);
                oscillator.stop(audioContext.currentTime + 0.3);
            } catch (e) {
                console.log('Audio playback failed:', e);
            }
        }

        // =====================================================================
        // Auto-map toggle
        // =====================================================================
        let autoMapEnabled = localStorage.getItem('p2000_automap_enabled') !== 'false';

        function initAutoMapToggle() {
            const btn = document.getElementById('automap-toggle');
            const icon = document.getElementById('automap-icon');
            btn.classList.toggle('off', !autoMapEnabled);
            icon.textContent = autoMapEnabled ? '📍' : '📌';
            btn.title = autoMapEnabled ? 'Auto-focus ON - new messages show on map' : 'Auto-focus OFF - click to enable';
        }

        function toggleAutoMap() {
            autoMapEnabled = !autoMapEnabled;
            localStorage.setItem('p2000_automap_enabled', autoMapEnabled);
            initAutoMapToggle();
        }

        function autoShowLatestOnMap(latest) {
            // Uses the already-filtered list passed in - re-fetching from
            // /api/messages here (as this used to) ignores the current
            // sensor filter and shows the newest message regardless of it.
            const target = (latest || []).find(m => m.latitude && m.longitude);
            if (target) {
                showMessageDetails(target);
            }
        }

        // =====================================================================
        // Browser text-to-speech
        // =====================================================================
        let ttsEnabled = localStorage.getItem('p2000_tts_enabled') === 'true';
        function initTtsToggle() {
            const btn = document.getElementById('tts-toggle');
            const icon = document.getElementById('tts-icon');
            btn.classList.toggle('off', !ttsEnabled);
            icon.textContent = ttsEnabled ? '🗣️' : '🔇';
            btn.title = ttsEnabled ? 'Speech ON - new messages are read aloud' : 'Speech OFF - click to enable';
        }

        function toggleTts() {
            ttsEnabled = !ttsEnabled;
            localStorage.setItem('p2000_tts_enabled', ttsEnabled);
            initTtsToggle();
            if (!ttsEnabled && window.speechSynthesis) {
                speechSynthesis.cancel();
            }
        }

        function speakMessage(msg) {
            if (!ttsEnabled || !window.speechSynthesis || !msg || !msg.tts) return;
            // msg.tts is the spoken form built server-side with the TTS
            // replacement rules - the same text Home Assistant speakers get
            const utterance = new SpeechSynthesisUtterance(msg.tts);
            utterance.lang = 'nl-NL';
            speechSynthesis.cancel();  // Newest message takes priority
            speechSynthesis.speak(utterance);
        }

        // Tracks the top-of-list message's own timestamp, not the global
        // message count - the global count increases for every decoded
        // message regardless of the active sensor filter, which previously
        // caused TTS/notifications to re-fire (and re-speak the same,
        // unchanged filtered top message) whenever any non-matching message
        // arrived, instead of only on a genuinely new matching one.
        let lastSeenTopTimestamp = null;

        function checkForNewMessages(latest) {
            const top = (latest || [])[0];
            const topTimestamp = top ? top.timestamp : null;
            if (lastSeenTopTimestamp === null) {
                lastSeenTopTimestamp = topTimestamp;
                return;
            }
            if (topTimestamp && topTimestamp !== lastSeenTopTimestamp) {
                playNotificationSound();
                speakMessage(top);
                const earlier = top.incident_id && currentMessages.filter(m => m.incident_id === top.incident_id);
                if (earlier && earlier.length) {
                    toast(`Incident escalated: ${top.address || top.city || ''} (${earlier.length + 1} messages)`, 'error');
                }
                if (autoMapEnabled) {
                    autoShowLatestOnMap(latest);
                }
            }
            lastSeenTopTimestamp = topTimestamp;
        }

        initSoundToggle();
        initAutoMapToggle();
        initTtsToggle();

        // =====================================================================
        // Formatting helpers
        // =====================================================================
        function formatTime(ts) { return new Date(ts).toLocaleTimeString(); }
        function formatDate(ts) { return new Date(ts).toLocaleString(); }

        // Safe for element content AND quoted attribute values
        const HTML_ESCAPES = {'&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;'};
        function escapeHtml(text) {
            if (text === null || text === undefined) return '';
            return String(text).replace(/[&<>"']/g, ch => HTML_ESCAPES[ch]);
        }

        function countUnique(obj) {
            const values = new Set();
            Object.keys(obj || {}).forEach(k => k.split(',').forEach(p => { const t = p.trim(); if (t) values.add(t); }));
            return values.size;
        }

        function switchTab(tabName) {
            document.querySelectorAll('.tab').forEach(t => t.classList.toggle('active', t.dataset.tab === tabName));
            document.querySelectorAll('.tab-content').forEach(c => c.classList.remove('active'));
            document.getElementById('tab-' + tabName).classList.add('active');
            if (tabName === 'advanced') loadReceiver();
            if (tabName === 'dashboard') {
                setTimeout(() => map.invalidateSize(), 100);
                resyncIfStale();
            }
        }

        // The live stream can silently die (dropped by a proxy, or the browser
        // tab/this in-app tab being away for a while) with no visible error -
        // reconnect + refetch on return instead of leaving it frozen until a
        // manual page reload.
        function resyncIfStale() {
            if (typeof evtSource === 'undefined' || !evtSource || evtSource.readyState === EventSource.CLOSED) {
                connectStream();
            }
            loadMessagesList();
        }

        document.addEventListener('visibilitychange', () => {
            if (!document.hidden) resyncIfStale();
        });

        // Uptime is recomputed client-side so the SSE stream can stay quiet
        // when nothing changed.
        let lastStartTime = null;
        function renderUptime() {
            if (!lastStartTime) return;
            const start = new Date(lastStartTime);
            const totalMinutes = Math.max(0, Math.floor((Date.now() - start) / 60000));
            const hours = Math.floor(totalMinutes / 60);
            const mins = totalMinutes % 60;
            const text = hours > 0 ? `${hours}h ${mins}m` : `${mins}m`;
            uptimeEl.textContent = text;
            const advUp = document.getElementById('adv-uptime');
            if (advUp) advUp.textContent = text;
        }
        setInterval(renderUptime, 30000);

        // =====================================================================
        // Reception quality & ppm calibration (Advanced tab)
        // =====================================================================
        function formatAgo(epochSeconds) {
            if (!epochSeconds) return 'never';
            const secs = Math.max(0, Math.round(Date.now() / 1000 - epochSeconds));
            if (secs < 60) return `${secs}s ago`;
            if (secs < 3600) return `${Math.floor(secs / 60)}m ago`;
            return `${Math.floor(secs / 3600)}h ${Math.floor(secs % 3600 / 60)}m ago`;
        }

        // Single-series bar chart: one bar per minute, hover shows the details
        function renderFramesChart(history) {
            const el = document.getElementById('frames-chart');
            if (!history.length) { el.innerHTML = '<div class="empty-state" style="padding: 20px;">No data yet</div>'; return; }
            const width = 600, height = 120, slot = width / 60;
            const max = Math.max(1, ...history.map(b => b.frames));
            const now = Math.floor(Date.now() / 60000) * 60;
            const bars = history.map(b => {
                const x = width - (now - b.minute) / 60 * slot;
                const h = Math.max(b.frames ? 2 : 0, b.frames / max * (height - 4));
                const tip = `${new Date(b.minute * 1000).toLocaleTimeString([], {timeStyle: 'short'})}: ${b.frames} frames, ${b.messages} messages, ${b.corrected_bits} corrected bits, ${b.failed} failed`;
                // Rounded top, square base on the axis; the full-height rect is the hover target
                const r = Math.min(2, h);
                return `<g><title>${escapeHtml(tip)}</title>` +
                    `<rect x="${x}" y="0" width="${slot}" height="${height}" fill="transparent"/>` +
                    `<path d="M${x + 1},${height} v${r - h} q0,${-r} ${r},${-r} h${slot - 2 - 2 * r} q${r},0 ${r},${r} v${h - r} z" fill="var(--primary-color)"/></g>`;
            }).join('');
            el.innerHTML = `<svg viewBox="0 0 ${width} ${height + 16}" style="width:100%;height:auto;display:block;">` +
                `<line x1="0" y1="${height}" x2="${width}" y2="${height}" stroke="var(--divider-color)"/>${bars}` +
                `<text x="0" y="${height + 13}" font-size="10" fill="var(--secondary-text-color)">-60 min</text>` +
                `<text x="${width}" y="${height + 13}" font-size="10" text-anchor="end" fill="var(--secondary-text-color)">now · max ${max}/min</text></svg>`;
        }

        function renderCalibration(rx) {
            const cal = rx.calibration;
            const status = document.getElementById('cal-status');
            const results = document.getElementById('cal-results');
            const running = !!(cal && cal.running);
            document.getElementById('cal-start').style.display = running ? 'none' : '';
            document.getElementById('cal-cancel').style.display = running ? '' : 'none';
            if (!cal) {
                status.textContent = `Current ppm correction: ${rx.ppm}`;
                results.innerHTML = '';
                return;
            }
            if (running) {
                status.textContent = `Calibrating the ${cal.source} receiver: ${cal.results.length} of ${cal.candidates.length} done, trying ${cal.current} ppm…`;
            } else if (cal.error) {
                status.textContent = cal.error;
            } else {
                status.textContent = `Best: ${cal.best} ppm (configured: ${cal.configured_ppm} ppm)`;
            }
            const maxFrames = Math.max(1, ...cal.results.map(r => r.frames));
            results.innerHTML = cal.results.length ? `
                <table class="db-table">
                    <thead><tr><th>ppm</th><th>Frames</th><th>Failed</th><th></th></tr></thead>
                    <tbody>${cal.results.map(r => `
                        <tr>
                            <td>${r.ppm}${r.ppm === cal.best ? ' ★' : ''}</td>
                            <td><div class="progress" style="width: 120px; display: inline-block; vertical-align: middle;"><div style="width: ${r.frames / maxFrames * 100}%"></div></div> ${r.frames}</td>
                            <td>${r.failed}</td>
                            <td>${!running && rx.can_apply ? `<button class="btn" onclick="applyPpm(${r.ppm})">Use</button>` : ''}</td>
                        </tr>`).join('')}
                    </tbody>
                </table>` : '';
        }

        let receiverTimer = null;
        function loadReceiver() {
            clearTimeout(receiverTimer);
            if (!document.getElementById('tab-advanced').classList.contains('active')) return;
            fetch(basePath + 'api/receiver').then(r => r.json()).then(rx => {
                const active = rx.sources.find(src => src.active);
                document.getElementById('rx-source').textContent = active
                    ? `${active.label} (${active.type}, ${rx.ppm} ppm)` + (rx.sources.length > 1 ? ' · failover on' : '')
                    : '-';
                document.getElementById('rx-last').textContent = rx.silent_for === null ? 'never' : formatAgo(Date.now() / 1000 - rx.silent_for);
                document.getElementById('rx-frames-min').textContent = rx.frames_last_minute;
                document.getElementById('rx-msgs-hour').textContent = rx.messages_last_hour;
                document.getElementById('rx-corrected').textContent = rx.corrected_bits_per_frame ?? '-';
                document.getElementById('rx-failed').textContent = rx.failed_last_hour;
                document.getElementById('rx-sync').textContent = rx.sync_lost_last_hour;
                renderFramesChart(rx.history);
                renderCalibration(rx);
            }).catch(() => {}).finally(() => { receiverTimer = setTimeout(loadReceiver, 5000); });
        }

        function startCalibration() {
            const body = {
                span: Number(document.getElementById('cal-span').value),
                step: Number(document.getElementById('cal-step').value),
                dwell: Number(document.getElementById('cal-dwell').value),
            };
            fetch(basePath + 'api/calibration', { method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify(body) })
                .then(r => r.json())
                .then(result => {
                    if (result.error) toast(result.error, 'error');
                    loadReceiver();
                })
                .catch(() => toast('Failed to start calibration', 'error'));
        }

        function cancelCalibration() {
            fetch(basePath + 'api/calibration', { method: 'DELETE' }).then(() => loadReceiver());
        }

        async function applyPpm(ppm) {
            const ok = await confirmDialog({
                title: 'Use this ppm correction?',
                message: `Saves ${ppm} ppm in the add-on configuration and restarts the receiver with it.`,
                confirmLabel: 'Save',
            });
            if (!ok) return;
            fetch(basePath + 'api/calibration/apply', { method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify({ppm}) })
                .then(r => r.json())
                .then(result => {
                    toast(result.error || `ppm correction set to ${ppm}`, result.error ? 'error' : 'success');
                    loadReceiver();
                })
                .catch(() => toast('Failed to save ppm correction', 'error'));
        }

        // =====================================================================
        // Dashboard rendering
        // =====================================================================
        function updateAdvanced(stats) {
            const el = (id) => document.getElementById(id);
            if (el('adv-network')) el('adv-network').textContent = stats.network_name || '-';
            if (el('adv-frequency')) el('adv-frequency').textContent = stats.frequency || '-';
            if (el('adv-samplerate')) el('adv-samplerate').textContent = stats.sample_rate ? stats.sample_rate + ' Hz' : '-';
            if (el('adv-decoder')) el('adv-decoder').textContent = stats.decoder || '-';
            if (el('adv-device')) el('adv-device').textContent = stats.device_type || '-';
            if (el('adv-driver')) el('adv-driver').textContent = stats.device_driver || '-';
            if (el('adv-total')) el('adv-total').textContent = stats.total_messages;
            if (el('adv-status')) el('adv-status').textContent = stats.decoder_running ? 'Running' : (stats.decoder_error || 'Stopped');
            if (el('adv-geocoding')) {
                el('adv-geocoding').textContent = !stats.geocoding_configured
                    ? 'Disabled'
                    : (stats.geocoding_enabled ? 'Enabled' : 'Invalid API key (disabled)');
            }
            if (el('adv-geocode-quota')) {
                el('adv-geocode-quota').textContent = (stats.geocoding_configured && stats.geocoding_enabled)
                    ? (stats.geocode_rate_limited
                        ? 'Exhausted - resets at midnight UTC'
                        : (stats.geocode_rate_remaining !== undefined ? stats.geocode_rate_remaining + ' left today' : '-'))
                    : '-';
            }
            if (el('adv-geocodes')) el('adv-geocodes').textContent = stats.db_geocodes || 0;
            if (el('adv-places')) el('adv-places').textContent = stats.db_places || 0;
            if (el('adv-streets')) el('adv-streets').textContent = stats.db_streets || 0;
            if (el('adv-capcodes')) el('adv-capcodes').textContent = stats.db_capcodes || 0;
            if (el('adv-texts')) el('adv-texts').textContent = stats.db_texts || 0;
        }

        function showMessageDetails(msg, idx) {
            selectedMsg = msg;
            selectedMsgKey = (typeof idx === 'number') ? msgKey(msg) : null;
            highlightSelectedMessage();

            // Filter capcodes to only show unmatched ones
            const matchedCapcodes = new Set();
            if (msg.receivers) {
                const matches = msg.receivers.match(/\\((\\d+)\\)/g);
                if (matches) {
                    matches.forEach(m => matchedCapcodes.add(m.replace(/[()]/g, '')));
                }
            }
            const unmatchedCapcodes = (msg.capcodes || []).filter(c => !matchedCapcodes.has(c));

            const geocodeLevel = (msg.latitude && msg.longitude)
                ? (msg.location_accuracy === 'street' ? 'Street' : (msg.location_accuracy === 'city' ? 'City' : 'Unknown'))
                : '—';
            const hasDistance = msg.distance_km !== null && msg.distance_km !== undefined;

            const leftRows = [
                ['Time', formatDate(msg.timestamp)],
                ['Priority', msg.priority ? `P${msg.priority}` : '—'],
                ['Discipline', msg.discipline || '—'],
                ['Region', msg.region || '—'],
            ];

            const rightRows = [
                ['Address', msg.address || '—'],
                ['City', msg.city || '—'],
                ['Geocode level', geocodeLevel],
                hasDistance ? ['Distance from home', `${msg.distance_km} km`] : null,
                msg.repeats ? ['Repeats', `resent ${msg.repeats} times`] : null,
                unmatchedCapcodes.length > 0 ? ['Capcodes', unmatchedCapcodes.join(', ')] : null,
                ['Receivers', msg.receivers || '—'],
            ].filter(Boolean);

            const renderCol = (rows) => rows.map(([label, value]) => `
                <div class="detail-row">
                    <span class="detail-label">${label}</span>
                    <span class="detail-value">${escapeHtml(value)}</span>
                </div>
            `).join('');

            const abbrevTags = (msg.abbreviations || []).map(a =>
                `<span class="abbrev-tag"><strong>${escapeHtml(a.abbreviation)}</strong>: ${escapeHtml(a.full_text)}</span>`
            ).join('');

            const rawBlock = msg.raw_message ? `
                <details class="raw-block">
                    <summary>Raw message</summary>
                    <pre>${escapeHtml(msg.raw_message)}</pre>
                </details>` : '';

            const vehicleBlock = msg.vehicle_category ? `
                <div class="vehicle-side">
                    <div class="vehicle-visual" id="vehicle-visual" onclick="openVehiclePhotoUpload()">
                        <span id="vehicle-icon-fallback">${escapeHtml(msg.vehicle_icon || '📻')}</span>
                    </div>
                    <div class="vehicle-category-label">${escapeHtml(prettyVehicleCategory(msg.vehicle_category))}</div>
                    ${msg.vehicle_number ? `<div class="vehicle-number-label">#${escapeHtml(msg.vehicle_number)}</div>` : ''}
                    <button class="btn" onclick="openVehiclePhotoUpload()">📷 Add/change photo</button>
                </div>` : '';

            detailContent.innerHTML = `
                <div class="detail-grid">
                    <div class="detail-body">${escapeHtml(msg.body || 'No message')}</div>
                    ${abbrevTags ? `<div style="grid-column: 1 / -1; margin-bottom: 4px;">${abbrevTags}</div>` : ''}
                    <div style="grid-column: 1;">${renderCol(leftRows)}</div>
                    <div style="grid-column: 2;">${renderCol(rightRows)}</div>
                    ${rawBlock}
                </div>
                ${vehicleBlock}
            `;

            if (msg.vehicle_category) loadVehiclePhoto(msg);

            document.getElementById('history-section').style.display = 'block';

            if (msg.latitude && msg.longitude) {
                try {
                    const lat = parseFloat(msg.latitude);
                    const lng = parseFloat(msg.longitude);
                    if (!isNaN(lat) && !isNaN(lng)) {
                        if (currentMarker) map.removeLayer(currentMarker);
                        currentMarker = L.marker([lat, lng]).addTo(map);
                        currentMarker.bindPopup(`<b>${escapeHtml(msg.discipline || 'P2000')}</b><br><small>${escapeHtml(msg.address || '')}</small><br><div style="margin-top:6px;max-width:250px;word-wrap:break-word;">${escapeHtml(msg.body || 'No message')}</div>`).openPopup();
                        map.setView([lat, lng], 13);
                    }
                } catch (e) {}
            }

            renderIncident(msg);

            // Location history (exact address preferred, city as fallback)
            const renderHistory = (items, extraFilter) => {
                historyList.innerHTML = items
                    .filter(h => h.timestamp !== msg.timestamp && (!extraFilter || extraFilter(h)))
                    .slice(0, 10)
                    .map(h => `
                        <div class="history-item">
                            <div class="time">${formatDate(h.timestamp)}</div>
                            <div>${escapeHtml((h.body || '-').substring(0, 80))}…</div>
                        </div>
                    `).join('') || '<div class="empty-state" style="padding: 12px;">No history for this location</div>';
            };
            if (msg.address) {
                fetch(basePath + `api/history/${encodeURIComponent(msg.address)}`)
                    .then(r => r.json())
                    .then(history => renderHistory(history, h => h.address === msg.address))
                    .catch(() => {});
            } else if (msg.city) {
                fetch(basePath + `api/history/${encodeURIComponent(msg.city)}`)
                    .then(r => r.json())
                    .then(history => renderHistory(history))
                    .catch(() => {});
            } else {
                historyList.innerHTML = '<div class="empty-state" style="padding: 12px;">No location data</div>';
            }
        }

        function renderIncident(msg) {
            const section = document.getElementById('incident-section');
            const list = document.getElementById('incident-list');
            section.style.display = 'none';
            if (!msg.incident_id) return;
            fetch(basePath + `api/incident/${encodeURIComponent(msg.incident_id)}`)
                .then(r => r.json())
                .then(items => {
                    if (selectedMsg !== msg || items.length < 2) return;
                    list.innerHTML = items.map(h => `
                        <div class="history-item">
                            <div class="time">${formatTime(h.timestamp)} · ${escapeHtml(h.discipline || 'Unknown')}${h.priority ? ' · P' + h.priority : ''}</div>
                            <div>${escapeHtml(h.body || '-')}</div>
                        </div>
                    `).join('');
                    section.style.display = 'block';
                })
                .catch(() => {});
        }

        // ----- Vehicle photos (user-uploaded, keyed by voertuignummer or category) -----
        function prettyVehicleCategory(category) {
            return (category || '').split('_').map(w => w.charAt(0).toUpperCase() + w.slice(1)).join(' ');
        }

        function vehiclePhotoUrl(keyType, keyValue) {
            return `${basePath}api/vehicle-photo/${keyType}/${encodeURIComponent(keyValue)}`;
        }

        // Tries the exact voertuignummer photo first, falls back to the category
        // photo, and finally leaves the emoji fallback showing.
        function loadVehiclePhoto(msg) {
            const visual = document.getElementById('vehicle-visual');
            const fallback = document.getElementById('vehicle-icon-fallback');
            if (!visual || !fallback) return;

            const tryUrls = [];
            if (msg.vehicle_number) tryUrls.push(['number', msg.vehicle_number]);
            if (msg.vehicle_category) tryUrls.push(['category', msg.vehicle_category]);

            const attempt = (i) => {
                if (i >= tryUrls.length) return; // nothing found, keep the emoji fallback
                const [keyType, keyValue] = tryUrls[i];
                const img = new Image();
                img.onload = () => {
                    fallback.style.display = 'none';
                    img.alt = prettyVehicleCategory(msg.vehicle_category);
                    visual.appendChild(img);
                };
                img.onerror = () => attempt(i + 1);
                img.src = vehiclePhotoUrl(keyType, keyValue);
            };
            attempt(0);
        }

        function openVehiclePhotoUpload() {
            const msg = selectedMsg;
            if (!msg || !msg.vehicle_category) return;

            const options = [];
            if (msg.vehicle_number) {
                options.push(`<label class="form-check"><input type="radio" name="vp-key" value="number" checked> This vehicle only (#${escapeHtml(msg.vehicle_number)})</label>`);
            }
            options.push(`<label class="form-check"><input type="radio" name="vp-key" value="category" ${msg.vehicle_number ? '' : 'checked'}> All "${escapeHtml(prettyVehicleCategory(msg.vehicle_category))}" vehicles (fallback)</label>`);

            const overlay = openModal({
                title: 'Upload vehicle photo',
                bodyHTML: `
                    <div class="form-field">
                        <label>Photo</label>
                        <input type="file" accept="image/*" class="input" data-vp-file>
                    </div>
                    <div class="form-field">${options.join('')}</div>
                    <div class="form-field">
                        <label>Label (optional)</label>
                        <input type="text" class="input" data-vp-label placeholder="e.g. 17106 - Rotterdam-Rijnmond">
                    </div>
                `,
                footHTML: `<button class="btn" data-close>Cancel</button><button class="btn btn-success" data-save>Upload</button>`
            });

            overlay.querySelector('[data-save]').addEventListener('click', () => {
                const fileInput = overlay.querySelector('[data-vp-file]');
                const file = fileInput.files[0];
                if (!file) { toast('Choose an image file', 'error'); return; }
                const keyType = overlay.querySelector('input[name="vp-key"]:checked').value;
                const keyValue = keyType === 'number' ? msg.vehicle_number : msg.vehicle_category;
                const label = overlay.querySelector('[data-vp-label]').value.trim();

                const form = new FormData();
                form.append('key_type', keyType);
                form.append('key_value', keyValue);
                form.append('label', label);
                form.append('file', file);

                fetch(`${basePath}api/vehicle-photo`, { method: 'POST', body: form })
                    .then(r => r.json())
                    .then(result => {
                        if (result.error || result.success === false) {
                            toast(result.error || 'Upload failed', 'error');
                        } else {
                            toast('Photo saved', 'success');
                            closeModal();
                            loadVehiclePhoto(msg);
                        }
                    })
                    .catch(() => toast('Upload failed', 'error'));
            });
        }

        // Standalone upload from the Database tab's Vehicle Photos table (no selected message).
        function dbOpenVehiclePhotoUpload() {
            fetch(`${basePath}api/vehicle-categories`)
                .then(r => r.json())
                .then(data => dbShowVehiclePhotoUploadModal(data.categories || []))
                .catch(() => dbShowVehiclePhotoUploadModal([]));
        }

        function dbShowVehiclePhotoUploadModal(categories) {
            const categoryOptions = categories.map(c =>
                `<option value="${escapeHtml(c.category)}">${c.icon} ${escapeHtml(prettyVehicleCategory(c.category))}</option>`
            ).join('');

            const overlay = openModal({
                title: 'Upload vehicle photo',
                bodyHTML: `
                    <div class="form-field">
                        <label>Photo</label>
                        <input type="file" accept="image/*" class="input" data-vp-file>
                    </div>
                    <div class="form-field">
                        <label class="form-check"><input type="radio" name="db-vp-key" value="category" checked> Vehicle category (fallback photo)</label>
                        <label class="form-check"><input type="radio" name="db-vp-key" value="number"> Specific vehicle number (voertuignummer)</label>
                    </div>
                    <div class="form-field" data-db-vp-category-field>
                        <label>Category</label>
                        <select class="input" data-db-vp-category>${categoryOptions}</select>
                    </div>
                    <div class="form-field" data-db-vp-number-field style="display:none;">
                        <label>Voertuignummer</label>
                        <input type="text" class="input" data-db-vp-number placeholder="e.g. 07-1782 or 18-187">
                        <div style="font-size: 12px; color: var(--secondary-text-color); margin-top: 4px;">The RR-NNN number only, without the vehicle-type prefix - e.g. "07-1782" (fire brigade) or "18-187" (ambulance region-unit). Check the vehicle's own #tag on a matching message's detail panel for the exact value to use.</div>
                    </div>
                    <div class="form-field">
                        <label>Label (optional)</label>
                        <input type="text" class="input" data-db-vp-label placeholder="e.g. 17106 - Rotterdam-Rijnmond">
                    </div>
                `,
                footHTML: `<button class="btn" data-close>Cancel</button><button class="btn btn-success" data-save>Upload</button>`
            });

            const categoryField = overlay.querySelector('[data-db-vp-category-field]');
            const numberField = overlay.querySelector('[data-db-vp-number-field]');
            overlay.querySelectorAll('input[name="db-vp-key"]').forEach(radio => {
                radio.addEventListener('change', () => {
                    const isNumber = radio.value === 'number' && radio.checked;
                    if (radio.checked) {
                        categoryField.style.display = radio.value === 'category' ? '' : 'none';
                        numberField.style.display = radio.value === 'number' ? '' : 'none';
                    }
                });
            });

            overlay.querySelector('[data-save]').addEventListener('click', () => {
                const fileInput = overlay.querySelector('[data-vp-file]');
                const file = fileInput.files[0];
                if (!file) { toast('Choose an image file', 'error'); return; }
                const keyType = overlay.querySelector('input[name="db-vp-key"]:checked').value;
                const keyValue = keyType === 'category'
                    ? overlay.querySelector('[data-db-vp-category]').value
                    : overlay.querySelector('[data-db-vp-number]').value.trim();
                if (!keyValue) { toast(keyType === 'category' ? 'Choose a category' : 'Enter a voertuignummer', 'error'); return; }
                const label = overlay.querySelector('[data-db-vp-label]').value.trim();

                const form = new FormData();
                form.append('key_type', keyType);
                form.append('key_value', keyValue);
                form.append('label', label);
                form.append('file', file);

                fetch(`${basePath}api/vehicle-photo`, { method: 'POST', body: form })
                    .then(r => r.json())
                    .then(result => {
                        if (result.error || result.success === false) {
                            toast(result.error || 'Upload failed', 'error');
                        } else {
                            toast('Photo saved', 'success');
                            closeModal();
                            dbLoadData();
                            dbLoadStats();
                        }
                    })
                    .catch(() => toast('Upload failed', 'error'));
            });
        }

        let currentMessages = [];

        // Messages sharing an incident id (same address, close in time) are
        // shown as one card, newest message on top. Returns groups of
        // {idx, msgs} where idx is the newest message's index in `messages`.
        function groupByIncident(messages) {
            const groups = [];
            const byIncident = {};
            messages.forEach((msg, idx) => {
                const group = msg.incident_id && byIncident[msg.incident_id];
                if (group) {
                    group.msgs.push(msg);
                } else {
                    const created = {idx, msgs: [msg]};
                    groups.push(created);
                    if (msg.incident_id) byIncident[msg.incident_id] = created;
                }
            });
            return groups;
        }

        function renderMessage(group) {
            const msg = group.msgs[0];
            const idx = group.idx;
            const prio = Math.min(...group.msgs.map(m => m.priority || 9));
            const disciplines = [...new Set(group.msgs.flatMap(m => (m.discipline || '').split(',').map(d => d.trim()).filter(Boolean)))];
            return `
                <div class="message ${prio < 9 ? 'prio-' + prio : ''}" data-msg-index="${idx}" onclick="handleMessageClick(${idx})">
                    <div class="message-header">
                        <div class="message-badges">
                            ${disciplines.map(d => `<span class="badge discipline">${escapeHtml(d)}</span>`).join('')}
                            ${msg.region ? `<span class="badge">${escapeHtml(msg.region)}</span>` : ''}
                            ${prio < 9 ? `<span class="badge prio">P${prio}</span>` : ''}
                            ${group.msgs.length > 1 ? `<span class="badge incident" title="${group.msgs.length} messages for this incident">🚨 ${group.msgs.length}</span>` : ''}
                            ${msg.repeats ? `<span class="badge" title="Resent ${msg.repeats} times by the pager network">🔁 ${msg.repeats}</span>` : ''}
                        </div>
                        <span class="message-time">${formatTime(msg.timestamp)}</span>
                    </div>
                    <div class="message-body">${escapeHtml((msg.body || 'No message').substring(0, 100))}${(msg.body || '').length > 100 ? '…' : ''}</div>
                    ${msg.address ? `<div class="message-location">${escapeHtml(msg.address)}</div>` : ''}
                </div>
            `;
        }

        function handleMessageClick(idx) {
            if (currentMessages[idx]) {
                showMessageDetails(currentMessages[idx], idx);
            }
        }

        function renderPieChart(data, el) {
            const sorted = Object.entries(data || {}).sort((a, b) => b[1] - a[1]).slice(0, 6);
            const total = sorted.reduce((sum, [, v]) => sum + v, 0);
            if (total === 0) { el.innerHTML = '<div class="empty-state" style="padding: 20px;">No data</div>'; return; }

            const colors = ['#03a9f4', '#ff9800', '#4caf50', '#e91e63', '#9c27b0', '#00bcd4'];
            let cumulativePercent = 0;
            let paths = '';
            let legend = '';

            sorted.forEach(([label, value], i) => {
                const percent = value / total;
                const startX = Math.cos(2 * Math.PI * cumulativePercent);
                const startY = Math.sin(2 * Math.PI * cumulativePercent);
                cumulativePercent += percent;
                const endX = Math.cos(2 * Math.PI * cumulativePercent);
                const endY = Math.sin(2 * Math.PI * cumulativePercent);
                const largeArc = percent > 0.5 ? 1 : 0;
                paths += `<path d="M 0 0 L ${startX} ${startY} A 1 1 0 ${largeArc} 1 ${endX} ${endY} Z" fill="${colors[i % colors.length]}" opacity="0.85"/>`;
                const pct = Math.round(percent * 100);
                legend += `<div class="chart-legend-item" title="${escapeHtml(label)}: ${value} (${pct}%)">` +
                    `<span class="swatch" style="background:${colors[i % colors.length]}"></span>` +
                    `<span class="label">${escapeHtml(label)}</span>` +
                    `<span class="count">${value}</span>` +
                    `</div>`;
            });

            el.innerHTML = `<div class="chart-row"><svg viewBox="-1.1 -1.1 2.2 2.2" style="width:110px;height:110px;flex-shrink:0;transform:rotate(-90deg)">${paths}</svg><div class="chart-legend">${legend}</div></div>`;
        }

        function updateMarkers(messages) {
            markers.forEach(m => map.removeLayer(m));
            markers.length = 0;
            messages.filter(m => m.latitude && m.longitude).slice(0, 20).forEach(msg => {
                const m = L.circleMarker([msg.latitude, msg.longitude], {
                    radius: 6, fillColor: msg.priority === 1 ? '#ff5252' : '#00d4ff',
                    color: '#fff', weight: 1, fillOpacity: 0.8
                }).addTo(map);
                m.bindPopup(`<b>${escapeHtml(msg.discipline || 'P2000')}</b><br><div style="max-width:200px;word-wrap:break-word;">${escapeHtml(msg.body || 'No message')}</div>`);
                markers.push(m);
            });
        }

        // Signatures let us skip DOM work when the stream delivers unchanged
        // data - this keeps scroll position and selection intact.
        let lastMsgsSig = null;
        let lastChartSig = null;

        // Freezes the region/discipline charts so incoming messages don't
        // redraw them mid-investigation; resuming immediately catches up.
        let chartsPaused = false;

        function toggleChartsPause() {
            chartsPaused = !chartsPaused;
            const btn = document.getElementById('charts-pause-toggle');
            const icon = document.getElementById('charts-pause-icon');
            btn.classList.toggle('paused', chartsPaused);
            icon.textContent = chartsPaused ? '▶️' : '⏸️';
            btn.title = chartsPaused
                ? 'Charts paused - click to resume live updates'
                : 'Pause chart updates to investigate without refreshes';
        }

        function renderMessagesList(latest) {
            const sig = latest.length + '|' + latest.map(m => m.timestamp + (m.repeats || 0)).join('|');
            if (sig === lastMsgsSig) return;
            lastMsgsSig = sig;

            currentMessages = latest;
            if (latest.length > 0) {
                messagesEl.innerHTML = groupByIncident(latest).map(renderMessage).join('');
                highlightSelectedMessage();
            } else {
                selectedMsgKey = null;
                messagesEl.innerHTML = '<div class="empty-state">No messages yet</div>';
            }
            updateMarkers(latest);
        }

        function update(data) {
            const stats = data.stats;

            renderSensorZones(stats.sensor_zones);
            checkForNewMessages(data.latest || []);

            totalEl.textContent = stats.total_messages;
            regionsEl.textContent = countUnique(stats.by_region);
            disciplinesEl.textContent = countUnique(stats.by_discipline);

            const msgCountEl = document.getElementById('msg-count');
            if (msgCountEl && data.latest) {
                msgCountEl.textContent = `${data.latest.length} shown`;
            }

            const networkEl = document.getElementById('network-name');
            if (networkEl && stats.network_name) {
                networkEl.textContent = stats.network_name;
            }
            const receiverEl = document.getElementById('receiver-type');
            if (receiverEl) {
                receiverEl.textContent = stats.device_type || '';
            }

            if (stats.decoder_running && !stats.receiver_online) {
                decoderEl.className = 'pill pill-warning';
                decoderEl.textContent = 'No FLEX frames';
                decoderEl.title = 'The decoder runs but has not decoded any FLEX frames recently - check the antenna, gain and ppm';
            } else if (stats.decoder_running) {
                decoderEl.className = 'pill pill-success';
                decoderEl.textContent = 'Decoder running';
                decoderEl.title = '';
            } else if (stats.decoder_error) {
                decoderEl.className = 'pill pill-danger';
                decoderEl.textContent = 'No SDR device';
                decoderEl.title = stats.decoder_error;
            } else {
                decoderEl.className = 'pill pill-danger';
                decoderEl.textContent = 'Decoder stopped';
                decoderEl.title = '';
            }

            lastStartTime = stats.start_time;
            renderUptime();

            const chartSig = JSON.stringify(stats.by_region || {}) + '|' + JSON.stringify(stats.by_discipline || {});
            if (!chartsPaused && chartSig !== lastChartSig) {
                lastChartSig = chartSig;
                renderPieChart(stats.by_region, regionChartEl);
                renderPieChart(stats.by_discipline, disciplineChartEl);
            }

            renderMessagesList(data.latest || []);
            updateAdvanced(stats);
        }

        // =====================================================================
        // Live stream + initial load
        // =====================================================================
        let currentSensorFilter = '';
        let evtSource = null;

        function streamUrl() {
            return basePath + 'api/stream' + (currentSensorFilter ? `?sensor=${encodeURIComponent(currentSensorFilter)}` : '');
        }

        function connectStream() {
            if (evtSource) evtSource.close();
            evtSource = new EventSource(streamUrl());
            evtSource.onopen = () => {
                streamStatusEl.className = 'conn live';
                streamStatusText.textContent = 'Live';
            };
            evtSource.onmessage = (e) => update(JSON.parse(e.data));
            evtSource.onerror = () => {
                streamStatusEl.className = 'conn reconnecting';
                streamStatusText.textContent = 'Reconnecting…';
            };
        }

        function loadMessagesList() {
            const url = basePath + 'api/messages' + (currentSensorFilter ? `?sensor=${encodeURIComponent(currentSensorFilter)}` : '');
            fetch(url).then(r => r.json()).then(msgs => {
                renderMessagesList((msgs || []).slice(0, 50));
            }).catch(() => toast('Failed to load messages', 'error'));
        }

        function clearSelectedMessage() {
            // The selected message's map pin/popup and the detail panel are set
            // by clicking a message, independent of the periodic message-list/
            // marker refresh - switching filters must clear them explicitly, or
            // a message that no longer matches the new filter keeps showing.
            selectedMsgKey = null;
            if (currentMarker) { map.removeLayer(currentMarker); currentMarker = null; }
            detailContent.innerHTML = '<div class="empty-state">Click a message to view details</div>';
            document.getElementById('history-section').style.display = 'none';
            document.getElementById('incident-section').style.display = 'none';
        }

        function selectSensorFilter(name) {
            if (name === currentSensorFilter) return;
            currentSensorFilter = name;
            document.querySelectorAll('.sensor-filter-pill').forEach(el => {
                el.classList.toggle('active', el.dataset.sensor === name);
            });
            lastMsgsSig = null;  // force re-render even if the next batch looks identical in size/timestamps
            clearSelectedMessage();
            connectStream();
            loadMessagesList();
        }

        function loadSensorFilterBar() {
            fetch(basePath + 'api/sensors').then(r => r.json()).then(sensors => {
                const bar = document.getElementById('sensor-filter-bar');
                if (sensors && sensors.length > 0) {
                    document.getElementById('sensor-filter-section').style.display = 'block';
                    // Default to "only messages matching one of your configured
                    // sensors" - the whole point of defining sensors is to narrow
                    // the feed, so showing everything by default defeats that.
                    currentSensorFilter = '__any__';
                    const pills = [
                        `<button class="sensor-filter-pill active" data-sensor="__any__">Sensors</button>`,
                        `<button class="sensor-filter-pill" data-sensor="">All</button>`,
                    ].concat(sensors.map(s => `<button class="sensor-filter-pill" data-sensor="${escapeHtml(s.name)}">${escapeHtml(s.name)}</button>`));
                    bar.innerHTML = pills.join('');
                    bar.addEventListener('click', (e) => {
                        const pill = e.target.closest('.sensor-filter-pill');
                        if (pill) selectSensorFilter(pill.dataset.sensor);
                    });
                }
                connectStream();
                loadMessagesList();
            }).catch(() => {
                connectStream();
                loadMessagesList();
            });
        }

        loadSensorFilterBar();
        fetch(basePath + 'api/stats').then(r => r.json()).then(stats => update({stats, latest: []}))
            .catch(() => toast('Failed to load stats', 'error'));

        // =====================================================================
        // Database tab
        // =====================================================================
        let dbCurrentTable = null;
        let dbCurrentPage = 1;
        let dbTotalPages = 1;
        let dbSearchTimeout = null;
        let dbCurrentItems = [];

        const dbColumns = {
            capcodes: ['capcode', 'discipline', 'region', 'location', 'description'],
            places: ['city', 'abbreviation', 'province'],
            streets: ['street', 'city_name', 'postalcode'],
            messages: ['timestamp', 'discipline', 'city', 'street', 'body'],
            texts: ['abbreviation', 'full_text'],
            geocodes: ['query', 'city', 'street', 'latitude', 'longitude'],
            tts: ['pattern', 'replacement', 'enabled'],
            ignore_text: ['pattern', 'enabled'],
            ignore_capcodes: ['capcode', 'enabled'],
            vehicle_photos: ['thumbnail', 'key_type', 'key_value', 'label']
        };

        const dbTitles = {
            capcodes: '📟 Capcodes',
            places: '🏙️ Places',
            streets: '🛤️ Streets',
            messages: '📨 Messages',
            texts: '📖 Texts',
            geocodes: '📍 Geocodes',
            tts: '🗣️ TTS Replacements',
            ignore_text: '🙈 Ignore Text',
            ignore_capcodes: '🚫 Ignore Capcodes',
            vehicle_photos: '📷 Vehicle Photos'
        };

        const dbTableButtons = {
            capcodes: [
                {label: '+ Add', cls: 'btn-success', fn: 'dbAdd'},
                {label: '📥 Export', cls: 'btn-primary', fn: 'dbExport'},
                {label: '📤 Import CSV', cls: '', fn: 'dbImportDialog'},
                {label: '📟 Import Bommel', cls: '', fn: 'dbImportBommel'},
                {label: '🗑️ Clear table', cls: 'btn-danger', fn: 'dbClearTable'}
            ],
            places: [
                {label: '+ Add', cls: 'btn-success', fn: 'dbAdd'},
                {label: '📥 Export', cls: 'btn-primary', fn: 'dbExport'},
                {label: '📤 Import CSV', cls: '', fn: 'dbImportDialog'},
                {label: '🌍 Import All Places', cls: '', fn: 'importAllPlaces'},
                {label: '🇳🇱 Import BAG', cls: '', fn: 'dbImportBAG'},
                {label: '🗑️ Clear table', cls: 'btn-danger', fn: 'dbClearTable'}
            ],
            streets: [
                {label: '+ Add', cls: 'btn-success', fn: 'dbAdd'},
                {label: '📥 Export', cls: 'btn-primary', fn: 'dbExport'},
                {label: '📤 Import CSV', cls: '', fn: 'dbImportDialog'},
                {label: '🇳🇱 Import BAG', cls: '', fn: 'dbImportBAG'},
                {label: '🗑️ Clear table', cls: 'btn-danger', fn: 'dbClearTable'}
            ],
            messages: [
                {label: '📥 Export', cls: 'btn-primary', fn: 'dbExport'},
                {label: '🗑️ Delete all', cls: 'btn-danger', fn: 'dbDeleteAllMessages'}
            ],
            texts: [
                {label: '+ Add', cls: 'btn-success', fn: 'dbAdd'},
                {label: '📥 Export', cls: 'btn-primary', fn: 'dbExport'},
                {label: '📖 Import Texts', cls: '', fn: 'dbImportAbbreviations'},
                {label: '🗑️ Clear table', cls: 'btn-danger', fn: 'dbClearTable'}
            ],
            geocodes: [
                {label: '🗑️ Clear all', cls: 'btn-danger', fn: 'dbClearGeocodes'}
            ],
            tts: [
                {label: '+ Add', cls: 'btn-success', fn: 'dbAdd'},
                {label: '📥 Export', cls: 'btn-primary', fn: 'dbExport'},
                {label: '🗑️ Delete all', cls: 'btn-danger', fn: 'dbDeleteAllTts'}
            ],
            ignore_text: [
                {label: '+ Add', cls: 'btn-success', fn: 'dbAdd'},
                {label: '📥 Export', cls: 'btn-primary', fn: 'dbExport'},
                {label: '🗑️ Delete all', cls: 'btn-danger', fn: 'dbDeleteAllIgnoreText'}
            ],
            ignore_capcodes: [
                {label: '+ Add', cls: 'btn-success', fn: 'dbAdd'},
                {label: '📥 Export', cls: 'btn-primary', fn: 'dbExport'},
                {label: '🗑️ Delete all', cls: 'btn-danger', fn: 'dbDeleteAllIgnoreCapcodes'}
            ],
            vehicle_photos: [
                {label: '+ Add', cls: 'btn-success', fn: 'dbOpenVehiclePhotoUpload'},
                {label: '🗑️ Clear table', cls: 'btn-danger', fn: 'dbClearTable'}
            ]
        };

        // Fields shown in the "Add" form per table
        const dbAddFields = {
            capcodes: ['capcode', 'discipline', 'region', 'location', 'description', 'remark'],
            places: ['city', 'abbreviation', 'province', 'latitude', 'longitude'],
            streets: ['street', 'city_id', 'postalcode'],
            texts: ['abbreviation', 'full_text'],
            tts: ['pattern', 'replacement'],
            ignore_text: ['pattern'],
            ignore_capcodes: ['capcode']
        };

        function getApiTable(table) {
            if (table === 'texts') return 'abbreviations';
            if (table === 'tts') return 'tts_replacements';
            return table;
        }

        function dbLoadStats() {
            const tables = [
                ['capcodes', 'capcodes'],
                ['places', 'places'],
                ['streets', 'streets'],
                ['messages', 'messages'],
                ['abbreviations', 'texts'],
                ['geocodes', 'geocodes'],
                ['tts_replacements', 'tts'],
                ['ignore_text', 'ignore_text'],
                ['ignore_capcodes', 'ignore_capcodes'],
                ['vehicle_photos', 'vehicle_photos']
            ];
            tables.forEach(([api, card]) => {
                fetch(`${basePath}api/db/${api}?page=1&per_page=1`)
                    .then(r => r.json())
                    .then(data => {
                        const total = data.total || 0;
                        const countEl = document.getElementById('db-count-' + card);
                        if (countEl) countEl.textContent = total.toLocaleString();
                        const statusEl = document.getElementById('db-status-' + card);
                        if (statusEl) {
                            statusEl.textContent = total > 0 ? 'Ready' : 'Empty';
                            statusEl.className = 'db-card-status ' + (total > 0 ? 'ready' : 'empty');
                        }
                    })
                    .catch(() => {});
            });
        }

        function dbOpenTable(table) {
            dbCurrentTable = table;
            dbCurrentPage = 1;
            document.getElementById('db-search').value = '';
            document.getElementById('db-table-title').textContent = dbTitles[table];

            const buttons = dbTableButtons[table] || [];
            document.getElementById('db-buttons').innerHTML = buttons.map(b =>
                `<button class="btn ${b.cls}" onclick="${b.fn}(this)">${b.label}</button>`
            ).join('');

            document.getElementById('db-landing').style.display = 'none';
            document.getElementById('db-table-view').style.display = 'block';

            dbLoadData();
        }

        function dbBackToLanding() {
            dbCurrentTable = null;
            document.getElementById('db-landing').style.display = 'block';
            document.getElementById('db-table-view').style.display = 'none';
            dbLoadStats();
        }

        function dbLoadData() {
            const apiTable = getApiTable(dbCurrentTable);
            const search = document.getElementById('db-search').value;
            fetch(`${basePath}api/db/${apiTable}?page=${dbCurrentPage}&per_page=50&search=${encodeURIComponent(search)}`)
                .then(r => r.json())
                .then(data => dbRenderTable(data))
                .catch(e => console.error('Failed to load database:', e));
        }

        function dbRenderTable(data) {
            const cols = dbColumns[dbCurrentTable];
            const thead = document.getElementById('db-thead');
            const tbody = document.getElementById('db-tbody');
            dbCurrentItems = data.items || [];
            const editable = ['capcodes', 'places', 'streets', 'tts', 'ignore_text', 'ignore_capcodes'].includes(dbCurrentTable);

            thead.innerHTML = '<tr>' + cols.map(c => `<th>${c}</th>`).join('') + `<th style="width: ${editable ? 76 : 40}px;"></th></tr>`;

            if (dbCurrentItems.length === 0) {
                tbody.innerHTML = `<tr><td colspan="${cols.length + 1}" class="empty-state">No data found</td></tr>`;
            } else {
                tbody.innerHTML = dbCurrentItems.map((item, i) =>
                    '<tr>' +
                    cols.map(c => c === 'thumbnail'
                        ? `<td><img src="${vehiclePhotoUrl(item.key_type, item.key_value)}" alt="" style="width:36px;height:36px;object-fit:cover;border-radius:6px;" onerror="this.style.display='none'"></td>`
                        : `<td>${escapeHtml(item[c] ?? '')}</td>`).join('') +
                    `<td style="white-space: nowrap;">${editable ? `<button class="row-del" title="Edit row" onclick="dbEditRow(${i})">✏️</button>` : ''}<button class="row-del" title="Delete row" onclick="dbDeleteRow(${i})">🗑️</button></td>` +
                    '</tr>'
                ).join('');
            }

            document.getElementById('db-count').textContent = `${(data.total || 0).toLocaleString()} items`;
            dbCurrentPage = data.page || 1;
            dbTotalPages = Math.max(1, Math.ceil((data.total || 0) / (data.per_page || 50)));
            document.getElementById('db-page').textContent = `Page ${dbCurrentPage} / ${dbTotalPages}`;
            document.getElementById('db-prev').disabled = dbCurrentPage <= 1;
            document.getElementById('db-next').disabled = dbCurrentPage >= dbTotalPages;
        }

        function dbSearch() {
            clearTimeout(dbSearchTimeout);
            dbSearchTimeout = setTimeout(() => {
                dbCurrentPage = 1;
                dbLoadData();
            }, 300);
        }

        function dbPrev() { if (dbCurrentPage > 1) { dbCurrentPage--; dbLoadData(); } }
        function dbNext() { if (dbCurrentPage < dbTotalPages) { dbCurrentPage++; dbLoadData(); } }

        function dbRowId(item) {
            if (dbCurrentTable === 'capcodes') return item.capcode;
            if (dbCurrentTable === 'geocodes') return item.query;
            return item.id;
        }

        async function dbDeleteRow(i) {
            const item = dbCurrentItems[i];
            if (!item) return;
            const ok = await confirmDialog({
                title: 'Delete row',
                message: 'Delete this item? This cannot be undone.',
                confirmLabel: 'Delete',
                danger: true
            });
            if (!ok) return;
            const url = dbCurrentTable === 'vehicle_photos'
                ? `${basePath}api/vehicle-photo/${item.key_type}/${encodeURIComponent(item.key_value)}`
                : `${basePath}api/db/${getApiTable(dbCurrentTable)}/${encodeURIComponent(dbRowId(item))}`;
            fetch(url, {method: 'DELETE'})
                .then(r => r.json())
                .then(result => {
                    if (result.success === false) toast('Delete failed', 'error');
                    else toast('Row deleted', 'success');
                    dbLoadData();
                })
                .catch(() => toast('Delete failed', 'error'));
        }

        // ----- Known-value suggestions (e.g. capcodes already in the database) -----
        let capcodeSuggestTimeout = null;
        function updateCapcodeSuggestions(query) {
            clearTimeout(capcodeSuggestTimeout);
            capcodeSuggestTimeout = setTimeout(() => {
                fetch(`${basePath}api/db/capcodes?page=1&per_page=15&search=${encodeURIComponent(query || '')}`)
                    .then(r => r.json())
                    .then(data => {
                        const datalist = document.getElementById('capcode-suggestions');
                        if (!datalist) return;
                        datalist.innerHTML = (data.items || [])
                            .map(c => `<option value="${escapeHtml(c.capcode)}" label="${escapeHtml(c.description || '')}">`)
                            .join('');
                    })
                    .catch(() => {});
            }, 250);
        }

        // Builds the <input> for one field in the Add/Edit forms - fields with
        // known values (like capcodes already in the database) get suggestions,
        // gracefully offering nothing if that table hasn't been imported yet.
        function dbFieldInputHTML(f, value) {
            const valueAttr = value !== undefined ? ` value="${escapeHtml(value)}"` : '';
            if (dbCurrentTable === 'ignore_capcodes' && f === 'capcode') {
                return `<input type="text" class="input" data-field="${f}" list="capcode-suggestions" oninput="updateCapcodeSuggestions(this.value)"${valueAttr}>`;
            }
            // Editing a capcode row: the capcode is the key in the URL and can't change
            const readonly = value !== undefined && dbCurrentTable === 'capcodes' && f === 'capcode' ? ' readonly' : '';
            return `<input type="text" class="input" data-field="${f}"${valueAttr}${readonly}>`;
        }

        // ----- Add item (modal form) -----
        function dbAdd() {
            const fields = dbAddFields[dbCurrentTable];
            if (!fields) {
                toast('Rows cannot be added to this table', 'error');
                return;
            }
            if (dbCurrentTable === 'ignore_capcodes') updateCapcodeSuggestions('');
            const bodyHTML = fields.map(f => `
                <div class="form-field">
                    <label>${f}</label>
                    ${dbFieldInputHTML(f)}
                </div>`).join('');
            const overlay = openModal({
                title: `Add to ${dbTitles[dbCurrentTable]}`,
                bodyHTML,
                footHTML: `<button class="btn" data-close>Cancel</button><button class="btn btn-success" data-save>Add</button>`
            });
            overlay.querySelector('[data-save]').addEventListener('click', () => {
                const values = {};
                overlay.querySelectorAll('[data-field]').forEach(inp => { values[inp.dataset.field] = inp.value.trim(); });
                const required = fields[0];
                if (!values[required]) {
                    toast(`${required} is required`, 'error');
                    return;
                }
                fetch(`${basePath}api/db/${getApiTable(dbCurrentTable)}`, {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify(values)
                }).then(r => r.json()).then(result => {
                    if (result.error) toast(result.error, 'error');
                    else {
                        toast('Item added', 'success');
                        closeModal();
                        dbLoadData();
                    }
                }).catch(() => toast('Add failed', 'error'));
            });
        }

        // ----- Edit row (modal form, pre-filled) -----
        function dbEditRow(i) {
            const item = dbCurrentItems[i];
            if (!item) return;
            const fields = dbAddFields[dbCurrentTable] || [];
            if (dbCurrentTable === 'ignore_capcodes') updateCapcodeSuggestions('');
            const bodyHTML = fields.map(f => `
                <div class="form-field">
                    <label>${f}</label>
                    ${dbFieldInputHTML(f, item[f] ?? '')}
                </div>`).join('') +
                (['tts', 'ignore_text', 'ignore_capcodes'].includes(dbCurrentTable) ? `
                <label class="form-check"><input type="checkbox" data-field-enabled ${item.enabled ? 'checked' : ''}> Enabled</label>` : '');
            const overlay = openModal({
                title: `Edit ${dbTitles[dbCurrentTable]}`,
                bodyHTML,
                footHTML: `<button class="btn" data-close>Cancel</button><button class="btn btn-primary" data-save>Save</button>`
            });
            overlay.querySelector('[data-save]').addEventListener('click', () => {
                const values = {};
                overlay.querySelectorAll('[data-field]').forEach(inp => { values[inp.dataset.field] = inp.value.trim(); });
                const enabledBox = overlay.querySelector('[data-field-enabled]');
                if (enabledBox) values.enabled = enabledBox.checked;
                fetch(`${basePath}api/db/${getApiTable(dbCurrentTable)}/${encodeURIComponent(dbRowId(item))}`, {
                    method: 'PUT',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify(values)
                }).then(r => r.json()).then(result => {
                    if (result.error || result.success === false) toast(result.error || 'Update failed', 'error');
                    else {
                        toast('Row updated', 'success');
                        closeModal();
                        dbLoadData();
                    }
                }).catch(() => toast('Update failed', 'error'));
            });
        }

        // ----- CSV export/import -----
        function dbExport() {
            const apiTable = getApiTable(dbCurrentTable);
            window.location.href = `${basePath}api/db/export/${apiTable}`;
        }

        function dbImportDialog() {
            const overlay = openModal({
                title: `Import CSV into ${dbTitles[dbCurrentTable]}`,
                bodyHTML: `
                    <div class="form-field">
                        <label>Choose a .csv file</label>
                        <input type="file" accept=".csv,text/csv" class="input" data-file>
                    </div>
                    <div class="form-field">
                        <label>…or paste CSV content</label>
                        <textarea class="input" rows="8" data-csv placeholder="header1,header2&#10;value1,value2"></textarea>
                    </div>
                    <label class="form-check"><input type="checkbox" data-replace> Replace all existing data (default: merge)</label>`,
                footHTML: `<button class="btn" data-close>Cancel</button><button class="btn btn-primary" data-import>Import</button>`
            });
            const textarea = overlay.querySelector('[data-csv]');
            overlay.querySelector('[data-file]').addEventListener('change', (e) => {
                const file = e.target.files[0];
                if (!file) return;
                const reader = new FileReader();
                reader.onload = () => { textarea.value = reader.result; };
                reader.readAsText(file);
            });
            overlay.querySelector('[data-import]').addEventListener('click', () => {
                const csv = textarea.value.trim();
                if (!csv) { toast('No CSV content provided', 'error'); return; }
                const replace = overlay.querySelector('[data-replace]').checked;
                fetch(`${basePath}api/db/import/${getApiTable(dbCurrentTable)}`, {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({csv, replace})
                }).then(r => r.text().then(text => {
                    let result = null;
                    try { result = JSON.parse(text); } catch (e) { /* plain-text error page */ }
                    if (!r.ok) {
                        const msg = (result && result.error) || (r.status === 413 ? 'CSV file too large (max 50 MB)' : text);
                        throw new Error(msg || `HTTP ${r.status}`);
                    }
                    return result || {};
                })).then(result => {
                    if (result.error) toast(result.error, 'error');
                    else {
                        toast(`Imported ${result.imported} items, skipped ${result.skipped}`, 'success');
                        closeModal();
                        dbLoadData();
                    }
                }).catch(e => toast('Import failed: ' + e.message, 'error'));
            });
        }

        // ----- BAG import -----
        let bagProgressInterval = null;

        function dbImportBAG(btn) {
            fetch(`${basePath}api/db/bag/provinces`)
                .then(r => r.json())
                .then(data => {
                    const provinces = data.provinces || [];
                    const overlay = openModal({
                        title: 'Import BAG address data',
                        bodyHTML: `
                            <div class="form-field">
                                <label>Province</label>
                                <select class="input" data-province style="width:100%">
                                    ${provinces.map(p => `<option>${escapeHtml(p)}</option>`).join('')}
                                </select>
                            </div>
                            <div class="panel-sub">Imports cities and streets for the selected province. This may take 5-10 minutes for large provinces.</div>`,
                        footHTML: `<button class="btn" data-close>Cancel</button><button class="btn btn-primary" data-start>Start import</button>`
                    });
                    overlay.querySelector('[data-start]').addEventListener('click', () => {
                        const province = overlay.querySelector('[data-province]').value;
                        closeModal();

                        // Progress in the button itself, same as Import All Places
                        const origLabel = btn ? btn.textContent : '';
                        if (btn) { btn.disabled = true; btn.textContent = '⏳ Starting…'; }
                        const resetBtn = () => { if (btn) { btn.disabled = false; btn.textContent = origLabel; } };

                        const stopPolling = () => {
                            if (bagProgressInterval) { clearInterval(bagProgressInterval); bagProgressInterval = null; }
                        };

                        // Poll only once the server has reset progress for this run,
                        // or the first poll can read the previous run's result
                        fetch(`${basePath}api/db/bag/import/${encodeURIComponent(province)}`, {method: 'POST'})
                            .then(r => r.json())
                            .then(result => {
                                if (result.error) {
                                    toast(result.error, 'error');
                                    resetBtn();
                                    return;
                                }
                                stopPolling();
                                bagProgressInterval = setInterval(() => {
                                    fetch(`${basePath}api/db/bag/progress`)
                                        .then(r => r.json())
                                        .then(p => {
                                            if (btn) btn.textContent = `⏳ ${p.percent}%`;
                                            if (!p.running) {
                                                stopPolling();
                                                if (p.percent >= 100) {
                                                    toast(`BAG import complete: ${p.cities} cities, ${p.streets} streets`, 'success');
                                                } else {
                                                    toast('BAG import failed: ' + (p.status || 'unknown error'), 'error');
                                                }
                                                resetBtn();
                                                dbLoadStats();
                                                if (dbCurrentTable) dbLoadData();
                                            }
                                        })
                                        .catch(() => {
                                            stopPolling();
                                            toast('Lost track of BAG import progress', 'error');
                                            resetBtn();
                                        });
                                }, 500);
                            })
                            .catch(() => {
                                stopPolling();
                                toast('BAG import failed', 'error');
                                resetBtn();
                            });
                    });
                })
                .catch(() => toast('Failed to load provinces', 'error'));
        }

        function importAllPlaces(btn) {
            confirmDialog({
                title: 'Import all places',
                message: 'Import ~2600 Dutch places from PDOK?',
                confirmLabel: 'Import'
            }).then(ok => {
                if (!ok) return;
                if (btn) { btn.disabled = true; btn.textContent = '⏳ Importing…'; }
                const resetBtn = () => { if (btn) { btn.disabled = false; btn.textContent = '🌍 Import All Places'; } };

                fetch(`${basePath}api/db/bag/import-all-places`, {method: 'POST'})
                    .then(r => r.json())
                    .then(result => {
                        if (result.error) {
                            toast(result.error, 'error');
                            resetBtn();
                            return;
                        }
                        const pollInterval = setInterval(() => {
                            fetch(`${basePath}api/db/bag/progress`)
                                .then(r => r.json())
                                .then(p => {
                                    if (btn) btn.textContent = `⏳ ${p.percent}%`;
                                    if (!p.running) {
                                        clearInterval(pollInterval);
                                        if (p.percent >= 100) {
                                            toast(`Import complete: ${p.cities} places`, 'success');
                                        } else {
                                            toast('Import failed: ' + (p.status || 'unknown error'), 'error');
                                        }
                                        resetBtn();
                                        dbLoadStats();
                                    }
                                })
                                .catch(() => {
                                    clearInterval(pollInterval);
                                    toast('Lost track of import progress', 'error');
                                    resetBtn();
                                });
                        }, 500);
                    })
                    .catch(() => {
                        toast('Import failed', 'error');
                        resetBtn();
                    });
            });
        }

        // ----- Bommel capcode import -----
        let capcodeProgressInterval = null;

        function dbImportBommel(btn) {
            confirmDialog({
                title: 'Import capcodes',
                message: 'Import all capcodes from p2000.bommel.net?<br><br>This fetches ~10,000 capcodes from all 25 regions and takes about 1-2 minutes.',
                confirmLabel: 'Import'
            }).then(ok => {
                if (!ok) return;
                const origLabel = btn ? btn.textContent : '';
                if (btn) { btn.disabled = true; btn.textContent = '⏳ Starting…'; }
                const resetBtn = () => { if (btn) { btn.disabled = false; btn.textContent = origLabel; } };

                const stopPolling = () => {
                    if (capcodeProgressInterval) { clearInterval(capcodeProgressInterval); capcodeProgressInterval = null; }
                };

                // Poll only once the server has reset progress for this run
                fetch(`${basePath}api/db/capcodes/import-bommel`, {method: 'POST'})
                    .then(r => r.json())
                    .then(result => {
                        if (result.error) {
                            toast(result.error, 'error');
                            resetBtn();
                            return;
                        }
                        stopPolling();
                        capcodeProgressInterval = setInterval(() => {
                            fetch(`${basePath}api/db/capcodes/import-progress`)
                                .then(r => r.json())
                                .then(p => {
                                    if (btn) btn.textContent = `⏳ ${p.percent}%`;
                                    if (!p.running) {
                                        stopPolling();
                                        if (p.percent >= 100) {
                                            toast(`Capcode import complete: ${(p.imported || 0).toLocaleString()} capcodes`, 'success');
                                        } else {
                                            toast('Capcode import failed: ' + (p.status || 'unknown error'), 'error');
                                        }
                                        resetBtn();
                                        dbLoadStats();
                                        if (dbCurrentTable) dbLoadData();
                                    }
                                })
                                .catch(() => {
                                    stopPolling();
                                    toast('Lost track of capcode import progress', 'error');
                                    resetBtn();
                                });
                        }, 300);
                    })
                    .catch(() => {
                        stopPolling();
                        toast('Capcode import failed', 'error');
                        resetBtn();
                    });
            });
        }

        function dbImportAbbreviations(btn) {
            confirmDialog({
                title: 'Import abbreviations',
                message: 'Import the P2000 abbreviations database?<br><br>This includes 300+ common abbreviations used by Dutch emergency services.',
                confirmLabel: 'Import'
            }).then(ok => {
                if (!ok) return;
                const origLabel = btn ? btn.textContent : '';
                if (btn) { btn.disabled = true; btn.textContent = '⏳ Importing…'; }
                const resetBtn = () => { if (btn) { btn.disabled = false; btn.textContent = origLabel; } };
                fetch(`${basePath}api/db/abbreviations/import`, {method: 'POST'})
                    .then(r => r.json())
                    .then(result => {
                        if (result.error) toast(result.error, 'error');
                        else {
                            toast(`Imported ${result.count} abbreviations`, 'success');
                            dbLoadStats();
                            if (dbCurrentTable) dbLoadData();
                        }
                        resetBtn();
                    })
                    .catch(() => { toast('Import failed', 'error'); resetBtn(); });
            });
        }

        // ----- Destructive actions -----
        function dbClearTable() {
            const table = dbCurrentTable;
            confirmDialog({
                title: `Clear ${dbTitles[table]}`,
                message: `Delete ALL rows from ${dbTitles[table]}? This cannot be undone.`,
                confirmLabel: 'Delete everything',
                danger: true
            }).then(ok => {
                if (!ok) return;
                fetch(`${basePath}api/db/clear/${getApiTable(table)}`, {method: 'DELETE'})
                    .then(r => r.json())
                    .then(result => {
                        if (result.error) toast(result.error, 'error');
                        else toast(`Cleared ${result.deleted} rows`, 'success');
                        dbLoadStats();
                        dbLoadData();
                    })
                    .catch(() => toast('Clear failed', 'error'));
            });
        }

        function dbDeleteAllMessages() {
            confirmDialog({
                title: 'Delete all messages',
                message: 'Delete ALL messages from history? This cannot be undone.',
                confirmLabel: 'Delete everything',
                danger: true
            }).then(ok => {
                if (!ok) return;
                fetch(`${basePath}api/db/messages`, {method: 'DELETE'})
                    .then(r => r.json())
                    .then(result => {
                        toast(`Deleted ${result.deleted} messages`, 'success');
                        dbLoadData();
                    })
                    .catch(() => toast('Delete failed', 'error'));
            });
        }

        function dbDeleteAllTts() {
            confirmDialog({
                title: 'Delete all TTS replacements',
                message: 'Delete ALL TTS replacements? This cannot be undone.',
                confirmLabel: 'Delete everything',
                danger: true
            }).then(ok => {
                if (!ok) return;
                fetch(`${basePath}api/db/tts_replacements`, {method: 'DELETE'})
                    .then(r => r.json())
                    .then(result => {
                        toast(`Deleted ${result.deleted} TTS replacements`, 'success');
                        dbLoadData();
                    })
                    .catch(() => toast('Delete failed', 'error'));
            });
        }

        function dbDeleteAllIgnoreText() {
            confirmDialog({
                title: 'Delete all ignore-text patterns',
                message: 'Delete ALL ignore-text patterns? This cannot be undone.',
                confirmLabel: 'Delete everything',
                danger: true
            }).then(ok => {
                if (!ok) return;
                fetch(`${basePath}api/db/ignore_text`, {method: 'DELETE'})
                    .then(r => r.json())
                    .then(result => {
                        toast(`Deleted ${result.deleted} ignore-text patterns`, 'success');
                        dbLoadData();
                    })
                    .catch(() => toast('Delete failed', 'error'));
            });
        }

        function dbDeleteAllIgnoreCapcodes() {
            confirmDialog({
                title: 'Delete all ignored capcodes',
                message: 'Delete ALL ignored capcodes? This cannot be undone.',
                confirmLabel: 'Delete everything',
                danger: true
            }).then(ok => {
                if (!ok) return;
                fetch(`${basePath}api/db/ignore_capcodes`, {method: 'DELETE'})
                    .then(r => r.json())
                    .then(result => {
                        toast(`Deleted ${result.deleted} ignored capcodes`, 'success');
                        dbLoadData();
                    })
                    .catch(() => toast('Delete failed', 'error'));
            });
        }

        function dbClearGeocodes() {
            confirmDialog({
                title: 'Clear geocode cache',
                message: 'Clear all cached geocodes? This removes all address lookup cache entries.',
                confirmLabel: 'Clear cache',
                danger: true
            }).then(ok => {
                if (!ok) return;
                fetch(`${basePath}api/db/geocodes`, {method: 'DELETE'})
                    .then(r => r.json())
                    .then(result => {
                        if (result.error) toast(result.error, 'error');
                        else {
                            toast(`Cleared ${result.deleted} geocodes from cache`, 'success');
                            dbLoadData();
                        }
                    })
                    .catch(() => toast('Clear failed', 'error'));
            });
        }

        function dbResetDatabase() {
            confirmDialog({
                title: 'Reset database',
                message: 'Delete ALL database data - messages, capcodes, places, streets, geocodes, texts and TTS replacements?<br><br><strong>This cannot be undone.</strong>',
                confirmLabel: 'Reset database',
                danger: true
            }).then(ok => {
                if (!ok) return;
                fetch(`${basePath}api/db/reset`, {method: 'DELETE'})
                    .then(r => r.json())
                    .then(result => {
                        if (result.error) toast(result.error, 'error');
                        else {
                            toast('Database reset complete. Use Import Bommel and Import BAG to restore data.', 'success');
                            dbLoadStats();
                            if (dbCurrentTable) dbLoadData();
                        }
                    })
                    .catch(() => toast('Reset failed', 'error'));
            });
        }

        // Load database node counts on page load
        setTimeout(dbLoadStats, 100);
    </script>
</body>
</html>
"""
