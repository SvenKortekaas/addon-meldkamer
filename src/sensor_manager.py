"""Sensor manager for filtering and matching P2000 messages."""

import logging
import math
import re
import unicodedata
from dataclasses import dataclass
from typing import Any

from .message import P2000Message

logger = logging.getLogger(__name__)

# Priority filter entries: optional P/A/B/PRIO prefix + digit ("P1", "a 1", "PRIO 2", "1")
_PRIORITY_ENTRY_RE = re.compile(r"^(PRIO|P|A|B)?\s*([0-9])$", re.IGNORECASE)


def slugify(value: str) -> str:
    """Reduce a string to a valid HA object id: [a-z0-9_], no leading/trailing/double underscores.

    Accents are transliterated ("Café" -> "cafe", "Ĳsselmonde" -> "ijsselmonde");
    characters with no ASCII equivalent become underscores. May return "".
    """
    value = unicodedata.normalize("NFKD", value.casefold()).encode("ascii", "ignore").decode("ascii")
    return re.sub(r"[^a-z0-9]+", "_", value).strip("_")


def sanitize_mqtt_id(value: str) -> str:
    """Make a user-set mqtt_id safe as an MQTT topic level / discovery object id ([A-Za-z0-9_-]).

    Already-valid ids are returned unchanged so existing unique_ids keep working.
    """
    value = unicodedata.normalize("NFKD", value).encode("ascii", "ignore").decode("ascii")
    return re.sub(r"[^A-Za-z0-9_-]+", "_", value).strip("_")


def parse_priority(entry: str) -> int | None:
    """Parse a priority filter entry to the int the parser stores in msg.priority.

    Mirrors parser.PRIO_PATTERNS: P1/A1/PRIO 1 -> 1, P2/A2 -> 2, B1-B3/P3 -> 3, P4 -> 4.
    Returns None when the entry can't be parsed.
    """
    match = _PRIORITY_ENTRY_RE.match(entry.strip())
    if not match:
        return None
    prefix = (match.group(1) or "").upper()
    level = int(match.group(2))
    if prefix == "B":
        return 3 if 1 <= level <= 3 else None
    if prefix == "A":
        return level if level in (1, 2) else None
    return level if 1 <= level <= 4 else None


@dataclass
class SensorFilter:
    """Filter criteria for a P2000 sensor."""

    disciplines: list[str] | None = None
    cities: list[str] | None = None
    capcodes: list[str] | None = None
    priorities: list[str] | None = None
    region: str | None = None
    radius_km: float | None = None
    center_lat: float | None = None
    center_lon: float | None = None
    text_contains: str | None = None


@dataclass
class SensorConfig:
    """Configuration for a P2000 sensor."""

    name: str
    entity_id: str
    filters: SensorFilter
    icon: str = "mdi:radio-tower"
    # Optional stable identifier for this sensor's MQTT topics/unique_id.
    # Falls back to entity_id when not set.
    mqtt_id: str | None = None


class SensorManager:
    """Manages P2000 sensors and message filtering."""

    def __init__(self, sensors: list[SensorConfig]):
        self.sensors = sensors
        self._radius_warned: set = set()  # Sensors already warned about missing center coordinates
        self._priority_warned: set = set()  # (sensor, entry) pairs already warned about as unparseable
        logger.info(f"Loaded {len(sensors)} sensor(s)")

    def get_matching_sensors(self, msg: P2000Message) -> list[SensorConfig]:
        """Get all sensors that match the given message."""
        matching = []

        for sensor in self.sensors:
            if self._message_matches_filters(msg, sensor):
                matching.append(sensor)
                logger.debug(f"Message matched sensor: {sensor.entity_id}")

        return matching

    def _message_matches_filters(self, msg: P2000Message, sensor: SensorConfig) -> bool:
        """Check if message matches all filter criteria (AND logic)."""
        filters = sensor.filters

        # Discipline filter (msg.discipline may hold multiple, ", "-joined disciplines)
        if filters.disciplines:
            msg_disciplines = [d.strip().lower() for d in msg.discipline.split(",")] if msg.discipline else []
            filter_disciplines = [d.strip().lower() for d in filters.disciplines]
            if not any(d in filter_disciplines for d in msg_disciplines):
                return False

        # City filter (case-insensitive)
        if filters.cities and (
            not msg.city or msg.city.strip().casefold() not in {c.casefold() for c in filters.cities}
        ):
            return False

        # Capcode filter (match if ANY capcode in message matches; compared as 9-digit zero-padded)
        if filters.capcodes:
            filter_capcodes = {c.strip().zfill(9) for c in filters.capcodes}
            if not msg.capcodes or not any(str(code).strip().zfill(9) in filter_capcodes for code in msg.capcodes):
                return False

        # Priority filter (filter entries are strings like "P1"/"A1"/"1", msg.priority is an int)
        if filters.priorities:
            allowed_priorities = set()
            for entry in filters.priorities:
                priority = parse_priority(entry)
                if priority is not None:
                    allowed_priorities.add(priority)
                elif (sensor.name, entry) not in self._priority_warned:
                    logger.warning(
                        f"Sensor '{sensor.name}' has unrecognised priority '{entry}' (expected e.g. P1, A1, B1, 1) "
                        "- ignoring it"
                    )
                    self._priority_warned.add((sensor.name, entry))
            if not msg.priority or msg.priority not in allowed_priorities:
                return False

        # Region filter (case-insensitive)
        if filters.region and (not msg.region or filters.region.lower() not in msg.region.lower()):
            return False

        # Geographic radius filter
        if filters.radius_km is not None and not self._within_radius(msg, sensor):
            return False

        # Text contains filter (case-insensitive)
        return not (filters.text_contains and (not msg.body or filters.text_contains.lower() not in msg.body.lower()))

    def _within_radius(self, msg: P2000Message, sensor: SensorConfig) -> bool:
        """Check if message location is within radius of center point."""
        filters = sensor.filters
        if filters.center_lat is None or filters.center_lon is None:
            # Radius set without center coordinates can never match - warn once per sensor
            if sensor.name not in self._radius_warned:
                logger.warning(
                    f"Sensor '{sensor.name}' has radius_km set but no center_lat/center_lon - "
                    "radius filter will never match"
                )
                self._radius_warned.add(sensor.name)
            return False

        try:
            msg_lat = float(msg.latitude)
            msg_lon = float(msg.longitude)
        except (TypeError, ValueError):
            return False  # Message has no usable coordinates

        distance_km = round(self._haversine_distance(msg_lat, msg_lon, filters.center_lat, filters.center_lon), 2)

        # Record the computed distance on the message (keep the smallest across sensors).
        # msg.distance is "" for a freshly-decoded P2000Message, but messages reloaded
        # from the database (SQL NULL) or wrapped by _DictMessageView (missing/null key)
        # come through as None instead - treat both as "not yet set".
        if msg.distance in ("", None) or distance_km < msg.distance:
            msg.distance = distance_km

        return distance_km <= filters.radius_km

    @staticmethod
    def _haversine_distance(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
        """Calculate distance between two points in kilometers using Haversine formula."""
        R = 6371  # Earth's radius in kilometers

        lat1_rad = math.radians(lat1)
        lat2_rad = math.radians(lat2)
        delta_lat = math.radians(lat2 - lat1)
        delta_lon = math.radians(lon2 - lon1)

        a = math.sin(delta_lat / 2) ** 2 + math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(delta_lon / 2) ** 2
        c = 2 * math.asin(math.sqrt(a))

        return R * c


def load_sensors_from_config(config_data: list[dict[str, Any]]) -> list[SensorConfig]:
    """Load sensor configurations from config dict."""
    sensors = []
    seen_entity_ids: set = set()
    seen_mqtt_keys: set = set()

    for sensor_data in config_data:
        try:
            # Auto-generate entity_id from name if not provided; reduce either to a valid HA object id
            name = sensor_data["name"]
            entity_id = slugify(str(sensor_data.get("entity_id") or name))
            if not entity_id:
                logger.error(f"Sensor '{name}' skipped: its name gives an empty entity_id (use letters or digits)")
                continue

            # Parse comma-separated filter strings into lists
            def parse_csv(value):
                """Parse comma-separated string into list, return None if empty."""
                if not value or not value.strip():
                    return None
                return [item.strip() for item in value.split(",") if item.strip()]

            def parse_float(value):
                """Parse a float from config (number or string), return None if absent/invalid."""
                if value is None or (isinstance(value, str) and not value.strip()):
                    return None
                try:
                    return float(value)
                except (TypeError, ValueError):
                    logger.error(f"Invalid numeric sensor filter value: {value!r}")
                    return None

            filters = SensorFilter(
                disciplines=parse_csv(sensor_data.get("disciplines")),
                cities=parse_csv(sensor_data.get("cities")),
                capcodes=parse_csv(sensor_data.get("capcodes")),
                priorities=parse_csv(sensor_data.get("priorities")),
                region=sensor_data.get("region"),
                radius_km=parse_float(sensor_data.get("radius_km")),
                center_lat=parse_float(sensor_data.get("center_lat")),
                center_lon=parse_float(sensor_data.get("center_lon")),
                text_contains=sensor_data.get("text_contains"),
            )

            mqtt_id = sensor_data.get("mqtt_id")
            mqtt_id = sanitize_mqtt_id(str(mqtt_id)) if mqtt_id not in (None, "") else None
            if mqtt_id is not None and mqtt_id != str(sensor_data["mqtt_id"]).strip():
                logger.warning(
                    f"Sensor '{name}' mqtt_id '{sensor_data['mqtt_id']}' sanitised to '{mqtt_id or entity_id}'"
                )

            # Two sensors sharing an entity id or MQTT key would overwrite each other
            mqtt_key = mqtt_id or entity_id
            if entity_id in seen_entity_ids or mqtt_key in seen_mqtt_keys:
                logger.error(f"Sensor '{name}' skipped: entity_id '{entity_id}' / mqtt_id '{mqtt_key}' already in use")
                continue
            seen_entity_ids.add(entity_id)
            seen_mqtt_keys.add(mqtt_key)

            sensor = SensorConfig(
                name=name,
                entity_id=entity_id,
                filters=filters,
                icon=sensor_data.get("icon", "mdi:radio-tower"),
                mqtt_id=mqtt_id or None,
            )

            sensors.append(sensor)
            logger.info(f"Loaded sensor '{name}' with entity_id '{entity_id}'")

        except KeyError as e:
            logger.error(f"Invalid sensor config, missing field: {e}")

    return sensors
