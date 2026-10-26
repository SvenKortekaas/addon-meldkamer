"""Group messages for the same address into incidents and detect escalation."""

import re
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta

from .message import P2000Message

_GRIP_RE = re.compile(r"\bGRIP\s*:?\s*([1-5])\b", re.IGNORECASE)


def grip_level(text: str) -> int:
    """GRIP level mentioned in a message, 0 when none."""
    match = _GRIP_RE.search(text or "")
    return int(match.group(1)) if match else 0


def _disciplines(msg) -> set[str]:
    return {d.strip() for d in (msg.discipline or "").split(",") if d.strip()}


@dataclass
class Incident:
    """Messages to one address, each within the incident window of the previous one."""

    id: str
    address: str
    started: datetime
    last_seen: datetime
    disciplines: set[str] = field(default_factory=set)
    capcodes: set[str] = field(default_factory=set)
    priority: int = 0  # Most urgent priority seen (1 = highest, 0 = none)
    grip: int = 0
    message_count: int = 0

    def add(self, msg) -> list[str]:
        """Add a message and return what escalated (empty for the first message)."""
        reasons = []
        first = self.message_count == 0
        new_disciplines = _disciplines(msg) - self.disciplines
        new_capcodes = set(msg.capcodes or []) - self.capcodes
        grip = grip_level(msg.body)
        priority = msg.priority or 0

        if not first:
            if grip > self.grip:
                reasons.append(f"GRIP {grip}")
            if priority and (not self.priority or priority < self.priority):
                reasons.append(f"priority {priority}")
            if new_disciplines:
                reasons.append(f"{', '.join(sorted(new_disciplines))} alerted")
            if new_capcodes:
                reasons.append(f"{len(new_capcodes)} more unit(s) alerted")

        self.disciplines |= new_disciplines
        self.capcodes |= new_capcodes
        self.grip = max(self.grip, grip)
        if priority and (not self.priority or priority < self.priority):
            self.priority = priority
        self.last_seen = max(self.last_seen, msg.timestamp)
        self.message_count += 1
        return reasons

    def to_dict(self) -> dict:
        return {
            "incident_id": self.id,
            "address": self.address,
            "started": self.started.isoformat(),
            "disciplines": sorted(self.disciplines),
            "units": len(self.capcodes),
            "priority": self.priority,
            "grip": self.grip,
            "message_count": self.message_count,
        }


class IncidentTracker:
    """Assigns incident ids: same street and city within the incident window."""

    def __init__(self, window_minutes: int):
        self.window = timedelta(minutes=window_minutes)
        self._open: dict[tuple[str, str], Incident] = {}

    @staticmethod
    def _key(msg) -> tuple[str, str] | None:
        # A city alone is too coarse: unrelated incidents in one city would merge
        if not msg.street or not msg.city:
            return None
        return (" ".join(msg.street.split()).casefold(), msg.city.strip().casefold())

    def _expire(self, now: datetime) -> None:
        for key in [k for k, inc in self._open.items() if now - inc.last_seen > self.window]:
            del self._open[key]

    def open_count(self) -> int:
        """Incidents with a message within the incident window."""
        self._expire(datetime.now().astimezone())
        return len(self._open)

    def restore(self, messages: list[dict]) -> None:
        """Rebuild open incidents from stored messages (oldest first) after a restart."""
        for data in messages:
            msg = _StoredMessage(data)
            key = self._key(msg)
            if not msg.incident_id or key is None:
                continue
            incident = self._open.get(key)
            if incident is None or incident.id != msg.incident_id:
                incident = Incident(
                    id=msg.incident_id, address=msg.address or "", started=msg.timestamp, last_seen=msg.timestamp
                )
                self._open[key] = incident
            incident.add(msg)
        if self._open:
            self._expire(max(inc.last_seen for inc in self._open.values()))

    def assign(self, msg: P2000Message) -> tuple[Incident | None, list[str]]:
        """Attach msg to its incident (setting msg.incident_id); return it with escalation reasons."""
        self._expire(msg.timestamp)
        key = self._key(msg)
        if key is None:
            return None, []
        incident = self._open.get(key)
        if incident is None:
            incident = Incident(
                id=uuid.uuid4().hex[:12], address=msg.address, started=msg.timestamp, last_seen=msg.timestamp
            )
            self._open[key] = incident
        msg.incident_id = incident.id
        return incident, incident.add(msg)


class _StoredMessage:
    """Attribute access to a stored message dict, with its timestamp parsed."""

    def __init__(self, data: dict):
        self._data = data
        self.timestamp = datetime.fromisoformat(data["timestamp"])

    def __getattr__(self, name):
        return self._data.get(name)
