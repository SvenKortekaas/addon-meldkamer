"""Receiver statistics: FLEX frames, decode quality and message rate per minute."""

import time
from collections import deque
from dataclasses import dataclass

HISTORY_MINUTES = 60


@dataclass
class MinuteBucket:
    """Counters for one wall-clock minute."""

    minute: int  # Unix time // 60
    frames: int = 0  # FLEX frames with a valid frame information word
    messages: int = 0  # Decoded alphanumeric messages
    corrected_bits: int = 0  # Bit errors fixed by BCH error correction
    failed: int = 0  # Blocks/frames that could not be decoded
    sync_lost: int = 0


class ReceiverStats:
    """Rolling per-minute receiver statistics for the last hour."""

    def __init__(self):
        self._buckets: deque[MinuteBucket] = deque(maxlen=HISTORY_MINUTES)
        self.last_activity: float | None = None  # time.time() of the last FLEX frame or message

    def _bucket(self) -> MinuteBucket:
        minute = int(time.time() // 60)
        if not self._buckets or self._buckets[-1].minute != minute:
            self._buckets.append(MinuteBucket(minute))
        return self._buckets[-1]

    def frame(self) -> None:
        self._bucket().frames += 1
        self.last_activity = time.time()

    def message(self) -> None:
        self._bucket().messages += 1
        self.last_activity = time.time()

    def corrected(self, bits: int) -> None:
        self._bucket().corrected_bits += bits

    def failed(self) -> None:
        self._bucket().failed += 1

    def sync_lost(self) -> None:
        self._bucket().sync_lost += 1

    def silent_for(self) -> float | None:
        """Seconds since the last FLEX activity, None when there was none yet."""
        return None if self.last_activity is None else time.time() - self.last_activity

    def online(self, offline_after_minutes: int) -> bool:
        silent = self.silent_for()
        return silent is not None and silent < offline_after_minutes * 60

    def _recent(self) -> list[MinuteBucket]:
        """Buckets of the last HISTORY_MINUTES minutes, excluding the current partial minute."""
        now = int(time.time() // 60)
        return [b for b in self._buckets if now - HISTORY_MINUTES <= b.minute < now]

    def messages_last_hour(self) -> int:
        now = int(time.time() // 60)
        return sum(b.messages for b in self._buckets if b.minute > now - HISTORY_MINUTES)

    def snapshot(self) -> dict:
        """Summary and per-minute history for the web UI."""
        recent = self._recent()
        frames = sum(b.frames for b in recent)
        last_minute = recent[-1] if recent and recent[-1].minute == int(time.time() // 60) - 1 else None
        return {
            "silent_for": self.silent_for(),
            "frames_last_minute": last_minute.frames if last_minute else 0,
            "messages_last_hour": self.messages_last_hour(),
            "frames_last_hour": frames,
            "failed_last_hour": sum(b.failed for b in recent),
            "sync_lost_last_hour": sum(b.sync_lost for b in recent),
            "corrected_bits_per_frame": round(sum(b.corrected_bits for b in recent) / frames, 2) if frames else None,
            "history": [
                {
                    "minute": b.minute * 60,
                    "frames": b.frames,
                    "messages": b.messages,
                    "failed": b.failed,
                    "corrected_bits": b.corrected_bits,
                }
                for b in recent
            ],
        }
