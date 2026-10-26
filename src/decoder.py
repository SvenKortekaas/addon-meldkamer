"""P2000 FLEX decoder using rtl_fm or rx_fm (SoapySDR) and multimon-ng."""

import asyncio
import contextlib
import logging
import os
import re
import shlex
import signal
import subprocess
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass

from .config import Config
from .ha_client import HAClient
from .stats import ReceiverStats

logger = logging.getLogger(__name__)

# multimon-ng prints FLEX frame info and error correction details at verbosity 3
MULTIMON_VERBOSITY = 3
_CORRECTED_RE = re.compile(r"^FLEX: Phase \w Fixed (\d+) errors")
_FAILED_MARKERS = ("Unable to fix errors", "Unable to decode FIW", "Garbled message", "Bad Checksum")

# How often the watchdog checks for a silent receiver
WATCHDOG_INTERVAL = 15


@dataclass
class Source:
    """One receiver the decoder can run on."""

    label: str  # "primary" or "backup"
    receiver_type: str  # rtl-sdr, hackrf, soapysdr, network
    ppm: int
    options_key: str  # receiver option holding this source's ppm correction
    host: str = ""
    port: int = 0


class Decoder:
    """Runs the SDR | multimon-ng pipeline, fails over between receivers and calibrates ppm."""

    def __init__(self, config: Config, ha_client: HAClient):
        self.config = config
        self.ha_client = ha_client
        self.stats = ReceiverStats()

        self.sources = [
            Source(
                "primary",
                config.receiver_type,
                config.ppm_correction,
                "ppm_correction",
                config.network_host,
                config.network_port,
            )
        ]
        if config.backup_host:
            self.sources.append(
                Source(
                    "backup",
                    "network",
                    config.backup_ppm_correction,
                    "backup_ppm_correction",
                    config.backup_host,
                    config.backup_port,
                )
            )
        self._active = 0

        # Decoded message lines go to this callback through a queue, so slow
        # geocoding/HA requests can't stall the reader and let the SDR pipe
        # overflow (dropping messages)
        self.on_line: Callable[[str], Awaitable[None]] | None = None
        self._line_queue: asyncio.Queue[str] = asyncio.Queue(maxsize=1000)
        self._worker_task: asyncio.Task | None = None

        self._process: asyncio.subprocess.Process | None = None
        self._running = False
        self._restart_requested = False

        # ppm calibration: overrides the active source's ppm while it runs
        self._ppm_override: int | None = None
        self._calibration_task: asyncio.Task | None = None
        self.calibration: dict | None = None
        self._calibration_frames = 0
        self._calibration_failed = 0

        # Last fatal SDR error (e.g. "No supported devices found"), shown in the UI
        self.last_error = ""

    @property
    def source(self) -> Source:
        return self.sources[self._active]

    @property
    def device_type(self) -> str:
        source = self.source
        if source.receiver_type == "network":
            return f"Network ({source.host}:{source.port})"
        return {"rtl-sdr": "RTL-SDR", "hackrf": "HackRF", "soapysdr": "SoapySDR"}[source.receiver_type]

    @property
    def device_driver(self) -> str:
        return {"network": "rx_fm (rtl_tcp)", "rtl-sdr": "rtl_fm"}.get(self.source.receiver_type, "rx_fm")

    @property
    def ppm(self) -> int:
        return self.source.ppm if self._ppm_override is None else self._ppm_override

    def validate(self) -> None:
        """Raise RuntimeError for receiver settings that can't work."""
        for source in self.sources:
            if source.receiver_type == "network" and not source.host:
                raise RuntimeError("Network receiver requires network_host to be set")
        if self.config.bias_tee and self.config.receiver_type != "rtl-sdr":
            raise RuntimeError("bias_tee is only supported with receiver type rtl-sdr")

    @property
    def command(self) -> str:
        """The SDR | multimon-ng command for the active source and ppm."""
        source = self.source
        freq = self.config.frequency
        rate = self.config.sample_rate
        gain = self.config.gain
        tuning = f"-f {freq} -M fm -s {rate} -p {self.ppm}"
        multimon = f"multimon-ng -a {self.config.decoder} -v {MULTIMON_VERBOSITY} -t raw -"

        # Gain 0 means automatic gain control: rtl_fm/rx_fm only use AGC when
        # -g is left out ("-g 0" would be a manual gain of ~0 dB)
        gain_arg = f" -g {gain}" if gain else ""

        if source.receiver_type == "network":
            # rtl_tcp server via SoapyRTLTCP (osmocom rtl_fm has no rtl_tcp client).
            # SoapyRTLTCP needs IPv6 addresses in brackets to split off the port.
            host = source.host
            address = f"[{host}]:{source.port}" if ":" in host and not host.startswith("[") else f"{host}:{source.port}"
            device = shlex.quote(f"driver=rtltcp,rtltcp={address}")
            return f"rx_fm -d {device}{gain_arg} {tuning} | {multimon}"

        if source.receiver_type == "hackrf":
            # Separate gain stages: LNA (0-40), VGA (0-62), AMP (0=off/1=on, ~14dB when enabled)
            amp = 1 if self.config.hackrf_amp_enable else 0
            gain_str = f"LNA={self.config.hackrf_lna_gain},VGA={self.config.hackrf_vga_gain},AMP={amp}"
            return f'rx_fm -d driver=hackrf -g "{gain_str}" {tuning} | {multimon}'

        if source.receiver_type == "soapysdr":
            # SoapySDR devices may not support auto gain
            return f"rx_fm -g {gain or 40} {tuning} | {multimon}"

        bias_tee = " -T" if self.config.bias_tee else ""
        return f"rtl_fm{gain_arg}{bias_tee} {tuning} | {multimon}"

    @property
    def is_running(self) -> bool:
        """True when the decoder pipeline process is alive and being read."""
        return self._running and self._process is not None and self._process.returncode is None

    async def start(self) -> None:
        """Start the decoder pipeline and supervise it: restart on exit, fail over when silent."""
        logger.info(f"Starting decoder for {self.config.network_name}")

        if self.source.receiver_type == "hackrf":
            await self._check_and_reset_hackrf()

        self._running = True
        self._worker_task = asyncio.create_task(self._process_queue())

        backoff = 5  # Seconds between restarts, doubles up to 60 on repeated failures
        while self._running:
            started = time.time()
            outcome = "exit"
            try:
                outcome = await self._run_process()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Decoder error: {e}")

            if not self._running:
                break  # Intentional stop() - do not restart
            if outcome == "restart":
                continue

            if outcome == "stalled":
                logger.warning(
                    f"No FLEX frames from the {self.source.label} receiver for "
                    f"{self.config.offline_after_minutes} minutes"
                )
            elif self.stats.last_activity and self.stats.last_activity >= started:
                backoff = 5  # The pipeline did decode, so it basically works
            self._switch_source()

            logger.warning(f"Decoder pipeline stopped, restarting in {backoff}s...")
            try:
                await asyncio.sleep(backoff)
            except asyncio.CancelledError:
                break
            backoff = min(backoff * 2, 60)

        logger.info("Decoder stopped")

    def _switch_source(self) -> None:
        """Move on to the next receiver (the same one again when there is no backup)."""
        if len(self.sources) == 1 or self._calibration_task:
            return
        self._active = (self._active + 1) % len(self.sources)
        logger.warning(f"Switching to the {self.source.label} receiver: {self.device_type}")

    async def _process_queue(self) -> None:
        """Hand decoded message lines to on_line, one at a time."""
        while True:
            line = await self._line_queue.get()
            try:
                if self.on_line:
                    await self.on_line(line)
            except Exception:
                logger.exception("Error processing line")
            finally:
                self._line_queue.task_done()

    def _handle_line(self, line: str) -> None:
        """Account a multimon-ng output line and queue decoded messages."""
        if line.startswith("FLEX|"):
            self.last_error = ""  # Receiving data again
            if "|ALN|" not in line:
                return  # Numeric/tone-only pages carry no P2000 text
            self.stats.message()
            try:
                self._line_queue.put_nowait(line)
            except asyncio.QueueFull:
                logger.warning("Message processing is falling behind, dropping line")
        elif line.startswith("FLEX: FrameInfoWord"):
            self.last_error = ""
            self.stats.frame()
            self._calibration_frames += 1
        elif line.startswith("FLEX:"):
            if corrected := _CORRECTED_RE.match(line):
                self.stats.corrected(int(corrected.group(1)))
            elif any(marker in line for marker in _FAILED_MARKERS):
                self.stats.failed()
                self._calibration_failed += 1
            elif "Synchronisation Lost" in line:
                self.stats.sync_lost()

    async def _run_process(self) -> str:
        """Run the pipeline once. Returns why it ended: "exit", "stalled" or "restart"."""
        command = self.command
        logger.info(f"Command: {command}")
        self._process = await asyncio.create_subprocess_shell(
            command,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            # Own process group, so stopping it also stops rtl_fm/rx_fm and
            # multimon-ng instead of only the shell (leaving the SDR busy)
            start_new_session=True,
        )
        process = self._process
        stderr_task = asyncio.create_task(self._log_stderr(process))
        started = time.time()
        offline_after = self.config.offline_after_minutes * 60
        outcome = "exit"

        try:
            while self._running:
                try:
                    line = await asyncio.wait_for(process.stdout.readline(), timeout=WATCHDOG_INTERVAL)
                except TimeoutError:
                    line = None
                if line == b"":
                    if not self._restart_requested:
                        logger.warning(f"Decoder process exited with code {process.returncode}")
                    break
                if line:
                    decoded = line.decode("utf-8", errors="ignore").strip()
                    if decoded:
                        logger.debug(f"multimon-ng: {decoded[:100]}")
                        self._handle_line(decoded)
                # Watchdog: calibration judges silence itself, per candidate ppm
                last_activity = max(self.stats.last_activity or 0, started)
                if not self._calibration_task and time.time() - last_activity > offline_after:
                    outcome = "stalled"
                    break
        finally:
            if self._restart_requested:
                self._restart_requested = False
                outcome = "restart"
            if not stderr_task.done():
                stderr_task.cancel()
            # Make sure the whole pipeline is really dead before a possible restart
            if process.returncode is None:
                try:
                    await self._stop_process(process)
                except Exception as e:
                    logger.debug(f"Error stopping decoder process: {e}")
            self._process = None

        return outcome

    async def _restart(self) -> None:
        """Restart the pipeline right away (e.g. with a new ppm), without failover or backoff."""
        if self._process and self._process.returncode is None:
            self._restart_requested = True
            await self._stop_process(self._process)

    async def _log_stderr(self, process: asyncio.subprocess.Process) -> None:
        """Log stderr output for debugging."""
        while self._running:
            try:
                line = await process.stderr.readline()
                if not line:
                    break
                decoded = line.decode("utf-8", errors="ignore").strip()
                if decoded:
                    lowered = decoded.lower()
                    # Fatal SDR device errors: surface them loudly and in the UI
                    if "no supported devices found" in lowered:
                        self.last_error = (
                            "No SDR device found - check that the USB receiver is "
                            "plugged in and that the addon can access it"
                        )
                        logger.error(self.last_error)
                    elif "usb_claim_interface error" in lowered or "failed to open rtlsdr device" in lowered:
                        self.last_error = decoded[:200]
                        logger.error(f"SDR: {decoded[:200]}")
                    # Format SDR device info nicely
                    elif (
                        ("Found" in decoded and "device" in decoded)
                        or "Using" in decoded
                        or "Tuned" in decoded
                        or "tuner" in lowered
                        or "bias-T" in decoded
                        or "Sampling" in decoded
                        or "Output" in decoded
                    ):
                        logger.info(f"SDR: {decoded}")
                    else:
                        logger.debug(f"stderr: {decoded[:200]}")
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.debug(f"Error reading stderr: {e}")
                break

    # =========================================================================
    # ppm calibration
    # =========================================================================

    def start_calibration(self, span: int, step: int, dwell: int) -> None:
        """Try ppm values around the active source's ppm and find the one decoding the most frames."""
        if self._calibration_task:
            raise RuntimeError("A calibration is already running")
        if not self.is_running:
            raise RuntimeError("The decoder is not running")
        center = self.source.ppm
        candidates = list(range(center - span, center + span + 1, step))
        self.calibration = {
            "running": True,
            "source": self.source.label,
            "options_key": self.source.options_key,
            "configured_ppm": center,
            "candidates": candidates,
            "dwell": dwell,
            "current": None,
            "results": [],
            "best": None,
            "error": "",
        }
        self._calibration_task = asyncio.create_task(self._calibrate(candidates, dwell))

    async def cancel_calibration(self) -> None:
        if self._calibration_task:
            self._calibration_task.cancel()
            await asyncio.gather(self._calibration_task, return_exceptions=True)

    async def _calibrate(self, candidates: list[int], dwell: int) -> None:
        calibration = self.calibration
        try:
            for ppm in candidates:
                calibration["current"] = ppm
                self._ppm_override = ppm
                await self._restart()
                # Let the tuner settle and multimon-ng lock on before counting
                await asyncio.sleep(3)
                self._calibration_frames = 0
                self._calibration_failed = 0
                await asyncio.sleep(dwell)
                calibration["results"].append(
                    {"ppm": ppm, "frames": self._calibration_frames, "failed": self._calibration_failed}
                )
            configured = calibration["configured_ppm"]
            best = max(
                calibration["results"],
                key=lambda r: (r["frames"], -r["failed"], -abs(r["ppm"] - configured)),
            )
            calibration["best"] = best["ppm"] if best["frames"] else None
            if calibration["best"] is None:
                calibration["error"] = "No FLEX frames decoded at any ppm - check the antenna and gain"
            logger.info(f"ppm calibration finished: {calibration['results']}, best: {calibration['best']}")
        except asyncio.CancelledError:
            calibration["error"] = "Cancelled"
        finally:
            calibration["running"] = False
            calibration["current"] = None
            self._ppm_override = None
            self._calibration_task = None
            if self._running:
                await self._restart()

    async def apply_ppm(self, ppm: int) -> None:
        """Store a ppm correction in the add-on options and use it right away."""
        if self._calibration_task:
            raise RuntimeError("Wait for the calibration to finish")
        source = self.source
        options = await self.ha_client.get_addon_options()
        options.setdefault("receiver", {})[source.options_key] = ppm
        await self.ha_client.set_addon_options(options)
        source.ppm = ppm
        logger.info(f"Set {source.options_key} to {ppm}")
        await self._restart()

    # =========================================================================
    # Process control
    # =========================================================================

    async def _check_and_reset_hackrf(self) -> bool:
        """Check HackRF status and reset if stuck. Returns True if OK."""
        logger.info("Checking HackRF status...")

        # First, kill any existing rx_fm or hackrf processes that might be holding the device
        try:
            await asyncio.to_thread(subprocess.run, ["pkill", "-9", "rx_fm"], capture_output=True, timeout=2)
            await asyncio.to_thread(subprocess.run, ["pkill", "-9", "hackrf"], capture_output=True, timeout=2)
            await asyncio.sleep(1)  # Let USB settle
        except Exception:
            pass  # pkill may not exist or may fail, that's OK

        try:
            # Try hackrf_info to see if device responds
            result = await asyncio.to_thread(subprocess.run, ["hackrf_info"], capture_output=True, timeout=5)

            if result.returncode == 0 and b"Serial number" in result.stdout:
                # Check if external clock is being used (causes no reception!)
                if b"clock source=external" in result.stdout:
                    logger.warning("HackRF is using EXTERNAL clock - forcing back to INTERNAL clock!")
                    try:
                        # Reset to internal clock
                        await asyncio.to_thread(subprocess.run, ["hackrf_clock", "-i"], capture_output=True, timeout=5)
                        await asyncio.sleep(2)  # Let device reconfigure
                        logger.info("HackRF clock reset to internal")
                    except FileNotFoundError:
                        # hackrf_clock might not exist, try spiflash reset instead
                        logger.warning("hackrf_clock not available, attempting full reset...")
                        try:
                            await asyncio.to_thread(
                                subprocess.run, ["hackrf_spiflash", "-R"], capture_output=True, timeout=10
                            )
                            await asyncio.sleep(5)
                            logger.info("HackRF reset via spiflash")
                        except Exception as e:
                            logger.error(f"HackRF reset failed: {e}")
                    except Exception as e:
                        logger.warning(f"HackRF clock reset failed: {e}")
                else:
                    logger.info("HackRF responding normally (internal clock)")
                return True

            logger.warning("HackRF not responding properly, attempting reset...")

        except subprocess.TimeoutExpired:
            logger.warning("HackRF timed out, attempting reset...")
        except FileNotFoundError:
            logger.debug("hackrf_info not found, skipping check")
            return True
        except Exception as e:
            logger.warning(f"HackRF check failed: {e}")

        # Try reset methods
        try:
            # Method 1: hackrf_spiflash to reset the device
            logger.info("Attempting HackRF reset via hackrf_spiflash...")
            await asyncio.to_thread(subprocess.run, ["hackrf_spiflash", "-R"], capture_output=True, timeout=10)
            await asyncio.sleep(5)  # Wait longer for USB re-enumeration

            # Check if it worked
            result = await asyncio.to_thread(subprocess.run, ["hackrf_info"], capture_output=True, timeout=5)
            if result.returncode == 0:
                logger.info("HackRF reset successful!")
                return True

        except Exception as e:
            logger.warning(f"HackRF reset failed: {e}")

        logger.error("HackRF could not be reset - may need physical power cycle")
        return False

    @staticmethod
    def _signal_group(process: asyncio.subprocess.Process, sig: int) -> None:
        """Send a signal to the whole pipeline (shell, SDR tool and multimon-ng)."""
        with contextlib.suppress(ProcessLookupError, PermissionError):
            os.killpg(process.pid, sig)

    async def _stop_process(self, process: asyncio.subprocess.Process) -> None:
        """Stop the pipeline gracefully, force killing it if it doesn't respond."""
        self._signal_group(process, signal.SIGTERM)
        try:
            await asyncio.wait_for(process.wait(), timeout=2.0)
        except TimeoutError:
            logger.warning("Process not responding, force killing...")
            self._signal_group(process, signal.SIGKILL)
            await asyncio.wait_for(process.wait(), timeout=3.0)

    async def stop(self) -> None:
        """Stop the decoder subprocess."""
        self._running = False
        await self.cancel_calibration()

        if self._process:
            try:
                await self._stop_process(self._process)
            except Exception as e:
                logger.error(f"Error stopping process: {e}")
            finally:
                # Don't manually call feed_eof() - it causes "feed_data after feed_eof" errors
                # Let the streams close naturally when the process terminates
                self._process = None
                # Give USB device time to reset (especially for HackRF)
                if self.source.receiver_type == "hackrf":
                    logger.info("Waiting for HackRF USB reset...")
                    await asyncio.sleep(3.0)

        # Give already queued messages a moment to be published, then stop the worker
        if self._worker_task:
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(self._line_queue.join(), timeout=2.0)
            self._worker_task.cancel()
            await asyncio.gather(self._worker_task, return_exceptions=True)
            self._worker_task = None
