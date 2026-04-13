#!/usr/bin/env python3
"""
Improved HWiNFO -> Prometheus exporter.

Summary of key behavior changes:
- Prevents stale sensor values from being exported when source data is too old.
- Exposes `hwinfo_exporter_up` as real source-connectivity health (not only process health).
- Adds configurable freshness timeout and structured logging.
"""

import json
import logging
import math
import os
import re
import signal
import socket
import threading
import time
from collections import Counter
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Dict, List, Optional, Tuple

import requests
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from prometheus_client.core import GaugeMetricFamily, REGISTRY

# -----------------------------------------------------------------------------
# Config
# -----------------------------------------------------------------------------

HWI_URL = os.getenv("HWI_URL", "http://127.0.0.1:34567")
LISTEN_HOST = os.getenv("LISTEN_HOST", "0.0.0.0")
LISTEN_PORT = int(os.getenv("LISTEN_PORT", "10445"))

POLL_INTERVAL = float(os.getenv("POLL_INTERVAL", "0.3"))
HTTP_TIMEOUT = float(os.getenv("HTTP_TIMEOUT", "2"))
DOWN_RETRY_INTERVAL = float(os.getenv("DOWN_RETRY_INTERVAL", "2"))
REQUEST_RETRIES = int(os.getenv("REQUEST_RETRIES", "1"))

# Freshness timeout is intentionally independent of poll interval.
# If no successful poll arrives within this window, sensor data is treated as stale
# and omitted from /metrics so Prometheus does not scrape old values as current.
DATA_FRESHNESS_TIMEOUT = float(os.getenv("DATA_FRESHNESS_TIMEOUT", "20"))

EXPORTER_HOST = os.getenv("EXPORTER_HOST", socket.gethostname())
METRIC_PREFIX = os.getenv("METRIC_PREFIX", "hwinfo")
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

INCLUDE_SENSORS = [x.strip().lower() for x in os.getenv("INCLUDE_SENSORS", "").split(",") if x.strip()]
EXCLUDE_SENSORS = [x.strip().lower() for x in os.getenv("EXCLUDE_SENSORS", "").split(",") if x.strip()]

INCLUDE_CLASSES = [x.strip().lower() for x in os.getenv("INCLUDE_CLASSES", "").split(",") if x.strip()]
EXCLUDE_CLASSES = [x.strip().lower() for x in os.getenv("EXCLUDE_CLASSES", "").split(",") if x.strip()]

INCLUDE_APPS = [x.strip().lower() for x in os.getenv("INCLUDE_APPS", "").split(",") if x.strip()]
EXCLUDE_APPS = [x.strip().lower() for x in os.getenv("EXCLUDE_APPS", "").split(",") if x.strip()]

# -----------------------------------------------------------------------------
# Logging
# -----------------------------------------------------------------------------

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger("hwinfo_exporter")

# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------

MULTI_UNDERSCORE_RE = re.compile(r"_+")
SAFE_NAME_RE = re.compile(r"[^a-zA-Z0-9_]")

UNIT_NORMALIZATION = {
    "°C": "celsius",
    "C": "celsius",
    "%": "percent",
    "V": "volts",
    "mV": "millivolts",
    "A": "amps",
    "W": "watts",
    "RPM": "rpm",
    "MHz": "mhz",
    "GHz": "ghz",
    "FPS": "fps",
    "fps": "fps",
    "MB": "megabytes",
    "GB": "gigabytes",
    "KB": "kilobytes",
    "MB/s": "megabytes_per_second",
    "GB/s": "gigabytes_per_second",
    "KB/s": "kilobytes_per_second",
    "ms": "milliseconds",
    "GT/s": "gigatransfers_per_second",
    "x": "ratio",
    "T": "ticks",
    "Yes/No": "bool",
    "": "none",
}


def sanitize_name(value: str) -> str:
    value = str(value).strip().lower()
    value = value.replace("/", "_per_")
    value = value.replace("+", "plus")
    value = value.replace("#", "num")
    value = value.replace("@", "at")
    value = value.replace("-", "_")
    value = value.replace(" ", "_")
    value = SAFE_NAME_RE.sub("_", value)
    value = MULTI_UNDERSCORE_RE.sub("_", value).strip("_")
    return value or "unknown"


def clean_text(value: Any) -> str:
    if value is None:
        return ""

    text = str(value)

    # Fix common mojibake from Windows/HTTP encoding mismatches.
    try:
        repaired = text.encode("latin1", "strict").decode("utf-8", "strict")
        text = repaired
    except (UnicodeEncodeError, UnicodeDecodeError):
        pass

    text = text.replace("Â°C", "°C")
    text = text.replace("\x00", "")

    return text.strip()


def normalize_unit_raw(unit: Any) -> str:
    return clean_text(unit)


def normalize_unit(unit: Any) -> str:
    raw = normalize_unit_raw(unit)
    return UNIT_NORMALIZATION.get(raw, sanitize_name(raw))


def safe_label(value: Any) -> str:
    return clean_text(value)


def parse_bool_like(text: str) -> Optional[float]:
    lowered = text.strip().lower()
    if lowered in ("yes", "true", "on", "enabled"):
        return 1.0
    if lowered in ("no", "false", "off", "disabled"):
        return 0.0
    return None


def safe_float(value: Any) -> Optional[float]:
    if value is None:
        return None

    if isinstance(value, (int, float)):
        val = float(value)
        if math.isnan(val) or math.isinf(val):
            return None
        return val

    text = clean_text(value)
    if not text:
        return None

    bool_val = parse_bool_like(text)
    if bool_val is not None:
        return bool_val

    text = text.replace(",", "").strip()

    try:
        val = float(text)
    except ValueError:
        return None

    if math.isnan(val) or math.isinf(val):
        return None
    return val


def token_match(value: str, includes: List[str], excludes: List[str]) -> bool:
    lowered = value.lower()

    if includes and not any(token in lowered for token in includes):
        return False

    if excludes and any(token in lowered for token in excludes):
        return False

    return True


def should_include(sensor_name: str, sensor_class: str, sensor_app: str) -> bool:
    if not token_match(sensor_name, INCLUDE_SENSORS, EXCLUDE_SENSORS):
        return False
    if not token_match(sensor_class, INCLUDE_CLASSES, EXCLUDE_CLASSES):
        return False
    if not token_match(sensor_app, INCLUDE_APPS, EXCLUDE_APPS):
        return False
    return True


# -----------------------------------------------------------------------------
# Shared state
# -----------------------------------------------------------------------------

class ExporterState:
    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.rows: List[Dict[str, Any]] = []
        self.last_success_ts: float = 0.0
        self.last_attempt_ts: float = 0.0
        self.last_poll_duration_seconds: float = 0.0
        self.last_error: str = ""
        self.last_http_status: int = 0
        self.source_up: int = 0
        self.successful_polls: int = 0
        self.failed_polls: int = 0
        self.raw_items_last: int = 0
        self.exported_items_last: int = 0


state = ExporterState()
stop_event = threading.Event()

# -----------------------------------------------------------------------------
# Parsing
# -----------------------------------------------------------------------------


def parse_hwinfo_payload(payload: Any) -> Tuple[List[Dict[str, Any]], int]:
    if not isinstance(payload, list):
        raise ValueError("HWiNFO payload is not a top-level JSON list")

    raw_count = len(payload)
    parsed_rows: List[Dict[str, Any]] = []

    for item in payload:
        if not isinstance(item, dict):
            continue

        sensor_app = safe_label(item.get("SensorApp", ""))
        sensor_class = safe_label(item.get("SensorClass", ""))
        sensor_name = safe_label(item.get("SensorName", ""))
        sensor_unit_raw = normalize_unit_raw(item.get("SensorUnit", ""))
        sensor_unit = normalize_unit(item.get("SensorUnit", ""))
        sensor_update_time = safe_float(item.get("SensorUpdateTime"))
        sensor_value = safe_float(item.get("SensorValue"))

        if not sensor_name:
            continue

        if not should_include(sensor_name, sensor_class, sensor_app):
            continue

        if sensor_value is None:
            # Prometheus samples must be numeric.
            continue

        parsed_rows.append(
            {
                "sensor_app": sensor_app,
                "sensor_class": sensor_class,
                "sensor_name": sensor_name,
                "sensor_unit": sensor_unit,
                "sensor_unit_raw": sensor_unit_raw,
                "sensor_update_time": sensor_update_time if sensor_update_time is not None else 0.0,
                "sensor_value": sensor_value,
            }
        )

    # Add occurrence label when multiple identical label sets appear.
    key_counts = Counter(
        (
            row["sensor_app"],
            row["sensor_class"],
            row["sensor_name"],
            row["sensor_unit"],
            row["sensor_unit_raw"],
        )
        for row in parsed_rows
    )

    occurrence_seen: Counter = Counter()
    final_rows: List[Dict[str, Any]] = []

    for row in parsed_rows:
        base_key = (
            row["sensor_app"],
            row["sensor_class"],
            row["sensor_name"],
            row["sensor_unit"],
            row["sensor_unit_raw"],
        )
        occurrence_seen[base_key] += 1
        occurrence = occurrence_seen[base_key]

        row = dict(row)
        row["occurrence"] = str(occurrence) if key_counts[base_key] > 1 else "1"
        final_rows.append(row)

    return final_rows, raw_count


# -----------------------------------------------------------------------------
# Polling / reconnect
# -----------------------------------------------------------------------------


def build_session() -> requests.Session:
    s = requests.Session()
    adapter = requests.adapters.HTTPAdapter(
        pool_connections=4,
        pool_maxsize=4,
        max_retries=REQUEST_RETRIES,
    )
    s.mount("http://", adapter)
    s.mount("https://", adapter)
    return s


session = build_session()


def _expired(now: float, last_success_ts: float) -> bool:
    return (last_success_ts <= 0) or ((now - last_success_ts) > DATA_FRESHNESS_TIMEOUT)


def poll_once() -> None:
    global session

    started = time.time()
    status_code = 0
    parsed_rows: List[Dict[str, Any]] = []
    raw_count = 0

    try:
        response = session.get(HWI_URL, timeout=HTTP_TIMEOUT)
        status_code = response.status_code
        response.raise_for_status()
        response.encoding = "utf-8"

        payload = json.loads(response.text)
        parsed_rows, raw_count = parse_hwinfo_payload(payload)

        # Empty numeric payload should be considered a source/data failure.
        if not parsed_rows:
            raise RuntimeError("Parsed zero numeric metrics from HWiNFO JSON")

        now = time.time()
        with state.lock:
            state.rows = parsed_rows
            state.last_success_ts = now
            state.last_attempt_ts = now
            state.last_poll_duration_seconds = now - started
            state.last_error = ""
            state.last_http_status = status_code
            state.source_up = 1
            state.successful_polls += 1
            state.raw_items_last = raw_count
            state.exported_items_last = len(parsed_rows)

    except Exception as exc:
        # Rebuild session for robust recovery after socket/network failures.
        try:
            session.close()
        except Exception:
            pass
        session = build_session()

        now = time.time()
        with state.lock:
            state.last_attempt_ts = now
            state.last_poll_duration_seconds = now - started
            state.last_error = str(exc)
            state.last_http_status = status_code
            state.source_up = 0
            state.failed_polls += 1
            state.raw_items_last = raw_count
            state.exported_items_last = len(parsed_rows)

            # Key stale-data fix:
            # If data is beyond freshness timeout, drop cached sensor rows so
            # they cannot be emitted as fresh samples.
            if _expired(now, state.last_success_ts):
                state.rows = []

        logger.warning("Poll failure from %s: %s", HWI_URL, exc)


def polling_loop() -> None:
    logger.info(
        "Polling %s every %.2fs (timeout %.2fs, freshness_timeout %.2fs)",
        HWI_URL,
        POLL_INTERVAL,
        HTTP_TIMEOUT,
        DATA_FRESHNESS_TIMEOUT,
    )
    logger.info("Fast retry while down: %.2fs", DOWN_RETRY_INTERVAL)

    while not stop_event.is_set():
        poll_once()

        with state.lock:
            is_up = bool(state.source_up)

        wait_time = POLL_INTERVAL if is_up else DOWN_RETRY_INTERVAL
        stop_event.wait(wait_time)


# -----------------------------------------------------------------------------
# Prometheus collector
# -----------------------------------------------------------------------------

class HwinfoCollector:
    def collect(self):
        prefix = sanitize_name(METRIC_PREFIX)

        with state.lock:
            rows_snapshot = list(state.rows)
            source_up = state.source_up
            last_success_ts = state.last_success_ts
            last_attempt_ts = state.last_attempt_ts
            last_poll_duration_seconds = state.last_poll_duration_seconds
            last_http_status = state.last_http_status
            successful_polls = state.successful_polls
            failed_polls = state.failed_polls
            raw_items_last = state.raw_items_last
            exported_items_last = state.exported_items_last

        now = time.time()
        age_seconds = (now - last_success_ts) if last_success_ts > 0 else 1e30
        stale = 1 if _expired(now, last_success_ts) else 0

        # Connectivity metric for Prometheus alerting.
        # Healthy only if the source is reachable *and* data is fresh.
        exporter_up_value = 1 if (source_up == 1 and stale == 0) else 0

        g = GaugeMetricFamily(
            f"{prefix}_exporter_up",
            "1 if source is reachable and data is fresh, else 0",
        )
        g.add_metric([], exporter_up_value)
        yield g

        g = GaugeMetricFamily(f"{prefix}_exporter_source_up", "1 if last source poll succeeded, else 0")
        g.add_metric([], source_up)
        yield g

        g = GaugeMetricFamily(f"{prefix}_exporter_stale", "1 if cached data is stale")
        g.add_metric([], stale)
        yield g

        g = GaugeMetricFamily(f"{prefix}_exporter_data_age_seconds", "Age of the last successful HWiNFO sample")
        g.add_metric([], age_seconds)
        yield g

        g = GaugeMetricFamily(
            f"{prefix}_exporter_freshness_timeout_seconds",
            "Configured freshness timeout; data older than this is not exported",
        )
        g.add_metric([], DATA_FRESHNESS_TIMEOUT)
        yield g

        g = GaugeMetricFamily(
            f"{prefix}_exporter_last_success_timestamp_seconds",
            "Unix timestamp of the last successful poll",
        )
        g.add_metric([], last_success_ts if last_success_ts > 0 else 0)
        yield g

        g = GaugeMetricFamily(
            f"{prefix}_exporter_last_attempt_timestamp_seconds",
            "Unix timestamp of the last poll attempt",
        )
        g.add_metric([], last_attempt_ts if last_attempt_ts > 0 else 0)
        yield g

        g = GaugeMetricFamily(f"{prefix}_exporter_last_poll_duration_seconds", "Duration of the last poll")
        g.add_metric([], last_poll_duration_seconds)
        yield g

        g = GaugeMetricFamily(f"{prefix}_exporter_last_http_status", "Last HTTP status from source")
        g.add_metric([], float(last_http_status))
        yield g

        g = GaugeMetricFamily(f"{prefix}_exporter_successful_polls_total", "Total successful source polls")
        g.add_metric([], float(successful_polls))
        yield g

        g = GaugeMetricFamily(f"{prefix}_exporter_failed_polls_total", "Total failed source polls")
        g.add_metric([], float(failed_polls))
        yield g

        g = GaugeMetricFamily(f"{prefix}_exporter_raw_items_last", "Number of raw items in last source payload")
        g.add_metric([], float(raw_items_last))
        yield g

        g = GaugeMetricFamily(
            f"{prefix}_exporter_exported_items_last",
            "Number of exported numeric items in last successful payload",
        )
        g.add_metric([], float(exported_items_last))
        yield g

        # Main stale-data protection at scrape-time:
        # If stale, do not export sensor series at all.
        if stale == 1:
            return

        label_keys = [
            "host",
            "sensor_app",
            "sensor_class",
            "sensor_name",
            "sensor_unit",
            "sensor_unit_raw",
            "occurrence",
        ]

        value_family = GaugeMetricFamily(
            f"{prefix}_sensor_value",
            "Numeric sensor value from HWiNFO",
            labels=label_keys,
        )

        update_family = GaugeMetricFamily(
            f"{prefix}_sensor_update_time_seconds",
            "SensorUpdateTime from HWiNFO",
            labels=label_keys,
        )

        present_family = GaugeMetricFamily(
            f"{prefix}_sensor_present",
            "1 if sensor was present in the most recent fresh payload",
            labels=label_keys,
        )

        for row in rows_snapshot:
            label_values = [
                EXPORTER_HOST,
                row["sensor_app"],
                row["sensor_class"],
                row["sensor_name"],
                row["sensor_unit"],
                row["sensor_unit_raw"],
                row["occurrence"],
            ]

            value_family.add_metric(label_values, row["sensor_value"])
            update_family.add_metric(label_values, row["sensor_update_time"])
            present_family.add_metric(label_values, 1.0)

        yield value_family
        yield update_family
        yield present_family


# -----------------------------------------------------------------------------
# HTTP server
# -----------------------------------------------------------------------------

class RequestHandler(BaseHTTPRequestHandler):
    server_version = "HWiNFOPromExporter/2.2"

    def _send_bytes(self, status: int, content_type: str, body: bytes) -> None:
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self) -> None:
        if self.path in ("/", ""):
            body = json.dumps(
                {
                    "name": "hwinfo_prom_exporter",
                    "metrics_path": "/metrics",
                    "health_path": "/healthz",
                    "ready_path": "/readyz",
                    "hwinfo_url": HWI_URL,
                    "listen": f"{LISTEN_HOST}:{LISTEN_PORT}",
                    "metric_prefix": sanitize_name(METRIC_PREFIX),
                    "freshness_timeout_seconds": DATA_FRESHNESS_TIMEOUT,
                },
                indent=2,
            ).encode("utf-8")
            self._send_bytes(200, "application/json; charset=utf-8", body)
            return

        if self.path in ("/healthz", "/readyz"):
            with state.lock:
                now = time.time()
                stale = _expired(now, state.last_success_ts)
                payload = {
                    "source_up": bool(state.source_up),
                    "exporter_up": bool(state.source_up and not stale),
                    "stale": stale,
                    "last_success_ts": state.last_success_ts,
                    "last_attempt_ts": state.last_attempt_ts,
                    "last_poll_duration_seconds": state.last_poll_duration_seconds,
                    "last_http_status": state.last_http_status,
                    "last_error": state.last_error,
                    "successful_polls": state.successful_polls,
                    "failed_polls": state.failed_polls,
                    "raw_items_last": state.raw_items_last,
                    "exported_items_last": state.exported_items_last,
                    "data_freshness_timeout_seconds": DATA_FRESHNESS_TIMEOUT,
                }

            status = 200 if payload["exporter_up"] else 503
            body = json.dumps(payload, indent=2).encode("utf-8")
            self._send_bytes(status, "application/json; charset=utf-8", body)
            return

        if self.path == "/metrics":
            body = generate_latest(REGISTRY)
            self._send_bytes(200, CONTENT_TYPE_LATEST, body)
            return

        self._send_bytes(404, "text/plain; charset=utf-8", b"Not found\n")

    def log_message(self, fmt: str, *args: Any) -> None:
        if logger.isEnabledFor(logging.DEBUG):
            super().log_message(fmt, *args)


# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------

collector_registered = False


def handle_signal(signum, frame):
    del signum, frame
    stop_event.set()


def main() -> None:
    global collector_registered

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    if not collector_registered:
        REGISTRY.register(HwinfoCollector())
        collector_registered = True

    poll_once()

    poll_thread = threading.Thread(target=polling_loop, daemon=True)
    poll_thread.start()

    server = ThreadingHTTPServer((LISTEN_HOST, LISTEN_PORT), RequestHandler)

    logger.info("Exporter listening on http://%s:%s", LISTEN_HOST, LISTEN_PORT)
    logger.info("HWiNFO URL: %s", HWI_URL)
    logger.info("Endpoints: /metrics /healthz /readyz")

    try:
        server.serve_forever()
    except Exception as exc:
        logger.exception("HTTP server fatal error: %s", exc)
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
