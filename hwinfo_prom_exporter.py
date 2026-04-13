#!/usr/bin/env python3
import json
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

def log(msg: str) -> None:
    if LOG_LEVEL in ("DEBUG", "INFO"):
        print(msg, flush=True)

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
        self.up: int = 0
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
            # Skip non-numeric values because Prometheus samples must be numeric
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

    # Add occurrence label when multiple identical label sets appear
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

        if not parsed_rows:
            raise RuntimeError("Parsed zero numeric metrics from HWiNFO JSON")

        with state.lock:
            state.rows = parsed_rows
            state.last_success_ts = time.time()
            state.last_attempt_ts = time.time()
            state.last_poll_duration_seconds = time.time() - started
            state.last_error = ""
            state.last_http_status = status_code
            state.up = 1
            state.successful_polls += 1
            state.raw_items_last = raw_count
            state.exported_items_last = len(parsed_rows)

    except Exception as exc:
        try:
            session.close()
        except Exception:
            pass
        session = build_session()

        with state.lock:
            state.last_attempt_ts = time.time()
            state.last_poll_duration_seconds = time.time() - started
            state.last_error = str(exc)
            state.last_http_status = status_code
            state.up = 0
            state.failed_polls += 1
            state.raw_items_last = raw_count
            state.exported_items_last = len(parsed_rows)

        if LOG_LEVEL in ("DEBUG", "INFO"):
            print(f"[poll error] {exc}", flush=True)

def polling_loop() -> None:
    log(f"Polling {HWI_URL} every {POLL_INTERVAL}s with timeout {HTTP_TIMEOUT}s")
    log(f"Fast retry while down: {DOWN_RETRY_INTERVAL}s")

    while not stop_event.is_set():
        poll_once()

        with state.lock:
            is_up = bool(state.up)

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
            up = state.up
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
        stale = 1 if age_seconds > (POLL_INTERVAL * 2) else 0

        # Health metrics
        g = GaugeMetricFamily(f"{prefix}_exporter_up", "1 if the last HWiNFO poll succeeded, else 0")
        g.add_metric([], up)
        yield g

        g = GaugeMetricFamily(f"{prefix}_exporter_stale", "1 if cached data is stale")
        g.add_metric([], stale)
        yield g

        g = GaugeMetricFamily(f"{prefix}_exporter_data_age_seconds", "Age of the last successful HWiNFO sample")
        g.add_metric([], age_seconds)
        yield g

        g = GaugeMetricFamily(f"{prefix}_exporter_last_success_timestamp_seconds", "Unix timestamp of the last successful poll")
        g.add_metric([], last_success_ts if last_success_ts > 0 else 0)
        yield g

        g = GaugeMetricFamily(f"{prefix}_exporter_last_attempt_timestamp_seconds", "Unix timestamp of the last poll attempt")
        g.add_metric([], last_attempt_ts if last_attempt_ts > 0 else 0)
        yield g

        g = GaugeMetricFamily(f"{prefix}_exporter_last_poll_duration_seconds", "Duration of the last poll")
        g.add_metric([], last_poll_duration_seconds)
        yield g

        g = GaugeMetricFamily(f"{prefix}_exporter_last_http_status", "Last HTTP status seen from HWiNFO source")
        g.add_metric([], float(last_http_status))
        yield g

        g = GaugeMetricFamily(f"{prefix}_exporter_successful_polls_total", "Total successful HWiNFO polls")
        g.add_metric([], float(successful_polls))
        yield g

        g = GaugeMetricFamily(f"{prefix}_exporter_failed_polls_total", "Total failed HWiNFO polls")
        g.add_metric([], float(failed_polls))
        yield g

        g = GaugeMetricFamily(f"{prefix}_exporter_raw_items_last", "Number of raw items in the last payload")
        g.add_metric([], float(raw_items_last))
        yield g

        g = GaugeMetricFamily(f"{prefix}_exporter_exported_items_last", "Number of exported numeric sensor items in the last payload")
        g.add_metric([], float(exported_items_last))
        yield g

        # Sensor metrics
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
            "1 if the sensor was present in the last successful payload",
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
    server_version = "HWiNFOPromExporter/2.1"

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
                    "hwinfo_url": HWI_URL,
                    "listen": f"{LISTEN_HOST}:{LISTEN_PORT}",
                    "metric_prefix": sanitize_name(METRIC_PREFIX),
                },
                indent=2,
            ).encode("utf-8")
            self._send_bytes(200, "application/json; charset=utf-8", body)
            return

        if self.path == "/healthz":
            with state.lock:
                payload = {
                    "up": bool(state.up),
                    "stale": bool((time.time() - state.last_success_ts) > (POLL_INTERVAL * 2)) if state.last_success_ts else True,
                    "last_success_ts": state.last_success_ts,
                    "last_attempt_ts": state.last_attempt_ts,
                    "last_poll_duration_seconds": state.last_poll_duration_seconds,
                    "last_http_status": state.last_http_status,
                    "last_error": state.last_error,
                    "successful_polls": state.successful_polls,
                    "failed_polls": state.failed_polls,
                    "raw_items_last": state.raw_items_last,
                    "exported_items_last": state.exported_items_last,
                }

            status = 200 if payload["up"] else 503
            body = json.dumps(payload, indent=2).encode("utf-8")
            self._send_bytes(status, "application/json; charset=utf-8", body)
            return

        if self.path == "/metrics":
            body = generate_latest(REGISTRY)
            self._send_bytes(200, CONTENT_TYPE_LATEST, body)
            return

        self._send_bytes(404, "text/plain; charset=utf-8", b"Not found\n")

    def log_message(self, format: str, *args: Any) -> None:
        if LOG_LEVEL == "DEBUG":
            super().log_message(format, *args)

# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------

collector_registered = False

def handle_signal(signum, frame):
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

    print(f"Exporter listening on http://{LISTEN_HOST}:{LISTEN_PORT}", flush=True)
    print(f"HWiNFO URL: {HWI_URL}", flush=True)
    print("Endpoints: /metrics /healthz", flush=True)

    try:
        server.serve_forever()
    finally:
        server.server_close()

if __name__ == "__main__":
    main()