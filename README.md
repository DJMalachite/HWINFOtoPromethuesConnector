### Disclaimer:  Very much a vibe coded app, needed something quick and easy to translate data from MyRSM to a scrapeable endpoint for prometheus and my own dashboards, if you find this and decide to use it please understand that it is ai made...

# HWiNFO Prometheus Exporter

A lightweight Prometheus exporter that converts hardware sensor data from HWiNFO (via Remote Sensor Monitor) into structured, queryable metrics for Prometheus and Grafana.

---

## 🚀 Overview

This project allows you to monitor real-time hardware metrics from a Windows machine (CPU, GPU, memory, drives, etc.) using:

* **HWiNFO + qdel's Remote Sensor Monitor [MyRSM](https://github.com/qdel/myrsm)** as the data source
* **Prometheus** for scraping and storage
* **Grafana** for visualisation

The exporter polls a JSON endpoint exposed by RSM and converts each sensor into a Prometheus metric with rich labels.

---

## 🧠 How It Works

1. **HWiNFO** collects hardware sensor data
2. **Remote Sensor Monitor (MyRSM)** exposes this data via HTTP JSON
3. This exporter:

   * Polls the JSON endpoint
   * Cleans and normalises the data
   * Converts sensors into Prometheus metrics
4. **Prometheus** scrapes the exporter
5. **Grafana** visualises the data

---

## 📊 Metric Format

All sensors are exposed under a single metric:

```
hwinfo_sensor_value
```

With labels:

| Label             | Description                         |
| ----------------- | ----------------------------------- |
| `host`            | Hostname of the exporter            |
| `sensor_app`      | Source application (HWiNFO, etc.)   |
| `sensor_class`    | Hardware category (CPU, GPU, etc.)  |
| `sensor_name`     | Name of the sensor                  |
| `sensor_unit`     | Normalised unit (celsius, watts...) |
| `sensor_unit_raw` | Raw unit from HWiNFO                |
| `occurrence`      | Distinguishes duplicate sensors     |

---

## 🔍 Example Queries (PromQL)

### All temperature sensors

```
hwinfo_sensor_value{sensor_unit="celsius"}
```

### GPU temperatures

```
hwinfo_sensor_value{sensor_class=~"GPU.*", sensor_unit="celsius"}
```

### CPU usage

```
hwinfo_sensor_value{sensor_name="Total CPU Usage"}
```

---

## ⚙️ Requirements

* Windows machine running:

  * HWiNFO
  * Remote Sensor Monitor By qdel https://github.com/qdel/myrsm
* Python 3.9+ or Docker
* Prometheus
* Grafana (optional but recommended)

---

## 🛠️ Configuration

Environment variables:

| Variable                 | Default                  | Description |
| ------------------------ | ------------------------ | ----------- |
| `HWI_URL`                | `http://127.0.0.1:34567` | RSM endpoint |
| `LISTEN_HOST`            | `0.0.0.0`                | Bind address |
| `LISTEN_PORT`            | `10445`                  | Exporter port |
| `POLL_INTERVAL`          | `0.3`                    | Normal poll interval (seconds) while source is healthy |
| `HTTP_TIMEOUT`           | `2`                      | HTTP timeout (seconds) for source requests |
| `DOWN_RETRY_INTERVAL`    | `2`                      | Faster retry interval (seconds) while source is down |
| `REQUEST_RETRIES`        | `1`                      | Number of request retries per poll cycle |
| `DATA_FRESHNESS_TIMEOUT` | `20`                     | Data older than this is considered stale and sensor series are withheld |
| `METRIC_PREFIX`          | `hwinfo`                 | Prefix for all exported metric names |
| `EXPORTER_HOST`          | system hostname          | `host` label value for sensor metrics |
| `LOG_LEVEL`              | `INFO`                   | Log level (`DEBUG`, `INFO`, etc.) |
| `INCLUDE_SENSORS`        | (empty)                  | Comma-separated sensor-name substring allowlist |
| `EXCLUDE_SENSORS`        | (empty)                  | Comma-separated sensor-name substring denylist |
| `INCLUDE_CLASSES`        | (empty)                  | Comma-separated sensor-class substring allowlist |
| `EXCLUDE_CLASSES`        | (empty)                  | Comma-separated sensor-class substring denylist |
| `INCLUDE_APPS`           | (empty)                  | Comma-separated sensor-app substring allowlist |
| `EXCLUDE_APPS`           | (empty)                  | Comma-separated sensor-app substring denylist |

---

## 🔀 Changes included from merged PRs

Recent merges added operational and reliability improvements that are now reflected in this README:

* **Stale metric protection:** when source data is older than `DATA_FRESHNESS_TIMEOUT`, sensor time series are omitted from `/metrics` to avoid exporting stale values.
* **Health model update:** `hwinfo_exporter_up` now reflects **source reachability + data freshness** rather than just process liveness.
* **Additional exporter health metrics:** source status, staleness, data age, poll timings, HTTP status, and poll counters are exported for alerting and troubleshooting.
* **Source downtime behavior:** while down, the exporter uses `DOWN_RETRY_INTERVAL` for faster reconnect attempts.
* **CI for Docker images:** GitHub Actions now supports CI build checks and GHCR publishing on `main` pushes and version tags.

### New/important health metrics

```
hwinfo_exporter_up
hwinfo_exporter_source_up
hwinfo_exporter_stale
hwinfo_exporter_data_age_seconds
hwinfo_exporter_last_http_status
hwinfo_exporter_successful_polls_total
hwinfo_exporter_failed_polls_total
```

---


## 🤖 CI/CD: Build and Publish Docker image

This repo includes a GitHub Actions workflow at `.github/workflows/docker-image.yml` that:

* Builds the container image on every PR to `main` (build only, no push)
* Builds **and pushes** on pushes to `main`
* Builds **and pushes** for version tags like `v1.2.3`
* Publishes to **GitHub Container Registry (GHCR)** as:

```
ghcr.io/<owner>/<repo>:latest
ghcr.io/<owner>/<repo>:<branch>
ghcr.io/<owner>/<repo>:<git-sha>
```

### Portainer usage

In Portainer, use this image in your stack:

```yaml
services:
  hwinfo-exporter:
    image: ghcr.io/<owner>/<repo>:latest
    restart: unless-stopped
    ports:
      - "10445:10445"
    environment:
      HWI_URL: "http://YOUR_WINDOWS_HOST:55555/"
      EXPORTER_HOST: "your-hostname"
      LISTEN_HOST: "0.0.0.0"
      LISTEN_PORT: "10445"
      POLL_INTERVAL: "1"
      HTTP_TIMEOUT: "1"
      DOWN_RETRY_INTERVAL: "2"
      LOG_LEVEL: "INFO"
```

> If your GHCR package is private, add registry credentials in Portainer (`ghcr.io`, GitHub username, and a PAT with `read:packages`).

## 🐳 Docker Usage

Example Dockerfile:

```dockerfile
FROM python:3.11-slim

WORKDIR /app
COPY hwinfo_prom_exporter.py .

RUN pip install prometheus_client requests

ENV LANG=C.UTF-8
ENV LC_ALL=C.UTF-8

CMD ["python", "hwinfo_prom_exporter.py"]
```

---

## 📡 Prometheus Config

```yaml
scrape_configs:
  - job_name: 'hwinfo'
    static_configs:
      - targets: ['your-host:10445']
```

---

## ⚠️ Notes

* Only **numeric sensor values** are exported
* Non-numeric values (e.g. "Yes/No") are ignored or converted
* Units are normalised to allow clean querying in Grafana
* Encoding issues (e.g. `Â°C`) are automatically corrected

---

## 🧩 Future Improvements

* Sensor filtering via config
* Alerting templates
* Multi-host aggregation
* Pushgateway support
* Auto-discovery of sensors

---

## 📄 License

MIT

---

## 🙌 Credits

* HWiNFO
* qdel MyRSM (Remote Sensor Monitor)
* Prometheus ecosystem

