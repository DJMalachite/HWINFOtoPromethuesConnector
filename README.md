# HWINFOtoPromethuesConnector
Very much a vibe coded app, used to translate data from MyRSM to a scrapeable endpoint for prometheus

# HWiNFO Prometheus Exporter

A lightweight Prometheus exporter that converts hardware sensor data from HWiNFO (via Remote Sensor Monitor) into structured, queryable metrics for Prometheus and Grafana.

---

## 🚀 Overview

This project allows you to monitor real-time hardware metrics from a Windows machine (CPU, GPU, memory, drives, etc.) using:

* **HWiNFO + qdel's Remote Sensor Monitor (MyRSM)[https://github.com/qdel/myrsm]** as the data source
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

| Variable        | Default                  | Description     |
| --------------- | ------------------------ | --------------- |
| `HWI_URL`       | `http://127.0.0.1:34567` | RSM endpoint    |
| `LISTEN_HOST`   | `0.0.0.0`                | Bind address    |
| `LISTEN_PORT`   | `10445`                  | Exporter port   |
| `POLL_INTERVAL` | `1`                      | Poll frequency  |
| `HTTP_TIMEOUT`  | `2`                      | Request timeout |
| `EXPORTER_HOST` | system hostname          | Host label      |

---

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


