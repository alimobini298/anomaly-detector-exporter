# Anomaly Detector

A lightweight, self-hosted anomaly detection service for Prometheus metrics.
It queries your Prometheus instance, builds seasonal baselines (per hour-of-day,
weekday/weekend), and exposes anomaly signals as new Prometheus metrics — ready
to scrape and visualize in Grafana.

---

## Features

- **Seasonal baseline** — compares current values against the same time window
  on the same weekday type over the past N days
- **False-positive guards** — minimum absolute change, minimum relative change,
  near-constant metric suppression, and confidence dampening for new baselines
- **Per-metric config** — override `z_threshold`, `min_delta`, `min_samples`,
  and `silence` per metric in `config.ini`
- **Rich metrics** — z-score, composite score, trend slope, spike score,
  consecutive cycles, baseline confidence, health state, severity
- **Grafana dashboards** — Overview (command center) + Detail (root-cause) with
  drilldown links
- **Timezone-aware** — all time calculations use local time via `TZ` env var
- **REST endpoints** — `/status`, `/config`, `/reload`, `/healthz`

---

## Quick Start

### 1. Clone

```bash
git clone https://github.com/YOUR_USERNAME/anomaly-detector.git
cd anomaly-detector
```

### 2. Edit `config.ini`

```ini
[general]
prometheus_url   = http://localhost:9090
listen_addr      = 0.0.0.0:9091
lookback_days    = 14
window_minutes   = 30
step_seconds     = 300
z_threshold      = 3.0
interval_seconds = 300
min_samples      = 10
timezone         = Asia/Tehran   # change to your timezone

[queries]
node_cpu_usage   = 100-(avg by(instance)(rate(node_cpu_seconds_total{mode="idle"}[5m]))*100)
nginx_waiting    = nginx_connections_waiting | min_delta=2.0 | z_threshold=4.0
redis_blocked    = redis_blocked_clients     | min_delta=1.0 | z_threshold=2.0
```

### 3. Run with Docker Compose

```bash
docker compose up -d
```

Metrics will be available at `http://localhost:9091/metrics`.

### 4. Add scrape job to Prometheus

```yaml
# prometheus.yml
scrape_configs:
  - job_name: anomaly-detector
    static_configs:
      - targets: ['localhost:9091']
    scrape_interval: 1m
```

### 5. Import Grafana dashboards

1. Open Grafana → **Dashboards → Import**
2. Upload `grafana/anomaly-detector-overview.json`
3. Upload `grafana/anomaly-detector-detail.json`
4. Select your Prometheus datasource when prompted

---

## Configuration Reference

### `[general]` section

| Key | Default | Description |
|-----|---------|-------------|
| `prometheus_url` | `http://localhost:9090` | Prometheus base URL |
| `listen_addr` | `0.0.0.0:9091` | Address to expose metrics on |
| `lookback_days` | `14` | How many past days to build baseline from |
| `window_minutes` | `30` | ±window around current hour for baseline |
| `step_seconds` | `300` | Query resolution step in seconds |
| `z_threshold` | `3.0` | Z-score threshold to declare anomaly |
| `interval_seconds` | `300` | Detection cycle interval |
| `min_samples` | `10` | Minimum baseline points required |
| `min_delta` | `0.0` | Minimum absolute change to be anomalous |
| `min_relative` | `0.0` | Minimum relative change (fraction of mean) |
| `timezone` | `UTC` | Local timezone (e.g. `Asia/Tehran`, `Europe/Berlin`) |

### `[queries]` section

Each line defines one metric to monitor:

```ini
metric_name = promql_query | option=value | option=value
```

| Option | Description |
|--------|-------------|
| `z_threshold=N` | Override global z_threshold for this metric |
| `min_samples=N` | Override global min_samples |
| `min_delta=N` | Minimum absolute change (useful for near-zero metrics) |
| `min_relative=N` | Minimum relative change as fraction of mean |
| `silence=true` | Disable detection for this metric (still exposes 0) |

**Example — near-zero metrics (most common false positive source):**
```ini
# nginx_waiting is often 0-2, needs a minimum delta to avoid false positives
nginx_waiting_connections = nginx_connections_waiting | min_delta=2.0 | z_threshold=4.0
# redis_blocked_clients spikes from 0 to 1 legitimately
redis_blocked_clients     = redis_blocked_clients     | min_delta=1.0 | z_threshold=2.0
```

---

## Exposed Metrics

All metrics carry labels: `metric_name`, `instance`, `exported_instance`

| Metric | Description |
|--------|-------------|
| `anomaly_detected` | `1` if anomalous, `0` if normal |
| `anomaly_z_score` | Standard deviations from baseline mean |
| `anomaly_composite_score` | Weighted blend of z-score + trend + spike |
| `anomaly_baseline_confidence` | `0.0–1.0` — low when few baseline samples (< 30) |
| `anomaly_percentage` | % of current window points that were anomalous |
| `anomaly_current_value` | Latest scraped value |
| `anomaly_baseline_mean` | Seasonal baseline mean |
| `anomaly_baseline_stddev` | Seasonal baseline std deviation |
| `anomaly_baseline_samples` | Number of historical samples in baseline |
| `anomaly_trend_slope` | Linear regression slope of current window |
| `anomaly_trend_slope_normalized` | Slope divided by stddev |
| `anomaly_spike_score` | Last-to-previous-point jump / stddev |
| `anomaly_consecutive_cycles` | How many consecutive cycles have been anomalous |
| `anomaly_health_state{state=...}` | `1` for active state: `normal/warning/recovering/critical` |
| `anomaly_severity{severity_label=...}` | `1` for active severity: `normal/low/medium/high/critical` |
| `anomaly_weekend_baseline` | `1` if weekend baseline was used |
| `anomaly_hour_of_day` | Local hour used for baseline selection |
| `anomaly_silenced` | `1` if metric is silenced in config |
| `anomaly_detector_last_scrape_timestamp_seconds` | Unix timestamp of last cycle |
| `anomaly_detector_scrape_errors_total` | Counter of Prometheus query errors |

---

## REST API

| Endpoint | Description |
|----------|-------------|
| `GET /metrics` | Prometheus metrics endpoint |
| `GET /healthz` | Health check — returns `{"status":"ok"}` |
| `GET /status` | JSON summary of all series with anomaly state |
| `GET /status?state=critical` | Filter by health state |
| `GET /status?severity=high` | Filter by severity |
| `GET /status?metric=nginx` | Filter by metric name substring |
| `GET /config` | Current loaded config as JSON |
| `GET /reload` | Validate config (hot reload on next cycle) |

---

## Architecture

```
Prometheus ──scrape──► anomaly-detector
                            │
                    ┌───────┴────────┐
                    │  Per-cycle:    │
                    │  1. Query now  │
                    │  2. Query N    │
                    │     past days  │
                    │  3. Compute    │
                    │     baseline   │
                    │  4. z-score,   │
                    │     composite, │
                    │     confidence │
                    │  5. Expose     │
                    │     metrics    │
                    └───────┬────────┘
                            │
              Prometheus ◄──scrape── /metrics
                            │
                        Grafana
                    ┌───────┴────────┐
                    │  Overview DB   │──drilldown──►  Detail DB
                    └────────────────┘
```

---

## Tuning Guide — Reducing False Positives

### 1. Near-zero metrics (most common)
Metrics like `nginx_waiting_connections` or `redis_blocked_clients` are normally 0.
A single connection gives a huge z-score against a near-zero baseline.

```ini
nginx_waiting_connections = nginx_connections_waiting | min_delta=2.0 | z_threshold=4.0
```

### 2. Noisy / spiky metrics
Metrics that naturally fluctuate a lot need a higher threshold:

```ini
node_network_rx = rate(node_network_receive_bytes_total{device!="lo"}[5m]) | z_threshold=4.0
```

### 3. New deployment (insufficient history)
The `anomaly_baseline_confidence` metric shows `0.3–1.0`.
Low confidence (< 0.5) means the baseline has fewer than 15 samples and composite
scores are automatically dampened. Wait 3–7 days for full accuracy.

### 4. Silence a metric temporarily
```ini
haproxy_queue = haproxy_backend_current_queue | silence=true
```

### 5. Raise `min_samples`
If you have patchy historical data:
```ini
[general]
min_samples = 20
```

---

## Grafana Dashboard Guide

### Overview Dashboard
- **Top stats row** — instant health: total anomalies, critical/warning/recovering
  counts, avg baseline confidence
- **Severity timeline** — see if anomaly rate is growing or shrinking
- **Confidence panel** — low avg confidence = baseline still warming up
- **Problem tables** — sorted by Composite Score, per-service breakdown
- **Confidence column** — red = possible false positive (new baseline)
- Click any **Metric** name → drills into Detail dashboard

### Detail Dashboard
- **Identity card** — current value, baseline, z-score, severity, confidence at a glance
- **Signal chart** — current vs baseline with ±1σ / ±2σ bands
- **Window Anomaly %** — how long in this window the metric has been bad
  (0% = just spiked once, 80% = bad the whole window)
- **Confidence over time** — shows baseline warming up over days
- **All instances panel** — compare same metric across all servers to spot
  whether it is one node or systemic
- **Sibling anomalies** — other metrics anomalous on the same instance

---

## Building from Source

```bash
go build -o anomaly-detector .
./anomaly-detector config.ini
```

Requirements: Go 1.21+

---

## License

MIT
