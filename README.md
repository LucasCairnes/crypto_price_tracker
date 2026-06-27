# 📈 Live Market Data:
![C++](https://img.shields.io/badge/C%2B%2B20-00599C?style=flat&logo=cplusplus&logoColor=white)
![Apache Flink](https://img.shields.io/badge/Apache_Flink-E6526F?style=flat&logo=apacheflink&logoColor=white)
![Redpanda](https://img.shields.io/badge/Redpanda_(Kafka)-C73A30?style=flat&logo=apachekafka&logoColor=white)
![TimescaleDB](https://img.shields.io/badge/TimescaleDB-FDB515?style=flat&logo=postgresql&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-2496ED?style=flat&logo=docker&logoColor=white)
![Prometheus](https://img.shields.io/badge/Prometheus-E6522C?style=flat&logo=prometheus&logoColor=white)
![Grafana](https://img.shields.io/badge/Grafana-F46800?style=flat&logo=grafana&logoColor=white)

A real-time crypto market data streaming pipeline built with C++ and Apache Flink.

**The Grafana dashboards can be seen locally at [http://localhost:3000](http://localhost:3000) when the stack is running.**

## Project Overview
This pipeline ingests live Bitcoin trades from the Binance WebSocket API and uses a custom C++ producer to buffer them into Redpanda. An Apache Flink processing job applies 10 second tumbling to calculate OHLC and VWAP values. The C++ consumer then batches this enriched data into TimescaleDB, with system health and market metrics monitored via via Prometheus and Grafana.

---

## Repository Structure
* `producer.cpp` — C++ service that streams trades from Binance and publishes Protobuf to Redpanda.
* `consumer.cpp` — C++ service that batches enriched candles from Redpanda into TimescaleDB.
* `trades_job.py` — The PyFlink job defining the VWAP/OHLC windowed aggregation.
* `trade.proto` — Shared Protocol Buffers schema for raw trades.
* `init.sql` — TimescaleDB schema and hypertable definition.
* `docker-compose.yml` — Orchestration for all ten services.
* `prometheus/` & `grafana/` — Monitoring configuration and provisioned dashboards.

---

## Requirements
* Docker & Docker Compose

---

## Setup & Execution

**1. Clone Repo:**
* `git clone https://github.com/LucasCairnes/crypto_price_tracker/`
* `cd crypto_price_tracker`

**2. Build and Start the Stack:**
Build the C++ services and bring up all containers.
* `docker compose up -d --build`

**3. Submit the Flink Job:**
Once the cluster is running, submit the windowed aggregation job.
* `docker compose exec jobmanager flink run -d -py /opt/flink/usrlib/trades_job.py`

**4. Inspect the Data:**
Watch enriched candles stream through Kafka, or query the database directly.
* `docker compose exec redpanda rpk topic consume enriched_trades`
* `docker compose exec timescaledb psql -U market -d market -c "SELECT * FROM enriched_trades ORDER BY window_start DESC LIMIT 10;"`

**5. View Metrics & Dashboards:**
* `curl localhost:9101/metrics` — producer metrics
* `curl localhost:9102/metrics` — consumer metrics
* [http://localhost:9090](http://localhost:9090) — Prometheus
* [http://localhost:3000](http://localhost:3000) — Grafana (admin/admin): Market Analytics + System Health
* [http://localhost:8081](http://localhost:8081) — Flink dashboard
* [http://localhost:8080](http://localhost:8080) — Redpanda Console

**6. Tear Down:**
* `docker compose down` (add `-v` to also remove data volumes)
