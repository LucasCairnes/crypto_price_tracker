```bash
docker compose up -d --build
docker compose exec jobmanager flink run -d -py /opt/flink/usrlib/trades_job.py
docker compose exec redpanda rpk topic consume enriched_trades
docker compose exec timescaledb psql -U market -d market -c "SELECT * FROM enriched_trades ORDER BY window_start DESC LIMIT 10;"
docker compose down
```
