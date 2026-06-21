```bash
docker compose up -d --build
docker compose exec jobmanager flink run -d -py /opt/flink/usrlib/trades_job.py
docker compose exec redpanda rpk topic consume enriched_trades
docker compose down
```
