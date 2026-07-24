# Flink v10 verification

Reference run: 2026-07-23, seed 42.

## Input

- Topic: `stock_market_events_v3`
- Partition offsets: 59,262 + 47,889 + 43,803 + 46,971 = 197,925
- Partition count: 4, keyed by ticker
- Topic retention: `retention.ms=-1`
- Consumer group: `flink-silver-stream-v10-final`
- Every partition started at offset 0

## Application

- Job ID: `377ab5e5eb029d8f905db390c54549b6`
- State: `RUNNING`
- Tasks: 12/12 running, 0 failed
- Source records: 197,925
- Deduplicated Silver files: 195,000 rows
- Trade-window input: 165,668 rows
- Five-minute volume-window files: 17,845 rows
- Quarantine rows: 0; all generated delays are within the 60-second policy
- Final watermark on all four source subtasks:
  `2025-10-09 14:43:58 UTC`
- Checkpoints at verification: 23 completed, 0 failed

The dedup delta is exactly the 2,925 replays reported by the generator. The
trade count and window count independently match the Spark Silver and Spark
feature outputs.

## Reproduction commands

```bash
docker compose exec -T kafka kafka-get-offsets \
  --bootstrap-server kafka:29092 --topic stock_market_events_v3

docker compose exec -T flink-jobmanager flink run -d \
  -py /opt/project/jobs/flink/silver_stream.py \
  --kafka-broker kafka:29092 \
  --topic stock_market_events_v3 \
  --group-id flink-silver-stream-v10-final \
  --output-dir /opt/flink/data/final_v10 \
  --watermark-sec 60 --parallelism 4

find data/flink/final_v10/stg_events -type f -name '*.jsonl' \
  -exec wc -l {} +
find data/flink/final_v10/feat_stream_volume_5m -type f -name '*.jsonl' \
  -exec wc -l {} +
```
