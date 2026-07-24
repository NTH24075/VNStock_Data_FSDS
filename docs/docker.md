# Docker and local deployment

## Deployable units

`docker-compose.yml` runs PostgreSQL, MinIO, Kafka/ZooKeeper, Spark
master/worker, Flink JobManager/TaskManager, Airflow webserver/scheduler, Hive
Metastore and Trino as independent services. Generator is an on-demand profile
so it exits after producing a reproducible vendor drop.

All service-to-service addresses use Docker DNS. Kafka exposes `kafka:29092`
inside the network and `localhost:9092` to the host; this avoids advertising a
host-only address to Spark or Flink.

## Generator image optimization

Image size is read from Docker's content metadata:

```bash
docker image inspect vnstock-generator:test --format '{{.Size}}'        # historical baseline
docker image inspect vnstock-data-pipeline-generator:latest --format '{{.Size}}'
```

| Build | Bytes | Approx. reduction |
|---|---:|---:|
| Baseline | 195,099,639 | — |
| Optimized | 158,641,463 | 18.7% |

The optimized `Dockerfile` uses a builder stage for the virtual environment, a
`python:3.12-slim` runtime, `pip --no-cache-dir`, and copies only runtime code
and configuration. The runtime no longer installs `vnstock` or Faker: `vnstock`
is used only to refresh the frozen seed and Faker is not used by generation.

The baseline value was recorded when the older image still existed. That
baseline tag is no longer present in the current Docker runtime, so it must
not be represented by a fabricated terminal capture. The optimized image can
still be inspected; reproducing a terminal side-by-side requires rebuilding
the historical baseline first.

No Docker screenshots are present in `docs/images/`. Consequently the
multi-stage implementation and recorded 18.7% reduction are documented, but
the rubric's visual proof requirement is not satisfied by the current
evidence set.

## Commands

```bash
make build-images
make up
docker compose ps
make generate-docker
```

Service health endpoints:

| Service | URL |
|---|---|
| Spark | <http://localhost:8080> |
| Flink | <http://localhost:8081> |
| Airflow | <http://localhost:8082> |
| Trino | <http://localhost:8083> |
| MinIO console | <http://localhost:9001> |

Do not use `make down` when retaining evidence; that target intentionally
removes project volumes.
