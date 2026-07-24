# Generator image — multi-stage build to minimize final image size
# Stage 1: install dependencies into a virtualenv
FROM python:3.12-slim AS builder

WORKDIR /app
RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"
RUN pip install --no-cache-dir \
    pandas numpy pyarrow pyyaml minio \
    kafka-python psycopg2-binary sqlalchemy

# Stage 2: runtime — copy only venv + source, no build tools
FROM python:3.12-slim AS runtime

WORKDIR /app
COPY --from=builder /opt/venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

COPY generator/ ./generator/
COPY config/ ./config/

ENTRYPOINT ["python", "-m", "generator.main"]
CMD ["--mode", "offline", "--config", "config/generator.yaml"]
