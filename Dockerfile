# Generator image — Python + project dependencies
FROM python:3.12-slim

WORKDIR /app
RUN pip install --no-cache-dir \
    pandas numpy pyarrow pyyaml minio \
    kafka-python psycopg2-binary faker vnstock

COPY generator/ ./generator/
COPY config/ ./config/

ENTRYPOINT ["python", "-m", "generator.main"]
CMD ["--mode", "offline", "--config", "config/generator.yaml"]
