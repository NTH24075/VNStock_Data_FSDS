.PHONY: up down build clean test lint generate generate-docker build-images \
	spark-capture-baseline spark-capture-optimized

# =============================================================================
# uv (fast Python package manager)
# =============================================================================

uv-sync:
	uv sync

uv-lock:
	uv lock

uv-run:
	uv run python -m generator.main --mode offline --config config/generator.yaml

uv-test:
	uv run pytest tests/ -v --cov=generator --cov=jobs --cov-report=term

uv-lint:
	uv run ruff check generator/ jobs/ dags/ tests/

# =============================================================================
# Docker — Build custom images (only where needed)
# =============================================================================

build-images:
	docker compose build generator airflow-webserver airflow-scheduler flink-jobmanager flink-taskmanager

build-generator:
	docker compose build generator

build-airflow:
	docker compose build airflow-webserver airflow-scheduler

# =============================================================================
# Docker Compose
# =============================================================================

up:
	docker compose up -d

down:
	docker compose down -v

build:
	docker compose build

logs:
	docker compose logs -f

# =============================================================================
# Generator (01)
# =============================================================================

generate-docker:
	docker compose run --rm generator --mode offline --config config/generator.yaml

generate-offline:
	python -m generator.main --mode offline --config config/generator.yaml

generate-stream:
	python -m generator.main --mode stream --config config/generator.yaml

generate-all:
	python -m generator.main --mode all --config config/generator.yaml

sync-gold:
	docker compose --profile tools run --rm minio-sync-gold

# =============================================================================
# Testing (M5)
# =============================================================================

test:
	PYTHONPATH=. .venv/bin/python3.12 -m pytest tests/ -v --cov=generator --cov=jobs --cov-report=term

test-container:
	@echo "=== Running unit tests in Spark container ==="
	docker exec vnstock-spark-master bash -c '\
		export PYTHONPATH=/opt/project ; \
		cd /opt/project ; \
		python3 -m pytest tests/unit/ -v'

test-container-all: test-container
	@echo "=== Running integration tests ==="
	docker exec vnstock-spark-master bash -c '\
		export PYTHONPATH=/opt/project ; \
		cd /opt/project ; \
		python3 -m pytest tests/integration/ -v'

test-coverage:
	pytest tests/ -v --cov=generator --cov=jobs --cov-report=html

# =============================================================================
# Spark UI evidence capture
# =============================================================================

spark-capture-baseline:
	SPARK_CAPTURE_MODE=baseline docker compose --profile capture run --rm \
		--service-ports --use-aliases spark-capture

spark-capture-optimized:
	SPARK_CAPTURE_MODE=optimized docker compose --profile capture run --rm \
		--service-ports --use-aliases spark-capture

# =============================================================================
# Linting
# =============================================================================

lint:
	ruff check generator/ jobs/ dags/ tests/

format:
	ruff format generator/ jobs/ dags/ tests/

# =============================================================================
# Airflow
# =============================================================================

airflow-init:
	docker compose run --rm airflow-webserver airflow db migrate
	docker compose run --rm airflow-webserver airflow users create \
		--username admin --password admin --firstname Admin --lastname User \
		--role Admin --email admin@example.com

# =============================================================================
# Flink (PyFlink streaming jobs)
# =============================================================================

build-flink:
	docker compose build flink-jobmanager flink-taskmanager

flink-submit:
	docker exec vnstock-flink-jobmanager flink run -py /opt/project/jobs/flink/silver_stream.py

flink-list:
	docker exec vnstock-flink-jobmanager flink list

flink-cancel:
	docker exec vnstock-flink-jobmanager flink cancel $(JOB_ID)

flink-ui:
	@echo "Flink Dashboard → http://localhost:8081"

# =============================================================================
# Clean
# =============================================================================

clean:
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name .pytest_cache -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name *.egg-info -exec rm -rf {} + 2>/dev/null || true
	rm -rf .ruff_cache htmlcov
