.PHONY: up down build clean test lint generate generate-docker build-images

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
	docker compose build

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

# =============================================================================
# Testing (M5)
# =============================================================================

test:
	pytest tests/ -v --cov=generator --cov=jobs --cov-report=term

test-coverage:
	pytest tests/ -v --cov=generator --cov=jobs --cov-report=html

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
	docker compose run --rm airflow-webserver airflow db init
	docker compose run --rm airflow-webserver airflow users create \
		--username admin --password admin --firstname Admin --lastname User \
		--role Admin --email admin@example.com

# =============================================================================
# Clean
# =============================================================================

clean:
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name .pytest_cache -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name *.egg-info -exec rm -rf {} + 2>/dev/null || true
	rm -rf .ruff_cache htmlcov
