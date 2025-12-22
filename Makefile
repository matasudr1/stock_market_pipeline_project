# Makefile for Stock Market Data Pipeline

.PHONY: help build up down logs clean test dbt-run dbt-test init-db

help:
	@echo "Stock Market Data Pipeline - Available Commands"
	@echo "================================================"
	@echo "make build         - Build Docker images"
	@echo "make up            - Start all services"
	@echo "make down          - Stop all services"
	@echo "make logs          - View logs"
	@echo "make clean         - Clean up volumes and containers"
	@echo "make test          - Run tests"
	@echo "make dbt-run       - Run dbt models"
	@echo "make dbt-test      - Run dbt tests"
	@echo "make init-db       - Initialize database"
	@echo "make extract       - Run stock data extraction"

build:
	docker-compose build

up:
	docker-compose up -d
	@echo "Services starting..."
	@echo "Airflow UI: http://localhost:8080 (admin/admin)"
	@echo "Dashboard: http://localhost:8501"
	@echo "PostgreSQL: localhost:5432"

down:
	docker-compose down

logs:
	docker-compose logs -f

logs-airflow:
	docker-compose logs -f airflow_webserver airflow_scheduler airflow_worker

logs-streamlit:
	docker-compose logs -f streamlit

clean:
	docker-compose down -v
	docker system prune -f

init-db:
	docker-compose exec postgres psql -U postgres -d stock_market -f /docker-entrypoint-initdb.d/init_db.sql

extract:
	docker-compose exec airflow_worker python /opt/airflow/ingestion/stock_extractor.py

dbt-run:
	docker-compose exec airflow_worker bash -c "cd /opt/airflow/dbt_stock && dbt run --profiles-dir . --project-dir ."

dbt-test:
	docker-compose exec airflow_worker bash -c "cd /opt/airflow/dbt_stock && dbt test --profiles-dir . --project-dir ."

dbt-docs:
	docker-compose exec airflow_worker bash -c "cd /opt/airflow/dbt_stock && dbt docs generate --profiles-dir . --project-dir ."

test:
	@echo "Running tests..."
	pytest tests/

restart:
	docker-compose restart

status:
	docker-compose ps

shell-airflow:
	docker-compose exec airflow_worker bash

shell-db:
	docker-compose exec postgres psql -U postgres -d stock_market

backup-db:
	docker-compose exec postgres pg_dump -U postgres stock_market > backup_$(shell date +%Y%m%d_%H%M%S).sql
