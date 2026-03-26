.PHONY: all generate spark load dbt test clean setup demo docs

PROJECT_DIR := $(shell pwd)
PYTHON := python3

all: generate spark load dbt

demo:
	@$(PYTHON) demo.py

generate:
	@echo "Generating synthetic data..."
	@$(PYTHON) -m src.generators.orchestrator

spark:
	@echo "Running Spark pipeline..."
	@$(PYTHON) -m src.spark_jobs.run_pipeline

load:
	@echo "Loading silver to DuckDB..."
	@$(PYTHON) -m src.loaders.silver_to_duckdb

dbt:
	@echo "Running dbt models..."
	cd dbt_project && dbt deps --quiet
	cd dbt_project && dbt seed --quiet
	cd dbt_project && dbt run
	cd dbt_project && dbt test

docs:
	@echo "Generating dbt documentation..."
	cd dbt_project && dbt docs generate
	@echo "Serving docs at http://localhost:8080"
	cd dbt_project && dbt docs serve

test:
	@echo "Running Python tests..."
	pytest tests/ -v

clean:
	rm -rf data/raw/*.csv data/raw/*.json
	rm -rf data/bronze/*
	rm -rf data/silver/*
	rm -f data/warehouse/analytics.duckdb
	cd dbt_project && dbt clean

setup:
	pip3 install -e .
	cd dbt_project && dbt deps
