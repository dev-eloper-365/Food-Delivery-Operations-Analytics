.PHONY: all generate spark load dbt test clean setup demo docs gh-pages

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

gh-pages:
	@echo "Generating dbt docs for GitHub Pages..."
	cd dbt_project && dbt docs generate
	@$(PYTHON) -c "\
	with open('dbt_project/target/index.html') as f: index = f.read(); \
	with open('dbt_project/target/manifest.json') as f: manifest = f.read(); \
	with open('dbt_project/target/catalog.json') as f: catalog = f.read(); \
	inject = '<script>window.dbt_manifest=' + manifest.strip() + ';window.dbt_catalog=' + catalog.strip() + ';</script>'; \
	out = index.replace('</head>', inject + '</head>'); \
	open('docs/index.html','w').write(out); \
	open('docs/manifest.json','w').write(manifest); \
	open('docs/catalog.json','w').write(catalog); \
	print(f'  Wrote docs/index.html ({len(out)//1024}KB)')"
	@echo "Done! Commit docs/ and push to deploy."

setup:
	pip3 install -e .
	cd dbt_project && dbt deps
