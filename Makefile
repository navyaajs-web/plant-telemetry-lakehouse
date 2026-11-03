COMPOSE ?= docker compose
SPARK   := $(COMPOSE) exec spark

.PHONY: setup up demo query down test lint fmt clean

setup:            ## install the package and dev tools into the current Python env
	pip install -e ".[dev]"

up:               ## start Redpanda and the Spark container
	$(COMPOSE) up -d --build --wait

demo:             ## produce 10 minutes of synthetic data, then run bronze, silver and gold
	$(SPARK) python -m plant_telemetry.generator.producer --minutes 10
	$(SPARK) python -m plant_telemetry.pipelines.run --mode once

query:            ## print gold tables and a data-quality summary
	$(SPARK) python -m plant_telemetry.query

down:             ## stop containers and drop the Kafka volume
	$(COMPOSE) down -v

test:
	pytest -q

lint:
	ruff check .
	ruff format --check .

fmt:
	ruff format .
	ruff check --fix .

clean:
	rm -rf lake spark-warehouse metastore_db derby.log .pytest_cache .ruff_cache
