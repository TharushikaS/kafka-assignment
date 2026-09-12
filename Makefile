# Convenience targets. On Windows without `make`, run the underlying commands
# directly (see README) or use the scripts in ./scripts.

PYTHON ?= python

.PHONY: help install up down topics produce consume dlq test clean

help:
	@echo "Targets:"
	@echo "  install   Install Python dependencies"
	@echo "  up        Start Kafka + Schema Registry + UI (docker compose)"
	@echo "  down      Stop the stack (add ARGS=-v to wipe data)"
	@echo "  topics    Create the orders and DLQ topics"
	@echo "  produce   Produce sample orders (ARGS to override, e.g. ARGS='--count 200')"
	@echo "  consume   Start the consumer"
	@echo "  dlq       Inspect the Dead Letter Queue"
	@echo "  test      Run the unit test suite"
	@echo "  clean     Remove caches and virtualenv"

install:
	$(PYTHON) -m pip install -r requirements.txt

up:
	docker compose up -d

down:
	docker compose down $(ARGS)

topics:
	$(PYTHON) -m order_pipeline.admin

produce:
	$(PYTHON) -m order_pipeline.producer $(ARGS)

consume:
	$(PYTHON) -m order_pipeline.consumer $(ARGS)

dlq:
	$(PYTHON) -m order_pipeline.dlq_inspector $(ARGS)

test:
	$(PYTHON) -m pytest

clean:
	rm -rf .pytest_cache **/__pycache__ .venv
