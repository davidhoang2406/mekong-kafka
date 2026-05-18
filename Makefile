.PHONY: install \
        run-smoke-producer run-smoke-consumer \
        run-stock-price-producer run-crypto-price-producer run-storage-consumer \
        test test-unit test-integration

PYTHON := .venv/bin/python

install: ## Create venv and install dependencies
	python3.12 -m venv .venv
	.venv/bin/pip install -r requirements.txt

run-smoke-producer: ## Send one hardcoded message to Kafka
	PYTHONPATH=. $(PYTHON) main.py smoke-producer

run-smoke-consumer: ## Print messages on stock.price.realtime
	PYTHONPATH=. $(PYTHON) main.py smoke-consumer

run-stock-price-producer: ## Poll vnstock → Kafka (every 30 s)
	PYTHONPATH=. $(PYTHON) main.py stock-price-producer

run-crypto-price-producer: ## Poll crypto exchange → Kafka (every 5 s)
	PYTHONPATH=. $(PYTHON) main.py crypto-price-producer

run-storage-consumer: ## Kafka → MinIO Avro
	PYTHONPATH=. $(PYTHON) main.py storage-consumer

test: ## Run all tests
	PYTHONPATH=. $(PYTHON) -m pytest

test-unit: ## Run unit tests only (no Docker needed)
	PYTHONPATH=. $(PYTHON) -m pytest -m unit

test-integration: ## Run integration tests (Docker must be running)
	PYTHONPATH=. $(PYTHON) -m pytest -m integration
