.PHONY: help install install-dev test lint format security eval eval-check calibrate clean docker-up docker-down

help: ## Show this help
	@egrep -h '\s##\s' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

install: ## Install production dependencies (pyproject.toml is the source of truth)
	pip install .

install-dev: ## Install development dependencies
	pip install -e ".[dev]"

test: ## Run tests with coverage
	pytest tests/ -v --cov=core --cov-report=term-missing

lint: ## Run linters (matches CI: ruff + mypy)
	ruff check .
	ruff format --check .
	mypy core config models scripts

format: ## Auto-format code
	ruff check --fix .
	ruff format .

security: ## Run security scans (matches CI: bandit + pip-audit)
	bandit -c pyproject.toml -r api core config models scripts
	pip-audit

eval: ## Measure every detector and regenerate docs/benchmarks.md
	wash-eval --write-docs --json evaluation/results.json

eval-check: ## Fail if a detector regressed or the shipped calibration is stale (CI gate)
	wash-eval --check

calibrate: ## Re-measure detector rates and accept them as calibration + regression baseline
	wash-eval --write-calibration --write-baseline --write-docs --json evaluation/results.json

clean: ## Remove build artifacts
	rm -rf build/ dist/ *.egg-info .eggs/ __pycache__ .pytest_cache/ .mypy_cache/ .coverage htmlcov/

docker-up: ## Start services
	docker-compose up -d

docker-down: ## Stop services
	docker-compose down
