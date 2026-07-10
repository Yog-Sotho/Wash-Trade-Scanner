.PHONY: help install install-dev test lint format security clean docker-up docker-down

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
	bandit -c pyproject.toml -r core config models scripts
	pip-audit

clean: ## Remove build artifacts
	rm -rf build/ dist/ *.egg-info .eggs/ __pycache__ .pytest_cache/ .mypy_cache/ .coverage htmlcov/

docker-up: ## Start services
	docker-compose up -d

docker-down: ## Stop services
	docker-compose down
