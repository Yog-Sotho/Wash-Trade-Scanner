.PHONY: help install install-dev test lint format clean docker-up docker-down

help: ## Show this help
	@egrep -h '\s##\s' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

install: ## Install production dependencies
	pip install -r requirements.txt

install-dev: ## Install development dependencies
	pip install -r requirements-dev.txt
	pip install -e .

test: ## Run tests with coverage
	pytest tests/ -v --cov=core --cov-report=term-missing

lint: ## Run linters
	black --check config core models scripts tests setup.py
	isort --check-only config core models scripts tests setup.py
	flake8 config core models scripts tests setup.py
	mypy config core models scripts tests

format: ## Auto-format code
	black config core models scripts tests setup.py
	isort config core models scripts tests setup.py

clean: ## Remove build artifacts
	rm -rf build/ dist/ *.egg-info .eggs/ __pycache__ .pytest_cache/ .mypy_cache/ .coverage htmlcov/

docker-up: ## Start services
	docker-compose up -d

docker-down: ## Stop services
	docker-compose down
