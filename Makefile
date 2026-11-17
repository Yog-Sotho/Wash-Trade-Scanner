.PHONY: help install install-dev lock test lint format security eval eval-check calibrate clean docker-up docker-down

help: ## Show this help
	@egrep -h '\s##\s' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

install: ## Install the hash-locked runtime dependencies, then the package
	pip install --require-hashes -r requirements.txt
	pip install --no-deps .

install-dev: ## Install the hash-locked development dependencies, then the package (editable)
	pip install --require-hashes -r requirements-dev.txt
	pip install --no-deps -e .

lock: ## Re-resolve requirements*.txt from pyproject.toml (needs uv: pip install uv)
	uv pip compile pyproject.toml --universal --python-version 3.11 --generate-hashes \
		--custom-compile-command "make lock" -o requirements.txt
	uv pip compile pyproject.toml --extra dev --universal --python-version 3.11 --generate-hashes \
		--custom-compile-command "make lock" -o requirements-dev.txt
	printf 'setuptools>=84.1.0\nwheel\n' | uv pip compile - --universal --python-version 3.11 \
		--generate-hashes --custom-compile-command "make lock" -o requirements-build.txt

test: ## Run tests with coverage
	pytest tests/ -v --cov=core --cov-report=term-missing

lint: ## Run linters (matches CI: ruff + mypy)
	ruff check .
	ruff format --check .
	mypy .

format: ## Auto-format code
	ruff check --fix .
	ruff format .

security: ## Run security scans (matches CI: bandit + pip-audit)
	bandit -c pyproject.toml -r api config core evaluation models scripts
	pip-audit --skip-editable

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
