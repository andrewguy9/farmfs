.PHONY: install dev check test typecheck lint perf build publish coverage clean

# Install for regular use
install:
	pip install .

# Install in editable mode with dev dependencies
dev:
	pip install -e ".[dev]"

# Run all validations
check: test typecheck lint

# Run regression tests with coverage
test:
	pytest --cov farmfs --cov-report=term-missing

# Show HTML coverage report
coverage:
	pytest --cov farmfs --cov-report=html
	python -m webbrowser htmlcov/index.html

# Type check (must stay zero errors)
typecheck:
	mypy farmfs --ignore-missing-imports

# Lint
lint:
	flake8 farmfs tests perf

# Performance tests
perf:
	pytest -s perf/transducer.py

# Build source dist and wheel
build:
	pip install --quiet build
	python -m build

# Publish to PyPI (requires ~/.pypirc or TWINE_USERNAME/TWINE_PASSWORD env vars)
publish: build
	pip install --quiet twine
	twine upload dist/*

# Remove build artifacts
clean:
	rm -rf dist/ build/ *.egg-info htmlcov/ .coverage*
