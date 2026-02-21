.PHONY: test typecheck lint perf

test:
	pytest

typecheck:
	mypy farmfs --ignore-missing-imports

lint:
	flake8 farmfs tests perf

perf:
	pytest -s perf/transducer.py
