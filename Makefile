
PYTHON ?= python3
FLAKE ?= pyflakes3
PEP ?= pep8

# doc:


flake:
	$(FLAKE) aioredis tests
	$(PEP) aioredis tests

test:
	$(PYTHON) runtests.py -v

cov coverage:
	$(PYTHON) runtests.py --coverage


.PHONY: doc flake
