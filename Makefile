
PYTHON ?= python3
FLAKE ?= pyflakes3
PEP ?= pep8

# doc:

all: flake test

flake:
	$(FLAKE) aioredis tests
	$(PEP) aioredis tests

test:
	$(PYTHON) runtests.py -v

cov coverage:
	$(PYTHON) runtests.py --coverage


.PHONY: all
