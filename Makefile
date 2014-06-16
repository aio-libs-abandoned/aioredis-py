
PYTHON ?= python3
FLAKE ?= pyflakes
PEP ?= pep8

doc:
	make -C docs html

all: flake doc test

flake:
	$(FLAKE) aioredis tests
	$(PEP) aioredis tests

test:
	$(PYTHON) runtests.py -v

cov coverage:
	$(PYTHON) runtests.py --coverage


.PHONY: all
