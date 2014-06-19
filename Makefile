
PYTHON ?= python3
FLAKE ?= pyflakes
PEP ?= pep8

doc:
	make -C docs html

all: flake doc test

flake:
	$(FLAKE) aioredis tests examples
	$(PEP) aioredis tests examples

test:
	$(PYTHON) runtests.py -v

cov coverage:
	$(PYTHON) runtests.py --coverage


.PHONY: all
