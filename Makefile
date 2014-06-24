
PYTHON ?= python3
FLAKE ?= pyflakes
PEP ?= pep8
REDIS_VERSION ?= "$(shell redis-server --version)"

doc:
	make -C docs html

all: flake doc test

flake:
	$(FLAKE) aioredis tests examples
	$(PEP) aioredis tests examples

test:
	REDIS_VERSION=$(REDIS_VERSION) $(PYTHON) runtests.py -v

cov coverage:
	REDIS_VERSION=$(REDIS_VERSION) $(PYTHON) runtests.py --coverage


.PHONY: all
