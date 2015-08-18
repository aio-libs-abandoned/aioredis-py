
PYTHON ?= python3
FLAKE ?= pyflakes
PEP ?= pep8
REDIS_VERSION ?= "$(shell redis-cli INFO SERVER | sed -n 2p)"

.PHONY: all flake doc test cov dist devel
all: aioredis.egg-info flake doc cov

doc:
	make -C docs html

flake:
	$(FLAKE) aioredis tests examples
	$(PEP) aioredis tests examples

test:
	redis-cli FLUSHALL
	REDIS_VERSION=$(REDIS_VERSION) $(PYTHON) runtests.py -v

cov coverage:
	redis-cli FLUSHALL
	REDIS_VERSION=$(REDIS_VERSION) $(PYTHON) runtests.py --coverage

dist:
	-rm -r build dist aioredis.egg-info
	$(PYTHON) setup.py sdist bdist_wheel

devel: aioredis.egg-info
	pip install -U pip
	pip install -U pyflakes pep8 sphinx coverage bumpversion wheel

aioredis.egg-info:
	pip install -Ue .
