
PYTHON ?= python3
FLAKE ?= pyflakes
PEP ?= pep8
REDIS_VERSION ?= "$(shell redis-server --version)"

.PHONY: all flake doc test cov dist
all: flake doc cov

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
