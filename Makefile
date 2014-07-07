
PYTHON ?= python3
FLAKE ?= pyflakes
PEP ?= pep8
REDIS_VERSION ?= "$(shell redis-server --version)"

.PHONY: all flake doc test cov
all: flake doc cov

doc:
	make -C docs html

flake:
	$(FLAKE) aioredis tests examples
	$(PEP) aioredis tests examples

test:
	REDIS_VERSION=$(REDIS_VERSION) $(PYTHON) runtests.py -v

cov coverage:
	REDIS_VERSION=$(REDIS_VERSION) $(PYTHON) runtests.py --coverage

.PHONY: full_cov

full_cov:
	redis-server --unixsocket /tmp/aioredis.sock --port 6380 \
		--save "" --daemonize yes --pidfile /tmp/aioredis.pid
	REDIS_VERSION=$(REDIS_VERSION) REDIS_SOCKET=/tmp/aioredis.sock \
		$(PYTHON) runtests.py --coverage
	kill -s SIGTERM $(shell cat /tmp/aioredis.pid)
