PYTHON ?= python3
FLAKE ?= flake8
PYTEST ?= pytest
MYPY ?= mypy

REDIS_VERSION ?= "$(shell redis-cli INFO SERVER | sed -n 2p)"
REDIS_TAGS ?= 2.6.17 2.8.22 3.0.7 3.2.8 4.0.11 5.0.1

ARCHIVE_URL = https://github.com/antirez/redis/archive
INSTALL_DIR ?= build

REDIS_TARGETS = $(foreach T,$(REDIS_TAGS),$(INSTALL_DIR)/$T/redis-server)

# Python implementation
PYTHON_IMPL = $(shell $(PYTHON) -c "import sys; print(sys.implementation.name)")

EXAMPLES = $(shell find examples -name "*.py")

.PHONY: all flake doc man-doc spelling test cov dist devel clean mypy
all: aioredis.egg-info flake doc cov

doc: spelling
	$(MAKE) -C docs html
man-doc: spelling
	$(MAKE) -C docs man
spelling:
	@echo "Running spelling check"
	$(MAKE) -C docs spelling

ifeq ($(PYTHON_IMPL), cpython)
flake:
	$(FLAKE) aioredis tests examples
else
flake:
	@echo "Job is not configured to run on $(PYTHON_IMPL); skipped."
endif

mypy:
	$(MYPY) aioredis --ignore-missing-imports

test:
	$(PYTEST)

cov coverage:
	$(PYTEST) --cov

dist: clean man-doc
	$(PYTHON) setup.py sdist bdist_wheel

clean:
	-rm -r docs/_build
	-rm -r build dist aioredis.egg-info

devel: aioredis.egg-info
	pip install -U pip
	pip install -U \
		-r tests/requirements.txt \
		-r docs/requirements.txt \
		bumpversion \
		wheel

aioredis.egg-info:
	pip install -Ue .


ifdef TRAVIS
examples: .start-redis $(EXAMPLES)
else
examples: $(EXAMPLES)
endif

$(EXAMPLES):
	@export REDIS_VERSION="$(redis-cli INFO SERVER | sed -n 2p)"
	$(PYTHON) $@

.start-redis:
	$(shell which redis-server) ./examples/redis.conf
	$(shell which redis-server) ./examples/redis-sentinel.conf --sentinel
	sleep 5s
	echo "QUIT" | nc localhost 6379
	echo "QUIT" | nc localhost 26379

.PHONY: $(EXAMPLES)


certificate:
	$(MAKE) -C tests/ssl

ci-test: $(REDIS_TARGETS)
	@$(call echo, "Tests run")
	pytest --cov \
		$(foreach T,$(REDIS_TARGETS),--redis-server=$T)

ci-test-%: $(INSTALL_DIR)/%/redis-server
	pytest --cov --redis-server=$<

ci-build-redis: $(REDIS_TARGETS)

$(INSTALL_DIR)/%/redis-server: /tmp/redis-%/src/redis-server
	mkdir -p $(abspath $(INSTALL_DIR))/$*
	cp -p /tmp/redis-$*/src/redis-server $(abspath $(INSTALL_DIR))/$*/
	@echo "Done building redis-$*"

/tmp/redis-%/src/redis-server:
	@echo "Building redis-$*..."
	wget -nv -c $(ARCHIVE_URL)/$*.tar.gz -O - | tar -xzC /tmp
	$(MAKE) -j -C $(dir $@) redis-server >/dev/null 2>/dev/null
