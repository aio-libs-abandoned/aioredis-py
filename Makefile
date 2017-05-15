PYTHON ?= python3
FLAKE ?= flake8
PYTEST ?= py.test

REDIS_VERSION ?= "$(shell redis-cli INFO SERVER | sed -n 2p)"
REDIS_TAGS ?= 2.6.17 2.8.22 3.0.7 3.2.8 4.0-rc2

ARCHIVE_URL = https://github.com/antirez/redis/archive
INSTALL_DIR ?= build

TEST_ARGS ?= "-n 4"

REDIS_TARGETS = $(foreach T,$(REDIS_TAGS),$(INSTALL_DIR)/$T/redis-server)

# Python version and implementation
PYTHON_IMPL = $(shell $(PYTHON) -c "import sys; print(sys.implementation.name)")
PYTHON_35 = $(shell $(PYTHON) -c "import sys; print(sys.version_info >= (3, 5))")

ifeq ($(PYTHON_35), True)
FLAKE_ARGS = aioredis tests examples
EXAMPLES = $(shell find examples -name "*.py")
else
FLAKE_ARGS = --exclude=py35_* aioredis tests examples/py34
EXAMPLES = $(shell find examples/py34 -name "*.py")
endif

.PHONY: all flake doc man-doc spelling test cov dist devel clean
all: aioredis.egg-info flake doc cov

doc: spelling
	make -C docs html
man-doc: spelling
	make -C docs man
spelling:
	@echo "Running spelling check"
	make -C docs spelling

ifeq ($(PYTHON_IMPL), cpython)
flake:
	$(FLAKE) $(FLAKE_ARGS)
else
flake:
	@echo "Job is not configured to run on $(PYTHON_IMPL); skipped."
endif

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
		sphinx \
		sphinx_rtd_theme \
		bumpversion \
		wheel
	pip install -Ur tests/requirements.txt
	pip install -Ur docs/requirements.txt

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

.start-redis: $(lastword $(REDIS_TARGETS))
	$< ./examples/redis.conf
	$< ./examples/redis-sentinel.conf --sentinel
	sleep 5s
	echo "QUIT" | nc localhost 6379
	echo "QUIT" | nc localhost 26379

.PHONY: $(EXAMPLES)


certificate:
	make -C tests/ssl

ci-test: $(REDIS_TARGETS)
	@$(call echo, "Tests run")
	py.test -rsxX --cov \
		$(foreach T,$(REDIS_TARGETS),--redis-server=$T) $(TEST_ARGS)

ci-test-%: $(INSTALL_DIR)/%/redis-server
	py.test -rsxX --cov --redis-server=$< $(TEST_ARGS)

ci-build-redis: $(REDIS_TARGETS)

$(INSTALL_DIR)/%/redis-server:
	@echo "Building redis-$*..."
	wget -nv -c $(ARCHIVE_URL)/$*.tar.gz -O - | tar -xzC /tmp
	make -j -C /tmp/redis-$* \
		INSTALL_BIN=$(abspath $(INSTALL_DIR))/$* install >/dev/null 2>/dev/null
	@echo "Done building redis-$*"
