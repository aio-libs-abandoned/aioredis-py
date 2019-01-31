PYTHON ?= python3
FLAKE ?= flake8
PYTEST ?= pytest

REDIS_VERSION ?= "$(shell redis-cli INFO SERVER | sed -n 2p)"
REDIS_TAGS ?= 2.6.17 2.8.22 3.0.7 3.2.8 4.0.11 5.0.1

ARCHIVE_URL = https://github.com/antirez/redis/archive
INSTALL_DIR ?= build

TEST_ARGS ?= "-n 4"

REDIS_TARGETS = $(foreach T,$(REDIS_TAGS),$(INSTALL_DIR)/$T/redis)

# Python implementation
PYTHON_IMPL = $(shell $(PYTHON) -c "import sys; print(sys.implementation.name)")

EXAMPLES = $(shell find examples -name "*.py")

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
	$(FLAKE) aioredis tests examples
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
	pytest --cov \
		$(foreach T,$(REDIS_TARGETS),--redis-server=$T-server) $(TEST_ARGS)

ci-test-%: $(INSTALL_DIR)/%/redis
	pytest --cov --redis-server=$(abspath $(INSTALL_DIR))/$*/redis-server $(TEST_ARGS) 

ci-build-redis: $(REDIS_TARGETS)


$(INSTALL_DIR)/%/redis:
	@echo "Building redis-$*..."
	@if [ -d "$(abspath $(INSTALL_DIR))/$*" ]; then \
		echo 'Сache building: $(abspath $(INSTALL_DIR))/$*'; \
		echo 'ls -la $(abspath $(INSTALL_DIR))/$*/'; \
		ls -la $(abspath $(INSTALL_DIR))/$*/; \
	else \
		echo 'Instal building: $(abspath $(INSTALL_DIR))/$*'; \
		echo 'wget -nv -c $(ARCHIVE_URL)/$*.tar.gz -O - | tar -xzC /tmp;'; \
		wget -nv -c $(ARCHIVE_URL)/$*.tar.gz -O - | tar -xzC /tmp; \
		echo 'make -j -C /tmp/redis-$* INSTALL_BIN=$(abspath $(INSTALL_DIR))/$* install >/dev/null 2>/dev/null'; \
		make -j -C /tmp/redis-$* INSTALL_BIN=$(abspath $(INSTALL_DIR))/$* install >/dev/null 2>/dev/null; \
	fi

	@echo "Done building redis-$*"
