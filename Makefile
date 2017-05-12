PYTHON ?= python3
FLAKE ?= flake8
PYTEST ?= py.test
REDIS_VERSION ?= "$(shell redis-cli INFO SERVER | sed -n 2p)"
REDIS_TAGS ?= 2.6.17 2.8.22 3.0.7 3.2.8 4.0-rc2

ARCHIVE_URL = https://github.com/antirez/redis/archive
INSTALL_DIR ?= build

TEST_ARGS ?= "-n 4"

REDIS_TARGETS = $(foreach T,$(REDIS_TAGS),$(INSTALL_DIR)/$T/redis-server)

.PHONY: all flake doc man-doc spelling test cov dist devel clean
all: aioredis.egg-info flake doc cov

doc: spelling
	make -C docs html
man-doc: spelling
	make -C docs man
spelling:
	$(call travis_start,spellchek)
	@echo "Running spelling check"
	make -C docs spelling
	$(call travis_end,spellchek)

flake:
	$(call travis_start,flake)
	@echo "Running flake8"
	if $(PYTHON) -c "import sys; sys.exit(sys.version_info < (3, 5))"; then \
		$(FLAKE) aioredis tests examples; \
	else \
		$(FLAKE) --exclude=py35_* aioredis tests examples/py34; \
	fi;
	$(call travis_end,flake)

test:
	$(PYTEST)

cov coverage:
	$(PYTEST) --cov=aioredis

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


ifeq ($(shell python -c "import sys; print(sys.version_info < (3,5))"), "True")
EXAMPLES = $(shell find examples -name "*.py")
else
EXAMPLES = $(shell find examples/py34 -name "*.py")
endif

ifdef TRAVIS
examples: .start-redis $(EXAMPLES)
else
examples: $(EXAMPLES)
endif

$(EXAMPLES):
	$(call travis_start,$@)
	@export REDIS_VERSION="$(redis-cli INFO SERVER | sed -n 2p)"
	@echo "Running example '$@'"
	python $@
	$(call travis_end,$@)

.start-redis: $(lastword $(REDIS_TARGETS))
	$< --daemonize yes \
		--pidfile ./aioredis-server.pid \
		--unixsocket /tmp/aioredis.sock \
		--port 6379 \
		--save ""
	sleep 3s

.PHONY: $(EXAMPLES)


certificate:
	make -C tests/ssl

ci-test: $(REDIS_TARGETS)
	$(call travis_start,tests)
	@echo "Tests run"
	py.test -rsxX --cov \
		$(foreach T,$(REDIS_TARGETS),--redis-server=$T) $(TEST_ARGS)
	$(call travis_end,tests)

ci-build-redis: $(REDIS_TARGETS)

$(INSTALL_DIR)/%/redis-server:
	@echo "Building redis-$*..."
	wget -nv -c $(ARCHIVE_URL)/$*.tar.gz -O - | tar -xzC /tmp
	make -j -C /tmp/redis-$* \
		INSTALL_BIN=$(abspath $(INSTALL_DIR))/$* install >/dev/null 2>/dev/null
	@echo "Done building redis-$*"


# ifdef TRAVIS
#
# define travis_start
# 	@echo "travis_fold:start:$1"
# endef
#
# define travis_end
# 	@echo "travis_fold:end:$1"
# endef
#
# endif
