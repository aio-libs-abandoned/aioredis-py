
PYTHON ?= python3
FLAKE ?= pyflakes
PEP ?= pep8
PYTEST ?= py.test
REDIS_VERSION ?= "$(shell redis-cli INFO SERVER | sed -n 2p)"

.PHONY: all flake doc test cov dist devel
all: aioredis.egg-info flake doc cov

doc:
	make -C docs html

flake:
	$(FLAKE) aioredis tests examples
	$(PEP) aioredis tests examples

test:
	$(PYTEST) -v

cov coverage:
	$(PYTEST) -v --cov --cov-report=term --cov-report=html

dist:
	-rm -r build dist aioredis.egg-info
	$(PYTHON) setup.py sdist bdist_wheel

devel: aioredis.egg-info
	pip install -U pip
	pip install -U pyflakes pep8 sphinx coverage bumpversion wheel
	pip install -Ur docs/requirements.txt

aioredis.egg-info:
	pip install -Ue .


CERT_DIR ?= tests/ssl

certificate: $(CERT_DIR)/test.pem $(CERT_DIR)/test.crt

$(CERT_DIR)/test.pem: $(CERT_DIR)/test.crt $(CERT_DIR)/.test.key
	cat $^ > $@

$(CERT_DIR)/test.crt: $(CERT_DIR)/.test.key
	openssl req -new -key $< -x509 -out $@ -batch

$(CERT_DIR)/.test.key:
	mkdir -p $(CERT_DIR)
	openssl genrsa -out $@ 1024
