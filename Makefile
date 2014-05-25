
FLAKE ?= pyflakes3
PEP ?= pep8

# doc:


flake:
	$(FLAKE) aioredis tests
	$(PEP) aioredis tests


.PHONY: doc flake
