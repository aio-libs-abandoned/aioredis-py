
all: dhparam.pem cert.pem cafile.crt

dhparam.pem:
	openssl dhparam -out $@ 2048
cafile.crt:
	openssl req -newkey rsa:2048 -keyout private.key \
		-batch -nodes -x509 -out $@
cert.pem: cafile.crt
	cat private.key $^ > $@

clean:
	rm private.key cert.pem cafile.crt dhparam.pem

.PHONY: all
