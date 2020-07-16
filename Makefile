

build-test:
	docker build -t falcano:test --target test .

develop:
	docker run -it --rm \
	-v ${PWD}:/work \
	falcano:test bash

unit: build-test
	docker run -it --rm \
	-v ${PWD}:/work \
	falcano:test
