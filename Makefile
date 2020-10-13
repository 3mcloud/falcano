AWS_DEFAULT_REGION?=us-east-1
PROJECT=falcano

build-test:
	docker build -t falcano:test --target test .

develop: build-test
	mkdir -p data/dynamodb
	docker-compose up -d
	docker-compose exec falcano-develop bash


unit:
	python -m pytest -vvv \
		-W ignore::DeprecationWarning \
		--cov-report html \
		--cov-report term-missing \
		--cov=$(PROJECT) \
		tests/unit$(target)

integration:
	python -m pytest -s tests/integration

e2e:
	python -m pytest -s tests/e2e

dynamo-local:
	docker run --rm \
		-p 8000:8000 \
		--name dynamodb \
		--hostname dynamodb \
		-it amazon/dynamodb-local \
		-jar DynamoDBLocal.jar -sharedDb

debug-%:
	# allows you to attach the vscode debugger to a test.
	# Use the debug console and attach the debugger after
	# you run `make debug`
	python -m ptvsd --host 0.0.0.0 --port 5678 --wait \
		-m pytest -vvv \
		tests/$*$(target)
