FROM python:slim as base
RUN apt update && apt install make
WORKDIR /work
ENV AWS_DEFAULT_REGION=us-east-1
COPY setup.py setup.py
COPY README.md README.md

FROM base as test
RUN pip install .[dev]
COPY . .
CMD ["python", "-m", "pytest", "-s", "tests/unit/"]
