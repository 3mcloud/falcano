FROM python:slim as base
RUN apt update && apt install make
WORKDIR /work
COPY requirements.txt requirements.txt
COPY requirements-test.txt requirements-test.txt
ENV AWS_DEFAULT_REGION=us-east-1


FROM base as test
RUN pip install -r requirements-test.txt
COPY . .
CMD ["python", "-m", "pytest", "-s", "tests/unit/"]
