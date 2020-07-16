FROM python:slim as base
WORKDIR /work
COPY requirements.txt requirements.txt
COPY requirements-test.txt requirements-test.txt



FROM base as test
RUN pip install -r requirements-test.txt
COPY . .
CMD ["python", "-m", "pytest", "-s", "tests/unit/"]
