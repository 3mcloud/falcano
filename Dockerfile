FROM python:slim as base
RUN apt update && apt install make
WORKDIR /work
ENV AWS_DEFAULT_REGION=us-east-1
COPY setup.py setup.py
COPY README.md README.md

FROM base as test
RUN pip install .[dev]
# Use the below pip install command instead of the above if you experience SSL certificate issues locally
# RUN pip install --trusted-host pypi.python.org --trusted-host pypi.org --trusted-host files.pythonhosted.org .[dev]
COPY . .
CMD ["python", "-m", "pytest", "-s", "tests/unit/"]
