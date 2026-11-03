# Spark driver image for the local demo: Python + a JRE. Spark runs in local
# mode inside this container; Delta and Kafka connector jars are resolved by
# Spark on first start and cached in a named volume.
FROM python:3.13-slim-bookworm

RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /workspace
COPY pyproject.toml README.md LICENSE ./
COPY src ./src
RUN pip install --no-cache-dir -e .

CMD ["sleep", "infinity"]
