FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
WORKDIR /app

COPY pyproject.toml README.md ./
COPY src ./src
RUN pip install --no-cache-dir .

RUN useradd --system --uid 10001 --home /app controller \
    && mkdir -p /data \
    && chown controller:controller /data
USER controller

ENTRYPOINT ["python", "-m", "cdn_controller"]

