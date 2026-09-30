# syntax=docker/dockerfile:1

FROM python:3.12-slim AS builder

ENV PIP_NO_CACHE_DIR=1 PIP_DISABLE_PIP_VERSION_CHECK=1
RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

WORKDIR /build
COPY pyproject.toml poetry.lock README.md ./
COPY src ./src
RUN pip install .

FROM python:3.12-slim

ARG VERSION=dev
ARG REVISION=unknown
LABEL org.opencontainers.image.title="lido-keys-exporter" \
      org.opencontainers.image.description="Prometheus exporter for deposits, Lido exit requests and EIP-7002 triggered withdrawals of Lido validator keys" \
      org.opencontainers.image.version="${VERSION}" \
      org.opencontainers.image.revision="${REVISION}"

COPY --from=builder /opt/venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH" \
    PYTHONUNBUFFERED=1 \
    LKE_CONFIG=/opt/app/config.yaml

RUN groupadd --gid 2000 app \
    && useradd --uid 2000 --gid 2000 --no-create-home --shell /usr/sbin/nologin app \
    && mkdir -p /opt/app/data \
    && chown -R 2000:2000 /opt/app/data

WORKDIR /opt/app
VOLUME /opt/app/data
EXPOSE 9800
USER 2000:2000

ENTRYPOINT ["python", "-m", "src"]
