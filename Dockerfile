# ting-exporter v3 (design 13.1). Built by `docker compose build ting-exporter` from deploy/compose.yml.
# PYTHON_IMAGE must be pinned by its multi-arch index digest (one pin serves aarch64 and x86_64), e.g.
#   python:3.13-slim@sha256:<digest>      (docker buildx imagetools inspect python:3.13-slim)
ARG PYTHON_IMAGE=python:3.13-slim
FROM ${PYTHON_IMAGE} AS build
ENV PIP_NO_CACHE_DIR=1 PIP_DISABLE_PIP_VERSION_CHECK=1
COPY requirements.lock /tmp/requirements.lock
RUN python -m venv /venv && /venv/bin/pip install --require-hashes --no-deps -r /tmp/requirements.lock

FROM ${PYTHON_IMAGE}
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PATH=/venv/bin:$PATH PYTHONPATH=/app
COPY --from=build /venv /venv
COPY src/ting_exporter /app/ting_exporter
# Runtime user 1000:1000 (compose sets TING_UID:TING_GID too): it owns DATA_ROOT/ting and reads the 0600 secrets.
USER 1000:1000
EXPOSE 9786
HEALTHCHECK --interval=30s --timeout=5s --start-period=30s --retries=3 \
  CMD ["python", "-c", "import urllib.request,sys; sys.exit(urllib.request.urlopen('http://127.0.0.1:9786/healthz', timeout=4).status != 200)"]
ENTRYPOINT ["python", "-m", "ting_exporter"]
CMD ["serve"]
