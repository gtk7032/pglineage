FROM python:3.10-slim

ARG USERNAME=pglineage
ARG GROUPNAME=pglineage
ARG UID=1000
ARG GID=1000
ARG APP_DIR=/usr/local/pglineage

RUN groupadd -g "$GID" "$GROUPNAME" \
    && useradd -m -s /bin/bash -u "$UID" -g"$GID" "$USERNAME" \
    && apt-get update && apt-get install -y --no-install-recommends graphviz \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* \
    && mkdir -p "$APP_DIR" 

COPY ./setup.cfg "$APP_DIR"
COPY ./setup.py "$APP_DIR"

USER "$USERNAME"
WORKDIR "$APP_DIR"
