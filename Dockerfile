FROM python:3.10-slim

ARG USERNAME=pglineage
ARG GROUPNAME=pglineage
ARG UID=1000
ARG GID=1000
ARG APP_DIR=/usr/local/pglineage

RUN groupadd -g "$GID" "$GROUPNAME" \
    && useradd -m -s /bin/bash -u "$UID" -g"$GID" "$USERNAME" \
    && mkdir -p "$APP_DIR" \
    && chown -R "$USERNAME" "$APP_DIR" \
    && apt-get update && apt-get install -y --no-install-recommends graphviz \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* \
    && pip install --no-cache-dir \
    graphviz==0.20.1  \
    pglast==5.1 \
    tqdm==4.65.0 \
    chardet==5.2.0

USER "$USERNAME"
WORKDIR "$APP_DIR"
