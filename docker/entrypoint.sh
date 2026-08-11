#!/usr/bin/env bash
# Dev entrypoint for the `jupyter` service.
#
# Runs on every `docker compose up` / restart, against a plain, pre-built
# Python+Node image — there is no docker/Dockerfile and nothing is ever
# built. This script installs/refreshes the JS and Python dependencies for
# whatever is currently on disk (bind-mounted from the host), then starts
# JupyterLab with both halves live-reloading:
#   - frontend: a background `jlpm watch` rebuilds TS/CSS on every change
#   - backend:  `jupyter lab --autoreload` restarts the server process
#               whenever an imported .py file (incl. jupyterlab_airflow/)
#               changes
set -euo pipefail

cd /workspace

echo "==> Installing JupyterLab + build tooling"
pip install --no-cache-dir \
    "jupyterlab>=4.0.0,<5" \
    "hatchling>=1.5.0" \
    "hatch-jupyter-builder>=0.5" \
    "hatch-nodejs-version>=0.3.2"

echo "==> Installing JS dependencies (jlpm install)"
jlpm install

echo "==> Installing Python package in editable mode"
# `jupyter labextension develop --overwrite` (below) replaces
# /usr/local/share/jupyter/labextensions/jupyterlab-airflow — a real
# directory pip's editable install populated via the shared-data mapping —
# with a SYMLINK into the bind-mounted /workspace/jupyterlab_airflow/labextension.
# With /usr/local persisted in a volume, that symlink persists too, so the
# *next* `pip install -e .` finds an "existing installation" and does its
# uninstall-then-reinstall dance; its uninstall step expects the RECORD's real
# files under that path (incl. build_log.json) and instead finds a symlink to
# a directory that doesn't have that file, crashing mid-rollback. Wiping this
# package's own install footprint first makes every install a clean "not
# currently installed" install — its dependencies (jupyterlab, hatchling,
# pytest, ...) stay untouched and cached in the volume regardless.
SITE_PACKAGES=$(python3 -c 'import sysconfig; print(sysconfig.get_path("purelib"))')
rm -rf "$SITE_PACKAGES"/jupyterlab_airflow-*.dist-info \
       "$SITE_PACKAGES"/_editable_impl_jupyterlab_airflow.pth \
       /usr/local/share/jupyter/labextensions/jupyterlab-airflow \
       /usr/local/etc/jupyter/jupyter_server_config.d/jupyterlab-airflow.json \
       /usr/local/etc/jupyter/jupyter_notebook_config.d/jupyterlab-airflow.json
pip install --no-cache-dir -e ".[test]"

echo "==> Linking labextension for development"
jupyter labextension develop . --overwrite

echo "==> Enabling server extension"
jupyter server extension enable jupyterlab_airflow

echo "==> Starting TypeScript/labextension watch (background, logs: /tmp/jlpm-watch.log)"
jlpm watch >/tmp/jlpm-watch.log 2>&1 &

echo "==> Starting JupyterLab on :8888 (--autoreload picks up backend .py changes)"
exec jupyter lab \
    --ip=0.0.0.0 \
    --port=8888 \
    --no-browser \
    --allow-root \
    --autoreload \
    --ServerApp.token='' \
    --notebook-dir=/workspace/notebooks
