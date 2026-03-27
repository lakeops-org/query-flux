#!/bin/sh
set -e
export PYTHONPATH="$(/opt/venv/bin/python3 -c 'import site; print(site.getsitepackages()[0])')"
exec /app/queryflux "$@"
