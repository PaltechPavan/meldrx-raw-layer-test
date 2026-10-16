#!/bin/sh
# One job = ingest, then the downstream step. If ingest fails, the downstream step never runs.
set -e
python /app/meldrx_ingest.py
exec "$@"
