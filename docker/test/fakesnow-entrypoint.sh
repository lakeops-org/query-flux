#!/bin/sh
# Start fakesnow for e2e. Patches installed package for snowflake-connector-rs (see fakesnow-apply-patches.py).
set -e
if [ ! -r /fakesnow-apply-patches.py ]; then
	echo "fakesnow: missing /fakesnow-apply-patches.py (compose must mount docker/test/fakesnow-apply-patches.py)" >&2
	exit 1
fi
# Pin version so patch anchors stay stable; bump when updating docker/test/fakesnow-apply-patches.py.
pip install --quiet 'fakesnow[server]==0.11.4'
python3 /fakesnow-apply-patches.py
exec fakesnow -s -p 8085 --host 0.0.0.0
