#!/usr/bin/env python3
"""Patch installed fakesnow for snowflake-connector-rs (QueryFlux SnowflakeAdapter).

1. Login: Rust omits data.SESSION_PARAMETERS.
2. Query: Rust only accepts queryResultFormat=json and rowset[][]; fakesnow defaults to Arrow.
"""
from __future__ import annotations

import pathlib
import re
import sys

HELPER = '''def _sf_json_rowset_from_cursor(cur: Any) -> list[list[str | None]]:
    """snowflake-connector-rs expects JSON rowset; fakesnow serves Arrow to Python by default."""
    at = cur._arrow_table  # noqa: SLF001
    if at is None or at.num_rows == 0:
        return []
    names = at.column_names

    def cell(v: object) -> str | None:
        if v is None:
            return None
        if isinstance(v, bool):
            return "true" if v else "false"
        return str(v)

    return [[cell(rec.get(nm)) for nm in names] for rec in at.to_pylist()]


'''

# Wheel / Black-style indentation (8 spaces for `if`, 12 for body inside query_request).
ARROW_BLOCKS = (
    (
        "        if cur._arrow_table: # noqa: SLF001\n"
        "            batch_bytes = to_ipc(to_sf(cur._arrow_table, rowtype)) # noqa: SLF001\n"
        "            rowset_b64 = b64encode(batch_bytes).decode(\"utf-8\")\n"
        "        else:\n"
        "            rowset_b64 = \"\"\n"
    ),
    (
        "        if cur._arrow_table:\n"
        "            batch_bytes = to_ipc(to_sf(cur._arrow_table, rowtype))\n"
        "            rowset_b64 = b64encode(batch_bytes).decode(\"utf-8\")\n"
        "        else:\n"
        "            rowset_b64 = \"\"\n"
    ),
)

ARROW_REPLACEMENT = "        json_rowset = _sf_json_rowset_from_cursor(cur)\n"


def main() -> None:
    try:
        import fakesnow.server as fsrv  # noqa: PLC0415 — only after pip install in container
    except ImportError as e:
        sys.exit(f"fakesnow.server not importable (pip install fakesnow[server] first): {e}")
    path = pathlib.Path(fsrv.__file__)
    text = path.read_text()
    text = text.replace("\r\n", "\n")
    orig = text

    # --- 1) SESSION_PARAMETERS ---
    if 'body_json["data"]["SESSION_PARAMETERS"]' in text:
        text = text.replace(
            'body_json["data"]["SESSION_PARAMETERS"]',
            'body_json["data"].get("SESSION_PARAMETERS", {})',
            1,
        )

    # --- 2) Helper ---
    if "_sf_json_rowset_from_cursor" not in text:
        marker = "async def query_request"
        if marker not in text:
            sys.exit(f"patch: {marker!r} not found in {path}")
        text = text.replace(marker, HELPER + marker, 1)

    # --- 3) Arrow -> json_rowset (string replace; more reliable than regex across versions) ---
    if "json_rowset = _sf_json_rowset_from_cursor" not in text:
        replaced = False
        for block in ARROW_BLOCKS:
            if block in text:
                text = text.replace(block, ARROW_REPLACEMENT, 1)
                replaced = True
                break
        if not replaced:
            # Last resort: flexible regex (optional # noqa, flexible spaces)
            arrow_re = re.compile(
                r"^[ \t]+if cur\._arrow_table:\s*(?:# noqa: SLF001)?\s*\n"
                r"[ \t]+batch_bytes = to_ipc\(to_sf\(cur\._arrow_table, rowtype\)\)\s*(?:# noqa: SLF001)?\s*\n"
                r"[ \t]+rowset_b64 = b64encode\(batch_bytes\)\.decode\(\"utf-8\"\)\s*\n"
                r"[ \t]+else:\s*\n"
                r"[ \t]+rowset_b64 = \"\"\s*\n",
                re.MULTILINE,
            )
            m = arrow_re.search(text)
            if not m:
                sys.exit(
                    f"patch: could not find arrow rowset block in {path}. "
                    "Open an issue or extend ARROW_BLOCKS in docker/test/fakesnow-apply-patches.py"
                )
            ind = re.match(r"^([ \t]+)", m.group(0), re.MULTILINE)
            prefix = ind.group(1) if ind else "        "
            text = arrow_re.sub(f"{prefix}json_rowset = _sf_json_rowset_from_cursor(cur)\n", text, count=1)

    # --- 4) Response payload ---
    text = text.replace('"rowsetBase64": rowset_b64,', '"rowset": json_rowset,', 1)
    text = text.replace('"queryResultFormat": "arrow"', '"queryResultFormat": "json"')

    # --- 5) Validate (connector-rs hard-fails on arrow) ---
    if '"queryResultFormat": "arrow"' in text:
        sys.exit(f"patch: still contains queryResultFormat arrow after patch: {path}")
    if '"rowset": json_rowset' not in text:
        sys.exit(f"patch: rowset/json_rowset wiring missing after patch: {path}")

    if text == orig:
        print(f"Already patched: {path}", file=sys.stderr)
        return

    path.write_text(text)
    print(f"Patched {path} for snowflake-connector-rs compatibility", file=sys.stderr)


if __name__ == "__main__":
    main()
