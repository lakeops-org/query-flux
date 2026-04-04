# python
"""
File: `snowflake-client/main.py`
Exercise QueryFlux's Snowflake-compat endpoint locally.

Every run executes all three paths in order:
  1. http      — raw urllib: login-request + query-request
  2. fetchall  — Snowflake Python connector (new session)
  3. cursor    — connector again, fetchmany loop (separate session)

A failure in one block is reported; the script continues with the rest. Exit code 1 if any block failed.

Install:
  pip3 install tabulate snowflake-connector-python pyarrow
  (`pyarrow` decodes HTTP `rowsetBase64` Arrow IPC into a printed table.)

Examples:
  python main.py
  python main.py --sql "SELECT 1 + 1 AS success" --batch-size 3
  python main.py --full-json
"""
from __future__ import annotations

import argparse
import base64
import io
import json
import sys
import traceback
import urllib.error
import urllib.request
import uuid

try:
    from tabulate import tabulate  # optional dependency
except Exception:
    tabulate = None

DEFAULT_SQL = "SELECT 1 + 1 AS success"
DEFAULT_BASE_URL = "http://127.0.0.1:8445"

# Same shape as `test_login_raw.py` — matches QueryFlux session handler.
LOGIN_BODY = {
    "data": {
        "CLIENT_APP_ID": "PythonConnector",
        "CLIENT_APP_VERSION": "1.0",
        "LOGIN_NAME": "myuser",
        "PASSWORD": "mypassword",
        "AUTHENTICATOR": "SNOWFLAKE",
    }
}


def format_table(headers, rows):
    if tabulate:
        return tabulate(rows, headers=headers, tablefmt="psql")
    # simple fallback fixed-width formatter
    widths = [
        max(len(str(h)), max((len(str(r[i])) for r in rows), default=0))
        for i, h in enumerate(headers)
    ]
    sep = "+".join("-" * (w + 2) for w in widths)
    sep = f"+{sep}+"
    hdr = "| " + " | ".join(str(h).ljust(widths[i]) for i, h in enumerate(headers)) + " |"
    lines = [sep, hdr, sep]
    for r in rows:
        lines.append(
            "| " + " | ".join(str(r[i]).ljust(widths[i]) for i in range(len(headers))) + " |"
        )
    lines.append(sep)
    return "\n".join(lines)


def run_fetchall(cursor, sql: str) -> None:
    cursor.execute(sql)
    rows = cursor.fetchall()
    headers = [col[0] for col in cursor.description] if cursor.description else []
    print("=== fetchall() ===")
    print(format_table(headers, rows))
    print(f"(rows: {len(rows)})")


def run_cursor_fetchmany(cursor, sql: str, batch_size: int) -> None:
    """Like iterating the cursor in chunks — avoids loading the whole result at once."""
    cursor.execute(sql)
    headers = [col[0] for col in cursor.description] if cursor.description else []
    print(f"=== fetchmany({batch_size}) loop (cursor-style) ===")

    total = 0
    chunk_idx = 0
    while True:
        chunk = cursor.fetchmany(batch_size)
        if not chunk:
            break
        chunk_idx += 1
        total += len(chunk)
        print(f"\n--- chunk {chunk_idx} ({len(chunk)} rows) ---")
        print(format_table(headers, chunk))

    print(f"\n(total rows streamed: {total})")


def _post_json(url: str, body: dict, headers: dict | None = None) -> tuple[int, dict]:
    h = {
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    if headers:
        h.update(headers)
    req = urllib.request.Request(
        url,
        data=json.dumps(body).encode("utf-8"),
        headers=h,
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=120) as resp:
            raw = resp.read().decode("utf-8")
            return resp.status, json.loads(raw)
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"HTTP {e.code} {e.reason}\n{body}") from None
    except OSError as e:
        raise RuntimeError(f"Request failed — is QueryFlux up? {e}") from e


def print_decoded_arrow_rowset(b64: str) -> None:
    """Decode QueryFlux `rowsetBase64` (Arrow IPC **stream** bytes) and print a table."""
    try:
        import pyarrow.ipc as ipc
    except ImportError:
        print(
            f"\nrowsetBase64: {len(b64)} chars "
            "(install pyarrow to print rows: pip install pyarrow)"
        )
        return
    try:
        raw = base64.b64decode(b64)
        reader = ipc.open_stream(io.BytesIO(raw))
        table = reader.read_all()
    except Exception as e:
        print(f"\nCould not decode rowsetBase64 as Arrow IPC stream: {e}")
        return

    n = table.num_rows
    print(f"\n=== rows from rowsetBase64 (Arrow IPC, {n} row(s)) ===")
    if n == 0:
        print("(empty)")
        return
    try:
        pydict = table.to_pydict()
        headers = list(pydict.keys())
        # column-oriented → list of row tuples for format_table
        cols = [pydict[h] for h in headers]
        rows = [tuple(cols[j][i] for j in range(len(headers))) for i in range(n)]
    except Exception:
        print(table.to_string())
        return
    print(format_table(headers, rows))


def run_http(base_url: str, sql: str, print_full_json: bool) -> None:
    """Login + query using only stdlib HTTP (mirrors what the connector does under the hood)."""
    base = base_url.rstrip("/")
    rid = uuid.uuid4().hex

    login_url = f"{base}/session/v1/login-request?requestId={rid}"
    print(f"POST {login_url.split('?')[0]}")
    status, login = _post_json(login_url, LOGIN_BODY)
    print(f"HTTP {status}, success={login.get('success')}")
    if not login.get("success"):
        raise RuntimeError(str(login.get("message", login)))

    token = login.get("data", {}).get("token")
    if not token:
        raise RuntimeError("No data.token in login response")

    if print_full_json:
        print(json.dumps(login, indent=2))

    q_rid = uuid.uuid4().hex
    query_url = f"{base}/queries/v1/query-request?requestId={q_rid}"
    auth_hdr = f'Snowflake Token="{token}"'
    print(f"\nPOST /queries/v1/query-request")
    print(f"Authorization: Snowflake Token=<{len(token)} chars>")
    _, result = _post_json(
        query_url,
        {"sqlText": sql},
        headers={"Authorization": auth_hdr},
    )

    data = result.get("data") or {}

    if print_full_json:
        print(json.dumps(result, indent=2))
    else:
        ok = result.get("success")
        print(f"success={ok}")
        if not ok:
            print(json.dumps(result, indent=2))
            return
        rowtype = data.get("rowtype") or []
        rowset = data.get("rowset")
        print(f"columns (rowtype): {len(rowtype)}")
        for col in rowtype:
            name = col.get("name", "?")
            t = col.get("type", col.get("data_type", "?"))
            print(f"  - {name}: {t}")
        if rowset is not None:
            n = len(rowset) if isinstance(rowset, list) else "?"
            print(f"rowset rows (JSON): {n}")

    b64 = data.get("rowsetBase64")
    if result.get("success") and isinstance(b64, str) and b64:
        print_decoded_arrow_rowset(b64)


def _snowflake_connect():
    import snowflake.connector

    try:
        return snowflake.connector.connect(
            host="127.0.0.1",
            port=8445,
            protocol="http",
            account="queryflux",
            user="myuser",
            password="mypassword",
            warehouse="COMPUTE_WH_XS",
            database="MY_DB",
            schema="PUBLIC",
            insecure_mode=True,
        )
    except snowflake.connector.errors.DatabaseError as e:
        raise RuntimeError(
            f"Login failed (check QueryFlux routing + Snowflake clusters): {e}"
        ) from e


def run_connector_fetchall(sql: str) -> None:
    conn = _snowflake_connect()
    cursor = conn.cursor()
    try:
        run_fetchall(cursor, sql)
    finally:
        cursor.close()
        conn.close()


def run_connector_cursor(sql: str, batch_size: int) -> None:
    conn = _snowflake_connect()
    cursor = conn.cursor()
    try:
        run_cursor_fetchmany(cursor, sql, max(1, batch_size))
    finally:
        cursor.close()
        conn.close()


def _banner(title: str) -> None:
    line = "#" * 22
    print(f"\n{line} {title} {line}\n")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Each run: http (urllib), then fetchall, then cursor — same --sql for all"
    )
    parser.add_argument(
        "--sql",
        default=DEFAULT_SQL,
        help=f"SQL for all paths (default: {DEFAULT_SQL!r})",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=10,
        help="fetchmany size for cursor path (default: 10)",
    )
    parser.add_argument(
        "--base-url",
        default=DEFAULT_BASE_URL,
        help=f"HTTP base for raw path (default: {DEFAULT_BASE_URL})",
    )
    parser.add_argument(
        "--full-json",
        action="store_true",
        help="http path: print full login and query JSON (can be huge)",
    )
    args = parser.parse_args()

    failures: list[str] = []

    _banner("1) http (urllib)")
    try:
        run_http(args.base_url, args.sql, args.full_json)
    except Exception as e:
        failures.append("http")
        print(f"ERROR: {e}", file=sys.stderr)
        traceback.print_exc()

    _banner("2) fetchall (snowflake-connector-python)")
    try:
        run_connector_fetchall(args.sql)
    except Exception as e:
        failures.append("fetchall")
        print(f"ERROR: {e}", file=sys.stderr)
        traceback.print_exc()

    _banner("3) cursor fetchmany (snowflake-connector-python)")
    try:
        run_connector_cursor(args.sql, args.batch_size)
    except Exception as e:
        failures.append("cursor")
        print(f"ERROR: {e}", file=sys.stderr)
        traceback.print_exc()

    if failures:
        sys.exit(1)


if __name__ == "__main__":
    try:
        main()
    except BrokenPipeError:
        sys.exit(0)
