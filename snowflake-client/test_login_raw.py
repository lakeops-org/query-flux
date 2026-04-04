#!/usr/bin/env python3
"""Minimal login test without the Snowflake connector — shows raw HTTP + JSON.

Run while QueryFlux is up with snowflakeHttp on 8445:

  python test_login_raw.py

Expect HTTP 200. Body is either success with token, or success:false with a clear message
(e.g. no routing / no warehouse sessions when Snowflake clusters are not configured).
"""
from __future__ import annotations

import json
import sys
import urllib.error
import urllib.request

URL = "http://127.0.0.1:8445/session/v1/login-request?requestId=test-req-1"

BODY = {
    "data": {
        "CLIENT_APP_ID": "PythonConnector",
        "CLIENT_APP_VERSION": "1.0",
        "LOGIN_NAME": "myuser",
        "PASSWORD": "mypassword",
        "AUTHENTICATOR": "SNOWFLAKE",
    }
}


def main() -> int:
    req = urllib.request.Request(
        URL,
        data=json.dumps(BODY).encode("utf-8"),
        headers={
            "Content-Type": "application/json",
            "Accept": "application/json",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            raw = resp.read().decode("utf-8")
            print("HTTP", resp.status)
            data = json.loads(raw)
            print(json.dumps(data, indent=2))
            if data.get("success"):
                print("\nOK: token present:", bool(data.get("data", {}).get("token")))
            else:
                print("\nLogin failed (expected if no Snowflake clusters in QueryFlux):", data.get("message"))
            return 0
    except urllib.error.HTTPError as e:
        print("HTTP error:", e.code, e.reason, file=sys.stderr)
        print(e.read().decode("utf-8", errors="replace"), file=sys.stderr)
        return 1
    except OSError as e:
        print("Connection failed — is QueryFlux listening on 8445?", e, file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
