import { NextRequest, NextResponse } from "next/server";
import {
  SESSION_COOKIE_NAME,
  basicAuthorizationHeader,
  decodeCredentialsFromCookie,
  encodeCredentialsForCookie,
} from "@/lib/admin-session-codec";

export const dynamic = "force-dynamic";

const ADMIN_BASE = process.env.ADMIN_API_URL ?? "http://localhost:9000";

/** Only mark Secure when the incoming request is HTTPS (or forwarded as such). */
function cookieSecure(req: NextRequest): boolean {
  return req.headers.get("x-forwarded-proto") === "https";
}

function sessionCookieOptions(req: NextRequest): {
  httpOnly: boolean;
  secure: boolean;
  sameSite: "strict";
  path: string;
  maxAge: number;
} {
  return {
    httpOnly: true,
    secure: cookieSecure(req),
    sameSite: "strict",
    path: "/",
    maxAge: 60 * 60 * 24,
  };
}

/** Whether the browser has an HttpOnly admin session (no secrets to the client). */
export async function GET(req: NextRequest) {
  const raw = req.cookies.get(SESSION_COOKIE_NAME)?.value;
  if (!raw) {
    return NextResponse.json({ authenticated: false });
  }
  const creds = decodeCredentialsFromCookie(raw);
  if (!creds) {
    return NextResponse.json({ authenticated: false });
  }
  return NextResponse.json({
    authenticated: true,
    username: creds.username,
  });
}

/** Validate credentials against the admin API and set HttpOnly session cookie. */
export async function POST(req: NextRequest) {
  let body: unknown;
  try {
    body = await req.json();
  } catch {
    return NextResponse.json({ error: "invalid json" }, { status: 400 });
  }
  if (
    typeof body !== "object" ||
    body === null ||
    !("username" in body) ||
    !("password" in body)
  ) {
    return NextResponse.json({ error: "invalid body" }, { status: 400 });
  }
  const { username, password } = body as { username: unknown; password: unknown };
  if (typeof username !== "string" || typeof password !== "string") {
    return NextResponse.json({ error: "invalid fields" }, { status: 400 });
  }
  const auth = basicAuthorizationHeader(username, password);
  const upstream = await fetch(`${ADMIN_BASE}/admin/auth/status`, {
    headers: { authorization: auth },
    cache: "no-store",
  });
  if (upstream.status === 401) {
    return NextResponse.json({ error: "unauthorized" }, { status: 401 });
  }
  if (!upstream.ok) {
    return NextResponse.json(
      { error: "admin api unavailable" },
      { status: 502 },
    );
  }
  const res = NextResponse.json({ ok: true });
  res.cookies.set(
    SESSION_COOKIE_NAME,
    encodeCredentialsForCookie(username, password),
    sessionCookieOptions(req),
  );
  // Drop legacy JS-readable cookie from older Studio builds.
  res.cookies.set("qf_auth", "", { path: "/", maxAge: 0 });
  return res;
}

/** Clear session cookie (logout). */
export async function DELETE(req: NextRequest) {
  const res = NextResponse.json({ ok: true });
  res.cookies.set(SESSION_COOKIE_NAME, "", {
    httpOnly: true,
    secure: cookieSecure(req),
    sameSite: "strict",
    path: "/",
    maxAge: 0,
  });
  return res;
}
