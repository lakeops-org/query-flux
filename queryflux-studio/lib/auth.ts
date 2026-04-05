import { basicAuthorizationHeader } from "./admin-session-codec";

const isBrowser = typeof document !== "undefined";

/** Notifies listeners (e.g. ClientShell) that auth state changed. */
function notifyAuthChanged(): void {
  if (!isBrowser) return;
  window.dispatchEvent(new Event("qf:auth-change"));
}

/**
 * Server validates credentials and sets an HttpOnly session cookie.
 * Client JS never receives or stores the password.
 */
export async function saveCredentials(
  username: string,
  password: string,
): Promise<void> {
  const res = await fetch("/api/session", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ username, password }),
    credentials: "include",
    cache: "no-store",
  });
  if (res.status === 401) {
    throw new Error("Unauthorized");
  }
  if (!res.ok) {
    throw new Error(`session: ${res.status}`);
  }
  notifyAuthChanged();
}

/** Clear HttpOnly session via logout endpoint. */
export async function clearCredentials(): Promise<void> {
  await fetch("/api/session", {
    method: "DELETE",
    credentials: "include",
    cache: "no-store",
  });
  notifyAuthChanged();
}

/** Whether an HttpOnly admin session exists (no password exposed). */
export async function fetchSessionStatus(): Promise<{
  authenticated: boolean;
  username?: string;
}> {
  const res = await fetch("/api/session", {
    credentials: "include",
    cache: "no-store",
  });
  if (!res.ok) {
    return { authenticated: false };
  }
  return (await res.json()) as {
    authenticated: boolean;
    username?: string;
  };
}

/**
 * Build Basic auth for one-off requests (e.g. probing before session exists).
 * Prefer `saveCredentials` + cookie-backed proxy for normal use.
 */
export function encodeBasicAuth(username: string, password: string): string {
  return basicAuthorizationHeader(username, password);
}

/**
 * Browser API calls use `credentials: 'include'` and the admin proxy reads the HttpOnly cookie.
 * Do not send Authorization from client-side JS for session-backed requests.
 */
export function getAuthHeader(): string | null {
  return null;
}
