/**
 * Encode/decode admin Basic credentials for the HttpOnly session cookie value.
 * Uses UTF-8–safe base64url so non-ASCII usernames/passwords work.
 */

export const SESSION_COOKIE_NAME = "qf_admin_session";

function utf8BytesToBase64(bytes: Uint8Array): string {
  let binary = "";
  for (const b of bytes) {
    binary += String.fromCharCode(b);
  }
  return btoa(binary);
}

function base64ToUtf8String(base64: string): string {
  const binary = atob(base64);
  const bytes = Uint8Array.from(binary, (c) => c.charCodeAt(0));
  return new TextDecoder().decode(bytes);
}

/** Standard Basic Authorization header value (for upstream admin API). */
export function basicAuthorizationHeader(username: string, password: string): string {
  const bytes = new TextEncoder().encode(`${username}:${password}`);
  return `Basic ${utf8BytesToBase64(bytes)}`;
}

/** Cookie-safe base64url payload (opaque to client JS — cookie is HttpOnly). */
export function encodeCredentialsForCookie(username: string, password: string): string {
  const bytes = new TextEncoder().encode(`${username}:${password}`);
  return utf8BytesToBase64(bytes)
    .replace(/\+/g, "-")
    .replace(/\//g, "_")
    .replace(/=+$/, "");
}

export function decodeCredentialsFromCookie(value: string): {
  username: string;
  password: string;
} | null {
  try {
    const pad = value.length % 4 === 0 ? "" : "=".repeat(4 - (value.length % 4));
    const base64 = value.replace(/-/g, "+").replace(/_/g, "/") + pad;
    const decoded = base64ToUtf8String(base64);
    const idx = decoded.indexOf(":");
    if (idx === -1) return null;
    return { username: decoded.slice(0, idx), password: decoded.slice(idx + 1) };
  } catch {
    return null;
  }
}

export function basicAuthFromCookieValue(token: string): string | null {
  const creds = decodeCredentialsFromCookie(token);
  if (!creds) return null;
  return basicAuthorizationHeader(creds.username, creds.password);
}
