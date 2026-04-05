const COOKIE_NAME = "qf_auth";
const isBrowser = typeof document !== "undefined";

/** Notifies listeners (e.g. `useSyncExternalStore` in ClientShell) that cookie auth changed. */
function notifyAuthChanged(): void {
  if (!isBrowser) return;
  window.dispatchEvent(new Event("qf:auth-change"));
}

/** Encode username + password as base64(utf8(user:pass)) — stored in the cookie. */
function encodePair(username: string, password: string): string {
  const bytes = new TextEncoder().encode(`${username}:${password}`);
  let binary = "";
  for (const byte of bytes) {
    binary += String.fromCharCode(byte);
  }
  return btoa(binary);
}

/** Encode username + password as a Basic auth header value. */
export function encodeBasicAuth(username: string, password: string): string {
  return "Basic " + encodePair(username, password);
}

/** Persist credentials as a browser cookie (readable by the Next.js server via next/headers). */
export function saveCredentials(username: string, password: string): void {
  if (!isBrowser) return;
  const value = encodeURIComponent(encodePair(username, password));
  const secure = window.location.protocol === "https:" ? "; Secure" : "";
  document.cookie = `${COOKIE_NAME}=${value}; SameSite=Strict; path=/; max-age=86400${secure}`;
  notifyAuthChanged();
}

/** Clear stored credentials. */
export function clearCredentials(): void {
  if (!isBrowser) return;
  document.cookie = `${COOKIE_NAME}=; path=/; max-age=0`;
  notifyAuthChanged();
}

/** Read stored credentials from the cookie, or null if not logged in / on server. */
export function loadCredentials(): { username: string; password: string } | null {
  if (!isBrowser) return null;
  const raw = readCookieClient();
  if (!raw) return null;
  try {
    const binary = atob(raw);
    const bytes = Uint8Array.from(binary, (c) => c.charCodeAt(0));
    const decoded = new TextDecoder().decode(bytes);
    const idx = decoded.indexOf(":");
    if (idx === -1) return null;
    return { username: decoded.slice(0, idx), password: decoded.slice(idx + 1) };
  } catch {
    return null;
  }
}

/**
 * Return `Authorization: Basic ...` header value from the cookie.
 * Client-side only — returns null on the server (use next/headers there).
 */
export function getAuthHeader(): string | null {
  if (!isBrowser) return null;
  const raw = readCookieClient();
  return raw ? `Basic ${raw}` : null;
}

function readCookieClient(): string | null {
  const match = document.cookie.match(
    new RegExp(`(?:^|; )${COOKIE_NAME}=([^;]*)`)
  );
  if (!match) return null;
  return decodeURIComponent(match[1]);
}
