"use client";

import { useCallback, useRef, useSyncExternalStore } from "react";
import { loadCredentials } from "@/lib/auth";

/**
 * Client-only auth snapshot: avoids setState-in-effect for reading the session cookie.
 * Snapshot `0` = not yet hydrated (matches SSR); `1` = hydrated, logged out; `2` = hydrated, logged in.
 */
export function useStudioAuth(): {
  ready: boolean;
  authenticated: boolean;
} {
  const hydratedRef = useRef(false);

  const getSnapshot = useCallback((): number => {
    if (typeof document === "undefined") return 0;
    if (!hydratedRef.current) return 0;
    return loadCredentials() ? 2 : 1;
  }, []);

  const getServerSnapshotCb = useCallback(() => 0, []);

  const subscribeWrapped = useCallback(
    (onStoreChange: () => void) => {
      if (typeof window === "undefined") return () => {};
      queueMicrotask(() => {
        hydratedRef.current = true;
        onStoreChange();
      });
      window.addEventListener("qf:auth-change", onStoreChange);
      return () => window.removeEventListener("qf:auth-change", onStoreChange);
    },
    [],
  );

  const state = useSyncExternalStore(
    subscribeWrapped,
    getSnapshot,
    getServerSnapshotCb,
  );

  return {
    ready: state !== 0,
    authenticated: state === 2,
  };
}
