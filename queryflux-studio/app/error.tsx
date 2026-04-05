"use client";

import { useEffect } from "react";
import { RefreshCw } from "lucide-react";

export default function GlobalError({
  error,
  reset,
}: {
  error: Error & { digest?: string };
  reset: () => void;
}) {
  useEffect(() => {
    if (error.name === "UnauthorizedError" || error.message === "Unauthorized") {
      window.dispatchEvent(new Event("qf:unauthorized"));
      reset();
    }
  }, [error, reset]);

  return (
    <div className="flex flex-col items-center justify-center h-full gap-4 text-center p-8">
      <p className="text-sm font-medium text-slate-700">Something went wrong</p>
      {error.digest && (
        <p className="text-xs text-slate-400 font-mono">digest: {error.digest}</p>
      )}
      <button
        onClick={reset}
        className="flex items-center gap-2 text-xs text-indigo-600 hover:text-indigo-800 font-medium transition-colors"
      >
        <RefreshCw size={12} />
        Try again
      </button>
    </div>
  );
}
