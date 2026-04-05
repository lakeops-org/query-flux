"use client";

import React, { FormEvent, useState } from "react";
import { Zap } from "lucide-react";
import { encodeBasicAuth, saveCredentials } from "@/lib/auth";

export function LoginDialog() {
  const [username, setUsername] = useState("admin");
  const [password, setPassword] = useState("");
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);

  async function handleSubmit(e: FormEvent) {
    e.preventDefault();
    setError(null);
    setLoading(true);

    try {
      // Probe the health endpoint — but with auth header — to validate credentials.
      // We use /admin/auth/status (a protected endpoint) as the validation call.
      const res = await fetch("/api/admin-proxy/admin/auth/status", {
        headers: { authorization: encodeBasicAuth(username, password) },
        cache: "no-store",
      });

      if (res.status === 401) {
        setError("Invalid username or password.");
        return;
      }
      if (!res.ok) {
        setError(`Unexpected error (${res.status}). Is QueryFlux running?`);
        return;
      }

      saveCredentials(username, password);
    } catch {
      setError("Could not reach QueryFlux. Check that the server is running.");
    } finally {
      setLoading(false);
    }
  }

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-slate-900/60 backdrop-blur-sm">
      <div className="w-full max-w-sm bg-white rounded-2xl shadow-2xl p-8">
        {/* Brand */}
        <div className="flex items-center gap-3 mb-8">
          <div className="w-10 h-10 rounded-xl bg-gradient-to-br from-indigo-500 to-violet-600 flex items-center justify-center shadow-sm">
            <Zap size={18} className="text-white" />
          </div>
          <div>
            <p className="font-bold text-slate-900 text-sm leading-none">QueryFlux</p>
            <p className="text-[11px] text-slate-400 font-medium tracking-wide">Studio</p>
          </div>
        </div>

        <h1 className="text-lg font-bold text-slate-900 mb-1">Sign in</h1>
        <p className="text-xs text-slate-500 mb-6">Enter your admin credentials to continue.</p>

        <form onSubmit={handleSubmit} className="space-y-4">
          <div>
            <label className="block text-[10px] font-semibold text-slate-400 uppercase tracking-widest mb-1">
              Username
            </label>
            <input
              type="text"
              autoComplete="username"
              required
              value={username}
              onChange={(e) => setUsername(e.target.value)}
              className="w-full px-3 py-2 text-sm rounded-lg border border-slate-200 bg-white text-slate-900 focus:outline-none focus:ring-2 focus:ring-indigo-300"
            />
          </div>

          <div>
            <label className="block text-[10px] font-semibold text-slate-400 uppercase tracking-widest mb-1">
              Password
            </label>
            <input
              type="password"
              autoComplete="current-password"
              required
              value={password}
              onChange={(e) => setPassword(e.target.value)}
              className="w-full px-3 py-2 text-sm rounded-lg border border-slate-200 bg-white text-slate-900 focus:outline-none focus:ring-2 focus:ring-indigo-300"
            />
          </div>

          {error && (
            <p className="text-xs text-red-600 bg-red-50 rounded-lg px-3 py-2">{error}</p>
          )}

          <button
            type="submit"
            disabled={loading}
            className="w-full py-2 rounded-lg bg-indigo-600 text-white text-sm font-semibold hover:bg-indigo-700 transition-colors disabled:opacity-60"
          >
            {loading ? "Signing in…" : "Sign in"}
          </button>
        </form>
      </div>
    </div>
  );
}
