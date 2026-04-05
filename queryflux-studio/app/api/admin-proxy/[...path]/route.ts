import { NextRequest, NextResponse } from "next/server";

export const dynamic = "force-dynamic";

const ADMIN_BASE = process.env.ADMIN_API_URL ?? "http://localhost:9000";

type RouteContext = { params: Promise<{ path?: string[] }> };

async function forward(req: NextRequest, pathSegments: string[]) {
  const suffix = pathSegments.length ? `/${pathSegments.join("/")}` : "";
  const target = `${ADMIN_BASE}${suffix}${req.nextUrl.search}`;

  const headers = new Headers();
  const contentType = req.headers.get("content-type");
  if (contentType) headers.set("content-type", contentType);
  // Forward the Basic auth credential from the browser to the admin API.
  const authorization = req.headers.get("authorization");
  if (authorization) headers.set("authorization", authorization);

  const init: RequestInit = {
    method: req.method,
    headers,
    cache: "no-store",
  };

  if (req.method !== "GET" && req.method !== "HEAD") {
    const buf = await req.arrayBuffer();
    if (buf.byteLength > 0) {
      init.body = buf;
    }
  }

  const upstream = await fetch(target, init);

  const outHeaders = new Headers();
  const upstreamCt = upstream.headers.get("content-type");
  if (upstreamCt) outHeaders.set("content-type", upstreamCt);

  return new NextResponse(upstream.body, {
    status: upstream.status,
    headers: outHeaders,
  });
}

async function handle(req: NextRequest, ctx: RouteContext) {
  const { path = [] } = await ctx.params;
  return forward(req, path);
}

export const GET = handle;
export const POST = handle;
export const PUT = handle;
export const PATCH = handle;
export const DELETE = handle;
export const HEAD = handle;
