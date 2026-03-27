"use client";

import type { PatchClusterConfig, FlatClusterConfig } from "./types";

const AUTH_NONE = "";
const AUTH_ACCESS_KEY = "accessKey";
const AUTH_ROLE_ARN = "roleArn";

/**
 * Athena: AWS region, S3 output location, optional workgroup/catalog,
 * and optional static IAM credentials (access key ID + secret).
 * When auth is left unset, the default AWS credential chain is used.
 *
 * @see `athenaStudioEngine` in `@/lib/studio-engines/engines/athena`
 */
export function AthenaClusterConfig({
  flat,
  onPatch,
}: {
  flat: FlatClusterConfig;
  onPatch: PatchClusterConfig;
}) {
  const authType = flat["auth.type"] ?? AUTH_NONE;

  function setAuthType(next: string) {
    const patch: Record<string, string> = {
      "auth.type": next,
      "auth.username": "",
      "auth.password": "",
      "auth.token": "",
    };
    onPatch(patch);
  }

  return (
    <div className="space-y-4">
      {/* Region */}
      <div>
        <label
          htmlFor="athena-region"
          className="block text-[11px] font-semibold text-slate-500 uppercase tracking-wide mb-1.5"
        >
          AWS Region <span className="text-red-500">*</span>
        </label>
        <input
          id="athena-region"
          type="text"
          value={flat.region ?? ""}
          onChange={(e) => onPatch({ region: e.target.value })}
          placeholder="us-east-1"
          className="w-full text-sm border border-slate-200 rounded-lg px-3 py-2 font-mono focus:outline-none focus:ring-2 focus:ring-indigo-300 focus:border-indigo-400"
          autoComplete="off"
        />
        <p className="text-[10px] text-slate-400 mt-1">
          AWS region where your Athena workgroup and S3 bucket reside.
        </p>
      </div>

      {/* S3 Output Location */}
      <div>
        <label
          htmlFor="athena-s3"
          className="block text-[11px] font-semibold text-slate-500 uppercase tracking-wide mb-1.5"
        >
          S3 Output Location <span className="text-red-500">*</span>
        </label>
        <input
          id="athena-s3"
          type="text"
          value={flat.s3OutputLocation ?? ""}
          onChange={(e) => onPatch({ s3OutputLocation: e.target.value })}
          placeholder="s3://my-bucket/athena-results/"
          className="w-full text-sm border border-slate-200 rounded-lg px-3 py-2 font-mono focus:outline-none focus:ring-2 focus:ring-indigo-300 focus:border-indigo-400"
          autoComplete="off"
        />
        <p className="text-[10px] text-slate-400 mt-1">
          S3 URI where Athena writes query result files.
        </p>
      </div>

      {/* Workgroup */}
      <div>
        <label
          htmlFor="athena-workgroup"
          className="block text-[11px] font-semibold text-slate-500 uppercase tracking-wide mb-1.5"
        >
          Workgroup
        </label>
        <input
          id="athena-workgroup"
          type="text"
          value={flat.workgroup ?? ""}
          onChange={(e) => onPatch({ workgroup: e.target.value })}
          placeholder="primary"
          className="w-full text-sm border border-slate-200 rounded-lg px-3 py-2 font-mono focus:outline-none focus:ring-2 focus:ring-indigo-300 focus:border-indigo-400"
          autoComplete="off"
        />
        <p className="text-[10px] text-slate-400 mt-1">
          Athena workgroup to run queries in. Defaults to <code className="font-mono">primary</code>.
        </p>
      </div>

      {/* Catalog */}
      <div>
        <label
          htmlFor="athena-catalog"
          className="block text-[11px] font-semibold text-slate-500 uppercase tracking-wide mb-1.5"
        >
          Catalog
        </label>
        <input
          id="athena-catalog"
          type="text"
          value={flat.catalog ?? ""}
          onChange={(e) => onPatch({ catalog: e.target.value })}
          placeholder="AwsDataCatalog"
          className="w-full text-sm border border-slate-200 rounded-lg px-3 py-2 font-mono focus:outline-none focus:ring-2 focus:ring-indigo-300 focus:border-indigo-400"
          autoComplete="off"
        />
        <p className="text-[10px] text-slate-400 mt-1">
          Default Glue catalog. Defaults to <code className="font-mono">AwsDataCatalog</code>.
        </p>
      </div>

      {/* AWS Credentials */}
      <div>
        <label
          htmlFor="athena-auth-type"
          className="block text-[11px] font-semibold text-slate-500 uppercase tracking-wide mb-1.5"
        >
          AWS Credentials
        </label>
        <select
          id="athena-auth-type"
          value={authType}
          onChange={(e) => setAuthType(e.target.value)}
          className="w-full text-sm border border-slate-200 rounded-lg px-3 py-2 bg-white focus:outline-none focus:ring-2 focus:ring-indigo-300"
        >
          <option value={AUTH_NONE}>Default credential chain</option>
          <option value={AUTH_ACCESS_KEY}>Static access key</option>
          <option value={AUTH_ROLE_ARN}>IAM Role ARN (AssumeRole)</option>
        </select>
        <p className="text-[10px] text-slate-400 mt-1">
          Default chain uses env vars <code className="font-mono">AWS_ACCESS_KEY_ID</code> /{" "}
          <code className="font-mono">AWS_SECRET_ACCESS_KEY</code>, ECS task role, or EC2 instance profile.
        </p>
      </div>

      {authType === AUTH_ACCESS_KEY && (
        <div className="space-y-4 rounded-xl border border-slate-100 bg-slate-50/60 p-4">
          <p className="text-[10px] font-semibold text-slate-500 uppercase tracking-widest">
            Static IAM credentials
          </p>
          <div>
            <label
              htmlFor="athena-key-id"
              className="block text-[11px] font-semibold text-slate-500 uppercase tracking-wide mb-1.5"
            >
              Access Key ID
            </label>
            <input
              id="athena-key-id"
              type="text"
              value={flat["auth.username"] ?? ""}
              onChange={(e) => onPatch({ "auth.username": e.target.value })}
              placeholder="AKIAIOSFODNN7EXAMPLE"
              className="w-full text-sm border border-slate-200 rounded-lg px-3 py-2 font-mono focus:outline-none focus:ring-2 focus:ring-indigo-300"
              autoComplete="off"
            />
          </div>
          <div>
            <label
              htmlFor="athena-secret"
              className="block text-[11px] font-semibold text-slate-500 uppercase tracking-wide mb-1.5"
            >
              Secret Access Key
            </label>
            <input
              id="athena-secret"
              type="password"
              value={flat["auth.password"] ?? ""}
              onChange={(e) => onPatch({ "auth.password": e.target.value })}
              className="w-full text-sm border border-slate-200 rounded-lg px-3 py-2 font-mono focus:outline-none focus:ring-2 focus:ring-indigo-300"
              autoComplete="new-password"
            />
          </div>
          <div>
            <label
              htmlFor="athena-session-token"
              className="block text-[11px] font-semibold text-slate-500 uppercase tracking-wide mb-1.5"
            >
              Session Token
            </label>
            <input
              id="athena-session-token"
              type="password"
              value={flat["auth.token"] ?? ""}
              onChange={(e) => onPatch({ "auth.token": e.target.value })}
              placeholder="optional — for temporary/STS-vended credentials"
              className="w-full text-sm border border-slate-200 rounded-lg px-3 py-2 font-mono focus:outline-none focus:ring-2 focus:ring-indigo-300"
              autoComplete="new-password"
            />
            <p className="text-[10px] text-slate-400 mt-1">
              Corresponds to <code className="font-mono">AWS_SESSION_TOKEN</code>. Leave empty for long-term IAM user credentials.
            </p>
          </div>
        </div>
      )}

      {authType === AUTH_ROLE_ARN && (
        <div className="space-y-4 rounded-xl border border-slate-100 bg-slate-50/60 p-4">
          <p className="text-[10px] font-semibold text-slate-500 uppercase tracking-widest">
            IAM Role (STS AssumeRole)
          </p>
          <div>
            <label
              htmlFor="athena-role-arn"
              className="block text-[11px] font-semibold text-slate-500 uppercase tracking-wide mb-1.5"
            >
              Role ARN <span className="text-red-500">*</span>
            </label>
            <input
              id="athena-role-arn"
              type="text"
              value={flat["auth.username"] ?? ""}
              onChange={(e) => onPatch({ "auth.username": e.target.value })}
              placeholder="arn:aws:iam::123456789012:role/AthenaQueryRole"
              className="w-full text-sm border border-slate-200 rounded-lg px-3 py-2 font-mono focus:outline-none focus:ring-2 focus:ring-indigo-300"
              autoComplete="off"
            />
            <p className="text-[10px] text-slate-400 mt-1">
              The proxy assumes this role via STS and uses the resulting temporary credentials.
            </p>
          </div>
          <div>
            <label
              htmlFor="athena-external-id"
              className="block text-[11px] font-semibold text-slate-500 uppercase tracking-wide mb-1.5"
            >
              External ID
            </label>
            <input
              id="athena-external-id"
              type="text"
              value={flat["auth.token"] ?? ""}
              onChange={(e) => onPatch({ "auth.token": e.target.value })}
              placeholder="optional — required only if the role trust policy mandates it"
              className="w-full text-sm border border-slate-200 rounded-lg px-3 py-2 font-mono focus:outline-none focus:ring-2 focus:ring-indigo-300"
              autoComplete="off"
            />
          </div>
        </div>
      )}
    </div>
  );
}
