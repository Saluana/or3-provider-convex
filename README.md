# or3-provider-convex

Convex sync, storage, and backend provider for OR3 Chat. Provides real-time sync, cloud file storage, rate limiting, background jobs, and workspace management via Convex.

## Installation

```bash
bun add or3-provider-convex
```

Or for local development (sibling repo):

```bash
# From the or3-chat root:
bun add or3-provider-convex@link:../or3-provider-convex
```

## Setup

### 1. Add to `or3.providers.generated.ts`

```typescript
export const or3ProviderModules = [
    'or3-provider-convex/nuxt',
    // ... other providers
] as const;
```

### 2. Initialize Convex backend

Scaffold the Convex backend templates into your host project:

```bash
bunx or3-provider-convex init
```

Flags: `--update` adds missing files without touching existing ones, and
`--force` deletes and rewrites the entire `convex/` directory.

Then run codegen:

```bash
bunx convex dev --once
```

This generates `convex/_generated/` in your host repo. The `_generated/` directory should be gitignored. `convex dev --once` fails when `convex/` is absent, so `init` must run first.

### 3. Required environment variables

| Variable | Required | Description |
|---|---|---|
| `VITE_CONVEX_URL` | yes | Convex deployment URL |
| `CONVEX_SELF_HOSTED_ADMIN_KEY` | yes (server persistence) | Server-only admin credential required for internal auth/session, background-job, notification, webhook, and rate-limit functions. Keep it out of public runtime config and browser bundles |
| `SSR_AUTH_ENABLED` | yes | Set to `true` to enable SSR auth |
| `OR3_SYNC_PROVIDER` | when sync enabled | Set to `convex` (the default when sync is enabled) |
| `NUXT_PUBLIC_STORAGE_PROVIDER` | when storage enabled | Set to `convex` (the default when storage is enabled) |

Optional:

| Variable | Description |
|---|---|
| `OR3_CONVEX_ALLOW_INSECURE_HTTP` | Set to `true` only for a local/self-hosted `http://` Convex endpoint; the provider refuses plain-HTTP URLs without it |
| `OR3_CONNECT_PROVIDER` | Set to `convex` to back OR3 Connect device enrollment and connected-computer persistence with Convex |

Deployment-side variables (set in the Convex deployment, e.g. `bunx convex env set`):

- `CLERK_ISSUER_URL` — consumed by `convex/auth.config.ts` for Clerk JWT validation; must be HTTPS. Backend-only values like this and the shared `OR3_ADMIN_JWT_SECRET` never appear in `.env` or browser bundles.
- `CLERK_SECRET_KEY` may be used instead of `CONVEX_SELF_HOSTED_ADMIN_KEY` for admin-store authentication when Clerk is the auth provider.

### 4. Host integration

The provider registers itself via the OR3 hook/registry system at startup (Nitro server plugin `src/runtime/server/plugins/register.ts`):

- **Auth workspace store**: `ConvexAuthWorkspaceStore` — workspace/user management and session resolution
- **Sync gateway adapter**: `ConvexSyncGatewayAdapter` — server-side gateway sync (pull/push/snapshot/canonical storage)
- **Storage gateway adapter**: `ConvexStorageGatewayAdapter` — server-side gateway storage (presign/commit/delete/GC)
- **Connect store**: encrypted device enrollment and connected-computer persistence when `OR3_CONNECT_PROVIDER=convex`
- **Admin sync + storage adapters**: deployment health status and maintenance actions for the admin panel
- **Admin store provider**: Convex workspace access, workspace settings, and admin user stores
- **Background jobs**: Convex-backed job queue for background AI streaming (abort is poll-based, not AbortController)
- **Rate limiter**: Convex-backed request rate limiting with an in-memory fallback
- **Webhook store**: `ConvexWebhookStore` — webhook definitions, signing secrets, and delivery logs
- **Notifications**: Convex-backed notification emitter for background-job completion/error
- **Deployment admin checker**: verifies `admin_users` grants in Convex

`ConvexAuthWorkspaceStore` keeps provider subjects and internal user IDs as
separate identifiers. Provisioning and existing-user lookup both return the
canonical Convex `users` document ID; provider subjects (including Basic Auth
UUIDs) are used only to authenticate and resolve the corresponding account.

Auxiliary persistence is not part of the public Convex API. Background-job,
notification, webhook, and rate-limit functions are registered as internal and
are called only by admin-authenticated server adapters. Background-job owner
checks require an exact user ID and do not support wildcard access.

Sync `change_log` and tombstone retention is available through internal,
admin-authenticated mutations. Collection is bounded, requires the explicit
`snapshot-v1` capability, and deletes only old revisions acknowledged by every
registered device; fresh devices bootstrap from canonical snapshots.

The provider exposes the shared materialized snapshot contract in both direct
and gateway modes. The first page records one Convex server-version
high-watermark and an expiring session. Every continuation page is bound to
that workspace and normalized table filter, examines at most the requested
page size, and orders canonical rows/tombstones by `(tableName, pk, kind)`.
Applied pre-images keep later pages stable when writes occur after page one;
incremental replay then starts strictly after the returned watermark.

Gateway storage lifecycle also uses bounded canonical pages over materialized
`file_meta` and live message/post file-reference edges. Quota and filesystem GC
never reconstruct state from retained sync logs. Cursor filters are immutable
across pages and each request is capped at 500 records. Active reservations are
an explicit empty view until upload-intent persistence is installed.

## What `init` scaffolds

The `init` command installs the Convex backend into `convex/`:

- `schema.ts` — full Convex schema (auth, sync, storage, admin, webhooks, rate limits, background jobs, connect)
- `auth.config.ts` — Clerk JWT issuer configuration (`CLERK_ISSUER_URL`)
- `authz.ts` — subject-bound identity, invite, and workspace authorization guards
- `users.ts`, `workspaces.ts` — identity/account lookups and workspace lifecycle/membership functions
- `sync.ts` — push/pull/watch, device cursors, snapshot pages, canonical storage pages, internal bounded GC
- `snapshot.ts` — snapshot cursor/winner helpers shared with the contract fixtures
- `storage.ts` — upload intents, `file_meta` commits, blob and deleted-file GC
- `backgroundJobs.ts`, `rateLimits.ts`, `notifications.ts`, `webhooks.ts`, `connect.ts` — internal auxiliary persistence
- `admin.ts` — admin queries/mutations with audit logging
- `crons.ts` — daily rate-limit cleanup cron
- `syncHistoryGcPolicy.ts` — fail-closed retention gate (snapshot-v1)
- `_generated/` — placeholder type stubs refreshed by `convex dev --once`; gitignore the generated files

## How it works

- **Sync** — Direct mode: the client sync plugin binds the token-broker JWT to the Convex client and pushes/pulls via Convex functions with reactive subscriptions (`watchChanges`). Gateway mode (SSR auth enabled): the sync gateway adapter resolves the SSR session, mints an identity-bound client, and calls the same functions through the Nuxt server; the browser never talks to Convex directly.
- **Storage** — The client storage provider calls `/api/storage/*` SSR endpoints that proxy Convex upload URL generation, commit, and signed URLs. Uploads are reserved via upload intents; quota and GC read only canonical `file_meta` + message/post `file_hashes` pages.
- **Internal persistence** (background jobs, notifications, webhooks, rate limits, connect) is invoked only by admin-authenticated server adapters; the underlying Convex functions are internal-only.

## Development

```bash
bun install
bun run test             # vitest unit tests
bun run type-check       # tsc --noEmit
bun run build            # nuxt-module-build (dist/)
bun run build:templates  # rebuild templates/convex.pack.json.gz
bun run init             # run the scaffolder against the current directory
```

## Troubleshooting

- `convex dev --once` fails when `convex/` is missing or empty: run `bunx or3-provider-convex init` first.
- Startup error `Convex URL must use HTTPS`: set `OR3_CONVEX_ALLOW_INSECURE_HTTP=true` only for intentionally `http://` local endpoints.
- Startup error `Missing Convex URL`: `VITE_CONVEX_URL` must be present when the Nuxt module evaluates (or `convex.url` in nuxt.config).
- Background jobs, notifications, and webhook storage fail closed without `CONVEX_SELF_HOSTED_ADMIN_KEY`; the rate-limit provider uses its in-memory fallback instead.
- Background-job abort is poll-based (`checkJobAborted`); there is no in-process AbortController.
- Never hand-edit `convex/_generated/`; codegen regenerates it.
- `init --update` never overwrites modified scaffold files — it reports them as conflicts.

## Runtime entrypoints

| File | Purpose |
|---|---|
| `src/module.ts` | Nuxt module entry — installs convex-nuxt, registers plugins |
| `src/runtime/plugins/convex-auth.client.ts` | Client plugin — Convex auth bridge (direct mode only) |
| `src/runtime/plugins/convex-sync.client.ts` | Client plugin — real-time sync (direct mode only) |
| `src/runtime/plugins/convex-storage.client.ts` | Client plugin — file upload/download via SSR endpoints |
| `src/runtime/server/plugins/register.ts` | Registers all Convex adapters into core registries |
| `src/runtime/server/sync/convex-sync-gateway-adapter.ts` | Server sync gateway adapter |
| `src/runtime/server/storage/convex-storage-gateway-adapter.ts` | Server storage gateway adapter |
| `src/runtime/server/auth/convex-auth-workspace-store.ts` | Workspace/user store |
| `src/runtime/server/admin/stores/convex-store.ts` | Admin workspace access/settings/user stores |
| `src/runtime/server/admin/adapters/sync-convex.ts` | Admin sync adapter (status + GC gating) |
| `src/runtime/server/admin/adapters/storage-convex.ts` | Admin storage adapter (blob GC) |
| `src/runtime/server/admin/deployment-admin-checker.ts` | Deployment health checker |
| `src/runtime/server/background-jobs/convex-provider.ts` | Background job provider |
| `src/runtime/server/rate-limit/convex-provider.ts` | Rate limit provider |
| `src/runtime/server/notifications/emit.ts` | Notification emitter |
| `src/runtime/server/connect/convex-connect-store.ts` | OR3 Connect persistence adapter |
| `src/runtime/server/webhooks/convex-webhook-store.ts` | Webhook definition/delivery store |
| `src/runtime/server/utils/convex-client.ts` | Convex HTTP client factory (admin-authenticated) |
| `src/runtime/server/utils/convex-gateway.ts` | Gateway identity/client helpers (LRU-cached) |
| `src/runtime/server/utils/convex-transport.ts` | Transport retry + service-unavailable mapping |
| `src/runtime/server/utils/provider-compat.ts` | Legacy Clerk-only backend compatibility |
| `src/runtime/utils/convex-api.ts` | `anyApi` proxies and public/internal function contract names |
| `src/runtime/utils/sync-history-gc-policy.ts` | Fail-closed history GC policy gate (snapshot-v1) |
| `src/runtime/app/sync/convex-sync-provider.ts` | Client-side sync provider (direct mode) |
| `src/runtime/app/storage/convex-storage-provider.ts` | Client-side storage provider (SSR-endpoint based) |
