# or3-provider-convex

Convex sync, storage, and backend provider for OR3 Chat. Provides real-time sync, cloud file storage, rate limiting, background jobs, and workspace management via Convex.

## Atomic webhook registration quotas

The webhook store advertises `registrationLimit: 'atomic-v1'` and passes the optional `createWebhook(input, limit)` argument to its backend mutation. The indexed scope check and insertion run in one Convex mutation; exhaustion throws `WEBHOOK_LIMIT_EXCEEDED` at the store boundary. Deploy the matching backend templates as well as updating the provider package before integrating the coordinated OR3 Chat registration change. Old backends reject the new argument rather than bypassing the quota.

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

Flags: `--update` updates untouched provider-owned files and stops before any
change if a file was customized. `--force` replaces provider-owned files after
preserving a source backup; unrelated custom functions are retained.

For a manual development scaffold, run codegen:

```bash
bunx convex dev --once
```

This generates `convex/_generated/` in your host repo. The `_generated/` directory should be gitignored. The scaffold includes `convex/tsconfig.json`, so `convex dev --typecheck enable` checks the generated functions instead of skipping them. `convex dev --once` fails when `convex/` is absent, so `init` must run first.

## Automatic updates from an OR3 source checkout

With the matching OR3 source launcher, `bun run dev` / `bun run dev:ssr` and
`bun run preview` check the deployed backend before starting the app. If the
provider's backend changed, they update the scaffold, deploy it, then verify
its actual code digest. Subsequent starts skip deployment. Builds, typechecks
and static generation never deploy. A bare Nitro server only verifies; it
cannot push backend code.

Configure a server-only deployment credential once: `CONVEX_DEPLOY_KEY` for
the exact Convex Cloud deployment, or `CONVEX_SELF_HOSTED_ADMIN_KEY` for a
self-hosted deployment. The app's `VITE_CONVEX_URL` is the explicit target;
`CONVEX_SELF_HOSTED_URL`, when supplied, must match it. Local CLI login state
cannot select another project. Existing deployment environment variables are
preserved, including Clerk issuer settings. No credential enters browser
config, command-line arguments, or the source hash metadata. A deployment key
takes precedence over a separate runtime admin credential for this operation.

The standalone installed command is `bunx or3-provider-convex deploy`. It uses
environment variables supplied by the source launcher; for direct invocation,
load the deployment environment in your shell first. The built-in Convex CLI
is taken from this installed provider, without downloading a latest CLI.

Untouched 0.0.12 scaffolds are adopted automatically. After that,
`.or3/convex-templates.json` tracks provider-owned file hashes, so later
versions can replace untouched files and refuse local edits or deletions.
Generated declarations and custom functions are not overwritten. Earlier or
customized scaffolds need one deliberate merge; a conflict applies no files.
Before a changed scaffold is installed, the old source is saved under
`.or3/convex-backups/`. Keep these backups until the update is accepted.
These are source backups, not backups of the remote Convex database.

A checkout lease rejects concurrent updates and recovers a dead owner.
Deployment failure stops app startup and leaves source backups available for
inspection/retry. A remote backend newer than the local package (or a different
digest at the same immutable version) is never downgraded automatically.
Shared backends should have one designated updater. App rollback does not
automatically roll back the remote backend or its data; use a compatible source
version and review any intentional backend rollback separately.

The public `or3Backend:version` query returns only `{ providerVersion, digest }`.
The provider's Nitro startup guard refuses a missing or mismatched digest before
registering persistence providers. Custom process managers should invoke the
installed provider's `deploy` command before starting Nitro.
Prerendering skips the remote compatibility probe, so a source production build
can finish before its backend update. Runtime startup still requires a matching
backend before persistence services register.

### 3. Required environment variables

| Variable | Required | Description |
|---|---|---|
| `VITE_CONVEX_URL` | yes | Convex deployment URL |
| `CONVEX_SELF_HOSTED_ADMIN_KEY` | yes (server persistence) | Server-only admin credential required for native storage, internal auth/session, background-job, notification, webhook, and rate-limit functions. Keep it out of public runtime config and browser bundles |
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
- **Background jobs**: Convex-backed background AI streaming with atomic
  per-user/global admission, duplicate suppression, renewable worker leases,
  fenced writes, and process-restart recovery (abort is poll-based). Admission
  cancellation is durable via `background_admission_cancels`: creation and
  `cancelAdmission` read the same table inside their mutations, so a Stop
  recorded before the job row commits cannot launch work
  Canonical chat history uses version-1 internal admission/finalization
  mutations and `background_generation_receipts`; each Convex mutation
  atomically allocates sync versions, applies LWW materialized rows, appends the
  change log, and records replay outcome before returning.
- **Rate limiter**: Convex-backed request rate limiting that fails closed when durable enforcement is unavailable
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

Server-authored notification and storage writes mint a fresh UUID `op_id`
(never a `server:*` prefix), allocate `server_version`, and append
`change_log`. Host enforcement settings do not: they live in the private
`host_settings` table and never enter sync, snapshots, or `change_log`. Pull
and watch skip historical non-UUID `op_id`s without failing the request. Pull
responses include `oldestRetainedVersion` and `requiresSnapshot`. Direct
subscribe advances the watch cursor after each page.

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
across pages and each request is capped at 500 records. Active reservations read
persisted native upload intents and expose one current charge per content hash.
The expiry index excludes historical expired intents from bounded group reads;
deploy the updated schema and `sync.ts` together when upgrading.

## What `init` scaffolds

The `init` command installs the Convex backend into `convex/`:

- `schema.ts` — full Convex schema (auth, sync, storage, admin, webhooks, rate limits, background jobs, connect)
- `auth.config.ts` — Clerk JWT issuer configuration (`CLERK_ISSUER_URL`)
- `authz.ts` — subject-bound identity, invite, and workspace authorization guards
- `users.ts`, `workspaces.ts` — identity/account lookups and workspace lifecycle/membership functions
- `sync.ts` — push/pull/watch, device cursors, snapshot pages, canonical storage pages, atomic background-generation history, internal bounded GC
- `snapshot.ts` — snapshot cursor/winner helpers shared with the contract fixtures
- `storage.ts` — upload intents, `file_meta` commits, blob and deleted-file GC
- `backgroundJobs.ts`, `rateLimits.ts`, `notifications.ts`, `webhooks.ts`, `connect.ts` — internal auxiliary persistence
- `hostSettings.ts` — private, workspace-scoped host enforcement settings (plugin enablement, consent, access policy, setup revisions, AI spend ledger) with atomic compare-and-set
- `admin.ts` — admin queries/mutations with audit logging (workspace settings bridge on private storage)
- `crons.ts` — daily rate-limit cleanup cron
- `syncHistoryGcPolicy.ts` — fail-closed retention gate (snapshot-v1)
- `_generated/` — placeholder type stubs refreshed by `convex dev --once`; gitignore the generated files

## How it works

- **Sync** — Direct mode: the client sync plugin binds the token-broker JWT to the Convex client and pushes/pulls via Convex functions with reactive subscriptions (`watchChanges`). Gateway mode (SSR auth enabled): the sync gateway adapter resolves the SSR session, mints an identity-bound client, and calls the same functions through the Nuxt server; the browser never talks to Convex directly. Both paths enforce the shared 256 KB serialized payload ceiling per operation.
- **Storage** — The client uses the host's shared storage gateway (`mode: 'gateway'`) for `/api/storage/*`, including scoped `deleteObject`. It sends the current generic-file capability and mutation-intent headers for upload, download, and commit. Upload quota counts the maximum size of each unexpired active content hash once, promotes remaining intents after cancellation/expiry, and exempts hashes already present in live metadata. Reservation pages expose that same current per-hash charge and fail closed when a group exceeds 500 rows; download URLs use a provider-owned, authenticated streaming route with live canonical `file_meta` and storage identity revalidation (soft-deleted/pending/replaced rows return no bytes); quota and GC read only canonical `file_meta` + message/post `file_hashes` pages.
- **Native storage controls** — `storage:generateUploadUrl`, `storage:commitUpload`, `storage:cancelUploadIntent`, `storage:deleteObject`, and `storage:gcDeletedFiles` are internal mutations. The SSR gateway uses `CONVEX_SELF_HOSTED_ADMIN_KEY` with the verified session's provider identity; browser JWTs cannot invoke these mutations or choose a quota, generic-file policy, or GC retention outside the host's checks. The read-only `storage:deletionCapability` query remains public and requires workspace membership. Deploy the updated `storage.ts` and rebuild the provider together; a host-only update leaves the old public mutation entry points available.
- **Generic files** — The host admits them only with its explicit `allowAnyFileType` setting and applies the canonical attachment/octet-stream download policy. Gateway clients advertise the typed file-kind `v1` capability; older readers and writers receive an explicit HTTP 426 update boundary before generic metadata or bytes cross the gateway. The Convex download route enforces canonical MIME, sanitized filenames, disposition, `nosniff`, and private/no-store caching. Native Convex URLs stay inside the server-only `storage:getFileUrl` query; redeploy the updated backend to remove its former public entry point. Downloads require the server-only `CONVEX_SELF_HOSTED_ADMIN_KEY` (`runtimeConfig.sync.convexAdminKey`), use user-bound signed URLs (5-minute default, requested TTL capped at 15 minutes), and recheck the current session and workspace membership on retrieval. Bytes stream through the host server; native URL headers and redirects are never forwarded.

Native download URLs issued by earlier provider versions remain permanent
bearer links until the corresponding blob is deleted or rotated. Redeploying
the backend secures newly issued downloads; it cannot revoke historical native
URLs. See [Convex file storage](https://docs.convex.dev/file-storage/overview).

- **Internal persistence** (background jobs, notifications, webhooks, rate limits, connect) is invoked only by admin-authenticated server adapters; the underlying Convex functions are internal-only.
- **Mixed-runtime tools** — Internal atomic claim/settle mutations assign each
  browser-only call to one tab while Convex keeps the server job parked.
  Settling the result releases the lease and resumes the same model turn.
  Parked handoffs have separate global and per-user admission caps.

## Development

```bash
bun install
bun run test             # vitest unit tests
bun run type-check       # tsc --noEmit
bun run build            # nuxt-module-build (dist/)
bun run build:templates  # rebuild templates/convex.pack.json.gz
bun run init             # run the scaffolder against the current directory
```

`bun run type-check` checks the provider and every bundled Convex template with
strict schema-derived document, context and function-reference types. Development
checks use the sibling `or3-chat` checkout for host contracts, including relative
shared imports used after installation. The declarations under
`templates/convex/_generated/` support these checks; Convex codegen replaces them
in an installed project. Rebuild the template pack after changing templates or
their declarations.

## Troubleshooting

- `convex dev --once` fails when `convex/` is missing or empty: run `bunx or3-provider-convex init` first.
- Startup error `Convex URL must use HTTPS`: set `OR3_CONVEX_ALLOW_INSECURE_HTTP=true` only for intentionally `http://` local endpoints.
- Startup error `Missing Convex URL`: `VITE_CONVEX_URL` must be present when the Nuxt module evaluates (or `convex.url` in nuxt.config).
- Background jobs, notifications, and webhook storage fail closed without `CONVEX_SELF_HOSTED_ADMIN_KEY`; the rate-limit provider also fails closed.
- After upgrading the provider, run the provider init/update flow and deploy the
  generated Convex schema/functions before enabling background streaming; the
  lease and atomic-admission mutations must match the server adapter version.
- Background-job abort is poll-based (`checkJobAborted`); there is no in-process AbortController.
- Never hand-edit `convex/_generated/`; codegen regenerates it.
- `init --update` stops before changing files when a provider-owned file was customized.

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

### Private host settings

Security-sensitive workspace settings do not use the client-syncable `kv`
table. They live in `host_settings`, a workspace-scoped private table that is
excluded from sync push/pull, snapshots, and `change_log`. `sync.push` also
rejects reserved key families (`plugins.*`, `admin.guest_access.enabled`, and
`plugin:<id>:setup-values*`) so a workspace editor cannot even plant a copy in
`kv` for later code or migration to trust.

The workspace settings store implements
`compareAndSet(workspaceId, key, expectedValue, nextValue)` atomically. Bulk plugin rollout requires this operation for workspace enablement and reviewed permissions so a concurrent workspace choice is not overwritten. A `null`
expected value means the key must be absent; a conflict returns `false` without
overwriting it. The host uses this for concurrent plugin setup saves and
persistent AI spend reservations. A slow plugin write may outlive the HTTP
response: the host reports pending after eight seconds and keeps the plugin
lock until the atomic mutation settles.

Access model:

- The `hostSettings.*` functions are Convex-internal and accept only a trusted
  host-server identity (Convex admin key plus the `or3_server` marker), not
  ordinary client tokens. The OR3 host route stays the business authorization
  boundary, and workspace owners/editors never need deployment-admin
  membership to use plugins.
- The adapter fails closed when `CONVEX_SELF_HOSTED_ADMIN_KEY` is missing.
- `admin.getWorkspaceSetting`/`setWorkspaceSetting`/`compareAndSetWorkspaceSetting`
  are kept for the deployment-admin bridge and now read/write the same private
  table.

Legacy migration: `getLegacyWorkspaceSetting` lets the host inspect old `kv`
values, and the host policy decides what may be copied. User setup values and
the AI spend ledger (byte-for-byte, so an active window cannot reset) migrate;
plugin enablement, consent reviews, access policy, migration state, and guest
access are **not** copied — they must be re-established through trusted host
paths, with fresh approval where required.

For older or customized backends that cannot be adopted automatically, review
the provider templates and merge the following host-settings changes before
deploying. `init --update` refuses conflicting files before applying any update:

1. `schema.ts` — the `host_settings` table definition.
2. `sync.ts` — the reserved-key guard that blocks editor sync writes to
   `plugins.*`, `admin.guest_access.enabled`, and the `plugin:<id>:setup-values*`
   namespace. Without this merge, reserved KV writes stay enabled.
3. `admin.ts` — the deployment-admin settings bridge on private storage.
   Without this merge, admin settings still read/write the old client-writable
   `kv` table.
4. `workspaces.ts` — the `host_settings` cleanup in `deleteWorkspaceData`.
   Without this merge, hard-deleting a workspace retains its plugin
   configuration, consent reviews, and budget records.

Review each reported conflict against the bundled template and merge it
deliberately. Then use the automatic source launcher or the installed provider
`deploy` command before using the updated server adapter.


### Testing local changes in OR3 Chat

With this repository beside `or3-chat`, run `bun install` here once, then
`bun run dev:ssr` from Chat. Chat's dev wrapper rebuilds the local provider and
prints its selected path; restart it after provider edits. Missing repositories
or failed builds fall back to installed packages with a warning.
`OR3_LOCAL_PROVIDERS=false` disables local selection. Production builds use the
installed package, so local development does not publish these changes.

### Canonical compaction history reader (unreleased)

`canonicalChatHistory: 'v1'` calls the internal `sync.readChatHistory` service with the trusted execution actor and workspace. The service rechecks active membership, reads materialized rows, and pages the workspace/thread/canonical-order index. `chat_history_revisions` advances in applied chat writes so ancestor search continuation survives current-thread result writes and invalidates on source edits. The scaffold adds compacted/root/summary/provenance fields without rewriting legacy rows. No retained change log supplies history content. Workspace deletion also removes its canonical history revisions.

`bunx vitest run --config vitest.canonical-history.config.ts` runs the scaffold owner against committed current host history contracts. Its 54 cases pass locally and exercise materialized forward/backward keysets, bounds, foreign IDs, forged actors and revoked membership through the actual internal handler. Its existing storage fixture does not emulate deployed Convex validator/transaction isolation. Source types, module build and generated pack have separate evidence; deployed authorization/transactions, second-client reconciliation and released pins remain rollout gates. This compaction capability is not yet included in a published package.

The canonical-history qualification lane defaults to a sibling `../or3-chat` checkout. For an isolated host worktree, set `OR3_CANONICAL_HOST_ROOT=/absolute/path/to/or3-chat` before the command. Its contract-byte checks fail closed on uncommitted changes; this does not replace the pinned host fixture or certify deployed/released provider behavior.

## Workspace Files and Trash admission

Persistent Projects preserves opaque `or3:project-settings`, `or3:project-memory`, and `or3:project-source` posts, including complete JSON, metadata, and all original/extraction hash references. The project-aware `readChatHistory` scaffold resolves explicit and legacy project ownership within the authorized transaction and returns `project_ownership: resolved | conflict`. Ambiguous/malformed memberships refuse server admission. Rebuild the template pack and deploy the updated scaffold when upgrading; a host-only update cannot add this ownership check.

Canonical ownership reads verify explicit project pointers against live projects
in the same workspace. A missing, deleted, or foreign owner releases the chat
without falling back to old folder memberships, matching browser ownership
resolution when project deletion and chat detachment sync out of order. Legacy
membership reads also trim entry IDs and normalize chat-kind case/whitespace
like the browser, so these projects cannot bypass server execution policy.
Deploy the updated `sync.ts` scaffold to apply these corrections to an existing backend.

Version 0.0.12 declares `projectOwnership: 'v1'` on the canonical-history adapter. Deploy the updated sync scaffold before enabling server execution with this version; the runtime marker alone cannot upgrade an existing backend. Projects qualification: `OR3_CANONICAL_HOST_ROOT=/absolute/path/to/or3-chat bun x vitest run src/runtime/__tests__/convex-snapshot-contract.test.ts`, followed by the host's disposable deployed Convex cloud journey.

Updated direct and gateway transports send workspace_item_capability v1.
Deploy the matching sync.ts and workspaceItemCapability.ts templates before
using the updated runtime; older deployed argument validators reject the new
field. Use the documented init --update flow, review conflicts, and merge the
updated admission code into existing scaffold files before deployment. The
template pack must be rebuilt alongside the provider runtime. Do not use a
force reset of an existing backend as an upgrade shortcut.

The canonical mutation checks incoming and stored posts/projects before
applying catalog, logical Trash, or file-membership writes. Snapshot, pull and
watchChanges also reject readers that omit v1 when returning these semantics.
Gateway rejection becomes HTTP 426 with OR3_WORKSPACE_ITEM_UPDATE_REQUIRED;
direct clients receive the explicit Convex update error.

The source host exposes Files when an authorized session selects an adapter advertising workspace-item v1. Qualification against an official local Convex backend covers authenticated direct JWT calls and the Basic Auth gateway separately: schema/serialization, legacy admission, replay, catalog/Trash reference retention, native storage, viewer denial and revocation. The gateway was also exercised with filesystem storage. This is source qualification, not package publication or a live Clerk OAuth sign-in.

Deploy the updated schema and storage.ts alongside the sync templates. file_meta.storage_id is an opaque provider-owned string, permitting filesystem/S3 identifiers. Native Convex storage operations require matching storage_provider_id ownership; viewers may read but cannot reserve, upload, commit, cancel, delete or run GC. Runtime/template publication remains a separate step.

## Coordinated storage deletion

Deploy the updated schema, storage.ts, sync.ts, storageDeletion.ts and workspace cleanup together. Native deletion/GC writes a private storage_deletion_claims barrier in the same mutation as physical deletion; sync metadata/reference creation honors it. Verified upload commits alone release a hash claim. Stale native storage IDs are rejected even after re-upload, including legacy metadata with a missing or empty storage provider ID. Claims persist until verified re-upload or workspace purge, independently of sync history retention. The runtime probes storage.deletionCapability and refuses cleanup against older scaffolds. The host additionally requires a matching storage/sync deletion-coordination declaration. Filesystem storage remains fail closed because it cannot join the canonical Convex transaction.

## Instance signup invitations

The updated host distinguishes signup invites from existing team invites. Signup invites create a private workspace with only the recipient as owner; team invites retain their selected workspace and role. This requires the updated host and rebuilt provider artifacts, rather than the existing published release alone.

Signup invite functions are trusted internal Convex functions called through the existing SSR bridge. One mutation creates the canonical user/mapping/private workspace/owner membership/active pointer/receipt; same-identity replay performs no writes. Deploy matching updated backend functions with the rebuilt provider. Missing functions fail explicitly; existing backend digest/version guards remain in force. Millisecond history pages omit hashes, cap at 100 rows, and retain terminal receipts for 90 days with cleanup capped at 100 per create/list call. Transaction qualification uses the disposable backend lifecycle harness in signup mode.

Signup email uniqueness uses `users.normalized_email` and its index. After deploying the matching schema/functions, the SSR provider automatically backfills legacy users in transactions of at most 100 rows before creating, validating, or accepting a signup invitation. Progress persists between calls and resumes after an interruption. The first call on a large installation may take longer; retries keep completed work. Acceptance refuses while any legacy rows remain unindexed. All built-in user creation paths maintain the index, including users without an email; custom writers must also set `normalized_email` to the trimmed lowercase email, or `''` when absent. The lifecycle harness verifies migration and signup with 33,000 legacy users, including mixed-case duplicate rejection.

Invalid signup-invite history cursors return HTTP 400 (`Invalid invite cursor`) through a structured backend error. Other backend failures retain their server/service error behavior. The signup lifecycle harness verifies the host HTTP response and valid page continuation against a disposable Convex backend.

## Provider parity updates

The Convex provider now persists encrypted plugin connections and their test
results in private `plugin_connections` and `plugin_connection_tests` tables.
Owner/workspace listings, insert-only identities, revision compare-and-swap,
and rejection of stale test results match the SQLite store. These functions
are internal and require the server's Convex admin key. Credentials remain
ciphertext; the host's `OR3_PLUGIN_CONNECTION_SECRET` stays outside Convex.
Removing a workspace also removes its connection records and test evidence.
Deploy the updated schema, `pluginConnections.ts`, and `workspaces.ts` together.

Selecting Convex for background jobs or rate limits alone now performs the same
startup URL and backend compatibility checks as selecting it for sync/storage.
Convex URLs must be HTTP(S) endpoints without embedded credentials; local HTTP
still requires `OR3_CONVEX_ALLOW_INSECURE_HTTP=true`. Durable rate limits reject
missing credentials, invalid policies, and backend failures; they no longer
switch to process-local memory during an outage. Select the host's `memory`
rate-limit provider explicitly when process-local limits are intended.

Background jobs also implement owner-scoped lookup by admission idempotency
key, allowing the host to recover an admitted job when the original response
was lost. Deploy the updated `backgroundJobs.ts` before using this recovery.

The sync adapter advertises `retainedStorageMetadata: 'v1'` and supports bounded
`retained_metadata` pages of deleted materialized file metadata. This is
read-only accounting, independent of sync-history retention, and does not grant
permission to delete physical objects. Convex stores delete-before-put records
as tombstones rather than fabricated zero-size file metadata; only actual
materialized file records appear in these pages.

A native upload commit can safely retry its exact consumed receipt without
writing history again. Workspace, user, normalized hash, MIME type, size,
provider and storage ID must match; cancelled intents and changed receipts
remain rejected. Deploy the updated schema and `storage.ts` to enable this.

`bun scripts/test/provider-parity.mjs <cached-convex-local-backend> <host-source>
<evidence-dir>` runs these contracts against a disposable localhost backend and
writes `provider-parity.json`. It verifies owner-scoped admission recovery,
concurrent durable rate limiting,
invalid/missing configuration and outages, retained metadata pagination,
native upload retries, durable connection CAS/evidence, internal-only access,
and workspace cleanup. It never accepts a production deployment URL.
