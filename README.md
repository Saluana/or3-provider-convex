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

Then run codegen:

```bash
bunx convex dev --once
```

This generates `convex/_generated/` in your host repo. The `_generated/` directory should be gitignored.

### 3. Required environment variables

| Variable | Description |
|---|---|
| `VITE_CONVEX_URL` | Convex deployment URL |
| `CONVEX_SELF_HOSTED_ADMIN_KEY` | Admin key for self-hosted Convex (optional for cloud) |
| `SSR_AUTH_ENABLED` | Set to `true` to enable SSR auth |
| `OR3_SYNC_PROVIDER` | Set to `convex` (default when sync enabled) |
| `NUXT_PUBLIC_STORAGE_PROVIDER` | Set to `convex` (default when storage enabled) |

### 4. Host integration

The provider registers itself via the OR3 hook/registry system at startup:

- **Sync provider**: `ConvexSyncProvider` — real-time sync via Convex subscriptions
- **Storage provider**: `ConvexStorageProvider` — cloud file storage via Convex
- **Auth workspace store**: `ConvexAuthWorkspaceStore` — workspace/user management
- **Rate limiter**: Convex-backed request rate limiting
- **Background jobs**: Convex-backed job queue for background AI streaming
- **Notifications**: Convex-backed notification emitter
- **Admin stores**: Convex workspace access store for admin panel

## Runtime entrypoints

| File | Purpose |
|---|---|
| `src/module.ts` | Nuxt module entry — installs convex-nuxt, registers plugins |
| `src/runtime/plugins/convex-auth.client.ts` | Client plugin — Convex auth bridge |
| `src/runtime/plugins/convex-sync.client.ts` | Client plugin — real-time sync |
| `src/runtime/plugins/convex-storage.client.ts` | Client plugin — file upload/download |
| `src/runtime/server/plugins/register.ts` | Registers all Convex adapters into core registries |
| `src/runtime/server/sync/convex-sync-gateway-adapter.ts` | Server sync gateway adapter |
| `src/runtime/server/storage/convex-storage-gateway-adapter.ts` | Server storage gateway adapter |
| `src/runtime/server/auth/convex-auth-workspace-store.ts` | Workspace/user store |
| `src/runtime/server/admin/stores/convex-store.ts` | Admin workspace access store |
| `src/runtime/server/admin/adapters/sync-convex.ts` | Admin sync adapter |
| `src/runtime/server/admin/adapters/storage-convex.ts` | Admin storage adapter |
| `src/runtime/server/admin/deployment-admin-checker.ts` | Deployment health checker |
| `src/runtime/server/background-jobs/convex-provider.ts` | Background job provider |
| `src/runtime/server/rate-limit/convex-provider.ts` | Rate limit provider |
| `src/runtime/server/notifications/emit.ts` | Notification emitter |
| `src/runtime/server/utils/convex-client.ts` | Convex HTTP client factory |
| `src/runtime/server/utils/convex-gateway.ts` | Gateway utility helpers |
| `src/runtime/app/sync/convex-sync-provider.ts` | Client-side sync provider |
| `src/runtime/app/storage/convex-storage-provider.ts` | Client-side storage provider |
