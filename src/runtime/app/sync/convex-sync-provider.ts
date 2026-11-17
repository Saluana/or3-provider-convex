import { hasExplorerSemantics } from '../../shared/explorer-contract';
/**
 * @module app/core/sync/providers/convex-sync-provider
 *
 * Purpose:
 * Implements `SyncProvider` for the Convex backend using direct mode.
 * Authenticates via Clerk JWT template and communicates with Convex
 * functions for push, pull, and subscription.
 *
 * Behavior:
 * - `subscribe()`: Uses `client.onUpdate()` for real-time reactivity
 *   (Convex reactive queries). Validates incoming changes with Zod.
 * - `pull()`: Paginates through `api.sync.pull` query
 * - `push()`: Sends batched ops to `api.sync.push` mutation
 * - `updateCursor()`: Reports device cursor to `api.sync.updateDeviceCursor`
 * - Does not expose history GC while snapshot bootstrap is unavailable
 *
 * Constraints:
 * - Convex client must be captured in Vue setup context (uses `useConvexClient()`)
 * - Auth uses the Clerk JWT template name defined in `shared/cloud/provider-ids`
 * - Subscription cleanup handles a known Convex SDK race condition gracefully
 * - Responses are validated with Zod schemas for safety
 *
 * @see shared/sync/types for SyncProvider interface
 * @see convex/sync.ts for the server-side Convex functions
 * @see shared/cloud/provider-ids for provider/JWT constants
 */
import { useConvexClient } from 'convex-vue';
import { convexApi as api } from '../../utils/convex-api';
import type {
    SyncProvider,
    SyncScope,
    SyncChange,
    PullRequest,
    PullResponse,
    SnapshotRequest,
    SnapshotResponse,
    PushBatch,
    PushResult,
    PendingOp,
    SyncSubscribeOptions,
    SyncRequestOptions,
} from '~~/shared/sync/types';
import {
    PullResponseSchema,
    SnapshotResponseSchema,
    PushResultSchema,
    HistoryIncarnationSchema,
} from '~~/shared/sync/schemas';
import type { GenericId as Id } from 'convex/values';
import { CONVEX_JWT_TEMPLATE, CONVEX_PROVIDER_ID } from '~~/shared/cloud/provider-ids';

/** Tables to sync */
const SYNCED_TABLES = ['threads', 'messages', 'projects', 'posts', 'kv', 'file_meta', 'notifications'];

/** Type for the Convex client */
type ConvexClient = ReturnType<typeof useConvexClient>;

function requestWithSignal<T>(request: () => Promise<T>, signal?: AbortSignal): Promise<T> {
    if (signal?.aborted) return Promise.reject(new DOMException('Sync request aborted', 'AbortError'));
    return new Promise<T>((resolve, reject) => {
        const cleanup = () => { clearTimeout(timer); signal?.removeEventListener('abort', abort); };
        const abort = () => { cleanup(); reject(new DOMException('Sync request aborted', 'AbortError')); };
        const timer = setTimeout(() => { cleanup(); reject(new DOMException('Sync request timed out', 'TimeoutError')); }, 30_000);
        signal?.addEventListener('abort', abort, { once: true });
        try {
            request().then(value => { cleanup(); resolve(value); }, error => { cleanup(); reject(error); });
        } catch (error) { cleanup(); reject(error); }
    });
}

function protocolArgs(history: string | null | undefined) {
    return { history_incarnation: history, history_incarnation_capability: 'incarnation-v1', unavailable_record_capability: 'unavailable-v1' };
}
function assertHistoryResponse(value: { historyIncarnation?: unknown }) {
    if (!HistoryIncarnationSchema.safeParse(value?.historyIncarnation).success) {
        throw Object.assign(new Error('Convex backend requires history incarnation qualification'), { code: 'OR3_SYNC_HISTORY_UNSUPPORTED' });
    }
}

/**
 * Purpose:
 * Create a direct-mode SyncProvider backed by Convex.
 *
 * Behavior:
 * - Uses Convex queries/mutations for pull/push and device cursor updates
 * - Uses `client.onUpdate()` for subscription-style change delivery
 * - Declares `auth` config for AuthTokenBroker (Clerk JWT template)
 *
 * Constraints:
 * - `client` must be created in a Vue setup context (Convex Vue composable)
 * - Intended for deployments where the client can reach Convex directly
 */
export function createConvexSyncProvider(client: ConvexClient): SyncProvider {
    const explorerReadiness = new Map<string, { expires: number; supported: Promise<boolean> }>();
    function explorerSupported(workspaceId: string): Promise<boolean> {
        const entry = explorerReadiness.get(workspaceId);
        if (entry && entry.expires > Date.now()) return entry.supported;
        // Let a deployment or session change take effect without a page reload.
        if (explorerReadiness.size > 32) explorerReadiness.clear();
        const supported = Promise.resolve().then(() => client.query(api.sync.explorerCapability, {
            workspace_id: workspaceId as Id<'workspaces'>,
        })).then(value => value === 'v1', () => false);
        explorerReadiness.set(workspaceId, { expires: Date.now() + 30_000, supported });
        return supported;
    }

    const subscriptions = new Map<string, () => void>();

    const thisProvider: SyncProvider = {
        id: CONVEX_PROVIDER_ID,
        mode: 'direct',
        auth: {
            providerId: CONVEX_PROVIDER_ID,
            template: CONVEX_JWT_TEMPLATE, // Clerk JWT template name
        },
        capabilities: {
            snapshotBootstrap: 'snapshot-v1',
            historyIncarnation: 'v1',
            subscriptionPages: 'v1',
        },

        async subscribe(
            scope: SyncScope,
            tables: string[],
            onChanges: (changes: SyncChange[]) => void,
            options?: SyncSubscribeOptions
        ): Promise<() => void> {
            const tablesToWatch = tables.length > 0 ? tables : SYNCED_TABLES;
            let disposed = false;
            let currentCursor = options?.cursor ?? 0;
            const limit = options?.limit ?? 200;
            let unwatch: (() => void) | undefined;

            const explorer = await explorerSupported(scope.workspaceId);
            let generation = 0;
            let delivering = false;
            let pending = false;
            const startWatch = (): (() => void) => {
                const watchGeneration = ++generation;
                const deliver = async () => {
                    if (delivering) return;
                    delivering = true;
                    try {
                        while (pending && !disposed && generation === watchGeneration) {
                            pending = false;
                            const fromCursor = currentCursor;
                            const page = await thisProvider.pull({ scope, cursor: fromCursor, limit,
                                tables, historyIncarnation: options?.historyIncarnation });
                            if (disposed || generation !== watchGeneration) return;
                            if (options?.onPage) await options.onPage(fromCursor, page);
                            else if (page.unavailable?.length || page.requiresSnapshot) throw new Error('Checked sync page acknowledgement required');
                            else await onChanges(page.changes);
                            if (disposed || generation !== watchGeneration || page.requiresSnapshot) return;
                            currentCursor = page.nextCursor;
                            if (page.hasMore) pending = true;
                        }
                    } catch (error) {
                        console.error('[convex-sync] Watch page was not acknowledged:', error);
                    } finally { delivering = false; }
                };
                return client.onUpdate(api.sync.watchChanges, {
                    workspace_id: scope.workspaceId as Id<'workspaces'>,
                    ...protocolArgs(options?.historyIncarnation),
                    ...(explorer ? { explorer_capability: 'v1' as const } : {}),
                }, () => {
                    if (disposed || generation !== watchGeneration) return;
                    pending = true; void deliver();
                }, error => { console.error('[convex-sync] Watch failed:', error); });
            };

            if (disposed) return () => {};
            unwatch = startWatch();

            const key = `${scope.workspaceId}:${tablesToWatch.join(',')}:${options?.cursor ?? 0}:${limit}`;
            const cleanup = () => {
                if (disposed) return;
                disposed = true;
                if (subscriptions.get(key) === cleanup) subscriptions.delete(key);
                if (typeof unwatch === 'function') {
                try {
                    unwatch();
                } catch (err: unknown) {
                    // Suppress known race condition in Convex SDK where removing a subscriber
                    // from an already-cleared query map throws "Cannot read properties of undefined (reading 'numSubscribers')"
                    const isKnownRaceCondition =
                        err instanceof TypeError &&
                        err.message.includes("Cannot read properties of undefined") &&
                        err.message.includes("numSubscribers");

                    if (!isKnownRaceCondition) {
                        console.warn('[convex-sync] Cleanup unwatch failed:', err);
                    }
                }
                }
            };
            subscriptions.get(key)?.();
            subscriptions.set(key, cleanup);

            return cleanup;
        },

        async pull(request: PullRequest, options?: SyncRequestOptions): Promise<PullResponse> {
            const result = await requestWithSignal(async () => {
                const explorer = await explorerSupported(request.scope.workspaceId);
                if (options?.signal?.aborted) throw new DOMException('Sync request aborted', 'AbortError');
                return client.mutation(api.sync.pullChecked, {
                workspace_id: request.scope.workspaceId as Id<'workspaces'>,
                workspace_item_capability: 'v1',
                ...(explorer ? { explorer_capability: 'v1' as const } : {}),
                ...protocolArgs(request.historyIncarnation),
                cursor: request.cursor,
                limit: request.limit,
                tables: request.tables,
                });
            }, options?.signal);

            // Validate response to catch malformed server data
            assertHistoryResponse(result);
            const parsed = PullResponseSchema.safeParse({
                historyIncarnation: result.historyIncarnation,
                unavailable: result.unavailable,
                changes: result.changes,
                nextCursor: result.nextCursor,
                hasMore: result.hasMore,
                oldestRetainedVersion: result.oldestRetainedVersion,
                requiresSnapshot: result.requiresSnapshot,
            });

            if (!parsed.success) {
                console.error('[convex-sync] Invalid pull response:', parsed.error);
                throw new Error(`Invalid pull response: ${parsed.error.message}`);
            }

            return parsed.data;
        },

        async snapshot(request: SnapshotRequest, options?: SyncRequestOptions): Promise<SnapshotResponse> {
            const result = await requestWithSignal(async () => {
                const explorer = await explorerSupported(request.scope.workspaceId);
                if (options?.signal?.aborted) throw new DOMException('Sync request aborted', 'AbortError');
                return client.mutation(api.sync.snapshot, {
                workspace_id: request.scope.workspaceId as Id<'workspaces'>,
                workspace_item_capability: 'v1',
                ...(explorer ? { explorer_capability: 'v1' as const } : {}),
                ...protocolArgs(request.historyIncarnation),
                page_size: request.pageSize,
                page_token: request.pageToken,
                tables: request.tables,
                });
            }, options?.signal);

            assertHistoryResponse(result);
            const parsed = SnapshotResponseSchema.safeParse(result);
            if (!parsed.success) {
                console.error('[convex-sync] Invalid snapshot response:', parsed.error);
                throw new Error(`Invalid snapshot response: ${parsed.error.message}`);
            }

            return parsed.data;
        },

        async push(batch: PushBatch, options?: SyncRequestOptions): Promise<PushResult> {
            const explorer = await explorerSupported(batch.scope.workspaceId);
            if (!explorer && batch.ops.some(op => hasExplorerSemantics(op.tableName, op.pk, op.payload))) {
                throw new Error('The deployed sync backend does not support workspace folders.');
            }
            const result = await requestWithSignal(() => client.mutation(api.sync.push, {
                workspace_id: batch.scope.workspaceId as Id<'workspaces'>,
                workspace_item_capability: 'v1',
                ...(explorer ? { explorer_capability: 'v1' as const } : {}),
                stored_record_capability: 'stored-record-v1',
                ...protocolArgs(batch.historyIncarnation),
                ops: batch.ops.map((op: PendingOp) => ({
                    op_id: op.stamp.opId,
                    table_name: op.tableName,
                    operation: op.operation,
                    pk: op.pk,
                    payload: op.payload,
                    clock: op.stamp.clock,
                    hlc: op.stamp.hlc,
                    device_id: op.stamp.deviceId,
                })),
            }), options?.signal);

            // Validate response for consistency with pull()
            assertHistoryResponse(result);
            const parsed = PushResultSchema.safeParse({
                historyIncarnation: result.historyIncarnation,
                results: result.results,
                serverVersion: result.serverVersion,
            });

            if (!parsed.success) {
                console.error('[convex-sync] Invalid push response:', parsed.error);
                throw new Error(`Invalid push response: ${parsed.error.message}`);
            }

            return parsed.data;
        },

        async updateCursor(scope: SyncScope, deviceId: string, version: number, options?: SyncRequestOptions): Promise<void> {
            const result = await requestWithSignal(() => client.mutation(api.sync.updateDeviceCursor, {
                workspace_id: scope.workspaceId as Id<'workspaces'>,
                device_id: deviceId,
                last_seen_version: version,
                ...protocolArgs(options?.historyIncarnation),
            }), options?.signal);
            assertHistoryResponse(result);
            if (result.historyIncarnation !== options?.historyIncarnation) throw Object.assign(new Error('History changed'), { data: { code: 'OR3_SYNC_HISTORY_CHANGED', historyIncarnation: result.historyIncarnation } });
        },

        async dispose(): Promise<void> {
            // Clean up all subscriptions with error handling
            subscriptions.forEach((cleanup, key) => {
                try {
                    cleanup();
                } catch (err) {
                    console.warn('[convex-sync] Subscription cleanup failed:', key, err);
                }
            });
            subscriptions.clear();
        },
    };
    return thisProvider;
}
