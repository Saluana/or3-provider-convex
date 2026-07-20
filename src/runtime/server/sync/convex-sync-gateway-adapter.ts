/**
 * @module server/sync/gateway/impls/convex-sync-gateway-adapter.ts
 *
 * Purpose:
 * Convex implementation of SyncGatewayAdapter.
 *
 * DO NOT import this file directly in core. Use getActiveSyncGatewayAdapter()
 * after the Convex provider package registers the adapter.
 */
import type { H3Event } from 'h3';
import { createError } from 'h3';
import { useRuntimeConfig } from '#imports';
import type {
    CanonicalStorageQueryRequest,
    CanonicalStorageQueryResponse,
    SyncGatewayAdapter,
} from '~~/server/sync/gateway/types';
import type {
    PendingOp,
    PullRequest,
    PullResponse,
    SnapshotRequest,
    SnapshotResponse,
    PushBatch,
    PushResult,
} from '~~/shared/sync/types';
import type { GenericId as Id } from 'convex/values';
import { convexApi as api, convexInternalApi as internalApi } from '../../utils/convex-api';
import {
    buildGatewayAdminIdentity,
    getConvexAdminGatewayClient,
    getConvexGatewayClient,
} from '../utils/convex-gateway';
import {
    throwAsConvexServiceUnavailable,
    withConvexTransportRetry,
} from '../utils/convex-transport';
import { resolveConvexAuthProvider } from '../utils/provider-compat';
import { CONVEX_JWT_TEMPLATE, CONVEX_PROVIDER_ID } from '~~/shared/cloud/provider-ids';
import { resolveProviderToken } from '~~/server/auth/token-broker/resolve';
import { resolveSessionContext } from '~~/server/auth/session';
import { emitWebhookSystemHook } from '~~/server/utils/webhooks/runtime';
import { canRunSyncHistoryGc } from '../../utils/sync-history-gc-policy';

type ConvexPullChange = {
    serverVersion: number;
    tableName: string;
    pk: string;
    op: string;
    payload?: unknown;
    stamp: {
        clock: number;
        hlc: string;
        deviceId: string;
        opId: string;
    };
};

type HookEmission = {
    hookName: string;
    payload: Record<string, unknown>;
};

function nowEpoch(): number {
    return Math.floor(Date.now() / 1000);
}

function toWebhookEntityPayload(input: {
    op: PendingOp;
    workspaceId: string;
    now: number;
    userId?: string;
    deleted?: boolean;
}): Record<string, unknown> {
    const base =
        input.op.payload && typeof input.op.payload === 'object'
            ? { ...(input.op.payload as Record<string, unknown>) }
            : {};

    if (typeof base.id !== 'string' || base.id.length === 0) {
        base.id = input.op.pk;
    }
    if (
        typeof base.workspace_id !== 'string' ||
        base.workspace_id.length === 0
    ) {
        base.workspace_id = input.workspaceId;
    }
    if (
        input.userId &&
        (typeof base.user_id !== 'string' || base.user_id.length === 0)
    ) {
        base.user_id = input.userId;
    }
    if (input.deleted) {
        base.deleted = true;
    }
    if (typeof base.updated_at !== 'number') {
        base.updated_at = input.now;
    }

    return base;
}

function resolveHookEmission(input: {
    op: PendingOp;
    workspaceId: string;
    now: number;
    userId?: string;
    wasExisting: boolean;
    applied: boolean;
}): HookEmission | null {
    if (!input.applied) {
        return null;
    }

    const { op } = input;
    if (op.tableName === 'threads') {
        if (op.operation === 'delete') {
            return {
                hookName: 'db.threads.delete:action:soft:after',
                payload: toWebhookEntityPayload({
                    op,
                    workspaceId: input.workspaceId,
                    now: input.now,
                    userId: input.userId,
                    deleted: true,
                }),
            };
        }

        return {
            hookName: input.wasExisting
                ? 'db.threads.update:action:after'
                : 'db.threads.create:action:after',
            payload: toWebhookEntityPayload({
                op,
                workspaceId: input.workspaceId,
                now: input.now,
                userId: input.userId,
            }),
        };
    }

    if (op.tableName === 'messages') {
        if (op.operation === 'delete') {
            return {
                hookName: 'db.messages.delete:action:soft:after',
                payload: toWebhookEntityPayload({
                    op,
                    workspaceId: input.workspaceId,
                    now: input.now,
                    userId: input.userId,
                    deleted: true,
                }),
            };
        }

        return {
            hookName: input.wasExisting
                ? 'db.messages.update:action:after'
                : 'db.messages.create:action:after',
            payload: toWebhookEntityPayload({
                op,
                workspaceId: input.workspaceId,
                now: input.now,
                userId: input.userId,
            }),
        };
    }

    if (op.tableName === 'documents' || op.tableName === 'posts') {
        if (op.operation === 'delete') {
            return {
                hookName: 'db.documents.delete:action:soft:after',
                payload: toWebhookEntityPayload({
                    op,
                    workspaceId: input.workspaceId,
                    now: input.now,
                    userId: input.userId,
                    deleted: true,
                }),
            };
        }

        return {
            hookName: input.wasExisting
                ? 'db.documents.update:action:after'
                : 'db.documents.create:action:after',
            payload: toWebhookEntityPayload({
                op,
                workspaceId: input.workspaceId,
                now: input.now,
                userId: input.userId,
            }),
        };
    }

    if (op.tableName === 'notifications' && op.operation === 'put') {
        return {
            hookName: 'notify:action:push',
            payload: toWebhookEntityPayload({
                op,
                workspaceId: input.workspaceId,
                now: input.now,
                userId: input.userId,
            }),
        };
    }

    return null;
}

function inferWasExistingFallback(op: PendingOp): boolean {
    if (op.operation === 'delete') {
        return true;
    }

    if (op.payload && typeof op.payload === 'object') {
        const payload = op.payload as Record<string, unknown>;
        const createdAt =
            typeof payload.created_at === 'number'
                ? payload.created_at
                : typeof payload.createdAt === 'number'
                  ? payload.createdAt
                  : undefined;
        const updatedAt =
            typeof payload.updated_at === 'number'
                ? payload.updated_at
                : typeof payload.updatedAt === 'number'
                  ? payload.updatedAt
                  : undefined;

        if (
            typeof createdAt === 'number' &&
            typeof updatedAt === 'number'
        ) {
            return updatedAt > createdAt;
        }
    }

    return op.stamp.clock > 1;
}

function toWorkspaceId(workspaceId: string): Id<'workspaces'> {
    if (!workspaceId.trim()) {
        throw createError({
            statusCode: 400,
            statusMessage: 'workspaceId is required',
        });
    }
    return workspaceId as Id<'workspaces'>;
}

function toGatewayOperation(op: string): 'put' | 'delete' {
    if (op === 'put' || op === 'delete') return op;
    throw createError({
        statusCode: 502,
        statusMessage: `Invalid sync operation "${op}" from Convex`,
    });
}

async function getSyncGatewayClient(event: H3Event) {
    const token = await resolveProviderToken(event, {
        providerId: CONVEX_PROVIDER_ID,
        template: CONVEX_JWT_TEMPLATE,
    });
    if (token) {
        return getConvexGatewayClient(event, token);
    }

    const config = useRuntimeConfig(event);
    const adminKey = config.sync?.convexAdminKey;
    if (!adminKey) {
        throw createError({ statusCode: 401, statusMessage: 'Missing provider token' });
    }

    const session = await resolveSessionContext(event);
    if (!session.authenticated || !session.provider || !session.providerUserId) {
        throw createError({ statusCode: 401, statusMessage: 'Unauthorized' });
    }

    return getConvexAdminGatewayClient(
        event,
        adminKey,
        buildGatewayAdminIdentity(
            resolveConvexAuthProvider(session.provider),
            session.providerUserId
        )
    );
}

async function getSyncGcAdminClient(event: H3Event) {
    const config = useRuntimeConfig(event);
    const adminKey = config.sync?.convexAdminKey;
    if (!adminKey) {
        throw createError({ statusCode: 503, statusMessage: 'Convex admin key required for history retention' });
    }
    const session = await resolveSessionContext(event);
    if (!session.authenticated || !session.provider || !session.providerUserId) {
        throw createError({ statusCode: 401, statusMessage: 'Unauthorized' });
    }
    return getConvexAdminGatewayClient(
        event,
        adminKey,
        buildGatewayAdminIdentity(resolveConvexAuthProvider(session.provider), session.providerUserId)
    );
}

/**
 * Convex-backed SyncGatewayAdapter implementation.
 *
 * Implementation:
 * - Uses Convex HTTP client for server-side queries/mutations
 * - Calls api.sync.pull/push/etc
 * - Maps types between gateway interface and Convex API
 */
export class ConvexSyncGatewayAdapter implements SyncGatewayAdapter {
    id = 'convex';
    readonly capabilities = {
        snapshotBootstrap: 'snapshot-v1',
        historyRetention: 'snapshot-v1',
    } as const;

    async pull(event: H3Event, input: PullRequest): Promise<PullResponse> {
        const client = await getSyncGatewayClient(event);
        try {
            const result = await withConvexTransportRetry('sync.pull', () =>
                client.query(api.sync.pull, {
                    workspace_id: toWorkspaceId(input.scope.workspaceId),
                    cursor: input.cursor,
                    limit: input.limit,
                    tables: input.tables,
                })
            );

            // Map Convex result to PullResponse type
            // Convex returns op as string, but we need 'put' | 'delete'
            return {
                changes: result.changes.map((change: ConvexPullChange) => ({
                    serverVersion: change.serverVersion,
                    tableName: change.tableName,
                    pk: change.pk,
                    op: toGatewayOperation(change.op),
                    payload: change.payload,
                    stamp: change.stamp,
                })),
                nextCursor: result.nextCursor,
                hasMore: result.hasMore,
            };
        } catch (error) {
            throwAsConvexServiceUnavailable(error, 'Sync backend unavailable');
        }
    }

    async snapshot(event: H3Event, input: SnapshotRequest): Promise<SnapshotResponse> {
        const client = await getSyncGatewayClient(event);
        try {
            return await withConvexTransportRetry('sync.snapshot', () =>
                client.mutation(api.sync.snapshot, {
                    workspace_id: toWorkspaceId(input.scope.workspaceId),
                    page_size: input.pageSize,
                    page_token: input.pageToken,
                    tables: input.tables,
                })
            );
        } catch (error) {
            throwAsConvexServiceUnavailable(error, 'Sync backend unavailable');
        }
    }

    async queryCanonicalStorage(
        event: H3Event,
        input: CanonicalStorageQueryRequest
    ): Promise<CanonicalStorageQueryResponse> {
        const client = await getSyncGatewayClient(event);
        try {
            return await withConvexTransportRetry('sync.queryCanonicalStorage', () =>
                client.query(api.sync.queryCanonicalStorage, {
                    workspace_id: toWorkspaceId(input.scope.workspaceId),
                    kind: input.kind,
                    page_size: input.limit ?? 100,
                    cursor: input.cursor,
                    hash: input.hash,
                    now: input.now,
                })
            );
        } catch (error) {
            throwAsConvexServiceUnavailable(error, 'Sync backend unavailable');
        }
    }

    async push(event: H3Event, input: PushBatch): Promise<PushResult> {
        const client = await getSyncGatewayClient(event);
        try {
            const result = await withConvexTransportRetry('sync.push', () =>
                client.mutation(api.sync.push, {
                    workspace_id: toWorkspaceId(input.scope.workspaceId),
                    ops: input.ops.map((op) => ({
                        op_id: op.stamp.opId,
                        table_name: op.tableName,
                        operation: op.operation,
                        pk: op.pk,
                        payload: op.payload,
                        clock: op.stamp.clock,
                        hlc: op.stamp.hlc,
                        device_id: op.stamp.deviceId,
                    })),
                })
            );

            let sessionUserId: string | undefined;
            try {
                const session = await resolveSessionContext(event);
                if (
                    session.authenticated &&
                    session.user &&
                    typeof session.user.id === 'string' &&
                    session.user.id.trim().length > 0
                ) {
                    sessionUserId = session.user.id;
                }
            } catch {
                sessionUserId = undefined;
            }

            const opByOpId = new Map(input.ops.map((op) => [op.stamp.opId, op]));
            const now = nowEpoch();
            const emissions: HookEmission[] = [];

            for (const resultItem of result.results) {
                if (!resultItem.success) {
                    continue;
                }

                const sourceOp = opByOpId.get(resultItem.opId);
                if (!sourceOp) {
                    continue;
                }

                const op: PendingOp = {
                    ...sourceOp,
                    tableName: resultItem.tableName ?? sourceOp.tableName,
                    operation: resultItem.operation ?? sourceOp.operation,
                    payload: resultItem.payload ?? sourceOp.payload,
                };

                const emission = resolveHookEmission({
                    op,
                    workspaceId: input.scope.workspaceId,
                    now,
                    userId: sessionUserId,
                    wasExisting:
                        resultItem.wasExisting ?? inferWasExistingFallback(op),
                    applied: resultItem.applied ?? true,
                });

                if (emission) {
                    emissions.push(emission);
                }
            }

            for (const emission of emissions) {
                await emitWebhookSystemHook(emission.hookName, emission.payload);
            }

            return result;
        } catch (error) {
            throwAsConvexServiceUnavailable(error, 'Sync backend unavailable');
        }
    }

    async updateCursor(
        event: H3Event,
        input: { scope: { workspaceId: string }; deviceId: string; version: number }
    ): Promise<void> {
        const client = await getSyncGatewayClient(event);
        try {
            await withConvexTransportRetry('sync.updateDeviceCursor', () =>
                client.mutation(api.sync.updateDeviceCursor, {
                    workspace_id: toWorkspaceId(input.scope.workspaceId),
                    device_id: input.deviceId,
                    last_seen_version: input.version,
                })
            );
        } catch (error) {
            throwAsConvexServiceUnavailable(error, 'Sync backend unavailable');
        }
    }

    async gcTombstones(
        event: H3Event,
        input: { scope: { workspaceId: string }; retentionSeconds: number }
    ): Promise<void> {
        if (!canRunSyncHistoryGc()) return;
        const client = await getSyncGcAdminClient(event);
        await withConvexTransportRetry('sync.gcTombstones', () =>
            client.mutation(internalApi.sync.gcTombstones, {
                workspace_id: toWorkspaceId(input.scope.workspaceId),
                retention_seconds: input.retentionSeconds,
            })
        );
    }

    async gcChangeLog(
        event: H3Event,
        input: { scope: { workspaceId: string }; retentionSeconds: number }
    ): Promise<void> {
        if (!canRunSyncHistoryGc()) return;
        const client = await getSyncGcAdminClient(event);
        await withConvexTransportRetry('sync.gcChangeLog', () =>
            client.mutation(internalApi.sync.gcChangeLog, {
                workspace_id: toWorkspaceId(input.scope.workspaceId),
                retention_seconds: input.retentionSeconds,
            })
        );
    }
}

/**
 * Factory function for creating Convex SyncGatewayAdapter instances.
 */
export function createConvexSyncGatewayAdapter(): SyncGatewayAdapter {
    return new ConvexSyncGatewayAdapter();
}
