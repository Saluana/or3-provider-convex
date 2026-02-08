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
import type { SyncGatewayAdapter } from '~~/server/sync/gateway/types';
import type {
    PullRequest,
    PullResponse,
    PushBatch,
    PushResult,
} from '~~/shared/sync/types';
import type { Id } from '~~/convex/_generated/dataModel';
import { api } from '~~/convex/_generated/api';
import { getConvexGatewayClient } from '../utils/convex-gateway';
import { CONVEX_JWT_TEMPLATE, CONVEX_PROVIDER_ID } from '~~/shared/cloud/provider-ids';
import { resolveProviderToken } from '~~/server/auth/token-broker/resolve';

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

    async pull(event: H3Event, input: PullRequest): Promise<PullResponse> {
        const token = await resolveProviderToken(event, {
            providerId: CONVEX_PROVIDER_ID,
            template: CONVEX_JWT_TEMPLATE,
        });
        if (!token) {
            throw createError({ statusCode: 401, statusMessage: 'Missing provider token' });
        }

        const client = getConvexGatewayClient(event, token);
        const result = await client.query(api.sync.pull, {
            workspace_id: toWorkspaceId(input.scope.workspaceId),
            cursor: input.cursor,
            limit: input.limit,
            tables: input.tables,
        });

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
    }

    async push(event: H3Event, input: PushBatch): Promise<PushResult> {
        const token = await resolveProviderToken(event, {
            providerId: CONVEX_PROVIDER_ID,
            template: CONVEX_JWT_TEMPLATE,
        });
        if (!token) {
            throw createError({ statusCode: 401, statusMessage: 'Missing provider token' });
        }

        const client = getConvexGatewayClient(event, token);
        const result = await client.mutation(api.sync.push, {
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
        });

        return result;
    }

    async updateCursor(
        event: H3Event,
        input: { scope: { workspaceId: string }; deviceId: string; version: number }
    ): Promise<void> {
        const token = await resolveProviderToken(event, {
            providerId: CONVEX_PROVIDER_ID,
            template: CONVEX_JWT_TEMPLATE,
        });
        if (!token) {
            throw createError({ statusCode: 401, statusMessage: 'Missing provider token' });
        }

        const client = getConvexGatewayClient(event, token);
        await client.mutation(api.sync.updateDeviceCursor, {
            workspace_id: toWorkspaceId(input.scope.workspaceId),
            device_id: input.deviceId,
            last_seen_version: input.version,
        });
    }

    async gcTombstones(
        event: H3Event,
        input: { scope: { workspaceId: string }; retentionSeconds: number }
    ): Promise<void> {
        const token = await resolveProviderToken(event, {
            providerId: CONVEX_PROVIDER_ID,
            template: CONVEX_JWT_TEMPLATE,
        });
        if (!token) {
            throw createError({ statusCode: 401, statusMessage: 'Missing provider token' });
        }

        const client = getConvexGatewayClient(event, token);
        await client.mutation(api.sync.gcTombstones, {
            workspace_id: toWorkspaceId(input.scope.workspaceId),
            retention_seconds: input.retentionSeconds,
        });
    }

    async gcChangeLog(
        event: H3Event,
        input: { scope: { workspaceId: string }; retentionSeconds: number }
    ): Promise<void> {
        const token = await resolveProviderToken(event, {
            providerId: CONVEX_PROVIDER_ID,
            template: CONVEX_JWT_TEMPLATE,
        });
        if (!token) {
            throw createError({ statusCode: 401, statusMessage: 'Missing provider token' });
        }

        const client = getConvexGatewayClient(event, token);
        await client.mutation(api.sync.gcChangeLog, {
            workspace_id: toWorkspaceId(input.scope.workspaceId),
            retention_seconds: input.retentionSeconds,
        });
    }
}

/**
 * Factory function for creating Convex SyncGatewayAdapter instances.
 */
export function createConvexSyncGatewayAdapter(): SyncGatewayAdapter {
    return new ConvexSyncGatewayAdapter();
}
