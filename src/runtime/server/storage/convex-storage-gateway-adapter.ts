/**
 * @module server/storage/gateway/impls/convex-storage-gateway-adapter.ts
 *
 * Purpose:
 * Convex implementation of StorageGatewayAdapter.
 *
 * DO NOT import this file directly in core. Use getActiveStorageGatewayAdapter()
 * after the Convex provider package registers the adapter.
 */
import type { H3Event } from 'h3';
import { createError } from 'h3';
import { z } from 'zod';
import type {
    StorageGatewayAdapter,
    PresignUploadRequest,
    PresignUploadResponse,
    PresignDownloadRequest,
    PresignDownloadResponse,
} from '~~/server/storage/gateway/types';
import type { Id } from '~~/convex/_generated/dataModel';
import { api } from '~~/convex/_generated/api';
import { getConvexGatewayClient } from '../utils/convex-gateway';
import { CONVEX_JWT_TEMPLATE, CONVEX_PROVIDER_ID } from '~~/shared/cloud/provider-ids';
import { resolvePresignExpiresAt } from '~~/server/utils/storage/presign-expiry';
import { resolveProviderToken } from '~~/server/auth/token-broker/resolve';

const CommitInputSchema = z.object({
    workspace_id: z.string().min(1),
    hash: z.string().min(1),
    storage_id: z.string().min(1),
    storage_provider_id: z.string().min(1),
    mime_type: z.string().min(1),
    size_bytes: z.number().int().nonnegative(),
    name: z.string().min(1),
    kind: z.enum(['image', 'pdf']),
    width: z.number().int().positive().optional(),
    height: z.number().int().positive().optional(),
    page_count: z.number().int().positive().optional(),
});

const GcInputSchema = z.object({
    workspace_id: z.string().min(1),
    retention_seconds: z.number().int().nonnegative(),
    limit: z.number().int().positive().optional(),
});

function toWorkspaceId(workspaceId: string): Id<'workspaces'> {
    if (!workspaceId.trim()) {
        throw createError({ statusCode: 400, statusMessage: 'workspace_id is required' });
    }
    return workspaceId as Id<'workspaces'>;
}

function toStorageId(storageId: string): Id<'_storage'> {
    if (!storageId.trim()) {
        throw createError({ statusCode: 400, statusMessage: 'storage_id is required' });
    }
    return storageId as Id<'_storage'>;
}

/**
 * Convex-backed StorageGatewayAdapter implementation.
 *
 * Implementation:
 * - Uses Convex HTTP client for server-side queries/mutations
 * - Calls api.storage.generateUploadUrl/getFileUrl/etc
 * - Maps types between gateway interface and Convex API
 */
export class ConvexStorageGatewayAdapter implements StorageGatewayAdapter {
    id = 'convex';

    async presignUpload(event: H3Event, input: PresignUploadRequest): Promise<PresignUploadResponse> {
        const token = await resolveProviderToken(event, {
            providerId: CONVEX_PROVIDER_ID,
            template: CONVEX_JWT_TEMPLATE,
        });
        if (!token) {
            throw createError({ statusCode: 401, statusMessage: 'Missing provider token' });
        }

        const client = getConvexGatewayClient(event, token);
        const result = await client.mutation(api.storage.generateUploadUrl, {
            workspace_id: toWorkspaceId(input.workspaceId),
            hash: input.hash,
            mime_type: input.mimeType,
            size_bytes: input.sizeBytes,
        });

        const expiresAt = resolvePresignExpiresAt(result, undefined);

        return {
            url: result.uploadUrl,
            expiresAt,
        };
    }

    async presignDownload(event: H3Event, input: PresignDownloadRequest): Promise<PresignDownloadResponse> {
        const token = await resolveProviderToken(event, {
            providerId: CONVEX_PROVIDER_ID,
            template: CONVEX_JWT_TEMPLATE,
        });
        if (!token) {
            throw createError({ statusCode: 401, statusMessage: 'Missing provider token' });
        }

        const client = getConvexGatewayClient(event, token);
        const result = await client.query(api.storage.getFileUrl, {
            workspace_id: toWorkspaceId(input.workspaceId),
            hash: input.hash,
        });

        if (!result?.url) {
            throw createError({ statusCode: 404, statusMessage: 'File not found' });
        }

        const expiresAt = resolvePresignExpiresAt(result, undefined);

        return {
            url: result.url,
            expiresAt,
        };
    }

    async commit(event: H3Event, input: unknown): Promise<void> {
        const token = await resolveProviderToken(event, {
            providerId: CONVEX_PROVIDER_ID,
            template: CONVEX_JWT_TEMPLATE,
        });
        if (!token) {
            throw createError({ statusCode: 401, statusMessage: 'Missing provider token' });
        }

        const client = getConvexGatewayClient(event, token);
        const parsed = CommitInputSchema.safeParse(input);
        if (!parsed.success) {
            throw createError({ statusCode: 400, statusMessage: 'Invalid commit payload' });
        }
        const commitInput = parsed.data;

        await client.mutation(api.storage.commitUpload, {
            workspace_id: toWorkspaceId(commitInput.workspace_id),
            hash: commitInput.hash,
            storage_id: toStorageId(commitInput.storage_id),
            storage_provider_id: commitInput.storage_provider_id,
            mime_type: commitInput.mime_type,
            size_bytes: commitInput.size_bytes,
            name: commitInput.name,
            kind: commitInput.kind,
            width: commitInput.width,
            height: commitInput.height,
            page_count: commitInput.page_count,
        });
    }

    async gc(event: H3Event, input: unknown): Promise<unknown> {
        const token = await resolveProviderToken(event, {
            providerId: CONVEX_PROVIDER_ID,
            template: CONVEX_JWT_TEMPLATE,
        });
        if (!token) {
            throw createError({ statusCode: 401, statusMessage: 'Missing provider token' });
        }

        const client = getConvexGatewayClient(event, token);
        const parsed = GcInputSchema.safeParse(input);
        if (!parsed.success) {
            throw createError({ statusCode: 400, statusMessage: 'Invalid gc payload' });
        }
        const gcInput = parsed.data;
        const result = await client.mutation(api.storage.gcDeletedFiles, {
            workspace_id: toWorkspaceId(gcInput.workspace_id),
            retention_seconds: gcInput.retention_seconds,
            limit: gcInput.limit,
        });

        return { deleted_count: result.deletedCount };
    }
}

/**
 * Factory function for creating Convex StorageGatewayAdapter instances.
 */
export function createConvexStorageGatewayAdapter(): StorageGatewayAdapter {
    return new ConvexStorageGatewayAdapter();
}
