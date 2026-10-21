import { beforeEach, describe, expect, it, vi } from 'vitest';
import type { H3Event } from 'h3';
import { ConvexError } from 'convex/values';
import { ConvexStorageGatewayAdapter } from '../../storage/convex-storage-gateway-adapter';

const runtimeConfig = vi.hoisted(() => ({
    sync: {
        convexAdminKey: '',
    },
}));
vi.mock('#imports', () => ({
    useRuntimeConfig: () => runtimeConfig,
}));

vi.mock('convex/server', () => ({
    anyApi: {
        storage: {
            generateUploadUrl: 'storage.generateUploadUrl',
            getFileUrl: 'storage.getFileUrl',
            commitUpload: 'storage.commitUpload',
            deleteObject: 'storage.deleteObject',
            deletionCapability: 'storage.deletionCapability',
            gcDeletedFiles: 'storage.gcDeletedFiles',
        },
    },
}));

const resolveProviderTokenMock = vi.hoisted(() => vi.fn());
vi.mock('~~/server/auth/token-broker/resolve', () => ({
    resolveProviderToken: resolveProviderTokenMock as any,
}));
const resolveSessionContextMock = vi.hoisted(() => vi.fn());
vi.mock('~~/server/utils/storage/workspace-access', () => ({ requireStorageWorkspaceAccess: async () => {} }));
vi.mock('~~/server/auth/session', () => ({
    resolveSessionContext: resolveSessionContextMock as any,
}));

const resolvePresignExpiresAtMock = vi.hoisted(() => vi.fn(() => 111_111));
vi.mock('~~/server/utils/storage/presign-expiry', () => ({
    resolvePresignExpiresAt: resolvePresignExpiresAtMock as any,
}));

const queryMock = vi.hoisted(() => vi.fn());
const mutationMock = vi.hoisted(() => vi.fn());
const getConvexAdminGatewayClientMock = vi.hoisted(() => vi.fn(() => ({
    query: queryMock as any,
    mutation: mutationMock as any,
})));
vi.mock('../../utils/convex-gateway', () => ({
    getConvexGatewayClient: () => ({
        query: queryMock as any,
        mutation: mutationMock as any,
    }),
    getConvexAdminGatewayClient: (
        event: unknown,
        adminKey: string,
        identity: unknown
    ) => (getConvexAdminGatewayClientMock as any)(event, adminKey, identity),
    buildGatewayAdminIdentity: (provider: string, providerUserId: string) => ({
        provider,
        providerUserId,
    }),
}));

function makeEvent(): H3Event {
    return { context: {}, node: { req: { headers: {} } } } as unknown as H3Event;
}

describe('ConvexStorageGatewayAdapter', () => {
    beforeEach(() => {
        resolveProviderTokenMock.mockReset().mockResolvedValue('provider-jwt');
        resolveSessionContextMock.mockReset().mockResolvedValue({
            authenticated: true, user: { id: 'user-1' },
            provider: 'clerk', providerUserId: 'subject-1',
        });
        getConvexAdminGatewayClientMock.mockClear();
        runtimeConfig.sync.convexAdminKey = 'server-admin-key';
        resolvePresignExpiresAtMock.mockReset().mockReturnValue(111_111);
        queryMock.mockReset().mockResolvedValue({ version: 1 });
        mutationMock.mockReset();
    });

    it('does not delete against a legacy backend without its deletion barrier protocol', async () => {
        const adapter = new ConvexStorageGatewayAdapter();
        queryMock.mockRejectedValue(new Error('Function not found'));
        await expect(adapter.deleteObject(makeEvent(), { workspaceId: 'ws-1', hash: 'sha256:abc' })).rejects.toMatchObject({ statusCode: 503 });
        expect(mutationMock).not.toHaveBeenCalled();
    });

    it('requires a verified host session for storage operations', async () => {
        const adapter = new ConvexStorageGatewayAdapter();
        resolveProviderTokenMock.mockResolvedValue(null);
        resolveSessionContextMock.mockResolvedValue({ authenticated: false });

        await expect(
            adapter.presignUpload(makeEvent(), {
                workspaceId: 'ws-1',
                hash: 'sha256:abc',
                mimeType: 'image/png',
                sizeBytes: 100,
            })
        ).rejects.toMatchObject({ statusCode: 401 });

        await expect(
            adapter.presignDownload(makeEvent(), {
                workspaceId: 'ws-1',
                hash: 'sha256:abc',
            })
        ).rejects.toMatchObject({ statusCode: 401 });

        await expect(adapter.commit(makeEvent(), {})).rejects.toMatchObject({ statusCode: 401 });
        await expect(adapter.deleteObject(makeEvent(), {
            workspaceId: 'ws-1',
            hash: 'sha256:abc',
        })).rejects.toMatchObject({ statusCode: 401 });
        await expect(adapter.gc(makeEvent(), {})).rejects.toMatchObject({ statusCode: 401 });
    });

    it.each(['presignUpload', 'commit', 'deleteObject', 'gc'] as const)('%s refuses browser credentials without the server admin key', async operation => {
        const adapter = new ConvexStorageGatewayAdapter();
        runtimeConfig.sync.convexAdminKey = '';
        mutationMock.mockResolvedValue({ uploadUrl: 'https://upload.example', intentId: 'intent-1' });
        const result = operation === 'presignUpload'
            ? adapter.presignUpload(makeEvent(), {
                workspaceId: 'ws-1', hash: 'sha256:abc', mimeType: 'image/png', sizeBytes: 100,
            })
            : operation === 'commit' ? adapter.commit(makeEvent(), {
                workspace_id: 'ws-1', intent_id: 'intent-1', hash: 'sha256:abc',
                storage_id: 'st_1', storage_provider_id: 'convex',
                mime_type: 'image/png', size_bytes: 100, name: 'a.png', kind: 'image',
            })
            : operation === 'deleteObject' ? adapter.deleteObject(makeEvent(), { workspaceId: 'ws-1', hash: 'sha256:abc' })
            : adapter.gc(makeEvent(), { workspace_id: 'ws-1', retention_seconds: 0 });

        await expect(result).rejects.toMatchObject({ statusCode: 503 });
        expect(mutationMock).not.toHaveBeenCalled();
    });

    it('binds upload authority to the verified host session even when a browser token is available', async () => {
        const adapter = new ConvexStorageGatewayAdapter();
        runtimeConfig.sync.convexAdminKey = 'admin-key';
        resolveSessionContextMock.mockResolvedValue({
            authenticated: true,
            user: { id: 'user-1' },
            provider: 'basic-auth',
            providerUserId: 'verified-subject',
        });
        mutationMock.mockResolvedValueOnce({ uploadUrl: 'https://upload.example', intentId: 'intent-1' });

        await adapter.presignUpload(makeEvent(), {
            workspaceId: 'ws-1',
            hash: 'sha256:abc',
            mimeType: 'image/png',
            sizeBytes: 100,
        });

        expect(getConvexAdminGatewayClientMock).toHaveBeenCalledWith(
            expect.any(Object),
            'admin-key',
            { provider: 'basic-auth', providerUserId: 'verified-subject' }
        );
    });

    it('maps presign upload and resolves expiry', async () => {
        const adapter = new ConvexStorageGatewayAdapter();
        mutationMock.mockResolvedValueOnce({ uploadUrl: 'https://upload.example', intentId: 'intent-1', expiresAt: '2026-02-06T12:00:00Z' });

        await expect(
            adapter.presignUpload(makeEvent(), {
                workspaceId: 'ws-1',
                hash: 'sha256:abc',
                mimeType: 'image/png',
                sizeBytes: 100,
            })
        ).resolves.toEqual({ url: 'https://upload.example', expiresAt: 111_111, intentId: 'intent-1' });

        expect(mutationMock).toHaveBeenCalledWith('storage.generateUploadUrl', {
            workspace_id: 'ws-1',
            hash: 'sha256:abc',
            mime_type: 'image/png',
            size_bytes: 100,
            workspace_quota_bytes: undefined,
        });

    });

    // A permanent budget rejection must not look like a retryable backend outage.
    // Other transport failures must retain their original error semantics.
    it.each([true, false])('preserves permanent quota versus transient failure semantics (%s)', async quota => {
        const failure = quota ? new ConvexError('Workspace storage quota exceeded') : new Error('Backend unavailable');
        mutationMock.mockRejectedValueOnce(failure);
        const result = new ConvexStorageGatewayAdapter().presignUpload(makeEvent(), {
            workspaceId: 'ws-1', hash: 'sha256:' + 'a'.repeat(64), mimeType: 'image/png', sizeBytes: 100,
            workspaceQuotaBytes: 100,
        });
        if (quota) await expect(result).rejects.toMatchObject({ statusCode: 413 });
        else await expect(result).rejects.toBe(failure);
    });

    it('never exposes the permanent native URL when issuing a bounded download', async () => {
        runtimeConfig.sync.convexAdminKey = 'server-admin-key';
        resolveSessionContextMock.mockResolvedValue({
            authenticated: true, user: { id: 'user-1' },
            provider: 'basic-auth', providerUserId: 'subject-1',
        });
        queryMock.mockResolvedValue({
            url: 'https://native.example/permanent-blob',
            storageId: 'blob-1', mimeType: 'text/html', name: 'unsafe.html', fileKind: 'file',
        });
        const result = await new ConvexStorageGatewayAdapter().presignDownload(makeEvent(), {
            workspaceId: 'ws-1', hash: 'sha256:' + 'a'.repeat(64),
            storageId: 'blob-1', expiresInMs: 1234,
        });
        expect(result.url).toMatch(/^\/api\/storage\/convex\/download\?token=/);
        expect(result.expiresAt).toBeGreaterThan(Date.now());
        expect(result.expiresAt).toBeLessThanOrEqual(Date.now() + 1234);
        expect(result.url).not.toContain('native.example');
    });

    it('returns 404 when download URL is absent', async () => {
        const adapter = new ConvexStorageGatewayAdapter();
        runtimeConfig.sync.convexAdminKey = 'server-admin-key';
        resolveSessionContextMock.mockResolvedValue({ authenticated: true, user: { id: 'user-1' }, provider: 'basic-auth', providerUserId: 'subject-1' });
        queryMock.mockResolvedValue(null);

        await expect(
            adapter.presignDownload(makeEvent(), {
                workspaceId: 'ws-1',
                hash: 'sha256:' + 'a'.repeat(64),
            })
        ).rejects.toMatchObject({ statusCode: 404 });
    });

    it('maps commit payload including storage_id cast and metadata fields', async () => {
        const adapter = new ConvexStorageGatewayAdapter();

        await adapter.commit(makeEvent(), {
            workspace_id: 'ws-1',
            intent_id: 'intent-1',
            hash: 'sha256:abc',
            storage_id: 'st_1',
            storage_provider_id: 'convex',
            mime_type: 'image/png',
            size_bytes: 100,
            name: 'a.png',
            kind: 'image',
            width: 10,
            height: 20,
            page_count: 1,
        });

        expect(mutationMock).toHaveBeenCalledWith('storage.commitUpload', {
            workspace_id: 'ws-1',
            intent_id: 'intent-1',
            hash: 'sha256:abc',
            storage_id: 'st_1',
            storage_provider_id: 'convex',
            mime_type: 'image/png',
            size_bytes: 100,
            name: 'a.png',
            kind: 'image',
            width: 10,
            height: 20,
            page_count: 1,
        });
    });

    it('maps workspace-scoped deletion and permits an omitted storage id', async () => {
        const adapter = new ConvexStorageGatewayAdapter();

        await adapter.deleteObject(makeEvent(), {
            workspaceId: 'ws-1',
            hash: 'sha256:abc',
            storageId: 'st_1',
        });
        await adapter.deleteObject(makeEvent(), {
            workspaceId: 'ws-1',
            hash: 'sha256:missing',
        });

        expect(mutationMock).toHaveBeenNthCalledWith(1, 'storage.deleteObject', {
            workspace_id: 'ws-1',
            hash: 'sha256:abc',
            storage_id: 'st_1',
        });
        expect(mutationMock).toHaveBeenNthCalledWith(2, 'storage.deleteObject', {
            workspace_id: 'ws-1',
            hash: 'sha256:missing',
            storage_id: undefined,
        });
    });

    it('maps GC response to deleted_count shape', async () => {
        const adapter = new ConvexStorageGatewayAdapter();
        mutationMock.mockResolvedValue({ deletedCount: 7 });

        await expect(
            adapter.gc(makeEvent(), {
                workspace_id: 'ws-1',
                retention_seconds: 3600,
                limit: 50,
            })
        ).resolves.toEqual({ deleted_count: 7 });

        expect(mutationMock).toHaveBeenCalledWith('storage.gcDeletedFiles', {
            workspace_id: 'ws-1',
            retention_seconds: 3600,
            limit: 50,
        });
    });
});
