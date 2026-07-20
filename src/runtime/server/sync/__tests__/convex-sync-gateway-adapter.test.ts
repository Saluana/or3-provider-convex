import { beforeEach, describe, expect, it, vi } from 'vitest';
import type { H3Event } from 'h3';
import { ConvexSyncGatewayAdapter } from '../../sync/convex-sync-gateway-adapter';

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
        sync: {
            pull: 'sync.pull',
            queryCanonicalStorage: 'sync.queryCanonicalStorage',
            snapshot: 'sync.snapshot',
            push: 'sync.push',
            updateDeviceCursor: 'sync.updateDeviceCursor',
            gcTombstones: 'sync.gcTombstones',
            gcChangeLog: 'sync.gcChangeLog',
        },
    },
}));

const resolveProviderTokenMock = vi.hoisted(() => vi.fn());
vi.mock('~~/server/auth/token-broker/resolve', () => ({
    resolveProviderToken: (...args: unknown[]) => resolveProviderTokenMock(...args),
}));
const resolveSessionContextMock = vi.hoisted(() => vi.fn());
vi.mock('~~/server/auth/session', () => ({
    resolveSessionContext: (...args: unknown[]) => resolveSessionContextMock(...args),
}));

const emitWebhookSystemHookMock = vi.hoisted(() => vi.fn().mockResolvedValue(undefined));
vi.mock('~~/server/utils/webhooks/runtime', () => ({
    emitWebhookSystemHook: (...args: unknown[]) => emitWebhookSystemHookMock(...args),
}));

const queryMock = vi.hoisted(() => vi.fn());
const mutationMock = vi.hoisted(() => vi.fn());
const getConvexAdminGatewayClientMock = vi.hoisted(() => vi.fn(() => ({
    query: (...args: unknown[]) => queryMock(...args),
    mutation: (...args: unknown[]) => mutationMock(...args),
})));
vi.mock('../../utils/convex-gateway', () => ({
    getConvexGatewayClient: () => ({
        query: (...args: unknown[]) => queryMock(...args),
        mutation: (...args: unknown[]) => mutationMock(...args),
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

function createTransientTransportError(message: string = 'fetch failed'): Error {
    const error = new Error(message) as Error & {
        cause?: {
            code?: string;
            name?: string;
            message?: string;
        };
    };
    error.cause = {
        code: 'UND_ERR_CONNECT_TIMEOUT',
        name: 'ConnectTimeoutError',
        message: 'Connect Timeout Error',
    };
    return error;
}

describe('ConvexSyncGatewayAdapter', () => {
    beforeEach(() => {
        resolveProviderTokenMock.mockReset().mockResolvedValue('provider-jwt');
        resolveSessionContextMock.mockReset().mockResolvedValue({ authenticated: false });
        emitWebhookSystemHookMock.mockClear();
        getConvexAdminGatewayClientMock.mockClear();
        runtimeConfig.sync.convexAdminKey = '';
        queryMock.mockReset();
        mutationMock.mockReset();
    });

    it('requires token for operational methods and returns 401 when missing', async () => {
        const adapter = new ConvexSyncGatewayAdapter();
        resolveProviderTokenMock.mockResolvedValue(null);

        await expect(
            adapter.pull(makeEvent(), { scope: { workspaceId: 'ws-1' }, cursor: 0, limit: 10 })
        ).rejects.toMatchObject({ statusCode: 401 });

        await expect(
            adapter.push(makeEvent(), { scope: { workspaceId: 'ws-1' }, ops: [] })
        ).rejects.toMatchObject({ statusCode: 401 });

        await expect(
            adapter.updateCursor(makeEvent(), { scope: { workspaceId: 'ws-1' }, deviceId: 'd1', version: 1 })
        ).rejects.toMatchObject({ statusCode: 401 });

        expect(resolveProviderTokenMock).toHaveBeenCalledTimes(3);
    });

    it('falls back to admin gateway client when provider token is unavailable', async () => {
        const adapter = new ConvexSyncGatewayAdapter();
        resolveProviderTokenMock.mockResolvedValue(null);
        runtimeConfig.sync.convexAdminKey = 'admin-key';
        resolveSessionContextMock.mockResolvedValue({
            authenticated: true,
            provider: 'basic-auth',
            providerUserId: 'user-1',
        });
        queryMock.mockResolvedValue({ changes: [], nextCursor: 0, hasMore: false });

        await adapter.pull(makeEvent(), {
            scope: { workspaceId: 'ws-1' },
            cursor: 0,
            limit: 10,
        });

        expect(getConvexAdminGatewayClientMock).toHaveBeenCalledWith(
            expect.any(Object),
            'admin-key',
            { provider: 'basic-auth', providerUserId: 'user-1' }
        );
    });

    it('maps pull response and converts op strings to put|delete', async () => {
        const adapter = new ConvexSyncGatewayAdapter();
        queryMock.mockResolvedValue({
            changes: [
                {
                    serverVersion: 1,
                    tableName: 'messages',
                    pk: 'm1',
                    op: 'put',
                    payload: { id: 'm1' },
                    stamp: { clock: 1, hlc: '1:0:d', deviceId: 'd', opId: 'o1' },
                },
                {
                    serverVersion: 2,
                    tableName: 'messages',
                    pk: 'm2',
                    op: 'delete',
                    payload: undefined,
                    stamp: { clock: 2, hlc: '2:0:d', deviceId: 'd', opId: 'o2' },
                },
            ],
            nextCursor: 2,
            hasMore: false,
        });

        const result = await adapter.pull(makeEvent(), {
            scope: { workspaceId: 'ws-1' },
            cursor: 0,
            limit: 10,
        });

        expect(result).toEqual({
            changes: [
                expect.objectContaining({ op: 'put' }),
                expect.objectContaining({ op: 'delete' }),
            ],
            nextCursor: 2,
            hasMore: false,
        });

        expect(queryMock).toHaveBeenCalledWith('sync.pull', {
            workspace_id: 'ws-1',
            cursor: 0,
            limit: 10,
            tables: undefined,
        });
    });

    it('maps bounded snapshot pages through the Convex mutation', async () => {
        const adapter = new ConvexSyncGatewayAdapter();
        mutationMock.mockResolvedValue({
            workspaceId: 'ws-1',
            snapshotId: 'snapshot-1',
            highWatermark: 7,
            items: [
                {
                    kind: 'tombstone',
                    tableName: 'projects',
                    pk: 'project-1',
                    revision: { clock: 3, hlc: '7:0:dev', opId: 'op-7' },
                    serverDeletedAt: 1007,
                },
            ],
            nextPageToken: null,
        });

        await expect(adapter.snapshot(makeEvent(), {
            scope: { workspaceId: 'ws-1' },
            pageSize: 50,
            tables: ['projects'],
        })).resolves.toMatchObject({
            snapshotId: 'snapshot-1',
            highWatermark: 7,
            nextPageToken: null,
        });

        expect(mutationMock).toHaveBeenCalledWith('sync.snapshot', {
            workspace_id: 'ws-1',
            page_size: 50,
            page_token: undefined,
            tables: ['projects'],
        });
    });

    it('maps bounded canonical storage pages through the Convex query', async () => {
        queryMock.mockResolvedValue({
            items: [{ kind: 'metadata', hash: 'abc', sizeBytes: 12, updatedAt: 1 }],
            hasMore: false,
        });
        const adapter = new ConvexSyncGatewayAdapter();

        await expect(adapter.queryCanonicalStorage(makeEvent(), {
            scope: { workspaceId: 'ws-1' },
            kind: 'live_metadata',
            limit: 25,
            hash: 'sha256:abc',
            now: 123,
        })).resolves.toEqual({
            items: [{ kind: 'metadata', hash: 'abc', sizeBytes: 12, updatedAt: 1 }],
            hasMore: false,
        });
        expect(queryMock).toHaveBeenCalledWith('sync.queryCanonicalStorage', {
            workspace_id: 'ws-1',
            kind: 'live_metadata',
            page_size: 25,
            cursor: undefined,
            hash: 'sha256:abc',
            now: 123,
        });
    });

    it('maps push/updateCursor and verified retention methods to Convex API', async () => {
        const adapter = new ConvexSyncGatewayAdapter();
        mutationMock.mockResolvedValue({ results: [], serverVersion: 5 });

        await adapter.push(makeEvent(), {
            scope: { workspaceId: 'ws-1' },
            ops: [
                {
                    id: 'p1',
                    tableName: 'messages',
                    operation: 'put',
                    pk: 'm1',
                    payload: { id: 'm1' },
                    stamp: { opId: 'op-1', deviceId: 'd1', hlc: '1:0:d1', clock: 1 },
                    createdAt: Date.now(),
                    attempts: 0,
                    status: 'pending',
                },
            ],
        });

        expect(mutationMock).toHaveBeenCalledWith('sync.push', expect.objectContaining({
            workspace_id: 'ws-1',
            ops: [
                expect.objectContaining({
                    op_id: 'op-1',
                    table_name: 'messages',
                    operation: 'put',
                }),
            ],
        }));

        await adapter.updateCursor(makeEvent(), {
            scope: { workspaceId: 'ws-1' },
            deviceId: 'dev-1',
            version: 123,
        });
        expect(mutationMock).toHaveBeenCalledWith('sync.updateDeviceCursor', {
            workspace_id: 'ws-1',
            device_id: 'dev-1',
            last_seen_version: 123,
        });

        runtimeConfig.sync.convexAdminKey = 'admin-key';
        resolveSessionContextMock.mockResolvedValue({
            authenticated: true,
            provider: 'clerk',
            providerUserId: 'subject-1',
        });

        await adapter.gcTombstones(makeEvent(), {
            scope: { workspaceId: 'ws-1' },
            retentionSeconds: 3600,
        });

        await adapter.gcChangeLog(makeEvent(), {
            scope: { workspaceId: 'ws-1' },
            retentionSeconds: 3600,
        });
        expect(mutationMock).toHaveBeenCalledWith('sync.gcTombstones', {
            workspace_id: 'ws-1',
            retention_seconds: 3600,
        });
        expect(mutationMock).toHaveBeenCalledWith('sync.gcChangeLog', {
            workspace_id: 'ws-1',
            retention_seconds: 3600,
        });
        expect(resolveProviderTokenMock).toHaveBeenCalledTimes(2);
    });

    it('calls resolveProviderToken for each method with provider/template', async () => {
        const adapter = new ConvexSyncGatewayAdapter();
        queryMock.mockResolvedValue({ changes: [], nextCursor: 0, hasMore: false });
        mutationMock.mockResolvedValue({ results: [], serverVersion: 0 });

        await adapter.pull(makeEvent(), { scope: { workspaceId: 'ws-1' }, cursor: 0, limit: 10 });
        await adapter.push(makeEvent(), { scope: { workspaceId: 'ws-1' }, ops: [] });
        await adapter.updateCursor(makeEvent(), { scope: { workspaceId: 'ws-1' }, deviceId: 'd1', version: 1 });

        for (const call of resolveProviderTokenMock.mock.calls) {
            expect(call[1]).toEqual({ providerId: 'convex', template: 'convex' });
        }
        expect(resolveProviderTokenMock).toHaveBeenCalledTimes(3);
    });

    it('emits webhook runtime hooks for successful push operations', async () => {
        const adapter = new ConvexSyncGatewayAdapter();
        resolveSessionContextMock.mockResolvedValue({
            authenticated: true,
            user: { id: 'user-1' },
        });
        mutationMock.mockResolvedValue({
            results: [
                {
                    opId: 'op-1',
                    success: true,
                    serverVersion: 7,
                    tableName: 'threads',
                    operation: 'put',
                    payload: {
                        id: 'thread-1',
                        title: 'Renamed',
                    },
                    wasExisting: true,
                    applied: true,
                },
            ],
            serverVersion: 7,
        });

        await adapter.push(makeEvent(), {
            scope: { workspaceId: 'ws-1' },
            ops: [
                {
                    id: 'pending-1',
                    tableName: 'threads',
                    operation: 'put',
                    pk: 'thread-1',
                    payload: { id: 'thread-1', title: 'Renamed' },
                    stamp: {
                        opId: 'op-1',
                        deviceId: 'device-1',
                        hlc: '1:0:device-1',
                        clock: 2,
                    },
                    createdAt: Date.now(),
                    attempts: 0,
                    status: 'pending',
                },
            ],
        });

        expect(emitWebhookSystemHookMock).toHaveBeenCalledWith(
            'db.threads.update:action:after',
            expect.objectContaining({
                id: 'thread-1',
                workspace_id: 'ws-1',
                user_id: 'user-1',
            })
        );
    });

    it('maps posts table pushes to document webhook hooks', async () => {
        const adapter = new ConvexSyncGatewayAdapter();
        resolveSessionContextMock.mockResolvedValue({
            authenticated: true,
            user: { id: 'user-1' },
        });
        mutationMock.mockResolvedValue({
            results: [
                {
                    opId: 'op-post-1',
                    success: true,
                    serverVersion: 9,
                    tableName: 'posts',
                    operation: 'put',
                    payload: {
                        id: 'doc-1',
                        title: 'Doc title',
                        post_type: 'document',
                    },
                    wasExisting: false,
                    applied: true,
                },
            ],
            serverVersion: 9,
        });

        await adapter.push(makeEvent(), {
            scope: { workspaceId: 'ws-1' },
            ops: [
                {
                    id: 'pending-post-1',
                    tableName: 'posts',
                    operation: 'put',
                    pk: 'doc-1',
                    payload: {
                        id: 'doc-1',
                        title: 'Doc title',
                        post_type: 'document',
                    },
                    stamp: {
                        opId: 'op-post-1',
                        deviceId: 'device-1',
                        hlc: '1:0:device-1',
                        clock: 1,
                    },
                    createdAt: Date.now(),
                    attempts: 0,
                    status: 'pending',
                },
            ],
        });

        expect(emitWebhookSystemHookMock).toHaveBeenCalledWith(
            'db.documents.create:action:after',
            expect.objectContaining({
                id: 'doc-1',
                workspace_id: 'ws-1',
                user_id: 'user-1',
            })
        );
    });

    it('retries transient transport errors before succeeding', async () => {
        const adapter = new ConvexSyncGatewayAdapter();
        queryMock
            .mockRejectedValueOnce(createTransientTransportError())
            .mockResolvedValueOnce({ changes: [], nextCursor: 0, hasMore: false });

        await expect(
            adapter.pull(makeEvent(), {
                scope: { workspaceId: 'ws-1' },
                cursor: 0,
                limit: 10,
            })
        ).resolves.toEqual({
            changes: [],
            nextCursor: 0,
            hasMore: false,
        });

        expect(queryMock).toHaveBeenCalledTimes(2);
    });

    it('maps exhausted transient transport failures to 503', async () => {
        const adapter = new ConvexSyncGatewayAdapter();
        queryMock.mockRejectedValue(createTransientTransportError());

        await expect(
            adapter.pull(makeEvent(), {
                scope: { workspaceId: 'ws-1' },
                cursor: 0,
                limit: 10,
            })
        ).rejects.toMatchObject({
            statusCode: 503,
            statusMessage: 'Sync backend unavailable',
        });

        expect(queryMock).toHaveBeenCalledTimes(3);
    });
});
