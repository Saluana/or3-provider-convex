import { beforeEach, describe, expect, it, vi } from 'vitest';
import { createConvexSyncProvider } from '../../sync/convex-sync-provider';

vi.mock('convex/server', () => ({
    anyApi: {
        sync: {
            watchChanges: 'sync.watchChanges',
            pull: 'sync.pull',
            snapshot: 'sync.snapshot',
            push: 'sync.push',
            updateDeviceCursor: 'sync.updateDeviceCursor',
            gcTombstones: 'sync.gcTombstones',
            gcChangeLog: 'sync.gcChangeLog',
        },
    },
}));

describe('createConvexSyncProvider', () => {
    const onUpdateMock = vi.fn();
    const queryMock = vi.fn();
    const mutationMock = vi.fn();

    const client = {
        onUpdate: (...args: unknown[]) => onUpdateMock(...args),
        query: (...args: unknown[]) => queryMock(...args),
        mutation: (...args: unknown[]) => mutationMock(...args),
    };

    beforeEach(() => {
        onUpdateMock.mockReset();
        queryMock.mockReset();
        mutationMock.mockReset();
    });

    it('exposes provider metadata (id/mode/auth)', () => {
        const provider = createConvexSyncProvider(client as any);

        expect(provider.id).toBe('convex');
        expect(provider.mode).toBe('direct');
        expect(provider.auth).toEqual({
            providerId: 'convex',
            template: 'convex',
        });
    });

    it('subscribe filters by tables and ignores malformed payloads', async () => {
        const provider = createConvexSyncProvider(client as any);
        const onChanges = vi.fn();

        let callback: ((result: { changes: unknown; nextCursor: number; hasMore: boolean; oldestRetainedVersion: number; requiresSnapshot: boolean }) => void) | undefined;
        onUpdateMock.mockImplementation((_query, _args, cb) => {
            callback = cb;
            return vi.fn();
        });

        await provider.subscribe({ workspaceId: 'ws-1' }, ['messages'], onChanges);

        callback?.({
            nextCursor: 2, hasMore: false, oldestRetainedVersion: 1, requiresSnapshot: false,
            changes: [
                {
                    serverVersion: 1,
                    tableName: 'messages',
                    pk: 'm1',
                    op: 'put',
                    payload: {},
                    stamp: { clock: 1, hlc: '1:0:dev', deviceId: 'dev', opId: crypto.randomUUID() },
                },
                {
                    serverVersion: 2,
                    tableName: 'threads',
                    pk: 't1',
                    op: 'put',
                    payload: {},
                    stamp: { clock: 1, hlc: '2:0:dev', deviceId: 'dev', opId: crypto.randomUUID() },
                },
            ],
        });

        expect(onChanges).toHaveBeenCalledTimes(1);
        expect(onChanges.mock.calls[0]?.[0]).toEqual([
            expect.objectContaining({ tableName: 'messages' }),
        ]);
        await vi.waitFor(() => expect(onUpdateMock).toHaveBeenCalledTimes(2));

        const errorSpy = vi.spyOn(console, 'error').mockImplementation(() => undefined);
        callback?.({ changes: [{ bad: 'shape' }], nextCursor: 2, hasMore: false, oldestRetainedVersion: 1, requiresSnapshot: false });
        expect(onChanges).toHaveBeenCalledTimes(1);
        expect(errorSpy).toHaveBeenCalled();
    });

    it('resubscribes with an advanced cursor after a filtered empty page', async () => {
        const provider = createConvexSyncProvider(client as any);
        const onChanges = vi.fn();
        const callbacks: Array<(result: {
            changes: unknown[];
            latestVersion?: number;
            nextCursor: number;
            hasMore: boolean;
            oldestRetainedVersion: number;
            requiresSnapshot: boolean;
        }) => void> = [];
        onUpdateMock.mockImplementation((_query, args, cb) => {
            callbacks.push(cb);
            return vi.fn();
        });

        await provider.subscribe(
            { workspaceId: 'ws-1' },
            ['messages'],
            onChanges,
            { cursor: 0 }
        );

        callbacks[0]?.({
            changes: [{
                serverVersion: 4,
                tableName: 'threads',
                pk: 't1',
                op: 'put',
                payload: {},
                stamp: {
                    clock: 1,
                    hlc: '1:0:dev',
                    deviceId: 'dev',
                    opId: crypto.randomUUID(),
                },
            }],
            latestVersion: 4,
            nextCursor: 4, hasMore: false, oldestRetainedVersion: 1, requiresSnapshot: false,
        });

        expect(onChanges).toHaveBeenCalledWith([]);
        await vi.waitFor(() => expect(onUpdateMock).toHaveBeenCalledTimes(2));
        expect(onUpdateMock.mock.calls[1]?.[1]).toMatchObject({ cursor: 4 });
    });

    it('suppresses known Convex unwatch race during cleanup', async () => {
        const warnSpy = vi.spyOn(console, 'warn').mockImplementation(() => undefined);
        const knownRaceError = new TypeError("Cannot read properties of undefined (reading 'numSubscribers')");

        onUpdateMock.mockReturnValue(() => {
            throw knownRaceError;
        });

        const provider = createConvexSyncProvider(client as any);
        const unsubscribe = await provider.subscribe({ workspaceId: 'ws-1' }, [], vi.fn());

        expect(() => unsubscribe()).not.toThrow();
        expect(warnSpy).not.toHaveBeenCalled();
    });

    it('releases each watch once when completed subscriptions are later disposed', async () => {
        // An unmounted client must release its callback and not retain an old
        // watch until global disposal. Reusing the same scope must remain safe.
        const stops = Array.from({ length: 3 }, () => vi.fn());
        stops.forEach((stop) => onUpdateMock.mockReturnValueOnce(stop));
        const provider = createConvexSyncProvider(client as any);
        for (let cursor = 0; cursor < 3; cursor += 1) {
            const stop = await provider.subscribe({ workspaceId: 'ws-1' }, ['messages'], vi.fn(), { cursor });
            stop();
            stop();
        }
        await provider.dispose?.();
        stops.forEach((stop) => expect(stop).toHaveBeenCalledTimes(1));
    });

    it('throws when pull response fails schema validation', async () => {
        const provider = createConvexSyncProvider(client as any);
        queryMock.mockResolvedValue({ changes: 'invalid', nextCursor: 1, hasMore: false });

        await expect(
            provider.pull({ scope: { workspaceId: 'ws-1' }, cursor: 0, limit: 10 })
        ).rejects.toThrow('Invalid pull response');
    });

    it('fences a cancelled direct request while its transport finishes later', async () => {
        let finish!: (value: unknown) => void;
        queryMock.mockReturnValue(new Promise(resolve => { finish = resolve; }));
        const provider = createConvexSyncProvider(client as any);
        const controller = new AbortController();
        const request = provider.pull({ scope: { workspaceId: 'ws-1' }, cursor: 0, limit: 10 }, { signal: controller.signal });
        controller.abort();
        await expect(request).rejects.toMatchObject({ name: 'AbortError' });
        finish({ changes: [], nextCursor: 3, hasMore: false });
        await expect(provider.pull({ scope: { workspaceId: 'ws-1' }, cursor: 0, limit: 10 }, { signal: controller.signal }))
            .rejects.toMatchObject({ name: 'AbortError' });
        expect(queryMock).toHaveBeenCalledTimes(1);
    });

    it('times out an offline direct query and permits a later recovery request', async () => {
        vi.useFakeTimers();
        try {
            queryMock.mockReturnValueOnce(new Promise(() => {}));
            const provider = createConvexSyncProvider(client as any);
            const waiting = provider.pull({ scope: { workspaceId: 'ws-1' }, cursor: 0, limit: 10 });
            const rejected = expect(waiting).rejects.toMatchObject({ name: 'TimeoutError' });
            await vi.advanceTimersByTimeAsync(30_000);
            await rejected;
            queryMock.mockResolvedValueOnce({ changes: [], nextCursor: 0, hasMore: false, oldestRetainedVersion: 1, requiresSnapshot: false });
            await expect(provider.pull({ scope: { workspaceId: 'ws-1' }, cursor: 0, limit: 10 })).resolves.toMatchObject({ nextCursor: 0 });
        } finally { vi.useRealTimers(); }
    });

    it('retains the cursor after a rejected page and advances only after successful redelivery', async () => {
        const callbacks: Array<(value: unknown) => void> = [];
        onUpdateMock.mockImplementation((_query, _args, callback) => { callbacks.push(callback); return vi.fn(); });
        const provider = createConvexSyncProvider(client as any);
        const onPage = vi.fn().mockRejectedValueOnce(new Error('Injected local apply failure')).mockResolvedValue(undefined);
        const stop = await provider.subscribe({ workspaceId: 'ws-1' }, ['messages'], vi.fn(), { cursor: 0, onPage });
        const page = { changes: [], nextCursor: 4, latestVersion: 4, hasMore: false, oldestRetainedVersion: 1, requiresSnapshot: false };
        callbacks[0]?.(page);
        await vi.waitFor(() => expect(onPage).toHaveBeenCalledTimes(1));
        expect(onUpdateMock).toHaveBeenCalledTimes(1);
        callbacks[0]?.(page);
        callbacks[0]?.(page);
        await vi.waitFor(() => expect(onUpdateMock).toHaveBeenCalledTimes(2));
        expect(onPage).toHaveBeenCalledTimes(2);
        expect(onPage).toHaveBeenLastCalledWith(0, expect.objectContaining({ nextCursor: 4 }));
        expect(onUpdateMock.mock.calls[1]?.[1]).toMatchObject({ cursor: 4 });
        stop();
    });

    it('throws when push response fails schema validation', async () => {
        const provider = createConvexSyncProvider(client as any);
        mutationMock.mockResolvedValue({ results: [{ opId: 'x', success: true }], serverVersion: -1 });

        await expect(
            provider.push({
                scope: { workspaceId: 'ws-1' },
                ops: [
                    {
                        id: 'p1',
                        tableName: 'messages',
                        operation: 'put',
                        pk: 'm1',
                        payload: {},
                        stamp: {
                            opId: crypto.randomUUID(),
                            deviceId: 'dev',
                            hlc: '1:0:dev',
                            clock: 1,
                        },
                        createdAt: Date.now(),
                        attempts: 0,
                        status: 'pending',
                    },
                ],
            })
        ).rejects.toThrow('Invalid push response');
    });

    it('maps and validates materialized snapshot pages', async () => {
        const provider = createConvexSyncProvider(client as any);
        mutationMock.mockResolvedValue({
            workspaceId: 'ws-1',
            snapshotId: 'snapshot-1',
            highWatermark: 12,
            items: [
                {
                    kind: 'row',
                    tableName: 'messages',
                    pk: 'message-1',
                    payload: { id: 'message-1', data: { text: 'hello' } },
                    revision: { clock: 2, hlc: '12:0:dev', opId: 'op-12' },
                },
            ],
            nextPageToken: 'opaque-next',
        });

        await expect(provider.snapshot?.({
            scope: { workspaceId: 'ws-1' },
            pageSize: 25,
            pageToken: 'opaque-current',
            tables: ['messages'],
        })).resolves.toMatchObject({
            snapshotId: 'snapshot-1',
            highWatermark: 12,
            nextPageToken: 'opaque-next',
        });

        expect(mutationMock).toHaveBeenCalledWith('sync.snapshot', {
            workspace_item_capability: 'v1',
            workspace_id: 'ws-1',
            page_size: 25,
            page_token: 'opaque-current',
            tables: ['messages'],
        });
    });

    it('maps pull/push/updateCursor calls and does not expose history GC', async () => {
        const provider = createConvexSyncProvider(client as any);
        queryMock.mockResolvedValue({
            changes: [],
            nextCursor: 1,
            hasMore: false,
            oldestRetainedVersion: 0,
            requiresSnapshot: false,
        });
        mutationMock.mockResolvedValue({ results: [], serverVersion: 1 });

        await provider.pull({ scope: { workspaceId: 'ws-1' }, cursor: 0, limit: 25, tables: ['messages'] });
        expect(queryMock).toHaveBeenCalledWith('sync.pull', {
            workspace_item_capability: 'v1',
            workspace_id: 'ws-1',
            cursor: 0,
            limit: 25,
            tables: ['messages'],
        });

        await provider.push({
            scope: { workspaceId: 'ws-1' },
            ops: [
                {
                    id: 'p1',
                    tableName: 'messages',
                    operation: 'put',
                    pk: 'm1',
                    payload: { text: 'hi' },
                    stamp: {
                        opId: crypto.randomUUID(),
                        deviceId: 'dev',
                        hlc: '1:0:dev',
                        clock: 1,
                    },
                    createdAt: Date.now(),
                    attempts: 0,
                    status: 'pending',
                },
            ],
        });

        expect(mutationMock).toHaveBeenCalledWith('sync.push', expect.objectContaining({
            workspace_item_capability: 'v1',
            workspace_id: 'ws-1',
            ops: [
                expect.objectContaining({
                    table_name: 'messages',
                    operation: 'put',
                    pk: 'm1',
                }),
            ],
        }));

        await provider.updateCursor({ workspaceId: 'ws-1' }, 'dev-1', 123);
        expect(mutationMock).toHaveBeenCalledWith('sync.updateDeviceCursor', {
            workspace_id: 'ws-1',
            device_id: 'dev-1',
            last_seen_version: 123,
        });

        expect(provider.gcTombstones).toBeUndefined();
        expect(provider.gcChangeLog).toBeUndefined();
        expect(mutationMock).not.toHaveBeenCalledWith(
            'sync.gcTombstones',
            expect.anything()
        );
        expect(mutationMock).not.toHaveBeenCalledWith(
            'sync.gcChangeLog',
            expect.anything()
        );
    });

    it('dispose unsubscribes all tracked subscriptions', async () => {
        const unwatchA = vi.fn();
        const unwatchB = vi.fn();
        onUpdateMock
            .mockReturnValueOnce(unwatchA)
            .mockReturnValueOnce(unwatchB);

        const provider = createConvexSyncProvider(client as any);
        await provider.subscribe({ workspaceId: 'ws-1' }, ['messages'], vi.fn());
        await provider.subscribe({ workspaceId: 'ws-2' }, ['threads'], vi.fn());

        await provider.dispose();

        expect(unwatchA).toHaveBeenCalledTimes(1);
        expect(unwatchB).toHaveBeenCalledTimes(1);
    });
});
