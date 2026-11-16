import { beforeEach, describe, expect, it, vi } from 'vitest';
import { createConvexSyncProvider } from '../convex-sync-provider';
vi.mock('convex/server', () => ({ anyApi: { sync: Object.fromEntries(['watchChanges', 'pullChecked', 'snapshot', 'push', 'updateDeviceCursor'].map(name => [name, `sync.${name}`])) } }));
const history = 'fixture-history';
const empty = { historyIncarnation: history, changes: [], unavailable: [], nextCursor: 0, hasMore: false, oldestRetainedVersion: 1, requiresSnapshot: false };
describe('Convex direct checked delivery', () => {
    const mutation = vi.fn(), onUpdate = vi.fn();
    const client = { mutation, onUpdate };
    beforeEach(() => { vi.restoreAllMocks(); mutation.mockReset(); onUpdate.mockReset(); });
    it('preserves identity, unavailable markers and structured reset errors across transport', async () => {
        const provider = createConvexSyncProvider(client as any);
        const marker = { tableName: 'kv', pk: 'bad', originalKind: 'put', serverVersion: 2, evidenceRef: 'e1', reason: 'invalid_payload' };
        mutation.mockResolvedValue({ ...empty, unavailable: [marker], nextCursor: 2 });
        expect(await provider.pull({ scope: { workspaceId: 'ws' }, historyIncarnation: history, cursor: 0, limit: 10 })).toMatchObject({ historyIncarnation: history, unavailable: [marker] });
        expect(mutation).toHaveBeenCalledWith('sync.pullChecked', expect.objectContaining({ history_incarnation: history, history_incarnation_capability: 'incarnation-v1', unavailable_record_capability: 'unavailable-v1' }));
        const error = Object.assign(new Error('reset'), { data: { code: 'OR3_SYNC_HISTORY_CHANGED', historyIncarnation: 'next', requiresSnapshot: true } });
        mutation.mockRejectedValue(error);
        await expect(provider.push({ scope: { workspaceId: 'ws' }, historyIncarnation: history, ops: [] })).rejects.toBe(error);
    });
    it('rejects incapable backends and malformed pages before delivery', async () => {
        const provider = createConvexSyncProvider(client as any);
        mutation.mockResolvedValue({ changes: [], nextCursor: 0, hasMore: false });
        await expect(provider.pull({ scope: { workspaceId: 'ws' }, cursor: 0, limit: 10 })).rejects.toMatchObject({ code: 'OR3_SYNC_HISTORY_UNSUPPORTED' });
        mutation.mockResolvedValue({ ...empty, changes: 'invalid' });
        await expect(provider.pull({ scope: { workspaceId: 'ws' }, cursor: 0, limit: 10 })).rejects.toThrow('Invalid pull response');
    });
    it('maps snapshot, push and ACK history and preserves response identities', async () => {
        const provider = createConvexSyncProvider(client as any);
        mutation.mockResolvedValueOnce({ historyIncarnation: history, workspaceId: 'ws', snapshotId: 's', highWatermark: 2, items: [], unavailable: [], nextPageToken: null });
        expect(await provider.snapshot!({ scope: { workspaceId: 'ws' }, historyIncarnation: history, pageSize: 10 })).toMatchObject({ historyIncarnation: history });
        mutation.mockResolvedValueOnce({ historyIncarnation: history, results: [], serverVersion: 2 });
        expect(await provider.push({ scope: { workspaceId: 'ws' }, historyIncarnation: history, ops: [] })).toMatchObject({ historyIncarnation: history });
        mutation.mockResolvedValueOnce({ historyIncarnation: history, ok: true });
        await provider.updateCursor({ workspaceId: 'ws' }, 'device', 2, { historyIncarnation: history });
        expect(mutation).toHaveBeenLastCalledWith('sync.updateDeviceCursor', expect.objectContaining({ history_incarnation: history }));
    });
    it('fences cancelled and timed-out mutation waits without settling late responses', async () => {
        const provider = createConvexSyncProvider(client as any);
        let finish!: (value: unknown) => void;
        mutation.mockReturnValueOnce(new Promise(resolve => { finish = resolve; }));
        const controller = new AbortController();
        const waiting = provider.pull({ scope: { workspaceId: 'ws' }, cursor: 0, limit: 10 }, { signal: controller.signal });
        controller.abort(); await expect(waiting).rejects.toMatchObject({ name: 'AbortError' });
        finish(empty);
        vi.useFakeTimers();
        try {
            mutation.mockReturnValueOnce(new Promise(() => {}));
            const timed = expect(provider.pull({ scope: { workspaceId: 'ws' }, cursor: 0, limit: 10 })).rejects.toMatchObject({ name: 'TimeoutError' });
            await vi.advanceTimersByTimeAsync(30_000); await timed;
        } finally { vi.useRealTimers(); }
    });
    it('uses reactive wakeups and retains coverage until the full page is acknowledged', async () => {
        const log = vi.spyOn(console, 'error').mockImplementation(() => {});
        let wake!: (value: unknown) => void;
        const stopWatch = vi.fn(); onUpdate.mockImplementation((_ref, _args, cb) => { wake = cb; return stopWatch; });
        const page = { ...empty, nextCursor: 4 };
        mutation.mockResolvedValue(page);
        const onPage = vi.fn().mockRejectedValueOnce(new Error('local quota')).mockResolvedValue(undefined);
        const provider = createConvexSyncProvider(client as any);
        const stop = await provider.subscribe({ workspaceId: 'ws' }, ['messages'], vi.fn(), { historyIncarnation: history, cursor: 0, onPage });
        wake({ historyIncarnation: history, version: 4 });
        await vi.waitFor(() => expect(log).toHaveBeenCalled());
        wake({ historyIncarnation: history, version: 4 });
        await vi.waitFor(() => expect(onPage).toHaveBeenCalledTimes(2));
        expect(onPage).toHaveBeenNthCalledWith(2, 0, page);
        wake({ historyIncarnation: 'restored', version: 0 });
        await vi.waitFor(() => expect(mutation).toHaveBeenCalledTimes(3));
        expect(mutation.mock.calls[2]?.[1]).toMatchObject({ cursor: 4, history_incarnation: history });
        stop(); stop(); await provider.dispose(); expect(stopWatch).toHaveBeenCalledTimes(1);
    });
    it('releases all watches and tolerates the SDK unsubscribe race', async () => {
        const stop = vi.fn(() => { throw new TypeError("Cannot read properties of undefined (reading 'numSubscribers')"); });
        onUpdate.mockReturnValue(stop);
        const provider = createConvexSyncProvider(client as any);
        await provider.subscribe({ workspaceId: 'a' }, [], vi.fn()); await provider.subscribe({ workspaceId: 'b' }, [], vi.fn());
        await provider.dispose(); expect(stop).toHaveBeenCalledTimes(2);
    });
});
