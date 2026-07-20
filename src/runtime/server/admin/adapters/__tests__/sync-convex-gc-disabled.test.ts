import { describe, expect, it, vi } from 'vitest';
import type { H3Event } from 'h3';
import { convexSyncAdminAdapter } from '../sync-convex';

vi.mock('#imports', () => ({
    useRuntimeConfig: () => ({
        sync: { convexUrl: 'https://example.convex.cloud' },
        auth: { provider: 'clerk' },
    }),
}));

vi.mock('~~/server/auth/token-broker/registry', () => ({
    listProviderTokenBrokerIds: () => ['clerk'],
}));

const event = {} as H3Event;
const statusContext = {
    enabled: true,
    providerId: 'convex',
} as never;
const actionContext = {
    session: {
        workspace: { id: 'ws-1' },
    },
} as never;

describe('Convex sync admin GC safety gate', () => {
    it('does not advertise destructive history actions', async () => {
        const status = await convexSyncAdminAdapter.getStatus(event, statusContext);

        expect(status.actions).toEqual([]);
        expect(status.warnings).toEqual(
            expect.arrayContaining([
                expect.objectContaining({
                    message: expect.stringContaining('snapshot bootstrap'),
                }),
            ])
        );
    });

    it.each(['sync.gc-change-log', 'sync.gc-tombstones'])(
        'rejects stale admin action %s',
        async (actionId) => {
            await expect(
                convexSyncAdminAdapter.runAction!(
                    event,
                    actionId,
                    { retentionSeconds: 1 },
                    actionContext
                )
            ).rejects.toMatchObject({
                statusCode: 503,
                message: expect.stringContaining('snapshot bootstrap'),
            });
        }
    );
});
