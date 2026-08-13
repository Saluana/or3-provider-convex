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
    it('advertises history GC actions when snapshot bootstrap is verified', async () => {
        const status = await convexSyncAdminAdapter.getStatus(event, statusContext);

        expect(status.actions.map((action) => action.id)).toEqual([
            'sync.gc-change-log',
            'sync.gc-tombstones',
        ]);
        expect(status.warnings).toEqual(
            expect.arrayContaining([
                expect.objectContaining({
                    message: expect.stringContaining('snapshot-v1'),
                }),
            ])
        );
    });
});
