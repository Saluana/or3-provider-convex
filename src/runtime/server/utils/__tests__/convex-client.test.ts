import { beforeEach, describe, expect, it, vi } from 'vitest';

const runtimeConfig = vi.hoisted(() => ({
    sync: {
        convexUrl: 'https://example.convex.cloud',
        convexAdminKey: 'admin-key',
    },
}));
const setAdminAuth = vi.hoisted(() => vi.fn());
const realtime = vi.hoisted(() => ({ opened: vi.fn(), close: vi.fn(async () => {}), unsubscribe: vi.fn(), onUpdate: vi.fn() }));

vi.mock('#imports', () => ({
    useRuntimeConfig: () => runtimeConfig,
}));

vi.mock('convex/browser', () => ({
    ConvexClient: class {
        constructor() { realtime.opened(); }
        setAdminAuth = setAdminAuth;
        close = realtime.close;
        onUpdate = realtime.onUpdate;
    },
    ConvexHttpClient: class {
        setAdminAuth = setAdminAuth;
    },
}));

describe('shared Convex server client', () => {
    beforeEach(() => {
        vi.resetModules();
        setAdminAuth.mockReset();
        runtimeConfig.sync.convexUrl = 'https://example.convex.cloud';
        runtimeConfig.sync.convexAdminKey = 'admin-key';
    });

    // No idle socket leak and no socket per viewer/workspace. This owns the
    // connection lifecycle; gateway auth is covered by the host SSE route.
    it('shares a realtime connection and closes it after the last watcher', async () => {
        realtime.onUpdate.mockImplementation(() => realtime.unsubscribe);
        const { watchConvexVersion } = await import('../convex-client');
        const first = watchConvexVersion('workspace-a', vi.fn(), vi.fn());
        const second = watchConvexVersion('workspace-b', vi.fn(), vi.fn());
        expect(realtime.opened).toHaveBeenCalledTimes(1);
        first();
        expect(realtime.close).not.toHaveBeenCalled();
        second(); second();
        expect(realtime.close).toHaveBeenCalledTimes(1);
        expect(realtime.unsubscribe).toHaveBeenCalledTimes(2);
    });

    it('uses admin authentication for internal auxiliary functions', async () => {
        const { getConvexClient } = await import('../convex-client');

        getConvexClient();

        expect(setAdminAuth).toHaveBeenCalledWith('admin-key', {
            subject: 'or3-auxiliary-persistence',
            issuer: 'https://or3.ai/internal',
            tokenIdentifier:
                'https://or3.ai/internal|or3-auxiliary-persistence',
        });
    });

    it('fails closed without the internal-function admin credential', async () => {
        runtimeConfig.sync.convexAdminKey = '';
        const { getConvexClient } = await import('../convex-client');

        expect(() => getConvexClient()).toThrow('Convex admin key not configured');
        expect(setAdminAuth).not.toHaveBeenCalled();
    });
});
