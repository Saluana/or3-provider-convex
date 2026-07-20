import { beforeEach, describe, expect, it, vi } from 'vitest';

const runtimeConfig = vi.hoisted(() => ({
    sync: {
        convexUrl: 'https://example.convex.cloud',
        convexAdminKey: 'admin-key',
    },
}));
const setAdminAuth = vi.hoisted(() => vi.fn());

vi.mock('#imports', () => ({
    useRuntimeConfig: () => runtimeConfig,
}));

vi.mock('convex/browser', () => ({
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
