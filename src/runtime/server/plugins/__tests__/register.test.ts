import { beforeEach, describe, expect, it, vi } from 'vitest';

const registerAuthWorkspaceStoreMock = vi.hoisted(() => vi.fn());
const registerSyncGatewayAdapterMock = vi.hoisted(() => vi.fn());
const registerStorageGatewayAdapterMock = vi.hoisted(() => vi.fn());
const registerProviderAdminAdapterMock = vi.hoisted(() => vi.fn());
const registerAdminStoreProviderMock = vi.hoisted(() => vi.fn());
const registerBackgroundJobProviderMock = vi.hoisted(() => vi.fn());
const registerRateLimitProviderMock = vi.hoisted(() => vi.fn());
const registerNotificationEmitterMock = vi.hoisted(() => vi.fn());
const registerDeploymentAdminCheckerMock = vi.hoisted(() => vi.fn());
const useRuntimeConfigMock = vi.hoisted(() => vi.fn());

vi.mock('~~/server/auth/store/registry', () => ({
    registerAuthWorkspaceStore: registerAuthWorkspaceStoreMock as unknown,
}));
vi.mock('~~/server/sync/gateway/registry', () => ({
    registerSyncGatewayAdapter: registerSyncGatewayAdapterMock as unknown,
}));
vi.mock('~~/server/storage/gateway/registry', () => ({
    registerStorageGatewayAdapter: registerStorageGatewayAdapterMock as unknown,
}));
vi.mock('~~/server/admin/providers/registry', () => ({
    registerProviderAdminAdapter: registerProviderAdminAdapterMock as unknown,
}));
vi.mock('~~/server/admin/stores/registry', () => ({
    registerAdminStoreProvider: registerAdminStoreProviderMock as unknown,
}));
vi.mock('~~/server/utils/background-jobs/registry', () => ({
    registerBackgroundJobProvider: registerBackgroundJobProviderMock as unknown,
}));
vi.mock('~~/server/utils/rate-limit/registry', () => ({
    registerRateLimitProvider: registerRateLimitProviderMock as unknown,
}));
vi.mock('~~/server/utils/notifications/registry', () => ({
    registerNotificationEmitter: registerNotificationEmitterMock as unknown,
}));
vi.mock('~~/server/auth/deployment-admin', () => ({
    registerDeploymentAdminChecker: registerDeploymentAdminCheckerMock as unknown,
}));
vi.mock('#imports', () => ({
    useRuntimeConfig: (...args: unknown[]) => useRuntimeConfigMock(...args),
}));

describe('convex register plugin', () => {
    beforeEach(() => {
        vi.resetModules();
        registerAuthWorkspaceStoreMock.mockReset();
        registerSyncGatewayAdapterMock.mockReset();
        registerStorageGatewayAdapterMock.mockReset();
        registerProviderAdminAdapterMock.mockReset();
        registerAdminStoreProviderMock.mockReset();
        registerBackgroundJobProviderMock.mockReset();
        registerRateLimitProviderMock.mockReset();
        registerNotificationEmitterMock.mockReset();
        registerDeploymentAdminCheckerMock.mockReset();
        useRuntimeConfigMock.mockReset();

        process.env.NODE_ENV = 'test';
        delete process.env.OR3_CONVEX_ALLOW_INSECURE_HTTP;

        (globalThis as typeof globalThis & { defineNitroPlugin?: unknown }).defineNitroPlugin = (
            plugin: () => unknown
        ) => plugin();
        useRuntimeConfigMock.mockReturnValue({
            auth: { enabled: true, provider: 'clerk' },
            sync: { enabled: true, provider: 'convex', convexUrl: 'https://example.convex.cloud' },
            storage: { enabled: false, provider: 's3' },
            public: { sync: { convexUrl: 'https://example.convex.cloud' } },
        });
    });

    it('registers providers when convex config is valid', async () => {
        await import('../register');

        expect(registerSyncGatewayAdapterMock).toHaveBeenCalledTimes(1);
        expect(registerStorageGatewayAdapterMock).toHaveBeenCalledTimes(1);
    });

    it('fails startup when convex is selected but URL is missing', async () => {
        useRuntimeConfigMock.mockReturnValue({
            auth: { enabled: true, provider: 'clerk' },
            sync: { enabled: true, provider: 'convex', convexUrl: '' },
            storage: { enabled: false, provider: 's3' },
            public: { sync: { convexUrl: '' } },
        });

        await expect(import('../register')).rejects.toThrow('Missing Convex URL');
        expect(registerSyncGatewayAdapterMock).not.toHaveBeenCalled();
    });

    it('fails startup for insecure HTTP convex URL by default', async () => {
        useRuntimeConfigMock.mockReturnValue({
            auth: { enabled: true, provider: 'clerk' },
            sync: { enabled: true, provider: 'convex', convexUrl: 'http://localhost:3210' },
            storage: { enabled: false, provider: 's3' },
            public: { sync: { convexUrl: 'http://localhost:3210' } },
        });

        await expect(import('../register')).rejects.toThrow(
            'Convex URL must use HTTPS unless OR3_CONVEX_ALLOW_INSECURE_HTTP=true is explicitly set.'
        );
        expect(registerSyncGatewayAdapterMock).not.toHaveBeenCalled();
    });

    it('does not fail when convex is not selected', async () => {
        useRuntimeConfigMock.mockReturnValue({
            auth: { enabled: true, provider: 'clerk' },
            sync: { enabled: true, provider: 'sqlite', convexUrl: '' },
            storage: { enabled: true, provider: 's3' },
            public: { sync: { convexUrl: '' } },
        });

        await expect(import('../register')).resolves.toBeDefined();
    });
});
