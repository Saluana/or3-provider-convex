import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { convexBackendContract } from '../../../backend-contract';

const registerAuthWorkspaceStoreMock = vi.hoisted(() => vi.fn());
const registerSyncGatewayAdapterMock = vi.hoisted(() => vi.fn());
const registerStorageGatewayAdapterMock = vi.hoisted(() => vi.fn());
const registerProviderAdminAdapterMock = vi.hoisted(() => vi.fn());
const registerAdminStoreProviderMock = vi.hoisted(() => vi.fn());
const registerBackgroundJobProviderMock = vi.hoisted(() => vi.fn());
const registerRateLimitProviderMock = vi.hoisted(() => vi.fn());
const registerNotificationEmitterMock = vi.hoisted(() => vi.fn());
const registerDeploymentAdminCheckerMock = vi.hoisted(() => vi.fn());
const registerConnectStoreMock = vi.hoisted(() => vi.fn());
const useRuntimeConfigMock = vi.hoisted(() => vi.fn());

vi.mock('nitropack/runtime/plugin', () => ({
    defineNitroPlugin: (plugin: () => unknown) => plugin(),
}));

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
vi.mock('~~/server/connect/store/registry', () => ({
    registerConnectStore: registerConnectStoreMock as unknown,
}));
vi.mock('#imports', () => ({
    useRuntimeConfig: (...args: unknown[]) => useRuntimeConfigMock(...args),
}));

describe('convex register plugin', () => {
    afterEach(() => vi.unstubAllGlobals());
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
        registerConnectStoreMock.mockReset();
        useRuntimeConfigMock.mockReset();

        process.env.NODE_ENV = 'test';
        delete process.env.OR3_CONVEX_ALLOW_INSECURE_HTTP;
        vi.stubGlobal('fetch', vi.fn().mockResolvedValue(new Response(JSON.stringify({ status: 'success', value: convexBackendContract }))));

        useRuntimeConfigMock.mockReturnValue({
            auth: { enabled: true, provider: 'clerk' },
            sync: { enabled: true, provider: 'convex', convexUrl: 'https://example.convex.cloud' },
            storage: { enabled: false, provider: 's3' },
            public: { sync: { convexUrl: 'https://example.convex.cloud' } },
        });
    });

    it('registers providers when convex config is valid', async () => {
        await (await import('../register')).default;

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

        await expect(import('../register').then((module) => module.default)).rejects.toThrow('Missing Convex URL');
        expect(registerSyncGatewayAdapterMock).not.toHaveBeenCalled();
    });

    it('fails startup for insecure HTTP convex URL by default', async () => {
        useRuntimeConfigMock.mockReturnValue({
            auth: { enabled: true, provider: 'clerk' },
            sync: { enabled: true, provider: 'convex', convexUrl: 'http://localhost:3210' },
            storage: { enabled: false, provider: 's3' },
            public: { sync: { convexUrl: 'http://localhost:3210' } },
        });

        await expect(import('../register').then((module) => module.default)).rejects.toThrow(
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

    it('registers Connect when Convex is selected only for Connect', async () => {
        useRuntimeConfigMock.mockReturnValue({
            auth: { enabled: true, provider: 'clerk' },
            sync: {
                enabled: true,
                provider: 'sqlite',
                convexUrl: 'https://example.convex.cloud',
            },
            storage: { enabled: true, provider: 's3' },
            connect: { enabled: true, provider: 'convex' },
            public: {
                sync: { convexUrl: 'https://example.convex.cloud' },
            },
        });

        await (await import('../register')).default;

        expect(registerConnectStoreMock).toHaveBeenCalledWith(
            expect.objectContaining({ id: 'convex' })
        );
    });

    it.each([
        { status: 'success', value: { ...convexBackendContract, digest: 'a'.repeat(64) } },
        { status: 'error', errorMessage: 'Could not find public function or3Backend:version' },
    ])('refuses startup before registering against an incompatible backend: %j', async (body) => {
        vi.stubGlobal('fetch', vi.fn().mockResolvedValue(new Response(JSON.stringify(body))));
        await expect(import('../register').then((module) => module.default)).rejects.toThrow(/backend.*match/i);
        expect(registerAuthWorkspaceStoreMock).not.toHaveBeenCalled();
    });

    it('does not contact Convex in the SQLite profile', async () => {
        useRuntimeConfigMock.mockReturnValue({ auth: { enabled: true }, sync: { enabled: true, provider: 'sqlite' }, storage: { enabled: true, provider: 'fs' } });
        await (await import('../register')).default;
        expect(fetch).not.toHaveBeenCalled();
    });
});
