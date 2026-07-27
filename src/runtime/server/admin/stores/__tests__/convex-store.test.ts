import { describe, it, expect, vi, beforeEach } from 'vitest';
import { verifyAdminStoreProviderContract } from '~~/shared/testing/contracts/admin';

const getConvexGatewayClient = vi.fn();
const getConvexAdminGatewayClient = vi.fn();
const useRuntimeConfig = vi.fn();
const resolveProviderToken = vi.fn<(...args: unknown[]) => Promise<string | null>>();
const listProviderTokenBrokerIds = vi.fn<() => string[]>();

vi.mock('../../../utils/convex-gateway', () => ({
    getConvexGatewayClient: (...args: any[]) => getConvexGatewayClient(...args),
    getConvexAdminGatewayClient: (...args: any[]) => getConvexAdminGatewayClient(...args),
}));

vi.mock('~~/server/auth/token-broker/resolve', () => ({
    resolveProviderToken: (...args: unknown[]) => resolveProviderToken(...args),
}));

vi.mock('~~/server/auth/token-broker/registry', () => ({
    listProviderTokenBrokerIds: () => listProviderTokenBrokerIds(),
}));

vi.mock('#imports', () => ({
    useRuntimeConfig: (...args: unknown[]) => useRuntimeConfig(...args),
}));

vi.mock('convex/server', () => ({
    anyApi: {
        admin: {
            createWorkspace: 'admin.createWorkspace',
            getWorkspace: 'admin.getWorkspace',
            getWorkspaceSetting: 'admin.getWorkspaceSetting',
            grantAdmin: 'admin.grantAdmin',
            isAdmin: 'admin.isAdmin',
            listAdmins: 'admin.listAdmins',
            listWorkspaceMembers: 'admin.listWorkspaceMembers',
            listWorkspaces: 'admin.listWorkspaces',
            removeWorkspaceMember: 'admin.removeWorkspaceMember',
            restoreWorkspace: 'admin.restoreWorkspace',
            revokeAdmin: 'admin.revokeAdmin',
            searchUsers: 'admin.searchUsers',
            setWorkspaceMemberRole: 'admin.setWorkspaceMemberRole',
            setWorkspaceSetting: 'admin.setWorkspaceSetting',
            softDeleteWorkspace: 'admin.softDeleteWorkspace',
            upsertWorkspaceMember: 'admin.upsertWorkspaceMember',
        },
    },
}));

vi.mock('~~/shared/cloud/provider-ids', () => ({
    CONVEX_JWT_TEMPLATE: 'convex',
    CONVEX_PROVIDER_ID: 'convex',
}));

vi.mock('~~/shared/cloud/admin-identity', () => ({
    ADMIN_IDENTITY_ISSUER: 'https://admin.or3.ai',
}));

describe('createConvexWorkspaceAccessStore', () => {
    beforeEach(() => {
        vi.clearAllMocks();
        getConvexGatewayClient.mockReset();
        getConvexAdminGatewayClient.mockReset();
        useRuntimeConfig.mockReset();
        resolveProviderToken.mockReset();
        listProviderTokenBrokerIds.mockReset();
    });

    it('implements the complete shared admin provider contract', async () => {
        const {
            createConvexAdminUserStore,
            createConvexWorkspaceAccessStore,
            createConvexWorkspaceSettingsStore,
        } = await import('../convex-store');
        const event = { context: {} } as any;

        expect(() => verifyAdminStoreProviderContract({
            name: 'convex',
            workspaceAccess: createConvexWorkspaceAccessStore(event),
            workspaceSettings: createConvexWorkspaceSettingsStore(event),
            adminUsers: createConvexAdminUserStore(event),
            capabilities: {
                supportsServerSideAdmin: true,
                supportsUserSearch: true,
                supportsWorkspaceList: true,
                supportsWorkspaceManagement: true,
                supportsDeploymentAdminGrants: true,
            },
        })).not.toThrow();
    });

    it('uses admin key for super_admin without Clerk', async () => {
        useRuntimeConfig.mockReturnValue({
            auth: { provider: 'clerk' },
            sync: { convexAdminKey: 'admin-key', convexUrl: 'https://example.convex.cloud' },
        });
        getConvexAdminGatewayClient.mockReturnValue({
            query: vi.fn().mockResolvedValue([]),
        });
        resolveProviderToken.mockResolvedValue('clerk-token');
        listProviderTokenBrokerIds.mockReturnValue(['clerk']);

        const { createConvexWorkspaceAccessStore } = await import('../convex-store');
        const event = {
            context: {
                admin: { principal: { kind: 'super_admin', username: 'root' } },
            },
        } as any;

        const store = createConvexWorkspaceAccessStore(event);
        await store.listMembers({ workspaceId: 'workspaces:123' });

        expect(getConvexAdminGatewayClient).toHaveBeenCalledTimes(1);
        expect(resolveProviderToken).not.toHaveBeenCalled();
    });
});
