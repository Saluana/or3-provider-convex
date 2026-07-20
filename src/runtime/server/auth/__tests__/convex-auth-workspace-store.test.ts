import { beforeEach, describe, expect, it, vi } from 'vitest';

const runtimeConfig = vi.hoisted(() => ({
    auth: {
        provider: 'clerk',
    },
    sync: {
        convexUrl: 'https://convex.example',
        convexAdminKey: 'admin-key',
    },
}));

vi.mock('#imports', () => ({
    useRuntimeConfig: () => runtimeConfig,
}));

vi.mock('convex/server', () => ({
    anyApi: {
        workspaces: {
            resolveSession: 'workspaces.resolveSession',
            listInvitesInternal: 'workspaces.listInvitesInternal',
            ensure: 'workspaces.ensure',
            listMyWorkspaces: 'workspaces.listMyWorkspaces',
            create: 'workspaces.create',
            update: 'workspaces.update',
            remove: 'workspaces.remove',
            setActive: 'workspaces.setActive',
            createInvite: 'workspaces.createInvite',
            consumeInvite: 'workspaces.consumeInvite',
            revokeInvite: 'workspaces.revokeInvite',
            validateInviteInternal: 'workspaces.validateInviteInternal',
            acceptInviteAndProvisionUser: 'workspaces.acceptInviteAndProvisionUser',
        },
        users: {
            getAuthAccountByProvider: 'users.getAuthAccountByProvider',
            getAuthAccountByUserId: 'users.getAuthAccountByUserId',
        },
    },
}));

const queryMock = vi.hoisted(() => vi.fn());
const mutationMock = vi.hoisted(() => vi.fn());
const createdClients = vi.hoisted(() => [] as Array<{ setAdminAuth: ReturnType<typeof vi.fn> }>);

vi.mock('convex/browser', () => ({
    ConvexHttpClient: class {
        url: string;
        setAdminAuth = vi.fn();
        constructor(url: string) {
            this.url = url;
            createdClients.push(this);
        }
        query(...args: unknown[]) {
            return queryMock(...args);
        }
        mutation(...args: unknown[]) {
            return mutationMock(...args);
        }
    },
}));

async function loadStore() {
    return import('../../auth/convex-auth-workspace-store');
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

describe('ConvexAuthWorkspaceStore', () => {
    beforeEach(() => {
        vi.resetModules();
        (globalThis as Record<string, unknown>).__or3_convex_legacy_clerk_only_backend__ = undefined;
        runtimeConfig.sync.convexUrl = 'https://convex.example';
        runtimeConfig.sync.convexAdminKey = 'admin-key';
        runtimeConfig.auth.provider = 'clerk';
        queryMock.mockReset();
        mutationMock.mockReset();
        createdClients.length = 0;
    });

    it('throws clear errors for missing convexUrl or convexAdminKey', async () => {
        const { ConvexAuthWorkspaceStore } = await loadStore();
        const store = new ConvexAuthWorkspaceStore();

        runtimeConfig.sync.convexUrl = '' as any;
        await expect(
            store.getOrCreateUser({ provider: 'clerk', providerUserId: 'u1' })
        ).rejects.toThrow('Convex URL not configured');

        runtimeConfig.sync.convexUrl = 'https://convex.example';
        runtimeConfig.sync.convexAdminKey = '' as any;
        await expect(
            store.getOrCreateUser({ provider: 'clerk', providerUserId: 'u1' })
        ).rejects.toThrow('Convex admin key not configured');
    });

    it('sets admin auth subject/tokenIdentifier from providerUserId', async () => {
        const { ConvexAuthWorkspaceStore } = await loadStore();
        const store = new ConvexAuthWorkspaceStore();

        queryMock.mockResolvedValue({ id: 'ws-1', user_id: 'internal-user-123' });
        await store.getOrCreateUser({ provider: 'clerk', providerUserId: 'user_123' });

        const client = createdClients[0] as { setAdminAuth: ReturnType<typeof vi.fn> };
        expect(client.setAdminAuth).toHaveBeenCalledWith('admin-key', {
            subject: 'user_123',
            issuer: 'https://clerk.or3.ai',
            tokenIdentifier: 'https://clerk.or3.ai|user_123',
            or3_server: true,
        });
    });

    it('returns the same internal user id on first provisioning and a later request', async () => {
        const { ConvexAuthWorkspaceStore } = await loadStore();
        const firstRequestStore = new ConvexAuthWorkspaceStore();

        queryMock.mockResolvedValueOnce(null);
        mutationMock.mockResolvedValueOnce({
            id: 'ws-created',
            user_id: 'internal-user-1',
        });
        await expect(
            firstRequestStore.getOrCreateUser({
                provider: 'clerk',
                providerUserId: 'provider-user-1',
                email: 'a@test.com',
                displayName: 'A',
            })
        ).resolves.toEqual({ userId: 'internal-user-1' });

        const laterRequestStore = new ConvexAuthWorkspaceStore();
        queryMock.mockResolvedValueOnce({
            user_id: 'internal-user-1',
            provider: 'clerk',
            provider_user_id: 'provider-user-1',
        });
        await expect(
            laterRequestStore.getUser({
                provider: 'clerk',
                providerUserId: 'provider-user-1',
            })
        ).resolves.toEqual({ userId: 'internal-user-1' });

        expect(mutationMock).toHaveBeenCalledWith('workspaces.ensure', {
            provider: 'clerk',
            provider_user_id: 'provider-user-1',
            email: 'a@test.com',
            name: 'A',
        });
    });

    it('validates invite state through the provider-neutral store contract', async () => {
        const { ConvexAuthWorkspaceStore } = await loadStore();
        const store = new ConvexAuthWorkspaceStore();
        queryMock.mockResolvedValueOnce({ ok: false, reason: 'already_used' });

        await expect(store.validateInvite({
            workspaceId: 'ws-1',
            email: 'invitee@example.com',
            tokenHash: 'token-hash',
        })).resolves.toEqual({ ok: false, reason: 'already_used' });
        expect(queryMock).toHaveBeenCalledWith('workspaces.validateInviteInternal', {
            workspace_id: 'ws-1',
            email: 'invitee@example.com',
            token_hash: 'token-hash',
        });
    });

    it('delegates atomic invite provisioning to one Convex mutation', async () => {
        const { ConvexAuthWorkspaceStore } = await loadStore();
        const store = new ConvexAuthWorkspaceStore();
        mutationMock.mockResolvedValueOnce({
            ok: true,
            user_id: 'internal-user-1',
            role: 'editor',
            createdUser: true,
        });

        await expect(store.acceptInviteAndProvisionUser({
            provider: 'basic-auth',
            providerUserId: 'provider-user-1',
            email: 'invitee@example.com',
            displayName: 'Invitee',
            workspaceId: 'ws-1',
            tokenHash: 'token-hash',
        })).resolves.toEqual({
            ok: true,
            userId: 'internal-user-1',
            role: 'editor',
            createdUser: true,
        });
        expect(mutationMock).toHaveBeenCalledTimes(1);
        expect(mutationMock).toHaveBeenCalledWith(
            'workspaces.acceptInviteAndProvisionUser',
            {
                provider: 'basic-auth',
                provider_user_id: 'provider-user-1',
                email: 'invitee@example.com',
                name: 'Invitee',
                workspace_id: 'ws-1',
                token_hash: 'token-hash',
            }
        );
        const client = createdClients[0] as { setAdminAuth: ReturnType<typeof vi.fn> };
        expect(client.setAdminAuth).toHaveBeenCalledWith('admin-key', {
            subject: 'provider-user-1',
            issuer: 'https://or3.ai/auth/basic-auth',
            tokenIdentifier: 'https://or3.ai/auth/basic-auth|provider-user-1',
            or3_server: true,
            email: 'invitee@example.com',
        });
    });

    it('returns the canonical internal id for an existing user without provisioning', async () => {
        const { ConvexAuthWorkspaceStore } = await loadStore();
        const store = new ConvexAuthWorkspaceStore();

        queryMock.mockResolvedValueOnce({
            id: 'workspace-1',
            user_id: 'internal-user-existing-1',
        });

        await expect(
            store.getOrCreateUser({
                provider: 'clerk',
                providerUserId: 'provider-user-existing-1',
            })
        ).resolves.toEqual({ userId: 'internal-user-existing-1' });
        expect(mutationMock).not.toHaveBeenCalled();
    });

    it('never treats a Basic Auth UUID provider subject as a Convex user id', async () => {
        const { ConvexAuthWorkspaceStore } = await loadStore();
        const store = new ConvexAuthWorkspaceStore();
        const providerUserId = '123e4567-e89b-12d3-a456-426614174000';
        runtimeConfig.auth.provider = 'basic-auth';

        queryMock
            .mockResolvedValueOnce(null)
            .mockResolvedValueOnce({
                id: 'workspace-1',
                name: 'Personal Workspace',
                user_id: 'internal-user-basic-1',
            });
        mutationMock
            .mockResolvedValueOnce({
                id: 'workspace-1',
                name: 'Personal Workspace',
                user_id: 'internal-user-basic-1',
            })
            .mockResolvedValueOnce({
                id: 'workspace-1',
                name: 'Personal Workspace',
                user_id: 'internal-user-basic-1',
            });

        const provisioned = await store.getOrCreateUser({
            provider: 'basic-auth',
            providerUserId,
        });
        expect(provisioned).toEqual({ userId: 'internal-user-basic-1' });
        await expect(
            store.getOrCreateDefaultWorkspace(provisioned.userId)
        ).resolves.toMatchObject({ workspaceId: 'workspace-1' });

        expect(queryMock).not.toHaveBeenCalledWith(
            'users.getAuthAccountByUserId',
            expect.objectContaining({ user_id: providerUserId })
        );
        expect(createdClients.at(-1)?.setAdminAuth).toHaveBeenCalledWith(
            'admin-key',
            expect.objectContaining({
                subject: providerUserId,
                issuer: 'https://or3.ai/auth/basic-auth',
            })
        );
    });

    it('falls back to clerk provider when backend only supports clerk', async () => {
        const { ConvexAuthWorkspaceStore } = await loadStore();
        const store = new ConvexAuthWorkspaceStore();

        queryMock.mockRejectedValueOnce(
            new Error("Invalid provider: basic-auth. Only 'clerk' is supported.")
        );
        queryMock.mockResolvedValueOnce(null);
        mutationMock.mockResolvedValueOnce({ id: 'ws-created', user_id: 'internal-legacy' });

        await expect(
            store.getOrCreateUser({
                provider: 'basic-auth',
                providerUserId: 'u-legacy',
                email: 'legacy@example.com',
            })
        ).resolves.toEqual({ userId: 'internal-legacy' });

        expect(mutationMock).toHaveBeenCalledWith('workspaces.ensure', {
            provider: 'clerk',
            provider_user_id: 'u-legacy',
            email: 'legacy@example.com',
            name: undefined,
        });
    });

    it('getOrCreateDefaultWorkspace maps workspace id/name', async () => {
        const { ConvexAuthWorkspaceStore } = await loadStore();
        const store = new ConvexAuthWorkspaceStore();

        queryMock
            .mockResolvedValueOnce({
                user_id: 'user-1',
                provider: 'clerk',
                provider_user_id: 'provider-user-1',
            })
            .mockResolvedValueOnce(null);
        mutationMock.mockResolvedValue({ id: 'ws-1', name: 'Default Workspace' });

        await expect(store.getOrCreateDefaultWorkspace('user-1')).resolves.toEqual({
            workspaceId: 'ws-1',
            workspaceName: 'Default Workspace',
            created: true,
        });
    });

    it('resolves internal user ids to provider user ids for workspace operations', async () => {
        const { ConvexAuthWorkspaceStore } = await loadStore();
        const store = new ConvexAuthWorkspaceStore();

        queryMock
            .mockResolvedValueOnce({
                user_id: 'internal_user_1',
                provider: 'clerk',
                provider_user_id: 'user_clerk_1',
            })
            .mockResolvedValueOnce(null);
        mutationMock.mockResolvedValueOnce({ id: 'ws-1', name: 'Default Workspace' });

        await expect(store.getOrCreateDefaultWorkspace('internal_user_1')).resolves.toEqual({
            workspaceId: 'ws-1',
            workspaceName: 'Default Workspace',
            created: true,
        });
        expect(queryMock).toHaveBeenNthCalledWith(1, 'users.getAuthAccountByUserId', {
            provider: 'clerk',
            user_id: 'internal_user_1',
        });

        const client = createdClients[0] as { setAdminAuth: ReturnType<typeof vi.fn> };
        expect(client.setAdminAuth).toHaveBeenCalledWith('admin-key', {
            subject: 'or3-server',
            issuer: 'https://clerk.or3.ai',
            tokenIdentifier: 'https://clerk.or3.ai|or3-server',
            or3_server: true,
        });
        const mappedClient = createdClients[1] as { setAdminAuth: ReturnType<typeof vi.fn> };
        expect(mappedClient.setAdminAuth).toHaveBeenCalledWith('admin-key', {
            subject: 'user_clerk_1',
            issuer: 'https://clerk.or3.ai',
            tokenIdentifier: 'https://clerk.or3.ai|user_clerk_1',
            or3_server: true,
        });
    });

    it('getWorkspaceRole returns null for workspace mismatch', async () => {
        const { ConvexAuthWorkspaceStore } = await loadStore();
        const store = new ConvexAuthWorkspaceStore();

        queryMock
            .mockResolvedValueOnce({
                user_id: 'u1',
                provider: 'clerk',
                provider_user_id: 'provider-u1',
            })
            .mockResolvedValueOnce({ id: 'ws-1', role: 'editor' });
        await expect(
            store.getWorkspaceRole({ userId: 'u1', workspaceId: 'ws-2' })
        ).resolves.toBeNull();
    });

    it('normalizes listUserWorkspaces response fields', async () => {
        const { ConvexAuthWorkspaceStore } = await loadStore();
        const store = new ConvexAuthWorkspaceStore();

        queryMock
            .mockResolvedValueOnce({
                user_id: 'u1',
                provider: 'clerk',
                provider_user_id: 'provider-u1',
            })
            .mockResolvedValueOnce([
                null,
                {
                    _id: 'ws-1',
                    name: 'Workspace One',
                    description: undefined,
                    role: 'owner',
                    created_at: 100,
                    is_active: 1,
                },
            ]);

        await expect(store.listUserWorkspaces('u1')).resolves.toEqual([
            {
                id: 'ws-1',
                name: 'Workspace One',
                description: null,
                role: 'owner',
                createdAt: 100,
                isActive: true,
            },
        ]);
    });

    it('maps create/update/remove/setActive mutation payloads', async () => {
        const { ConvexAuthWorkspaceStore } = await loadStore();
        const store = new ConvexAuthWorkspaceStore();

        queryMock.mockResolvedValueOnce({
            user_id: 'u1',
            provider: 'clerk',
            provider_user_id: 'provider-u1',
        });
        mutationMock.mockResolvedValueOnce('ws-new');
        await expect(
            store.createWorkspace({ userId: 'u1', name: 'New', description: 'Desc' })
        ).resolves.toEqual({ workspaceId: 'ws-new' });

        await store.updateWorkspace({ userId: 'u1', workspaceId: 'ws-1', name: 'Renamed', description: null });
        await store.removeWorkspace({ userId: 'u1', workspaceId: 'ws-1' });
        await store.setActiveWorkspace({ userId: 'u1', workspaceId: 'ws-2' });

        expect(mutationMock).toHaveBeenCalledWith('workspaces.update', {
            workspace_id: 'ws-1',
            name: 'Renamed',
            description: undefined,
        });
        expect(mutationMock).toHaveBeenCalledWith('workspaces.remove', { workspace_id: 'ws-1' });
        expect(mutationMock).toHaveBeenCalledWith('workspaces.setActive', { workspace_id: 'ws-2' });
    });

    it('derives invite actor ids in Convex instead of sending authoritative ids', async () => {
        const { ConvexAuthWorkspaceStore } = await loadStore();
        const store = new ConvexAuthWorkspaceStore();

        queryMock
            .mockResolvedValueOnce({
                user_id: 'user_owner',
                provider: 'clerk',
                provider_user_id: 'provider-owner',
            })
            .mockResolvedValueOnce({
                user_id: 'user_invitee',
                provider: 'clerk',
                provider_user_id: 'provider-invitee',
            });
        mutationMock
            .mockResolvedValueOnce({ invite_id: 'invite-1' })
            .mockResolvedValueOnce({ ok: true, role: 'viewer' });

        await expect(store.createInvite({
            workspaceId: 'ws-1',
            email: 'invitee@example.test',
            role: 'viewer',
            invitedByUserId: 'user_owner',
            expiresAt: 123,
            tokenHash: 'hash',
        })).resolves.toEqual({ inviteId: 'invite-1' });
        await expect(store.consumeInvite({
            workspaceId: 'ws-1',
            email: 'invitee@example.test',
            tokenHash: 'hash',
            acceptedUserId: 'user_invitee',
        })).resolves.toEqual({ ok: true, role: 'viewer' });

        expect(mutationMock).toHaveBeenNthCalledWith(1, 'workspaces.createInvite', {
            workspace_id: 'ws-1',
            email: 'invitee@example.test',
            role: 'viewer',
            token_hash: 'hash',
            expires_at: 123,
        });
        expect(mutationMock).toHaveBeenNthCalledWith(2, 'workspaces.consumeInvite', {
            workspace_id: 'ws-1',
            email: 'invitee@example.test',
            token_hash: 'hash',
        });

        const consumeClient = createdClients.at(-1) as {
            setAdminAuth: ReturnType<typeof vi.fn>;
        };
        expect(consumeClient.setAdminAuth).toHaveBeenCalledWith(
            'admin-key',
            expect.objectContaining({
                subject: 'provider-invitee',
                email: 'invitee@example.test',
                or3_server: true,
            })
        );
    });

    it('propagates backend errors without swallowing', async () => {
        const { ConvexAuthWorkspaceStore } = await loadStore();
        const store = new ConvexAuthWorkspaceStore();

        queryMock.mockResolvedValueOnce({
            user_id: 'u1',
            provider: 'clerk',
            provider_user_id: 'provider-u1',
        });
        mutationMock.mockRejectedValue(new Error('backend exploded'));

        await expect(
            store.createWorkspace({ userId: 'u1', name: 'Name' })
        ).rejects.toThrow('backend exploded');
    });

    it('retries transient transport errors during workspace reads', async () => {
        const { ConvexAuthWorkspaceStore } = await loadStore();
        const store = new ConvexAuthWorkspaceStore();

        queryMock
            .mockRejectedValueOnce(createTransientTransportError())
            .mockResolvedValueOnce({
                user_id: 'user_test_1',
                provider: 'clerk',
                provider_user_id: 'user_test_1',
            })
            .mockResolvedValueOnce({ id: 'ws-1', role: 'owner' });

        await expect(
            store.getWorkspaceRole({ userId: 'user_test_1', workspaceId: 'ws-1' })
        ).resolves.toBe('owner');
        expect(queryMock).toHaveBeenCalledTimes(3);
    });

    it('maps exhausted transient transport errors to 503', async () => {
        const { ConvexAuthWorkspaceStore } = await loadStore();
        const store = new ConvexAuthWorkspaceStore();

        queryMock.mockRejectedValue(createTransientTransportError());

        await expect(
            store.getWorkspaceRole({ userId: 'user_test_1', workspaceId: 'ws-1' })
        ).rejects.toMatchObject({
            statusCode: 503,
            statusMessage: 'Auth backend unavailable',
        });
        expect(queryMock).toHaveBeenCalledTimes(3);
    });
});
