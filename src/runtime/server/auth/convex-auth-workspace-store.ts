/**
 * @module server/auth/store/impls/convex-auth-workspace-store.ts
 *
 * Purpose:
 * Convex implementation of AuthWorkspaceStore.
 *
 * DO NOT import this file directly in core. Use getAuthWorkspaceStore('convex')
 * after the Convex provider package registers the store.
 */
import type {
    AuthWorkspaceStore,
    InviteProvisionResult,
    InviteValidationResult,
} from '~~/server/auth/store/types';
import type { WorkspaceRole } from '~~/app/core/hooks/hook-types';
import type { GenericId as Id } from 'convex/values';
import { ConvexHttpClient } from 'convex/browser';
import {
    convexApi as api,
    convexInternalApi as internalApi,
} from '../../utils/convex-api';
import { useRuntimeConfig } from '#imports';
import {
    isLegacyClerkOnlyError,
    markLegacyClerkOnlyBackend,
    resolveConvexAuthProvider,
} from '../utils/provider-compat';
import {
    throwAsConvexServiceUnavailable,
    withConvexTransportRetry,
} from '../utils/convex-transport';

/**
 * Get an admin-authenticated Convex client for server-to-server calls.
 * Uses the admin key to bypass JWT validation since these are trusted server operations.
 */
function getConfiguredAuthProvider(): string {
    const config = useRuntimeConfig();
    return resolveConvexAuthProvider(config.auth?.provider || 'clerk');
}

function buildProviderIssuer(provider: string): string {
    if (provider === 'clerk') {
        return 'https://clerk.or3.ai';
    }
    return `https://or3.ai/auth/${provider}`;
}

export function getAdminConvexClient(
    provider: string,
    providerUserId: string,
    identity?: { email?: string }
): ConvexHttpClient {
    const config = useRuntimeConfig();
    const { convexUrl: url, convexAdminKey: adminKey } = config.sync;

    if (!url) {
        throw new Error('Convex URL not configured');
    }
    if (!adminKey) {
        throw new Error('Convex admin key not configured - required for server-side auth operations');
    }

    const client = new ConvexHttpClient(url);
    const issuer = buildProviderIssuer(provider);

    // Use admin auth to authenticate as the provider user
    // This allows the Convex mutations to verify identity.subject matches the request
    client.setAdminAuth(adminKey, {
        subject: providerUserId,
        issuer,
        tokenIdentifier: `${issuer}|${providerUserId}`,
        or3_server: true,
        ...(identity?.email ? { email: identity.email } : {}),
    });

    return client;
}

/**
 * Convex-backed AuthWorkspaceStore implementation.
 *
 * Implementation:
 * - Uses Convex HTTP client with admin auth for server-side queries/mutations
 * - Calls workspaces.resolveSession and workspaces.ensure
 * - Maps Convex workspace data to AuthWorkspaceStore interface
 */
export class ConvexAuthWorkspaceStore implements AuthWorkspaceStore {
    private readonly providerIdentityByInternalUserId = new Map<
        string,
        { provider: string; providerUserId: string }
    >();

    private rememberProviderIdentity(input: {
        userId: string;
        provider: string;
        providerUserId: string;
    }): void {
        this.providerIdentityByInternalUserId.set(input.userId, {
            provider: input.provider,
            providerUserId: input.providerUserId,
        });
    }

    private async resolveProviderIdentityForInternalUser(input: {
        provider: string;
        userId: string;
    }): Promise<{ provider: string; providerUserId: string }> {
        const cached = this.providerIdentityByInternalUserId.get(input.userId);
        if (cached) return cached;

        // Resolve a declared internal ID under a neutral trusted-server identity.
        // The internal query normalizes this string before any document lookup, so
        // provider subjects (including Basic Auth UUIDs) are never cast to Convex IDs.
        const convex = getAdminConvexClient(input.provider, 'or3-server');
        const account = await this.runConvexOperation('users.getAuthAccountByUserId', () =>
            convex.query(internalApi.users.getAuthAccountByUserId, {
                provider: input.provider,
                user_id: input.userId,
            })
        );

        if (
            account?.user_id &&
            String(account.user_id) === input.userId &&
            account.provider &&
            account.provider_user_id
        ) {
            const identity = {
                provider: String(account.provider),
                providerUserId: String(account.provider_user_id),
            };
            this.rememberProviderIdentity({
                userId: String(account.user_id),
                ...identity,
            });
            return identity;
        }

        throw new Error(`Unable to resolve provider identity for internal user "${input.userId}"`);
    }

    private async runConvexOperation<T>(
        operation: string,
        run: () => Promise<T>
    ): Promise<T> {
        try {
            return await withConvexTransportRetry(operation, run);
        } catch (error) {
            throwAsConvexServiceUnavailable(error, 'Auth backend unavailable');
        }
    }

    private async ensureUserWithProvider(input: {
        provider: string;
        providerUserId: string;
        email?: string;
        displayName?: string;
    }): Promise<string> {
        const convex = getAdminConvexClient(input.provider, input.providerUserId);

        const resolved = await this.runConvexOperation('workspaces.resolveSession', () =>
            convex.query(internalApi.workspaces.resolveSession, {
                provider: input.provider,
                provider_user_id: input.providerUserId,
            })
        );
        if (resolved?.user_id) return String(resolved.user_id);

        if (resolved) {
            const existingAccount = await this.runConvexOperation(
                'users.getAuthAccountByProvider',
                () =>
                    convex.query(internalApi.users.getAuthAccountByProvider, {
                        provider: input.provider,
                        provider_user_id: input.providerUserId,
                    })
            );
            if (existingAccount?.user_id) return String(existingAccount.user_id);
        }

        const ensured = await this.runConvexOperation('workspaces.ensure', () =>
            convex.mutation(api.workspaces.ensure, {
                provider: input.provider,
                provider_user_id: input.providerUserId,
                email: input.email,
                name: input.displayName,
            })
        );
        if (ensured?.user_id) return String(ensured.user_id);

        // Compatibility with older deployed templates whose ensure result did
        // not yet include user_id.
        const createdAccount = await this.runConvexOperation(
            'users.getAuthAccountByProvider',
            () =>
                convex.query(internalApi.users.getAuthAccountByProvider, {
                    provider: input.provider,
                    provider_user_id: input.providerUserId,
                })
        );
        if (createdAccount?.user_id) return String(createdAccount.user_id);

        throw new Error('Convex user provisioning completed without an internal user ID');
    }

    async getOrCreateUser(input: {
        provider: string;
        providerUserId: string;
        email?: string;
        displayName?: string;
    }): Promise<{ userId: string }> {
        const configuredProvider = resolveConvexAuthProvider(input.provider);
        let resolvedProvider = configuredProvider;
        let userId: string;

        try {
            userId = await this.ensureUserWithProvider({
                ...input,
                provider: configuredProvider,
            });
        } catch (error) {
            if (configuredProvider !== 'clerk' && isLegacyClerkOnlyError(error)) {
                markLegacyClerkOnlyBackend();
                resolvedProvider = 'clerk';
                userId = await this.ensureUserWithProvider({
                    ...input,
                    provider: resolvedProvider,
                });
            } else {
                throw error;
            }
        }

        this.rememberProviderIdentity({
            userId,
            provider: resolvedProvider,
            providerUserId: input.providerUserId,
        });
        return { userId };
    }

    async getUser(input: {
        provider: string;
        providerUserId: string;
    }): Promise<
        | {
              userId: string;
              email?: string;
              displayName?: string;
          }
        | null
    > {
        const provider = resolveConvexAuthProvider(input.provider);
        const convex = getAdminConvexClient(provider, input.providerUserId);
        const account = await this.runConvexOperation('users.getAuthAccountByProvider', () =>
            convex.query(internalApi.users.getAuthAccountByProvider, {
                provider,
                provider_user_id: input.providerUserId,
            })
        );

        if (!account?.user_id) return null;
        const userId = String(account.user_id);
        this.rememberProviderIdentity({
            userId,
            provider: String(account.provider ?? provider),
            providerUserId: String(account.provider_user_id ?? input.providerUserId),
        });
        return { userId };
    }

    async getOrCreateDefaultWorkspace(
        userId: string
    ): Promise<{ workspaceId: string; workspaceName: string; created?: boolean }> {
        const configuredProvider = getConfiguredAuthProvider();
        const hadProviderIdentity = this.providerIdentityByInternalUserId.has(userId);
        const { provider, providerUserId } = await this.resolveProviderIdentityForInternalUser({
            provider: configuredProvider,
            userId,
        });
        const convex = getAdminConvexClient(provider, providerUserId);

        // Get or create workspace via ensure
        const existingWorkspace = await this.runConvexOperation('workspaces.resolveSession', () =>
            convex.query(internalApi.workspaces.resolveSession, {
                provider,
                provider_user_id: providerUserId,
            })
        );
        const workspaceInfo = await this.runConvexOperation('workspaces.ensure', () =>
            convex.mutation(api.workspaces.ensure, {
                provider,
                provider_user_id: providerUserId,
                email: undefined,
                name: undefined,
            })
        );

        return {
            workspaceId: workspaceInfo.id,
            workspaceName: workspaceInfo.name,
            created: !existingWorkspace && !hadProviderIdentity,
        };
    }

    async getWorkspaceRole(input: {
        userId: string;
        workspaceId: string;
    }): Promise<WorkspaceRole | null> {
        const configuredProvider = getConfiguredAuthProvider();
        const { provider, providerUserId } = await this.resolveProviderIdentityForInternalUser({
            provider: configuredProvider,
            userId: input.userId,
        });
        const convex = getAdminConvexClient(provider, providerUserId);

        const resolved = await this.runConvexOperation('workspaces.resolveSession', () =>
            convex.query(internalApi.workspaces.resolveSession, {
                provider,
                provider_user_id: providerUserId,
            })
        );

        if (!resolved || resolved.id !== input.workspaceId) {
            return null;
        }

        return resolved.role as WorkspaceRole;
    }

    async listUserWorkspaces(
        userId: string
    ): Promise<
        Array<{
            id: string;
            name: string;
            description?: string | null;
            role: WorkspaceRole;
            createdAt?: number;
            isActive?: boolean;
        }>
    > {
        const configuredProvider = getConfiguredAuthProvider();
        const { provider, providerUserId } = await this.resolveProviderIdentityForInternalUser({
            provider: configuredProvider,
            userId,
        });
        const convex = getAdminConvexClient(provider, providerUserId);

        const workspaces = await this.runConvexOperation('workspaces.listMyWorkspaces', () =>
            convex.query(api.workspaces.listMyWorkspaces, {})
        );
        const normalized = Array.isArray(workspaces)
            ? workspaces.filter(
                  (workspace): workspace is NonNullable<typeof workspace> =>
                      Boolean(workspace)
              )
            : [];

        return normalized.map((workspace) => ({
            id: workspace._id,
            name: workspace.name,
            description: workspace.description ?? null,
            role: workspace.role,
            createdAt: workspace.created_at,
            isActive: Boolean(workspace.is_active),
        }));
    }

    async createWorkspace(input: {
        userId: string;
        name: string;
        description?: string | null;
    }): Promise<{ workspaceId: string }> {
        const configuredProvider = getConfiguredAuthProvider();
        const { provider, providerUserId } = await this.resolveProviderIdentityForInternalUser({
            provider: configuredProvider,
            userId: input.userId,
        });
        const convex = getAdminConvexClient(provider, providerUserId);

        const workspaceId = await this.runConvexOperation('workspaces.create', () =>
            convex.mutation(api.workspaces.create, {
                name: input.name,
                description: input.description ?? undefined,
            })
        );

        return { workspaceId };
    }

    async updateWorkspace(input: {
        userId: string;
        workspaceId: string;
        name: string;
        description?: string | null;
    }): Promise<void> {
        const configuredProvider = getConfiguredAuthProvider();
        const { provider, providerUserId } = await this.resolveProviderIdentityForInternalUser({
            provider: configuredProvider,
            userId: input.userId,
        });
        const convex = getAdminConvexClient(provider, providerUserId);

        await this.runConvexOperation('workspaces.update', () =>
            convex.mutation(api.workspaces.update, {
                workspace_id: input.workspaceId as Id<'workspaces'>,
                name: input.name,
                description: input.description ?? undefined,
            })
        );
    }

    async removeWorkspace(input: { userId: string; workspaceId: string }): Promise<void> {
        const configuredProvider = getConfiguredAuthProvider();
        const { provider, providerUserId } = await this.resolveProviderIdentityForInternalUser({
            provider: configuredProvider,
            userId: input.userId,
        });
        const convex = getAdminConvexClient(provider, providerUserId);

        await this.runConvexOperation('workspaces.remove', () =>
            convex.mutation(api.workspaces.remove, {
                workspace_id: input.workspaceId as Id<'workspaces'>,
            })
        );
    }

    async setActiveWorkspace(input: {
        userId: string;
        workspaceId: string;
    }): Promise<void> {
        const configuredProvider = getConfiguredAuthProvider();
        const { provider, providerUserId } = await this.resolveProviderIdentityForInternalUser({
            provider: configuredProvider,
            userId: input.userId,
        });
        const convex = getAdminConvexClient(provider, providerUserId);

        await this.runConvexOperation('workspaces.setActive', () =>
            convex.mutation(api.workspaces.setActive, {
                workspace_id: input.workspaceId as Id<'workspaces'>,
            })
        );
    }

    async createInvite(input: {
        workspaceId: string;
        email: string;
        role: WorkspaceRole;
        invitedByUserId: string;
        expiresAt: number;
        tokenHash: string;
    }): Promise<{ inviteId: string }> {
        const configuredProvider = getConfiguredAuthProvider();
        const { provider, providerUserId } = await this.resolveProviderIdentityForInternalUser({
            provider: configuredProvider,
            userId: input.invitedByUserId,
        });
        const convex = getAdminConvexClient(provider, providerUserId);
        const result = await this.runConvexOperation('workspaces.createInvite', () =>
            convex.mutation(api.workspaces.createInvite, {
                workspace_id: input.workspaceId as Id<'workspaces'>,
                email: input.email,
                role: input.role,
                token_hash: input.tokenHash,
                expires_at: input.expiresAt,
            })
        );
        return { inviteId: String(result.invite_id) };
    }

    async listInvites(input: {
        workspaceId: string;
        status?: 'pending' | 'accepted' | 'revoked' | 'expired';
        limit?: number;
    }) {
        const provider = getConfiguredAuthProvider();
        const convex = getAdminConvexClient(provider, input.workspaceId);
        const rows = await this.runConvexOperation('workspaces.listInvitesInternal', () =>
            convex.query(internalApi.workspaces.listInvitesInternal, {
                workspace_id: input.workspaceId as Id<'workspaces'>,
                status: input.status,
                limit: input.limit,
            })
        );

        return (rows ?? []).map((row: any) => ({
            id: String(row.id),
            workspaceId: String(row.workspace_id),
            email: row.email,
            role: row.role,
            status: row.status,
            invitedByUserId: String(row.invited_by_user_id),
            tokenHash: row.token_hash,
            expiresAt: row.expires_at,
            acceptedAt: row.accepted_at ?? null,
            acceptedUserId: row.accepted_user_id ? String(row.accepted_user_id) : null,
            revokedAt: row.revoked_at ?? null,
            createdAt: row.created_at,
            updatedAt: row.updated_at,
        }));
    }

    async revokeInvite(input: {
        workspaceId: string;
        inviteId: string;
        revokedByUserId: string;
    }): Promise<void> {
        const configuredProvider = getConfiguredAuthProvider();
        const { provider, providerUserId } = await this.resolveProviderIdentityForInternalUser({
            provider: configuredProvider,
            userId: input.revokedByUserId,
        });
        const convex = getAdminConvexClient(provider, providerUserId);
        await this.runConvexOperation('workspaces.revokeInvite', () =>
            convex.mutation(api.workspaces.revokeInvite, {
                workspace_id: input.workspaceId as Id<'workspaces'>,
                invite_id: input.inviteId as any,
            })
        );
    }

    async validateInvite(input: {
        workspaceId: string;
        email: string;
        tokenHash: string;
    }): Promise<InviteValidationResult> {
        const provider = getConfiguredAuthProvider();
        const convex = getAdminConvexClient(provider, input.workspaceId);
        return await this.runConvexOperation('workspaces.validateInviteInternal', () =>
            convex.query(internalApi.workspaces.validateInviteInternal, {
                workspace_id: input.workspaceId as Id<'workspaces'>,
                email: input.email,
                token_hash: input.tokenHash,
            })
        ) as InviteValidationResult;
    }

    async acceptInviteAndProvisionUser(input: {
        provider: string;
        providerUserId: string;
        email: string;
        displayName?: string;
        workspaceId: string;
        tokenHash: string;
    }): Promise<InviteProvisionResult> {
        const provider = resolveConvexAuthProvider(input.provider);
        const convex = getAdminConvexClient(provider, input.providerUserId, {
            email: input.email,
        });
        const result = await this.runConvexOperation(
            'workspaces.acceptInviteAndProvisionUser',
            () => convex.mutation(internalApi.workspaces.acceptInviteAndProvisionUser, {
                provider,
                provider_user_id: input.providerUserId,
                email: input.email,
                name: input.displayName,
                workspace_id: input.workspaceId as Id<'workspaces'>,
                token_hash: input.tokenHash,
            })
        ) as InviteProvisionResult & { user_id?: string };

        if (!result.ok) return result;
        const userId = result.user_id ?? result.userId;
        if (!userId) {
            throw new Error('Convex atomic invite provisioning returned no internal user id');
        }
        this.rememberProviderIdentity({
            userId,
            provider,
            providerUserId: input.providerUserId,
        });
        return {
            ok: true,
            userId,
            role: result.role,
            createdUser: result.createdUser,
        };
    }

    async consumeInvite(input: {
        workspaceId: string;
        email: string;
        tokenHash: string;
        acceptedUserId: string;
    }): Promise<
        | { ok: true; role: WorkspaceRole }
        | {
              ok: false;
              reason:
                  | 'not_found'
                  | 'expired'
                  | 'revoked'
                  | 'already_used'
                  | 'token_mismatch';
          }
    > {
        const configuredProvider = getConfiguredAuthProvider();
        const { provider, providerUserId } = await this.resolveProviderIdentityForInternalUser({
            provider: configuredProvider,
            userId: input.acceptedUserId,
        });
        const convex = getAdminConvexClient(provider, providerUserId, {
            email: input.email,
        });
        const result = await this.runConvexOperation('workspaces.consumeInvite', () =>
            convex.mutation(api.workspaces.consumeInvite, {
                workspace_id: input.workspaceId as Id<'workspaces'>,
                email: input.email,
                token_hash: input.tokenHash,
            })
        );

        if (result?.ok === true) {
            return { ok: true, role: result.role as WorkspaceRole };
        }

        return {
            ok: false,
            reason: (result?.reason ?? 'not_found') as
                | 'not_found'
                | 'expired'
                | 'revoked'
                | 'already_used'
                | 'token_mismatch',
        };
    }
}

/**
 * Factory function for creating Convex AuthWorkspaceStore instances.
 */
export function createConvexAuthWorkspaceStore(): AuthWorkspaceStore {
    return new ConvexAuthWorkspaceStore();
}
