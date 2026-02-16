/**
 * @module server/auth/store/impls/convex-auth-workspace-store.ts
 *
 * Purpose:
 * Convex implementation of AuthWorkspaceStore.
 *
 * DO NOT import this file directly in core. Use getAuthWorkspaceStore('convex')
 * after the Convex provider package registers the store.
 */
import type { AuthWorkspaceStore } from '~~/server/auth/store/types';
import type { WorkspaceRole } from '~~/app/core/hooks/hook-types';
import type { GenericId as Id } from 'convex/values';
import { ConvexHttpClient } from 'convex/browser';
import { convexApi as api } from '../../utils/convex-api';
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

function getAdminConvexClient(provider: string, providerUserId: string): ConvexHttpClient {
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
    private readonly providerUserIdByInternalUserId = new Map<string, string>();

    private async resolveProviderUserIdForInternalUser(input: {
        provider: string;
        userId: string;
    }): Promise<string> {
        const cached = this.providerUserIdByInternalUserId.get(input.userId);
        if (cached) return cached;

        // Legacy shape where user.id already equals provider_user_id.
        if (input.userId.startsWith('user_')) {
            this.providerUserIdByInternalUserId.set(input.userId, input.userId);
            return input.userId;
        }

        const convex = getAdminConvexClient(input.provider, input.userId);
        const account = await this.runConvexOperation('users.getAuthAccountByUserId', () =>
            convex.query(api.users.getAuthAccountByUserId, {
                provider: input.provider,
                user_id: input.userId as Id<'users'>,
            })
        );

        if (account?.provider_user_id) {
            const providerUserId = String(account.provider_user_id);
            this.providerUserIdByInternalUserId.set(input.userId, providerUserId);
            return providerUserId;
        }

        // Fallback keeps compatibility with historical datasets where IDs match.
        this.providerUserIdByInternalUserId.set(input.userId, input.userId);
        return input.userId;
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
    }): Promise<void> {
        const convex = getAdminConvexClient(input.provider, input.providerUserId);

        const resolved = await this.runConvexOperation('workspaces.resolveSession', () =>
            convex.query(api.workspaces.resolveSession, {
                provider: input.provider,
                provider_user_id: input.providerUserId,
            })
        );
        if (resolved) return;

        await this.runConvexOperation('workspaces.ensure', () =>
            convex.mutation(api.workspaces.ensure, {
                provider: input.provider,
                provider_user_id: input.providerUserId,
                email: input.email,
                name: input.displayName,
            })
        );
    }

    async getOrCreateUser(input: {
        provider: string;
        providerUserId: string;
        email?: string;
        displayName?: string;
    }): Promise<{ userId: string }> {
        const configuredProvider = resolveConvexAuthProvider(input.provider);

        try {
            await this.ensureUserWithProvider({
                ...input,
                provider: configuredProvider,
            });
        } catch (error) {
            if (configuredProvider !== 'clerk' && isLegacyClerkOnlyError(error)) {
                markLegacyClerkOnlyBackend();
                await this.ensureUserWithProvider({
                    ...input,
                    provider: 'clerk',
                });
            } else {
                throw error;
            }
        }

        return { userId: input.providerUserId };
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
            convex.query(api.users.getAuthAccountByProvider, {
                provider,
                provider_user_id: input.providerUserId,
            })
        );

        if (!account?.user_id) return null;
        this.providerUserIdByInternalUserId.set(
            String(account.user_id),
            input.providerUserId
        );
        return { userId: String(account.user_id) };
    }

    async getOrCreateDefaultWorkspace(
        userId: string
    ): Promise<{ workspaceId: string; workspaceName: string }> {
        const provider = getConfiguredAuthProvider();
        const providerUserId = await this.resolveProviderUserIdForInternalUser({
            provider,
            userId,
        });
        const convex = getAdminConvexClient(provider, providerUserId);

        // Get or create workspace via ensure
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
        };
    }

    async getWorkspaceRole(input: {
        userId: string;
        workspaceId: string;
    }): Promise<WorkspaceRole | null> {
        const provider = getConfiguredAuthProvider();
        const providerUserId = await this.resolveProviderUserIdForInternalUser({
            provider,
            userId: input.userId,
        });
        const convex = getAdminConvexClient(provider, providerUserId);

        const resolved = await this.runConvexOperation('workspaces.resolveSession', () =>
            convex.query(api.workspaces.resolveSession, {
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
        const provider = getConfiguredAuthProvider();
        const providerUserId = await this.resolveProviderUserIdForInternalUser({
            provider,
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
        const provider = getConfiguredAuthProvider();
        const providerUserId = await this.resolveProviderUserIdForInternalUser({
            provider,
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
        const provider = getConfiguredAuthProvider();
        const providerUserId = await this.resolveProviderUserIdForInternalUser({
            provider,
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
        const provider = getConfiguredAuthProvider();
        const providerUserId = await this.resolveProviderUserIdForInternalUser({
            provider,
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
        const provider = getConfiguredAuthProvider();
        const providerUserId = await this.resolveProviderUserIdForInternalUser({
            provider,
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
        const provider = getConfiguredAuthProvider();
        const providerUserId = await this.resolveProviderUserIdForInternalUser({
            provider,
            userId: input.invitedByUserId,
        });
        const convex = getAdminConvexClient(provider, providerUserId);
        const result = await this.runConvexOperation('workspaces.createInvite', () =>
            convex.mutation(api.workspaces.createInvite, {
                workspace_id: input.workspaceId as Id<'workspaces'>,
                email: input.email,
                role: input.role,
                invited_by_user_id: input.invitedByUserId as Id<'users'>,
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
        const rows = await this.runConvexOperation('workspaces.listInvites', () =>
            convex.query(api.workspaces.listInvites, {
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
        const provider = getConfiguredAuthProvider();
        const providerUserId = await this.resolveProviderUserIdForInternalUser({
            provider,
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
        const provider = getConfiguredAuthProvider();
        const providerUserId = await this.resolveProviderUserIdForInternalUser({
            provider,
            userId: input.acceptedUserId,
        });
        const convex = getAdminConvexClient(provider, providerUserId);
        const result = await this.runConvexOperation('workspaces.consumeInvite', () =>
            convex.mutation(api.workspaces.consumeInvite, {
                workspace_id: input.workspaceId as Id<'workspaces'>,
                email: input.email,
                token_hash: input.tokenHash,
                accepted_user_id: input.acceptedUserId as Id<'users'>,
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
