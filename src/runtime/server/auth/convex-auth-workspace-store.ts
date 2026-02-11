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

    async getOrCreateDefaultWorkspace(
        userId: string
    ): Promise<{ workspaceId: string; workspaceName: string }> {
        const provider = getConfiguredAuthProvider();
        const convex = getAdminConvexClient(provider, userId);

        // Get or create workspace via ensure
        const workspaceInfo = await this.runConvexOperation('workspaces.ensure', () =>
            convex.mutation(api.workspaces.ensure, {
                provider,
                provider_user_id: userId,
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
        const convex = getAdminConvexClient(provider, input.userId);

        const resolved = await this.runConvexOperation('workspaces.resolveSession', () =>
            convex.query(api.workspaces.resolveSession, {
                provider,
                provider_user_id: input.userId,
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
        const convex = getAdminConvexClient(provider, userId);

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
        const convex = getAdminConvexClient(provider, input.userId);

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
        const convex = getAdminConvexClient(provider, input.userId);

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
        const convex = getAdminConvexClient(provider, input.userId);

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
        const convex = getAdminConvexClient(provider, input.userId);

        await this.runConvexOperation('workspaces.setActive', () =>
            convex.mutation(api.workspaces.setActive, {
                workspace_id: input.workspaceId as Id<'workspaces'>,
            })
        );
    }
}

/**
 * Factory function for creating Convex AuthWorkspaceStore instances.
 */
export function createConvexAuthWorkspaceStore(): AuthWorkspaceStore {
    return new ConvexAuthWorkspaceStore();
}
