/**
 * @module server/admin/stores/convex/convex-store.ts
 *
 * Purpose:
 * Provides a concrete implementation of the Admin Store interfaces using Convex
 * as the backend. This module handles the complexities of server-side
 * authentication and privileged data access.
 *
 * Responsibilities:
 * - Implementing WorkspaceAccessStore, WorkspaceSettingsStore, and AdminUserStore.
 * - Authenticating super admins via Clerk JWTs or Convex Admin Keys.
 * - Bridging admin identities for deployment-level mutation access.
 * - Validating and converting IDs between the Admin API and Convex internals.
 *
 * Architecture:
 * This implementation relies on the Convex Gateway Client utilities to perform
 * mutation and query requests. It uses a session-based caching mechanism
 * (`__or3_super_admin_grant_done`) to avoid redundant deployment grants.
 *
 * Workspace settings are different: they live in the private `host_settings`
 * table and are always accessed with the deployment admin key as a trusted
 * host-server identity. That keeps ordinary workspace users (including
 * workspace owners) out of deployment-admin membership while the OR3 host
 * route remains the business authorization boundary.
 *
 * Constraints:
 * - Requires either `CLERK_SECRET_KEY` or `CONVEX_SELF_HOSTED_ADMIN_KEY` for auth.
 * - Access and admin-user mutations require an authenticated principal Kind of
 *   `super_admin`; settings uses the admin key plus the `or3_server` marker.
 */
import type { H3Event } from 'h3';
import { createError } from 'h3';
import { createHmac } from 'crypto';
import { convexApi as api, convexInternalApi as internalApi } from '../../../utils/convex-api';
import type { GenericId as Id } from 'convex/values';
import type {
    WorkspaceAccessStore,
    WorkspaceSettingsStore,
    AdminUserStore,
    WorkspaceSummary,
    AdminUserInfo,
} from '~~/server/admin/stores/types';
import {
    getConvexAdminGatewayClient,
    getConvexGatewayClient,
} from '../../utils/convex-gateway';
import { CONVEX_JWT_TEMPLATE, CONVEX_PROVIDER_ID } from '~~/shared/cloud/provider-ids';
import { ADMIN_IDENTITY_ISSUER } from '~~/shared/cloud/admin-identity';
import { useRuntimeConfig } from '#imports';
import { resolveProviderToken } from '~~/server/auth/token-broker/resolve';
import { listProviderTokenBrokerIds } from '~~/server/auth/token-broker/registry';

type AdminContextShape = {
    principal?: { kind?: string; username?: string };
    session?: { providerUserId?: string };
};

/**
 * Constructs a mock OIDC identity for super admins when using Convex Admin Keys.
 *
 * Internal utility.
 */
function buildAdminIdentity(username: string) {
    const normalized = username.trim() || 'super_admin';
    return {
        subject: normalized,
        issuer: ADMIN_IDENTITY_ISSUER,
        name: normalized,
        preferredUsername: normalized,
    };
}

/**
 * Ensures that a super admin has the necessary deployment-level permissions
 * in Convex before performing sensitive mutations.
 *
 * Behavior:
 * Generates an HMAC signature (`bridge_signature`) using the `OR3_ADMIN_JWT_SECRET`
 * to verify the server's authority to grant admin status to a specific Clerk user.
 *
 * Constraints:
 * - Only runs once per request (cached on `event.context`).
 * - Requires `OR3_ADMIN_JWT_SECRET` to be synchronized between the Nuxt server
 *   and the Convex environment.
 */
async function ensureSuperAdminDeploymentGrant(
    event: H3Event,
    client: ReturnType<typeof getConvexGatewayClient>
): Promise<void> {
    const adminContext = event.context.admin as AdminContextShape | undefined;
    const principal = adminContext?.principal;
    if (!principal || principal.kind !== 'super_admin') return;
    if (event.context.__or3_super_admin_grant_done) return;
    event.context.__or3_super_admin_grant_done = true;

    const adminUsername = principal.username;
    const providerUserId = adminContext.session?.providerUserId;
    if (!adminUsername || !providerUserId) {
        throw createError({
            statusCode: 401,
            statusMessage: 'Missing Clerk session for super admin bridging',
        });
    }

    const config = useRuntimeConfig(event);
    const secret = config.admin.auth.jwtSecret;
    if (!secret) {
        throw createError({
            statusCode: 500,
            statusMessage:
                'OR3_ADMIN_JWT_SECRET is required for super admin bridging',
        });
    }

    const bridgeSignature = createHmac('sha256', secret)
        .update(`or3-admin-bridge:${providerUserId}:${adminUsername}`)
        .digest('hex');

    try {
        await client.mutation(api.admin.ensureDeploymentAdmin, {
            bridge_signature: bridgeSignature,
            admin_username: adminUsername,
        });
    } catch (error) {
        throw createError({
            statusCode: 403,
            statusMessage:
                'Super admin bridging failed. Ensure OR3_ADMIN_JWT_SECRET is set in the Convex environment.',
            data: {
                original: error instanceof Error ? error.message : String(error),
            },
        });
    }
}

/**
 * Validate and convert a workspace ID string to Convex Id type.
 * Throws if the ID is empty/invalid.
 */
function validateWorkspaceId(id: string): Id<'workspaces'> {
    if (!id || typeof id !== 'string') {
        throw createError({
            statusCode: 400,
            statusMessage: 'Invalid workspace ID: must be a non-empty string',
        });
    }
    // Convex Ids are opaque strings; some deployments prefix with "workspaces:"
    return id as Id<'workspaces'>;
}

/**
 * Validate and convert a user ID string to Convex Id type.
 *
 * @throws 400 Error if the ID is malformed.
 * Internal utility.
 */
function validateUserId(id: string): Id<'users'> {
    if (!id || typeof id !== 'string') {
        throw createError({
            statusCode: 400,
            statusMessage: 'Invalid user ID: must be a non-empty string',
        });
    }
    // Convex Ids are opaque strings; some deployments prefix with "users:"
    return id as Id<'users'>;
}

/**
 * Resolves an authenticated Convex client based on the current request context.
 *
 * Logic:
 * 1. If `super_admin` principal exists and a Convex Admin Key is provided,
 *    returns a client using manual OIDC identity bridging.
 * 2. Otherwise, attempts to retrieve a Clerk JWT from the current session.
 * 3. Validates that the provider is 'clerk'.
 * 4. Ensures super admin deployment grants are applied if necessary.
 *
 * @throws 401/403/501 Error if authentication fails or is misconfigured.
 */
async function getConvexClientWithAuth(event: H3Event) {
    const config = useRuntimeConfig(event);
    const authProvider = config.auth.provider;
    const adminContext = event.context.admin as AdminContextShape | undefined;
    const principal = adminContext?.principal;

    if (principal?.kind === 'super_admin') {
        const adminKey = config.sync.convexAdminKey.trim();
        if (adminKey) {
            const identity = buildAdminIdentity(principal.username || 'super_admin');
            return getConvexAdminGatewayClient(event, adminKey, identity);
        }
    }

    const brokerIds = listProviderTokenBrokerIds();
    if (!brokerIds.includes(authProvider)) {
        throw createError({
            statusCode: 501,
            statusMessage:
                `Convex-based admin dashboard requires a ProviderTokenBroker for ` +
                `auth provider "${authProvider}". Install the provider package that ` +
                `registers this broker (e.g. or3-provider-${authProvider}).`,
        });
    }

    const token = await resolveProviderToken(event, {
        providerId: CONVEX_PROVIDER_ID,
        template: CONVEX_JWT_TEMPLATE,
    });
    if (!token) {
        throw createError({
            statusCode: 401,
            statusMessage: 'Missing authentication token',
        });
    }
    const client = getConvexGatewayClient(event, token);
    if (principal?.kind === 'super_admin' && adminContext?.session?.providerUserId) {
        await ensureSuperAdminDeploymentGrant(event, client);
    }
    return client;
}

/**
 * Creates a WorkspaceAccessStore implemented for Convex.
 */
export function createConvexWorkspaceAccessStore(
    event: H3Event
): WorkspaceAccessStore {
    return {
        async listMembers({ workspaceId }) {
            const client = await getConvexClientWithAuth(event);
            return await client.query(api.admin.listWorkspaceMembers, {
                workspace_id: validateWorkspaceId(workspaceId),
            });
        },
        async upsertMember(input: {
            workspaceId: string;
            emailOrProviderId: string;
            role: 'owner' | 'editor' | 'viewer';
            provider?: string;
        }) {
            const { workspaceId, emailOrProviderId, role, provider } = input;
            const client = await getConvexClientWithAuth(event);
            await client.mutation(api.admin.upsertWorkspaceMember, {
                workspace_id: validateWorkspaceId(workspaceId),
                email_or_provider_id: emailOrProviderId,
                role,
                provider,
            });
        },
        async setMemberRole({ workspaceId, userId, role }) {
            const client = await getConvexClientWithAuth(event);
            await client.mutation(api.admin.setWorkspaceMemberRole, {
                workspace_id: validateWorkspaceId(workspaceId),
                user_id: userId,
                role,
            });
        },
        async removeMember({ workspaceId, userId }) {
            const client = await getConvexClientWithAuth(event);
            await client.mutation(api.admin.removeWorkspaceMember, {
                workspace_id: validateWorkspaceId(workspaceId),
                user_id: userId,
            });
        },
        async listWorkspaces({ search, includeDeleted, page, perPage }) {
            const client = await getConvexClientWithAuth(event);
            return await client.query(api.admin.listWorkspaces, {
                search,
                include_deleted: includeDeleted,
                page,
                per_page: perPage,
            });
        },
        async getWorkspace({ workspaceId }) {
            const client = await getConvexClientWithAuth(event);
            const result = await client.query(api.admin.getWorkspace, {
                workspace_id: validateWorkspaceId(workspaceId),
            });
            return result as WorkspaceSummary | null;
        },
        async createWorkspace({ name, description, ownerUserId }) {
            const client = await getConvexClientWithAuth(event);
            const result = await client.mutation(api.admin.createWorkspace, {
                name,
                description,
                owner_user_id: validateUserId(ownerUserId),
            });
            return { workspaceId: result.workspace_id };
        },
        async softDeleteWorkspace({ workspaceId, deletedAt }) {
            const client = await getConvexClientWithAuth(event);
            await client.mutation(api.admin.softDeleteWorkspace, {
                workspace_id: validateWorkspaceId(workspaceId),
                deleted_at: deletedAt,
            });
        },
        async restoreWorkspace({ workspaceId }) {
            const client = await getConvexClientWithAuth(event);
            await client.mutation(api.admin.restoreWorkspace, {
                workspace_id: validateWorkspaceId(workspaceId),
            });
        },
        async searchUsers({ query, limit }) {
            const client = await getConvexClientWithAuth(event);
            return await client.query(api.admin.searchUsers, {
                query,
                limit,
            });
        },
    };
}

/**
 * Builds the trusted host-server identity used for private host settings.
 *
 * Host settings are server-mediated: the OR3 host route checks the business
 * authorization (`can()`), then storage primitives run with the deployment
 * admin key. Ordinary workspace users never need deployment-admin membership.
 */
function buildHostSettingsServerIdentity() {
    const issuer = 'https://or3.ai/auth/server';
    return {
        subject: 'or3-host-settings',
        issuer,
        tokenIdentifier: `${issuer}|or3-host-settings`,
        or3_server: true,
    };
}

/**
 * Resolves the admin-key client used for the private host-settings table.
 *
 * Constraints:
 * - Fails closed without `sync.convexAdminKey`; clients must never be able to
 *   reach host enforcement state with their own token.
 */
function getConvexHostSettingsClient(event: H3Event) {
    const config = useRuntimeConfig(event);
    const adminKey = config.sync.convexAdminKey?.trim();
    if (!adminKey) {
        throw createError({
            statusCode: 503,
            statusMessage:
                'Convex admin key is required for private workspace host settings',
        });
    }
    return getConvexAdminGatewayClient(
        event,
        adminKey,
        buildHostSettingsServerIdentity()
    );
}

/**
 * Creates a WorkspaceSettingsStore implemented for Convex.
 *
 * All access goes through the private `host_settings` table using a trusted
 * server identity, so a workspace editor's sync writes to the old `kv` table
 * can never change enforcement state, and ordinary plugin users do not need
 * deployment-admin membership.
 */
export function createConvexWorkspaceSettingsStore(
    event: H3Event
): WorkspaceSettingsStore {
    return {
        async get(workspaceId, key) {
            const client = getConvexHostSettingsClient(event);
            return await client.query(internalApi.hostSettings.getHostSetting, {
                workspace_id: validateWorkspaceId(workspaceId),
                key,
            });
        },
        async getLegacy(workspaceId, key) {
            const client = getConvexHostSettingsClient(event);
            return await client.query(internalApi.hostSettings.getLegacyWorkspaceSetting, {
                workspace_id: validateWorkspaceId(workspaceId),
                key,
            });
        },
        async set(workspaceId, key, value) {
            const client = getConvexHostSettingsClient(event);
            await client.mutation(internalApi.hostSettings.setHostSetting, {
                workspace_id: validateWorkspaceId(workspaceId),
                key,
                value,
            });
        },
        async compareAndSet(workspaceId, key, expectedValue, value) {
            const client = getConvexHostSettingsClient(event);
            return await client.mutation(internalApi.hostSettings.compareAndSetHostSetting, {
                workspace_id: validateWorkspaceId(workspaceId),
                key,
                expected_value: expectedValue,
                value,
            });
        },
    };
}

/**
 * Creates an AdminUserStore implemented for Convex.
 */
export function createConvexAdminUserStore(event: H3Event): AdminUserStore {
    return {
        async listAdmins(): Promise<AdminUserInfo[]> {
            const client = await getConvexClientWithAuth(event);
            return await client.query(api.admin.listAdmins, {});
        },
        async grantAdmin({ userId }) {
            const client = await getConvexClientWithAuth(event);
            await client.mutation(api.admin.grantAdmin, {
                user_id: validateUserId(userId),
            });
        },
        async revokeAdmin({ userId }) {
            const client = await getConvexClientWithAuth(event);
            await client.mutation(api.admin.revokeAdmin, {
                user_id: validateUserId(userId),
            });
        },
        async isAdmin({ userId }) {
            const client = await getConvexClientWithAuth(event);
            return await client.query(api.admin.isAdmin, {
                user_id: validateUserId(userId),
            });
        },
        async searchUsers({ query, limit }) {
            const client = await getConvexClientWithAuth(event);
            return await client.query(api.admin.searchUsers, {
                query,
                limit,
            });
        },
    };
}
