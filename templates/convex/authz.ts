/**
 * Convex authorization guards for subject-bound identity, invitation, and
 * workspace operations.
 *
 * Public callers must be authenticated and are authorized from their Convex
 * identity plus workspace membership. Trusted SSR provider calls authenticate
 * with the Convex admin key and add the `or3_server` marker.
 */
import type { MutationCtx, QueryCtx } from './_generated/server';
import type { Id } from './_generated/dataModel';

type ConvexCtx = MutationCtx | QueryCtx;
type AuthIdentity = NonNullable<Awaited<ReturnType<ConvexCtx['auth']['getUserIdentity']>>>;
const TRUSTED_SERVER_ISSUER_PREFIX = 'https://or3.ai/auth/';
const LEGACY_CLERK_SERVER_ISSUER = 'https://clerk.or3.ai';

export function inferProviderFromIssuer(issuer: string | undefined): string {
    if (!issuer) return 'clerk';
    if (issuer.includes('clerk')) return 'clerk';

    const marker = '/auth/';
    const markerIndex = issuer.lastIndexOf(marker);
    if (markerIndex === -1) return 'clerk';

    const provider = issuer.slice(markerIndex + marker.length).trim();
    return provider || 'clerk';
}

export function isTrustedServerIdentity(identity: AuthIdentity): boolean {
    return (
        identity.or3_server === true &&
        typeof identity.issuer === 'string' &&
        (identity.issuer === LEGACY_CLERK_SERVER_ISSUER ||
            identity.issuer.startsWith(TRUSTED_SERVER_ISSUER_PREFIX))
    );
}

async function requireIdentity(ctx: ConvexCtx): Promise<AuthIdentity> {
    const identity = await ctx.auth.getUserIdentity();
    if (!identity) {
        throw new Error('Unauthorized');
    }
    return identity;
}

export async function requireCallerSubject(
    ctx: ConvexCtx,
    expected: { provider: string; providerUserId: string },
    options: { allowTrustedServer?: boolean } = {}
): Promise<AuthIdentity> {
    const identity = await requireIdentity(ctx);
    if (options.allowTrustedServer && isTrustedServerIdentity(identity)) {
        return identity;
    }

    const provider = inferProviderFromIssuer(identity.issuer);
    if (provider !== expected.provider || identity.subject !== expected.providerUserId) {
        throw new Error('Forbidden');
    }
    return identity;
}

export async function requireCallerUserId(
    ctx: ConvexCtx,
    expectedUserId: Id<'users'>,
    options: { allowTrustedServer?: boolean } = {}
): Promise<void> {
    const identity = await requireIdentity(ctx);
    if (options.allowTrustedServer && isTrustedServerIdentity(identity)) {
        return;
    }

    const provider = inferProviderFromIssuer(identity.issuer);
    const authAccount = await ctx.db
        .query('auth_accounts')
        // The provider package checks templates without generated Convex types;
        // deployed projects recover the concrete builder type during codegen.
        .withIndex('by_provider', (q: any) =>
            q.eq('provider', provider).eq('provider_user_id', identity.subject)
        )
        .first();

    if (!authAccount || authAccount.user_id !== expectedUserId) {
        throw new Error('Forbidden');
    }
}

export async function requireInviteAcceptance(
    ctx: ConvexCtx,
    expectedEmail: string
): Promise<Id<'users'>> {
    const identity = await requireIdentity(ctx);
    const identityEmail = typeof identity.email === 'string'
        ? identity.email.trim().toLowerCase()
        : '';
    if (!identityEmail || identityEmail !== expectedEmail.trim().toLowerCase()) {
        throw new Error('Forbidden');
    }

    const provider = inferProviderFromIssuer(identity.issuer);
    const authAccount = await ctx.db
        .query('auth_accounts')
        // The provider package checks templates without generated Convex types;
        // deployed projects recover the concrete builder type during codegen.
        .withIndex('by_provider', (q: any) =>
            q.eq('provider', provider).eq('provider_user_id', identity.subject)
        )
        .first();
    if (!authAccount) {
        throw new Error('Forbidden');
    }

    return authAccount.user_id;
}

export async function requireWorkspaceRole(
    ctx: ConvexCtx,
    workspaceId: Id<'workspaces'>,
    allowedRoles: ReadonlySet<'owner' | 'editor' | 'viewer'>,
    options: { allowTrustedServer?: boolean } = {}
): Promise<{ userId: Id<'users'> | null; role: 'owner' | 'editor' | 'viewer' }> {
    const identity = await requireIdentity(ctx);
    if (options.allowTrustedServer && isTrustedServerIdentity(identity)) {
        return { userId: null, role: 'owner' };
    }

    const provider = inferProviderFromIssuer(identity.issuer);
    const authAccount = await ctx.db
        .query('auth_accounts')
        .withIndex('by_provider', (q: any) =>
            q.eq('provider', provider).eq('provider_user_id', identity.subject)
        )
        .first();
    if (!authAccount) {
        throw new Error('Forbidden');
    }

    const membership = await ctx.db
        .query('workspace_members')
        .withIndex('by_workspace_user', (q: any) =>
            q.eq('workspace_id', workspaceId).eq('user_id', authAccount.user_id)
        )
        .first();
    if (!membership || !allowedRoles.has(membership.role)) {
        throw new Error('Forbidden');
    }

    return { userId: authAccount.user_id, role: membership.role };
}
