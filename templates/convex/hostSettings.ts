/**
 * @module convex/hostSettings
 *
 * Purpose:
 * Stores security-sensitive, workspace-scoped host settings in the private
 * `host_settings` table instead of the client-syncable `kv` table.
 *
 * Behavior:
 * - Every function is internal to the Convex deployment, so an ordinary client
 *   token cannot address it at all. Callers must also present a trusted
 *   host-server identity minted with the Convex admin key (`or3_server`
 *   marker); deployment-admin grants are not required for the host server.
 * - `compareAndSetHostSetting` is atomic: Convex mutations are serializable,
 *   so the read and conditional write cannot interleave.
 * - `getLegacyWorkspaceSetting` exposes the old client-writable `kv` value so
 *   the host can apply its own migration policy. It never copies by itself.
 *
 * Constraints:
 * - `host_settings` must stay out of sync push/pull, snapshots and
 *   `change_log`. Do not add it to the sync table maps or syncAuthoring.
 * - These functions are narrow storage primitives. The OR3 host route remains
 *   the business authorization boundary.
 *
 * Non-Goals:
 * - Deciding which key families are security authoritative; that policy lives
 *   in the host.
 */
import {
    internalMutation,
    internalQuery,
    type MutationCtx,
    type QueryCtx,
} from './_generated/server';
import { v } from 'convex/values';
import type { Id } from './_generated/dataModel';
import { isTrustedServerIdentity } from './authz';

/** Bounds keep a misbehaving host from writing unbounded rows. */
const MAX_SETTING_KEY_LENGTH = 512;
const MAX_SETTING_VALUE_BYTES = 1024 * 1024;

function assertSettingShape(key: string, value?: string): void {
    if (typeof key !== 'string' || key.length < 1 || key.length > MAX_SETTING_KEY_LENGTH) {
        throw new Error('Invalid host setting key');
    }
    if (value === undefined) return;
    if (typeof value !== 'string') {
        throw new Error('Invalid host setting value');
    }
    if (new TextEncoder().encode(value).byteLength > MAX_SETTING_VALUE_BYTES) {
        throw new Error('Host setting value too large');
    }
}

/**
 * Host settings are only readable by the host server. A workspace member's
 * own token is not sufficient because key-level policy (for example
 * owner-only consent approval) is enforced by the host route.
 */
async function requireHostSettingsServer(ctx: MutationCtx | QueryCtx): Promise<void> {
    const identity = await ctx.auth.getUserIdentity();
    if (!identity) {
        throw new Error('Not authenticated');
    }
    if (!isTrustedServerIdentity(identity)) {
        throw new Error('Forbidden: host server identity required');
    }
}

/**
 * Internal helper shared with the admin bridge functions.
 *
 * Purpose:
 * Reads one private host setting value.
 */
export async function readHostSettingValue(
    ctx: QueryCtx | MutationCtx,
    workspaceId: Id<'workspaces'>,
    key: string
): Promise<string | null> {
    const entry = await ctx.db
        .query('host_settings')
        .withIndex('by_workspace_key', (q) =>
            q.eq('workspace_id', workspaceId).eq('key', key)
        )
        .first();
    return entry?.value ?? null;
}

/**
 * Internal helper shared with the admin bridge functions.
 *
 * Purpose:
 * Upserts one private host setting value.
 */
export async function writeHostSettingValue(
    ctx: MutationCtx,
    workspaceId: Id<'workspaces'>,
    key: string,
    value: string
): Promise<void> {
    assertSettingShape(key, value);
    const now = Date.now();
    const existing = await ctx.db
        .query('host_settings')
        .withIndex('by_workspace_key', (q) =>
            q.eq('workspace_id', workspaceId).eq('key', key)
        )
        .first();
    if (existing) {
        await ctx.db.patch(existing._id, { value, updated_at: now });
        return;
    }
    await ctx.db.insert('host_settings', {
        workspace_id: workspaceId,
        key,
        value,
        updated_at: now,
    });
}

/**
 * `hostSettings.getHostSetting` (query)
 *
 * Purpose:
 * Reads one private workspace host setting. Returns null when absent.
 */
export const getHostSetting = internalQuery({
    args: {
        workspace_id: v.id('workspaces'),
        key: v.string(),
    },
    handler: async (ctx, args) => {
        await requireHostSettingsServer(ctx);
        assertSettingShape(args.key);
        return await readHostSettingValue(ctx, args.workspace_id, args.key);
    },
});

/**
 * `hostSettings.getLegacyWorkspaceSetting` (query)
 *
 * Purpose:
 * Reads the legacy `kv` value for one workspace key so the host can migrate
 * user data explicitly. Values here were writable by ordinary workspace sync
 * and must never be treated as authority without the host's migration policy.
 */
export const getLegacyWorkspaceSetting = internalQuery({
    args: {
        workspace_id: v.id('workspaces'),
        key: v.string(),
    },
    handler: async (ctx, args) => {
        await requireHostSettingsServer(ctx);
        assertSettingShape(args.key);
        const entry = await ctx.db
            .query('kv')
            .withIndex('by_workspace_name', (q) =>
                q.eq('workspace_id', args.workspace_id).eq('name', args.key)
            )
            .first();
        if (!entry || entry.deleted) return null;
        return entry.value ?? null;
    },
});

/**
 * `hostSettings.setHostSetting` (mutation)
 *
 * Purpose:
 * Upserts one private workspace host setting through the host server.
 */
export const setHostSetting = internalMutation({
    args: {
        workspace_id: v.id('workspaces'),
        key: v.string(),
        value: v.string(),
    },
    handler: async (ctx, args) => {
        await requireHostSettingsServer(ctx);
        await writeHostSettingValue(ctx, args.workspace_id, args.key, args.value);
    },
});

/**
 * `hostSettings.compareAndSetHostSetting` (mutation)
 *
 * Purpose:
 * Atomically writes a private workspace host setting only when its current
 * value equals the caller's expected value. Convex mutations are serializable,
 * so exactly one racing writer with the same expectation succeeds.
 */
export const compareAndSetHostSetting = internalMutation({
    args: {
        workspace_id: v.id('workspaces'),
        key: v.string(),
        expected_value: v.union(v.string(), v.null()),
        value: v.string(),
    },
    handler: async (ctx, args) => {
        await requireHostSettingsServer(ctx);
        assertSettingShape(args.key, args.value);
        const currentValue = await readHostSettingValue(
            ctx,
            args.workspace_id,
            args.key
        );
        if (currentValue !== args.expected_value) return false;
        await writeHostSettingValue(ctx, args.workspace_id, args.key, args.value);
        return true;
    },
});
