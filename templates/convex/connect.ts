/**
 * Durable account binding for OR3 Connect's browser device authorization.
 * All functions are internal: Nitro is the protocol boundary and performs
 * public authentication, rate limiting, hashing, encryption, and tunnel work.
 */
import { internalMutation, internalQuery } from './_generated/server';
import { v } from 'convex/values';

const hostValidator = v.object({
    name: v.string(),
    platform: v.string(),
    architecture: v.string(),
    intern_version: v.string(),
    host_id: v.optional(v.string()),
    signing_public_key: v.optional(v.string()),
    noise_public_key: v.optional(v.string()),
});

export const createDeviceAuthorization = internalMutation({
    args: {
        device_code_hash: v.string(),
        user_code_hash: v.string(),
        user_code_display: v.string(),
        host: hostValidator,
        expires_at: v.number(),
        now: v.number(),
    },
    handler: async (ctx, args) => {
        return await ctx.db.insert('connect_device_authorizations', {
            device_code_hash: args.device_code_hash,
            user_code_hash: args.user_code_hash,
            user_code_display: args.user_code_display,
            host: args.host,
            expires_at: args.expires_at,
            status: 'pending',
            created_at: args.now,
            updated_at: args.now,
        });
    },
});

export const pollDeviceAuthorization = internalMutation({
    args: { device_code_hash: v.string(), now: v.number() },
    handler: async (ctx, args) => {
        const record = await ctx.db
            .query('connect_device_authorizations')
            .withIndex('by_device_code_hash', (q) =>
                q.eq('device_code_hash', args.device_code_hash)
            )
            .unique();
        if (!record) return null;
        if (record.expires_at <= args.now && record.status === 'pending') {
            await ctx.db.patch(record._id, {
                status: 'expired',
                updated_at: args.now,
            });
            return { ...record, status: 'expired' as const };
        }
        if (record.status === 'approved') {
            await ctx.db.patch(record._id, {
                status: 'consumed',
                credential_ciphertext: undefined,
                updated_at: args.now,
            });
        }
        return record;
    },
});

export const getDeviceAuthorizationByUserHash = internalQuery({
    args: { user_code_hash: v.string(), now: v.number() },
    handler: async (ctx, args) => {
        const record = await ctx.db
            .query('connect_device_authorizations')
            .withIndex('by_user_code_hash', (q) =>
                q.eq('user_code_hash', args.user_code_hash)
            )
            .unique();
        if (!record || record.expires_at <= args.now) return null;
        return record;
    },
});

export const approveDeviceAuthorization = internalMutation({
    args: {
        authorization_id: v.id('connect_device_authorizations'),
        user_id: v.id('users'),
        workspace_id: v.id('workspaces'),
        environment: v.object({
            id: v.string(),
            name: v.string(),
            platform: v.string(),
            architecture: v.string(),
            host_id: v.optional(v.string()),
            signing_public_key: v.optional(v.string()),
            noise_public_key: v.optional(v.string()),
            hostname: v.string(),
            tunnel_id: v.string(),
            dns_record_id: v.string(),
            control_token_hash: v.string(),
            access_credential_ciphertext: v.string(),
        }),
        credential_ciphertext: v.string(),
        max_active_environments: v.number(),
        now: v.number(),
    },
    handler: async (ctx, args) => {
        const authorization = await ctx.db.get(args.authorization_id);
        if (
            !authorization ||
            authorization.status !== 'pending' ||
            authorization.expires_at <= args.now
        ) {
            throw new Error('This connection request is no longer available.');
        }
        const active = await ctx.db
            .query('connect_environments')
            .withIndex('by_user_status', (q) =>
                q.eq('user_id', args.user_id).eq('status', 'active')
            )
            .collect();
        if (active.length >= args.max_active_environments) {
            throw new Error(
                `This account already has ${args.max_active_environments} connected computers.`
            );
        }
        await ctx.db.insert('connect_environments', {
            ...args.environment,
            user_id: args.user_id,
            workspace_id: args.workspace_id,
            status: 'active',
            created_at: args.now,
            updated_at: args.now,
        });
        await ctx.db.patch(args.authorization_id, {
            status: 'approved',
            approved_user_id: args.user_id,
            approved_workspace_id: args.workspace_id,
            environment_id: args.environment.id,
            credential_ciphertext: args.credential_ciphertext,
            updated_at: args.now,
        });
        return { environment_id: args.environment.id };
    },
});

export const denyDeviceAuthorization = internalMutation({
    args: {
        authorization_id: v.id('connect_device_authorizations'),
        now: v.number(),
    },
    handler: async (ctx, args) => {
        const authorization = await ctx.db.get(args.authorization_id);
        if (!authorization || authorization.status !== 'pending') return false;
        await ctx.db.patch(args.authorization_id, {
            status: 'denied',
            updated_at: args.now,
        });
        return true;
    },
});

export const getEnvironmentByControlTokenHash = internalQuery({
    args: { control_token_hash: v.string() },
    handler: async (ctx, args) => {
        return await ctx.db
            .query('connect_environments')
            .withIndex('by_control_token_hash', (q) =>
                q.eq('control_token_hash', args.control_token_hash)
            )
            .unique();
    },
});

export const listEnvironmentsForUser = internalQuery({
    args: { user_id: v.id('users') },
    handler: async (ctx, args) => {
        return await ctx.db
            .query('connect_environments')
            .withIndex('by_user_status', (q) =>
                q.eq('user_id', args.user_id).eq('status', 'active')
            )
            .collect();
    },
});

export const revokeEnvironment = internalMutation({
    args: { environment_id: v.string(), now: v.number() },
    handler: async (ctx, args) => {
        const environment = await ctx.db
            .query('connect_environments')
            .withIndex('by_environment_id', (q) => q.eq('id', args.environment_id))
            .unique();
        if (!environment || environment.status === 'revoked') return false;
        await ctx.db.patch(environment._id, {
            status: 'revoked',
            revoked_at: args.now,
            updated_at: args.now,
        });
        return true;
    },
});
