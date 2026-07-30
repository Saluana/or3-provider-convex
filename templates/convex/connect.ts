/**
 * Durable account binding for OR3 Connect's browser device authorization.
 * All functions are internal: Nitro is the protocol boundary and performs
 * public authentication, rate limiting, hashing, encryption, and tunnel work.
 */
import { internalMutation, internalQuery } from './_generated/server';
import { ConvexError, v } from 'convex/values';

type ConnectApplicationErrorCode =
    | 'authorization_unavailable'
    | 'code_conflict'
    | 'environment_limit_reached';

function connectApplicationError(
    code: ConnectApplicationErrorCode,
    message: string
): ConvexError<{ code: ConnectApplicationErrorCode; message: string }> {
    return new ConvexError({ code, message });
}

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
        host: hostValidator,
        expires_at: v.number(),
        now: v.number(),
    },
    handler: async (ctx, args) => {
        const liveCodeCollision = await ctx.db
            .query('connect_device_authorizations')
            .withIndex('by_user_code_hash', (q) =>
                q.eq('user_code_hash', args.user_code_hash)
            )
            .filter((q) =>
                q.and(
                    q.eq(q.field('status'), 'pending'),
                    q.gt(q.field('expires_at'), args.now)
                )
            )
            .first();
        if (liveCodeCollision) {
            throw connectApplicationError(
                'code_conflict',
                'A live pairing code already uses this lookup.'
            );
        }
        return await ctx.db.insert('connect_device_authorizations', {
            device_code_hash: args.device_code_hash,
            user_code_hash: args.user_code_hash,
            host: args.host,
            expires_at: args.expires_at,
            status: 'pending',
            created_at: args.now,
            updated_at: args.now,
        });
    },
});

export const pollDeviceAuthorization = internalMutation({
    args: {
        device_code_hash: v.string(),
        now: v.number(),
        redelivery_window_ms: v.number(),
    },
    handler: async (ctx, args) => {
        const record = await ctx.db
            .query('connect_device_authorizations')
            .withIndex('by_device_code_hash', (q) =>
                q.eq('device_code_hash', args.device_code_hash)
            )
            .unique();
        if (!record) return null;
        if (
            record.expires_at <= args.now &&
            (record.status === 'pending' || record.status === 'approved')
        ) {
            await ctx.db.patch(record._id, {
                status: 'expired',
                credential_ciphertext: undefined,
                user_code_display: undefined,
                updated_at: args.now,
            });
            return {
                ...record,
                status: 'expired' as const,
                credential_ciphertext: undefined,
                user_code_display: undefined,
            };
        }
        if (record.status === 'approved') {
            if (
                !Number.isSafeInteger(args.redelivery_window_ms) ||
                args.redelivery_window_ms <= 0
            ) {
                throw new Error(
                    'Invalid Connect credential redelivery window.'
                );
            }
            const redeliveryWindowMs = Math.max(
                1,
                Math.min(5 * 60_000, Math.floor(args.redelivery_window_ms))
            );
            await ctx.db.patch(record._id, {
                status: 'delivering',
                credential_delivery_started_at: args.now,
                credential_redeliver_until:
                    args.now + redeliveryWindowMs,
                user_code_display: undefined,
                updated_at: args.now,
            });
            if (record.environment_id) {
                const environment = await ctx.db
                    .query('connect_environments')
                    .withIndex('by_environment_id', (q) =>
                        q.eq('id', record.environment_id)
                    )
                    .unique();
                if (
                    environment?.status === 'active' &&
                    environment.activation_claimed_at === undefined
                ) {
                    await ctx.db.patch(environment._id, {
                        activation_claimed_at: args.now,
                        activation_deadline_at: undefined,
                        updated_at: args.now,
                    });
                }
            }
            return { ...record, user_code_display: undefined };
        }
        if (record.status === 'delivering') {
            if (
                record.credential_ciphertext &&
                record.credential_redeliver_until !== undefined &&
                record.credential_redeliver_until >= args.now
            ) {
                return {
                    ...record,
                    status: 'approved' as const,
                    user_code_display: undefined,
                };
            }
            await ctx.db.patch(record._id, {
                status: 'consumed',
                credential_ciphertext: undefined,
                user_code_display: undefined,
                updated_at: args.now,
            });
            return {
                ...record,
                status: 'consumed' as const,
                credential_ciphertext: undefined,
                user_code_display: undefined,
            };
        }
        return { ...record, user_code_display: undefined };
    },
});

export const getDeviceAuthorizationByUserHash = internalMutation({
    args: { user_code_hash: v.string(), now: v.number() },
    handler: async (ctx, args) => {
        const record = await ctx.db
            .query('connect_device_authorizations')
            .withIndex('by_user_code_hash', (q) =>
                q.eq('user_code_hash', args.user_code_hash)
            )
            .filter((q) =>
                q.and(
                    q.gt(q.field('expires_at'), args.now),
                    q.neq(q.field('status'), 'denied'),
                    q.neq(q.field('status'), 'expired')
                )
            )
            .order('desc')
            .first();
        if (!record || record.expires_at <= args.now) return null;
        if (record.user_code_display !== undefined) {
            await ctx.db.patch(record._id, {
                user_code_display: undefined,
                updated_at: args.now,
            });
        }
        return { ...record, user_code_display: undefined };
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
        limit_scope: v.union(v.literal('account'), v.literal('workspace')),
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
            throw connectApplicationError(
                'authorization_unavailable',
                'This connection request is no longer available.'
            );
        }
        const active =
            args.limit_scope === 'workspace'
                ? await ctx.db
                      .query('connect_environments')
                      .withIndex('by_user_workspace_status', (q) =>
                          q
                              .eq('user_id', args.user_id)
                              .eq('workspace_id', args.workspace_id)
                              .eq('status', 'active')
                      )
                      .collect()
                : await ctx.db
                      .query('connect_environments')
                      .withIndex('by_user_status', (q) =>
                          q
                              .eq('user_id', args.user_id)
                              .eq('status', 'active')
                      )
                      .collect();
        if (active.length >= args.max_active_environments) {
            throw connectApplicationError(
                'environment_limit_reached',
                `This ${args.limit_scope} already has ${args.max_active_environments} connected computers.`
            );
        }
        await ctx.db.insert('connect_environments', {
            ...args.environment,
            user_id: args.user_id,
            workspace_id: args.workspace_id,
            status: 'active',
            lifecycle_attempts: 0,
            lifecycle_next_attempt_at: 0,
            created_at: args.now,
            updated_at: args.now,
        });
        await ctx.db.patch(args.authorization_id, {
            status: 'approved',
            approved_user_id: args.user_id,
            approved_workspace_id: args.workspace_id,
            environment_id: args.environment.id,
            credential_ciphertext: args.credential_ciphertext,
            user_code_display: undefined,
            updated_at: args.now,
        });
        return { environment_id: args.environment.id };
    },
});

export const reserveDeviceAuthorization = internalMutation({
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
            control_token_hash: v.string(),
            access_credential_ciphertext: v.string(),
            tunnel_secret_ciphertext: v.string(),
        }),
        limit_scope: v.union(v.literal('account'), v.literal('workspace')),
        max_active_environments: v.number(),
        claim_token: v.string(),
        claim_until: v.number(),
        provisioning_deadline_at: v.number(),
        activation_deadline_at: v.number(),
        authorization_expires_at: v.number(),
        now: v.number(),
    },
    handler: async (ctx, args) => {
        const authorization = await ctx.db.get(args.authorization_id);
        if (
            !authorization ||
            authorization.status !== 'pending' ||
            authorization.expires_at <= args.now
        ) {
            throw connectApplicationError(
                'authorization_unavailable',
                'This connection request is no longer available.'
            );
        }
        const environments =
            args.limit_scope === 'workspace'
                ? await ctx.db
                      .query('connect_environments')
                      .withIndex('by_user_workspace_status', (q) =>
                          q
                              .eq('user_id', args.user_id)
                              .eq('workspace_id', args.workspace_id)
                      )
                      .collect()
                : await ctx.db
                      .query('connect_environments')
                      .withIndex('by_user_status', (q) =>
                          q.eq('user_id', args.user_id)
                      )
                      .collect();
        const reservedCount = environments.filter((environment) =>
            ['provisioning', 'active', 'revoking'].includes(
                environment.status
            )
        ).length;
        if (reservedCount >= args.max_active_environments) {
            throw connectApplicationError(
                'environment_limit_reached',
                `This ${args.limit_scope} already has ${args.max_active_environments} connected computers.`
            );
        }
        const environmentDocumentId = await ctx.db.insert(
            'connect_environments',
            {
                ...args.environment,
                authorization_id: args.authorization_id,
                user_id: args.user_id,
                workspace_id: args.workspace_id,
                hostname: '',
                tunnel_id: '',
                dns_record_id: '',
                status: 'provisioning',
                lifecycle_attempts: 0,
                lifecycle_next_attempt_at: args.now,
                lifecycle_claim_token: args.claim_token,
                lifecycle_claimed_until: args.claim_until,
                provisioning_deadline_at:
                    args.provisioning_deadline_at,
                activation_deadline_at: args.activation_deadline_at,
                created_at: args.now,
                updated_at: args.now,
            }
        );
        await ctx.db.patch(args.authorization_id, {
            status: 'provisioning',
            approved_user_id: args.user_id,
            approved_workspace_id: args.workspace_id,
            environment_id: args.environment.id,
            expires_at: Math.max(
                authorization.expires_at,
                args.authorization_expires_at
            ),
            user_code_display: undefined,
            updated_at: args.now,
        });
        return await ctx.db.get(environmentDocumentId);
    },
});

export const claimNextEnvironmentLifecycle = internalMutation({
    args: {
        claim_token: v.string(),
        now: v.number(),
        claim_until: v.number(),
    },
    handler: async (ctx, args) => {
        const abandoned = await ctx.db
            .query('connect_environments')
            .withIndex('by_status_activation_due', (q) =>
                q
                    .eq('status', 'active')
                    .eq('activation_claimed_at', undefined)
                    .lte('activation_deadline_at', args.now)
            )
            .first();
        if (abandoned) {
            const authorization = abandoned.authorization_id
                ? await ctx.db.get(abandoned.authorization_id)
                : null;
            if (authorization) {
                await ctx.db.patch(authorization._id, {
                    status: [
                        'pending',
                        'provisioning',
                        'approved',
                        'delivering',
                    ].includes(authorization.status)
                        ? 'expired'
                        : authorization.status,
                    credential_ciphertext: undefined,
                    updated_at: args.now,
                });
            }
            await ctx.db.patch(abandoned._id, {
                status: 'revoking',
                access_credential_ciphertext: '',
                tunnel_secret_ciphertext: undefined,
                lifecycle_next_attempt_at: args.now,
                lifecycle_claim_token: args.claim_token,
                lifecycle_claimed_until: args.claim_until,
                lifecycle_error: 'Activation deadline expired.',
                updated_at: args.now,
            });
            return {
                ...abandoned,
                status: 'revoking' as const,
                access_credential_ciphertext: '',
                tunnel_secret_ciphertext: undefined,
                lifecycle_next_attempt_at: args.now,
                lifecycle_claim_token: args.claim_token,
                lifecycle_claimed_until: args.claim_until,
                lifecycle_error: 'Activation deadline expired.',
                updated_at: args.now,
            };
        }
        const provisioning = await ctx.db
            .query('connect_environments')
            .withIndex('by_status_lifecycle_due', (q) =>
                q
                    .eq('status', 'provisioning')
                    .lte('lifecycle_next_attempt_at', args.now)
            )
            .take(100);
        const revoking = await ctx.db
            .query('connect_environments')
            .withIndex('by_status_lifecycle_due', (q) =>
                q
                    .eq('status', 'revoking')
                    .lte('lifecycle_next_attempt_at', args.now)
            )
            .take(100);
        const candidate = [...provisioning, ...revoking]
            .filter(
                (environment) =>
                    environment.lifecycle_claimed_until === undefined ||
                    environment.lifecycle_claimed_until <= args.now
            )
            .sort(
                (left, right) =>
                    (left.lifecycle_next_attempt_at ?? 0) -
                        (right.lifecycle_next_attempt_at ?? 0) ||
                    left.created_at - right.created_at
            )[0];
        if (!candidate) return null;
        await ctx.db.patch(candidate._id, {
            lifecycle_claim_token: args.claim_token,
            lifecycle_claimed_until: args.claim_until,
            updated_at: args.now,
        });
        return {
            ...candidate,
            lifecycle_claim_token: args.claim_token,
            lifecycle_claimed_until: args.claim_until,
            updated_at: args.now,
        };
    },
});

export const saveEnvironmentRelayProgress = internalMutation({
    args: {
        environment_id: v.string(),
        expected_status: v.union(
            v.literal('provisioning'),
            v.literal('revoking')
        ),
        claim_token: v.string(),
        hostname: v.optional(v.string()),
        tunnel_id: v.optional(v.string()),
        dns_record_id: v.optional(v.string()),
        relay_authenticator: v.optional(v.string()),
        now: v.number(),
    },
    handler: async (ctx, args) => {
        const environment = await ctx.db
            .query('connect_environments')
            .withIndex('by_environment_id', (q) =>
                q.eq('id', args.environment_id)
            )
            .unique();
        if (
            !environment ||
            environment.status !== args.expected_status ||
            environment.lifecycle_claim_token !== args.claim_token
        ) {
            return false;
        }
        await ctx.db.patch(environment._id, {
            ...(args.hostname !== undefined
                ? { hostname: args.hostname }
                : {}),
            ...(args.tunnel_id !== undefined
                ? { tunnel_id: args.tunnel_id }
                : {}),
            ...(args.dns_record_id !== undefined
                ? { dns_record_id: args.dns_record_id }
                : {}),
            ...(args.relay_authenticator !== undefined
                ? { relay_authenticator: args.relay_authenticator }
                : {}),
            updated_at: args.now,
        });
        return true;
    },
});

export const completeEnvironmentProvisioning = internalMutation({
    args: {
        environment_id: v.string(),
        claim_token: v.string(),
        credential_ciphertext: v.string(),
        now: v.number(),
    },
    handler: async (ctx, args) => {
        const environment = await ctx.db
            .query('connect_environments')
            .withIndex('by_environment_id', (q) =>
                q.eq('id', args.environment_id)
            )
            .unique();
        if (
            !environment ||
            environment.status !== 'provisioning' ||
            environment.lifecycle_claim_token !== args.claim_token ||
            !environment.authorization_id ||
            !environment.hostname ||
            !environment.tunnel_id ||
            !environment.dns_record_id
        ) {
            return false;
        }
        const authorization = await ctx.db.get(
            environment.authorization_id
        );
        if (!authorization || authorization.status !== 'provisioning') {
            throw new Error(
                'The reserved connection request could not be finalized.'
            );
        }
        await ctx.db.patch(environment.authorization_id, {
            status: 'approved',
            credential_ciphertext: args.credential_ciphertext,
            updated_at: args.now,
        });
        await ctx.db.patch(environment._id, {
            status: 'active',
            tunnel_secret_ciphertext: undefined,
            lifecycle_claim_token: undefined,
            lifecycle_claimed_until: undefined,
            lifecycle_next_attempt_at: 0,
            lifecycle_error: undefined,
            updated_at: args.now,
        });
        return true;
    },
});

export const beginEnvironmentRevocation = internalMutation({
    args: {
        environment_id: v.string(),
        user_id: v.id('users'),
        workspace_id: v.id('workspaces'),
        claim_token: v.string(),
        claim_until: v.number(),
        now: v.number(),
    },
    handler: async (ctx, args) => {
        const environment = await ctx.db
            .query('connect_environments')
            .withIndex('by_user_workspace_environment_id', (q) =>
                q
                    .eq('user_id', args.user_id)
                    .eq('workspace_id', args.workspace_id)
                    .eq('id', args.environment_id)
            )
            .unique();
        if (!environment) return null;
        if (environment.status === 'revoked') {
            return { claimed: false, environment };
        }
        if (
            environment.status !== 'active' &&
            environment.status !== 'revoking'
        ) {
            return null;
        }
        const canClaim =
            environment.status === 'active' ||
            environment.lifecycle_claimed_until === undefined ||
            environment.lifecycle_claimed_until <= args.now;
        if (!canClaim) {
            return { claimed: false, environment };
        }
        const authorization =
            environment.authorization_id
                ? await ctx.db.get(environment.authorization_id)
                : await ctx.db
                      .query('connect_device_authorizations')
                      .withIndex('by_environment_id', (q) =>
                          q.eq('environment_id', environment.id)
                      )
                      .unique();
        if (authorization) {
            await ctx.db.patch(authorization._id, {
                status: [
                    'pending',
                    'provisioning',
                    'approved',
                    'delivering',
                ].includes(authorization.status)
                    ? 'expired'
                    : authorization.status,
                credential_ciphertext: undefined,
                updated_at: args.now,
            });
        }
        await ctx.db.patch(environment._id, {
            status: 'revoking',
            access_credential_ciphertext: '',
            tunnel_secret_ciphertext: undefined,
            lifecycle_claim_token: args.claim_token,
            lifecycle_claimed_until: args.claim_until,
            lifecycle_next_attempt_at: args.now,
            lifecycle_error: undefined,
            updated_at: args.now,
        });
        return {
            claimed: true,
            environment: {
                ...environment,
                status: 'revoking' as const,
                access_credential_ciphertext: '',
                tunnel_secret_ciphertext: undefined,
                lifecycle_claim_token: args.claim_token,
                lifecycle_claimed_until: args.claim_until,
                lifecycle_next_attempt_at: args.now,
                lifecycle_error: undefined,
                updated_at: args.now,
            },
        };
    },
});

export const abandonEnvironmentProvisioning = internalMutation({
    args: {
        environment_id: v.string(),
        claim_token: v.string(),
        now: v.number(),
    },
    handler: async (ctx, args) => {
        const environment = await ctx.db
            .query('connect_environments')
            .withIndex('by_environment_id', (q) =>
                q.eq('id', args.environment_id)
            )
            .unique();
        if (
            !environment ||
            environment.status !== 'provisioning' ||
            environment.lifecycle_claim_token !== args.claim_token
        ) {
            return null;
        }
        if (environment.authorization_id) {
            const authorization = await ctx.db.get(
                environment.authorization_id
            );
            if (
                authorization &&
                ['provisioning', 'approved', 'delivering'].includes(
                    authorization.status
                )
            ) {
                await ctx.db.patch(environment.authorization_id, {
                    status: 'expired',
                    credential_ciphertext: undefined,
                    updated_at: args.now,
                });
            }
        }
        await ctx.db.patch(environment._id, {
            status: 'revoking',
            access_credential_ciphertext: '',
            tunnel_secret_ciphertext: undefined,
            lifecycle_next_attempt_at: args.now,
            lifecycle_error: 'Provisioning deadline expired.',
            updated_at: args.now,
        });
        return {
            ...environment,
            status: 'revoking' as const,
            access_credential_ciphertext: '',
            tunnel_secret_ciphertext: undefined,
            lifecycle_next_attempt_at: args.now,
            lifecycle_error: 'Provisioning deadline expired.',
            updated_at: args.now,
        };
    },
});

export const completeEnvironmentRevocation = internalMutation({
    args: {
        environment_id: v.string(),
        claim_token: v.string(),
        now: v.number(),
    },
    handler: async (ctx, args) => {
        const environment = await ctx.db
            .query('connect_environments')
            .withIndex('by_environment_id', (q) =>
                q.eq('id', args.environment_id)
            )
            .unique();
        if (
            !environment ||
            environment.status !== 'revoking' ||
            environment.lifecycle_claim_token !== args.claim_token
        ) {
            return false;
        }
        if (environment.authorization_id) {
            const authorization = await ctx.db.get(
                environment.authorization_id
            );
            if (authorization?.status === 'provisioning') {
                await ctx.db.patch(environment.authorization_id, {
                    status: 'expired',
                    credential_ciphertext: undefined,
                    updated_at: args.now,
                });
            } else if (authorization?.credential_ciphertext) {
                await ctx.db.patch(environment.authorization_id, {
                    credential_ciphertext: undefined,
                    updated_at: args.now,
                });
            }
        }
        await ctx.db.patch(environment._id, {
            status: 'revoked',
            access_credential_ciphertext: '',
            tunnel_secret_ciphertext: undefined,
            hostname: '',
            tunnel_id: '',
            dns_record_id: '',
            relay_authenticator: undefined,
            lifecycle_claim_token: undefined,
            lifecycle_claimed_until: undefined,
            lifecycle_next_attempt_at: 0,
            lifecycle_error: undefined,
            revoked_at: args.now,
            updated_at: args.now,
        });
        return true;
    },
});

export const recordEnvironmentLifecycleFailure = internalMutation({
    args: {
        environment_id: v.string(),
        expected_status: v.union(
            v.literal('provisioning'),
            v.literal('revoking')
        ),
        claim_token: v.string(),
        error_message: v.string(),
        next_attempt_at: v.number(),
        now: v.number(),
    },
    handler: async (ctx, args) => {
        const environment = await ctx.db
            .query('connect_environments')
            .withIndex('by_environment_id', (q) =>
                q.eq('id', args.environment_id)
            )
            .unique();
        if (
            !environment ||
            environment.status !== args.expected_status ||
            environment.lifecycle_claim_token !== args.claim_token
        ) {
            return false;
        }
        await ctx.db.patch(environment._id, {
            lifecycle_attempts:
                (environment.lifecycle_attempts ?? 0) + 1,
            lifecycle_next_attempt_at: args.next_attempt_at,
            lifecycle_claim_token: undefined,
            lifecycle_claimed_until: undefined,
            lifecycle_error: args.error_message.slice(0, 500),
            updated_at: args.now,
        });
        return true;
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
            user_code_display: undefined,
            updated_at: args.now,
        });
        return true;
    },
});

export const getEnvironmentByControlTokenHash = internalQuery({
    args: {
        user_id: v.id('users'),
        workspace_id: v.id('workspaces'),
        control_token_hash: v.string(),
    },
    handler: async (ctx, args) => {
        return await ctx.db
            .query('connect_environments')
            .withIndex('by_user_workspace_control_token_hash', (q) =>
                q
                    .eq('user_id', args.user_id)
                    .eq('workspace_id', args.workspace_id)
                    .eq('control_token_hash', args.control_token_hash)
            )
            .unique();
    },
});

export const listEnvironmentsForScope = internalQuery({
    args: {
        user_id: v.id('users'),
        workspace_id: v.id('workspaces'),
    },
    handler: async (ctx, args) => {
        return await ctx.db
            .query('connect_environments')
            .withIndex('by_user_workspace_status', (q) =>
                q
                    .eq('user_id', args.user_id)
                    .eq('workspace_id', args.workspace_id)
                    .eq('status', 'active')
            )
            .collect();
    },
});

export const revokeEnvironment = internalMutation({
    args: {
        environment_id: v.string(),
        user_id: v.id('users'),
        workspace_id: v.id('workspaces'),
        now: v.number(),
    },
    handler: async (ctx, args) => {
        const environment = await ctx.db
            .query('connect_environments')
            .withIndex('by_user_workspace_environment_id', (q) =>
                q
                    .eq('user_id', args.user_id)
                    .eq('workspace_id', args.workspace_id)
                    .eq('id', args.environment_id)
            )
            .unique();
        if (!environment || environment.status === 'revoked') return false;
        const authorization = environment.authorization_id
            ? await ctx.db.get(environment.authorization_id)
            : null;
        if (authorization) {
            await ctx.db.patch(authorization._id, {
                status: [
                    'pending',
                    'provisioning',
                    'approved',
                    'delivering',
                ].includes(authorization.status)
                    ? 'expired'
                    : authorization.status,
                credential_ciphertext: undefined,
                updated_at: args.now,
            });
        }
        await ctx.db.patch(environment._id, {
            status: 'revoked',
            access_credential_ciphertext: '',
            tunnel_secret_ciphertext: undefined,
            hostname: '',
            tunnel_id: '',
            dns_record_id: '',
            relay_authenticator: undefined,
            revoked_at: args.now,
            updated_at: args.now,
        });
        return true;
    },
});

/**
 * Upgrade migration for deployments that briefly ran the plaintext schema.
 * The Nitro provider invokes this in bounded pages before serving Connect.
 */
export const purgeLegacyUserCodeDisplays = internalMutation({
    args: {
        cursor: v.union(v.string(), v.null()),
        batch_size: v.number(),
    },
    handler: async (ctx, args) => {
        const batchSize = Math.max(
            1,
            Math.min(100, Math.floor(args.batch_size))
        );
        const page = await ctx.db
            .query('connect_device_authorizations')
            .paginate({ cursor: args.cursor, numItems: batchSize });
        let purged = 0;
        for (const record of page.page) {
            if (record.user_code_display === undefined) continue;
            await ctx.db.patch(record._id, {
                user_code_display: undefined,
            });
            purged += 1;
        }
        return {
            continue_cursor: page.continueCursor,
            is_done: page.isDone,
            purged,
        };
    },
});

/** Bounded retention sweep; callers repeat pages on later lifecycle ticks. */
export const purgeConnectRetention = internalMutation({
    args: {
        authorization_updated_before: v.number(),
        revoked_environment_updated_before: v.number(),
        batch_size: v.number(),
    },
    handler: async (ctx, args) => {
        const batchSize = Math.max(
            1,
            Math.min(500, Math.floor(args.batch_size))
        );
        const terminalStatuses = [
            'denied',
            'consumed',
            'expired',
        ] as const;
        const terminalPages = await Promise.all(
            terminalStatuses.map((status) =>
                ctx.db
                    .query('connect_device_authorizations')
                    .withIndex('by_status_updated', (q) =>
                        q
                            .eq('status', status)
                            .lte(
                                'updated_at',
                                args.authorization_updated_before
                            )
                    )
                    .take(batchSize)
            )
        );
        const authorizations = terminalPages
            .flat()
            .sort((left, right) => left.updated_at - right.updated_at)
            .slice(0, batchSize);
        for (const authorization of authorizations) {
            await ctx.db.delete(authorization._id);
        }
        const environments = await ctx.db
            .query('connect_environments')
            .withIndex('by_status_updated', (q) =>
                q
                    .eq('status', 'revoked')
                    .lte(
                        'updated_at',
                        args.revoked_environment_updated_before
                    )
            )
            .take(batchSize);
        for (const environment of environments) {
            await ctx.db.delete(environment._id);
        }
        return {
            authorizations: authorizations.length,
            environments: environments.length,
        };
    },
});

export const rotateAuthorizationCredential = internalMutation({
    args: {
        authorization_id: v.id('connect_device_authorizations'),
        expected_ciphertext: v.string(),
        replacement_ciphertext: v.string(),
        now: v.number(),
    },
    handler: async (ctx, args) => {
        const authorization = await ctx.db.get(args.authorization_id);
        if (
            !authorization ||
            authorization.credential_ciphertext !==
                args.expected_ciphertext
        ) {
            return false;
        }
        await ctx.db.patch(authorization._id, {
            credential_ciphertext: args.replacement_ciphertext,
            updated_at: args.now,
        });
        return true;
    },
});

export const rotateEnvironmentCredential = internalMutation({
    args: {
        environment_id: v.string(),
        purpose: v.union(v.literal('access'), v.literal('tunnel')),
        expected_ciphertext: v.string(),
        replacement_ciphertext: v.string(),
        now: v.number(),
    },
    handler: async (ctx, args) => {
        const environment = await ctx.db
            .query('connect_environments')
            .withIndex('by_environment_id', (q) =>
                q.eq('id', args.environment_id)
            )
            .unique();
        const current =
            args.purpose === 'access'
                ? environment?.access_credential_ciphertext
                : environment?.tunnel_secret_ciphertext;
        if (!environment || current !== args.expected_ciphertext) {
            return false;
        }
        await ctx.db.patch(environment._id, {
            ...(args.purpose === 'access'
                ? {
                      access_credential_ciphertext:
                          args.replacement_ciphertext,
                  }
                : {
                      tunnel_secret_ciphertext:
                          args.replacement_ciphertext,
                  }),
            updated_at: args.now,
        });
        return true;
    },
});
