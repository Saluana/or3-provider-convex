import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { gunzipSync } from 'node:zlib';
import { describe, expect, it, vi } from 'vitest';
import {
    verifyAuthorizationContract,
    type AuthorizationCaseId,
} from '~~/shared/testing/contracts/authorization';
import {
    requireCallerSubject,
    requireCallerUserId,
    requireInviteAcceptance,
    requireWorkspaceRole,
} from '../../../templates/convex/authz';

const templateRootUrl = new URL('../../../templates/convex/', import.meta.url);
const syncFunctions = await import(
    /* @vite-ignore */ new URL('sync.ts', templateRootUrl).href
);
const userFunctions = await import(
    /* @vite-ignore */ new URL('users.ts', templateRootUrl).href
);
const workspaceFunctions = await import(
    /* @vite-ignore */ new URL('workspaces.ts', templateRootUrl).href
);
const backgroundJobFunctions = await import(
    /* @vite-ignore */ new URL('backgroundJobs.ts', templateRootUrl).href
);
const notificationFunctions = await import(
    /* @vite-ignore */ new URL('notifications.ts', templateRootUrl).href
);
const rateLimitFunctions = await import(
    /* @vite-ignore */ new URL('rateLimits.ts', templateRootUrl).href
);
const webhookFunctions = await import(
    /* @vite-ignore */ new URL('webhooks.ts', templateRootUrl).href
);

type Role = 'owner' | 'editor' | 'viewer';

type RegisteredFunction = {
    _handler: (ctx: any, args: any) => Promise<any>;
    isInternal?: boolean;
    isPublic?: boolean;
};

function registered(value: unknown): RegisteredFunction {
    return value as RegisteredFunction;
}

function makeCtx(options: {
    identity?: {
        subject: string;
        issuer?: string;
        email?: string;
        or3_server?: boolean;
    } | null;
    userId?: string;
    role?: Role;
}) {
    const userId = options.userId ?? 'user-1';
    const query = vi.fn((table: string) => ({
        withIndex: (_index: string, build: (q: { eq: () => unknown }) => unknown) => {
            const chain = {
                eq: vi.fn(() => chain),
            };
            build(chain);
            return {
                first: vi.fn(async () => {
                    if (table === 'auth_accounts') {
                        return { _id: 'account-1', user_id: userId };
                    }
                    if (table === 'workspace_members' && options.role) {
                        return { _id: 'member-1', user_id: userId, role: options.role };
                    }
                    return null;
                }),
            };
        },
    }));

    return {
        auth: {
            getUserIdentity: vi.fn(async () => options.identity ?? null),
        },
        db: { query },
    } as any;
}

function makeDirectCtx(options: {
    identity?: {
        subject: string;
        issuer?: string;
        email?: string;
        or3_server?: boolean;
    } | null;
    userId?: string;
    role?: Role;
    invite?: {
        workspace_id: string;
        status: 'pending' | 'accepted' | 'revoked' | 'expired';
    };
}) {
    const userId = options.userId ?? 'user-1';
    const inserted: Array<{ table: string; value: Record<string, unknown> }> = [];
    const query = vi.fn((table: string) => ({
        withIndex: (_index: string, build: (q: { eq: () => unknown; gt: () => unknown }) => unknown) => {
            const chain = {
                eq: vi.fn(() => chain),
                gt: vi.fn(() => chain),
            };
            build(chain);
            return {
                first: vi.fn(async () => {
                    if (table === 'auth_accounts') {
                        return { _id: 'account-1', user_id: userId };
                    }
                    if (table === 'workspace_members' && options.role) {
                        return { _id: 'member-1', user_id: userId, role: options.role };
                    }
                    return null;
                }),
                collect: vi.fn(async () => []),
                order: vi.fn(() => ({
                    first: vi.fn(async () => null),
                    take: vi.fn(async () => []),
                })),
                take: vi.fn(async () => []),
            };
        },
        order: vi.fn(() => ({
            first: vi.fn(async () => null),
            take: vi.fn(async () => []),
        })),
        take: vi.fn(async () => []),
    }));

    return {
        ctx: {
            auth: {
                getUserIdentity: vi.fn(async () => options.identity ?? null),
            },
            db: {
                query,
                insert: vi.fn(async (table: string, value: Record<string, unknown>) => {
                    inserted.push({ table, value });
                    return `${table}-1`;
                }),
                get: vi.fn(async () => options.invite ?? null),
                patch: vi.fn(async () => undefined),
            },
        } as any,
        inserted,
    };
}

function makeNotificationSyncCtx(changes: Array<Record<string, unknown>> = []) {
    const inserted: Array<{ table: string; value: Record<string, unknown> }> = [];
    const patch = vi.fn(async () => undefined);
    const query = vi.fn((table: string) => ({
        withIndex: (
            _index: string,
            build: (q: { eq: () => unknown; gt: () => unknown }) => unknown
        ) => {
            const chain = {
                eq: vi.fn(() => chain),
                gt: vi.fn(() => chain),
            };
            build(chain);
            return {
                first: vi.fn(async () => {
                    if (table === 'auth_accounts') {
                        return { _id: 'account-1', user_id: 'user-1' };
                    }
                    if (table === 'workspace_members') {
                        return { _id: 'member-1', user_id: 'user-1', role: 'editor' };
                    }
                    if (table === 'server_version_counter') {
                        return { _id: 'counter-1', value: 0 };
                    }
                    return null;
                }),
                order: vi.fn(() => ({
                    take: vi.fn(async () => changes),
                })),
            };
        },
    }));

    return {
        ctx: {
            auth: {
                getUserIdentity: vi.fn(async () => ({
                    subject: 'subject-1',
                    issuer: 'https://clerk.example.test',
                })),
            },
            db: {
                query,
                insert: vi.fn(async (table: string, value: Record<string, unknown>) => {
                    inserted.push({ table, value });
                    return `${table}-1`;
                }),
                patch,
            },
        } as any,
        inserted,
    };
}

describe('Convex authorization boundary', () => {
    it('executes the shared subject, role, and invite-email contract', async () => {
        const supported = new Set<AuthorizationCaseId>([
            'unauthenticated', 'subject-match', 'subject-mismatch', 'viewer-read',
            'viewer-write', 'editor-write', 'owner-manage',
            'invite-email-match', 'invite-email-mismatch',
        ]);
        const result = await verifyAuthorizationContract({
            name: 'convex-public',
            supports: supported,
            async evaluate(id) {
                try {
                    if (id === 'unauthenticated') {
                        await requireCallerSubject(makeCtx({ identity: null }), {
                            provider: 'clerk', providerUserId: 'subject-1',
                        });
                    } else if (id === 'subject-match' || id === 'subject-mismatch') {
                        await requireCallerSubject(makeCtx({ identity: {
                            subject: 'subject-1', issuer: 'https://clerk.example.test',
                        } }), {
                            provider: 'clerk',
                            providerUserId: id === 'subject-match' ? 'subject-1' : 'subject-2',
                        });
                    } else if (id.startsWith('invite-email')) {
                        await requireInviteAcceptance(makeCtx({
                            identity: {
                                subject: 'subject-1', issuer: 'https://clerk.example.test',
                                email: id === 'invite-email-match' ? 'Invited@Example.Test' : 'other@example.test',
                            },
                            userId: 'user-1',
                        }), 'invited@example.test');
                    } else {
                        const role = id.startsWith('viewer') ? 'viewer'
                            : id === 'editor-write' ? 'editor' : 'owner';
                        const allowed = id.endsWith('read')
                            ? new Set<Role>(['owner', 'editor', 'viewer'])
                            : id === 'owner-manage'
                                ? new Set<Role>(['owner'])
                                : new Set<Role>(['owner', 'editor']);
                        await requireWorkspaceRole(makeCtx({
                            identity: { subject: `subject-${role}`, issuer: 'https://clerk.example.test' },
                            role,
                        }), 'workspace-1' as any, allowed);
                    }
                    return 'allow';
                } catch {
                    return 'deny';
                }
            },
        });
        expect(result.executed).toEqual(Array.from(supported));
    });
    it('fails closed for direct unauthenticated identity lookups', async () => {
        const ctx = makeCtx({ identity: null });

        await expect(
            requireCallerSubject(ctx, {
                provider: 'clerk',
                providerUserId: 'subject-1',
            })
        ).rejects.toThrow('Unauthorized');
        expect(ctx.db.query).not.toHaveBeenCalled();
    });

    it('rejects a direct caller resolving another provider subject', async () => {
        const ctx = makeCtx({
            identity: {
                subject: 'subject-1',
                issuer: 'https://clerk.example.test',
            },
        });

        await expect(
            requireCallerSubject(ctx, {
                provider: 'clerk',
                providerUserId: 'subject-2',
            })
        ).rejects.toThrow('Forbidden');
    });

    it('does not trust a server marker from an external identity issuer', async () => {
        const ctx = makeCtx({
            identity: {
                subject: 'subject-1',
                issuer: 'https://clerk.example.test',
                or3_server: true,
            },
        });

        await expect(
            requireCallerSubject(
                ctx,
                { provider: 'clerk', providerUserId: 'subject-2' },
                { allowTrustedServer: true }
            )
        ).rejects.toThrow('Forbidden');
    });

    it('allows the trusted OR3 Clerk server identity marker', async () => {
        const ctx = makeCtx({
            identity: {
                subject: 'server-subject',
                issuer: 'https://clerk.or3.ai',
                or3_server: true,
            },
        });

        await expect(
            requireCallerSubject(
                ctx,
                { provider: 'clerk', providerUserId: 'another-subject' },
                { allowTrustedServer: true }
            )
        ).resolves.toMatchObject({ subject: 'server-subject' });
    });

    it('allows an authenticated caller to resolve only its own identity mapping', async () => {
        const ctx = makeCtx({
            identity: {
                subject: 'subject-1',
                issuer: 'https://clerk.example.test',
            },
            userId: 'user-1',
        });

        await expect(
            requireCallerSubject(ctx, {
                provider: 'clerk',
                providerUserId: 'subject-1',
            })
        ).resolves.toMatchObject({ subject: 'subject-1' });
        await expect(requireCallerUserId(ctx, 'user-1' as any)).resolves.toBeUndefined();
        await expect(requireCallerUserId(ctx, 'user-2' as any)).rejects.toThrow('Forbidden');
    });

    it('blocks viewer sync and invite writes', async () => {
        const ctx = makeCtx({
            identity: {
                subject: 'subject-viewer',
                issuer: 'https://clerk.example.test',
            },
            role: 'viewer',
        });

        await expect(
            requireWorkspaceRole(ctx, 'workspace-1' as any, new Set(['owner', 'editor']))
        ).rejects.toThrow('Forbidden');
        await expect(
            requireWorkspaceRole(ctx, 'workspace-1' as any, new Set(['owner']))
        ).rejects.toThrow('Forbidden');
    });

    it('rejects cross-user invite consumption and derives the accepted user', async () => {
        const crossUserCtx = makeCtx({
            identity: {
                subject: 'subject-attacker',
                issuer: 'https://clerk.example.test',
                email: 'attacker@example.test',
            },
            userId: 'user-attacker',
        });
        await expect(
            requireInviteAcceptance(crossUserCtx, 'invited@example.test')
        ).rejects.toThrow('Forbidden');

        const directCrossUser = makeDirectCtx({
            identity: {
                subject: 'subject-attacker',
                issuer: 'https://clerk.example.test',
                email: 'attacker@example.test',
            },
            userId: 'user-attacker',
        });
        await expect(registered(workspaceFunctions.consumeInvite)._handler(
            directCrossUser.ctx,
            {
                workspace_id: 'workspace-1',
                email: 'invited@example.test',
                token_hash: 'hash',
            }
        )).rejects.toThrow('Forbidden');

        const invitedCtx = makeCtx({
            identity: {
                subject: 'subject-invited',
                issuer: 'https://clerk.example.test',
                email: 'Invited@Example.Test',
            },
            userId: 'user-invited',
        });
        await expect(
            requireInviteAcceptance(invitedCtx, 'invited@example.test')
        ).resolves.toBe('user-invited');
    });

    it.each([
        ['editor', new Set<Role>(['owner', 'editor'])],
        ['owner', new Set<Role>(['owner'])],
    ] as const)('allows authorized %s operations', async (role, allowedRoles) => {
        const ctx = makeCtx({
            identity: {
                subject: `subject-${role}`,
                issuer: 'https://clerk.example.test',
            },
            role,
        });

        await expect(
            requireWorkspaceRole(ctx, 'workspace-1' as any, allowedRoles)
        ).resolves.toMatchObject({ role, userId: 'user-1' });
    });

    it('registers identity and session enumeration as internal-only functions', () => {
        for (const fn of [
            userFunctions.getAuthAccountByProvider,
            userFunctions.getAuthAccountByUserId,
            workspaceFunctions.resolveSession,
        ]) {
            expect(registered(fn).isInternal).toBe(true);
            expect(registered(fn).isPublic).not.toBe(true);
        }
    });

    it('registers every auxiliary persistence function as internal-only', () => {
        const functions = [
            backgroundJobFunctions.create,
            backgroundJobFunctions.get,
            backgroundJobFunctions.update,
            backgroundJobFunctions.complete,
            backgroundJobFunctions.fail,
            backgroundJobFunctions.abort,
            backgroundJobFunctions.checkAborted,
            backgroundJobFunctions.cleanup,
            backgroundJobFunctions.getActiveCount,
            notificationFunctions.create,
            notificationFunctions.getByUser,
            notificationFunctions.markRead,
            rateLimitFunctions.checkAndRecord,
            rateLimitFunctions.getStats,
            rateLimitFunctions.cleanup,
            webhookFunctions.createWebhook,
            webhookFunctions.updateWebhook,
            webhookFunctions.deleteWebhook,
            webhookFunctions.getWebhook,
            webhookFunctions.listWebhooks,
            webhookFunctions.listAdminWebhooks,
            webhookFunctions.listWebhooksByEvent,
            webhookFunctions.listWebhooksByCustomHook,
            webhookFunctions.listActiveCustomHookNames,
            webhookFunctions.updateWebhookHealth,
            webhookFunctions.disableAllWebhooks,
            webhookFunctions.createDeliveryLog,
            webhookFunctions.updateDeliveryLog,
            webhookFunctions.getDeliveryLogs,
            webhookFunctions.getRecentTerminalDeliveries,
            webhookFunctions.claimPendingDeliveries,
            webhookFunctions.resetStaleInFlightDeliveries,
            webhookFunctions.cancelDeliveriesByWebhook,
            webhookFunctions.deleteDeliveryLogsByWebhook,
            webhookFunctions.purgeExpiredLogs,
        ];

        for (const fn of functions) {
            expect(registered(fn).isInternal).toBe(true);
            expect(registered(fn).isPublic).not.toBe(true);
        }
    });

    it('rejects the legacy background-job wildcard owner bypass', async () => {
        const get = vi.fn(async () => ({
            _id: 'job-1',
            user_id: 'user-1',
            thread_id: 'thread-1',
            message_id: 'message-1',
            model: 'model-1',
            kind: 'chat',
            status: 'streaming',
            content: '',
            chunks_received: 0,
            started_at: Date.now(),
        }));
        const patch = vi.fn(async () => undefined);
        const ctx = { db: { get, patch } } as any;

        await expect(registered(backgroundJobFunctions.get)._handler(ctx, {
            job_id: 'job-1',
            user_id: '*',
        })).resolves.toBeNull();
        await expect(registered(backgroundJobFunctions.abort)._handler(ctx, {
            job_id: 'job-1',
            user_id: '*',
        })).resolves.toBe(false);
        expect(patch).not.toHaveBeenCalled();
    });

    it('subject-binds notification changes crossing the public sync boundary', async () => {
        const changes = [
            {
                server_version: 1,
                table_name: 'threads',
                pk: 'thread-1',
                op: 'put',
                payload: { id: 'thread-1' },
                clock: 1,
                hlc: '1:a',
                device_id: 'device-1',
                op_id: 'op-thread',
            },
            {
                server_version: 2,
                table_name: 'notifications',
                pk: 'notification-own',
                op: 'put',
                payload: { id: 'notification-own', user_id: 'user-1' },
                clock: 2,
                hlc: '2:a',
                device_id: 'server',
                op_id: 'op-own',
            },
            {
                server_version: 3,
                table_name: 'notifications',
                pk: 'notification-other',
                op: 'put',
                payload: { id: 'notification-other', user_id: 'user-2' },
                clock: 3,
                hlc: '3:a',
                device_id: 'server',
                op_id: 'op-other',
            },
        ];
        const readCtx = makeNotificationSyncCtx(changes);

        const pulled = await registered(syncFunctions.pull)._handler(readCtx.ctx, {
            workspace_id: 'workspace-1',
            cursor: 0,
            limit: 10,
        });
        expect(pulled.changes.map((change: { pk: string }) => change.pk)).toEqual([
            'thread-1',
            'notification-own',
        ]);
        expect(pulled.nextCursor).toBe(3);

        const watched = await registered(syncFunctions.watchChanges)._handler(
            readCtx.ctx,
            {
                workspace_id: 'workspace-1',
                cursor: 0,
                limit: 10,
            }
        );
        expect(watched.changes.map((change: { pk: string }) => change.pk)).toEqual([
            'thread-1',
            'notification-own',
        ]);
        expect(watched.latestVersion).toBe(3);

        const writeCtx = makeNotificationSyncCtx();
        const pushed = await registered(syncFunctions.push)._handler(writeCtx.ctx, {
            workspace_id: 'workspace-1',
            ops: [
                {
                    op_id: 'op-cross-user',
                    table_name: 'notifications',
                    operation: 'put',
                    pk: 'notification-cross-user',
                    payload: {
                        id: 'notification-cross-user',
                        user_id: 'user-2',
                    },
                    clock: 1,
                    hlc: '1:device',
                    device_id: 'device-1',
                },
            ],
        });
        expect(pushed.results).toEqual([
            expect.objectContaining({
                opId: 'op-cross-user',
                success: false,
                error: expect.stringContaining('notification owner mismatch'),
            }),
        ]);
        expect(writeCtx.inserted).toEqual([]);

        const ownWriteCtx = makeNotificationSyncCtx();
        const ownPush = await registered(syncFunctions.push)._handler(
            ownWriteCtx.ctx,
            {
                workspace_id: 'workspace-1',
                ops: [
                    {
                        op_id: 'op-own-user',
                        table_name: 'notifications',
                        operation: 'put',
                        pk: 'notification-own-user',
                        payload: {
                            id: 'notification-own-user',
                            type: 'test',
                            title: 'Own notification',
                            deleted: false,
                        },
                        clock: 1,
                        hlc: '1:device',
                        device_id: 'device-1',
                    },
                ],
            }
        );
        expect(ownPush.results).toEqual([
            expect.objectContaining({
                opId: 'op-own-user',
                success: true,
                payload: expect.objectContaining({ user_id: 'user-1' }),
            }),
        ]);
        expect(ownWriteCtx.inserted).toContainEqual({
            table: 'notifications',
            value: expect.objectContaining({ user_id: 'user-1' }),
        });
    });

    it('fails closed when unauthenticated callers invoke invite handlers directly', async () => {
        const { ctx } = makeDirectCtx({ identity: null });

        await expect(registered(workspaceFunctions.createInvite)._handler(ctx, {
            workspace_id: 'workspace-1',
            email: 'invitee@example.test',
            role: 'viewer',
            token_hash: 'hash',
            expires_at: Date.now() + 60_000,
        })).rejects.toThrow('Unauthorized');
        await expect(registered(workspaceFunctions.listInvites)._handler(ctx, {
            workspace_id: 'workspace-1',
        })).rejects.toThrow('Unauthorized');
        await expect(registered(workspaceFunctions.revokeInvite)._handler(ctx, {
            workspace_id: 'workspace-1',
            invite_id: 'invite-1',
        })).rejects.toThrow('Unauthorized');
    });

    it('denies viewer invite management and derives the inviter for an owner', async () => {
        const viewer = makeDirectCtx({
            identity: {
                subject: 'viewer-subject',
                issuer: 'https://clerk.example.test',
            },
            userId: 'viewer-user',
            role: 'viewer',
        });
        await expect(registered(workspaceFunctions.createInvite)._handler(viewer.ctx, {
            workspace_id: 'workspace-1',
            email: 'invitee@example.test',
            role: 'viewer',
            token_hash: 'hash',
            expires_at: Date.now() + 60_000,
        })).rejects.toThrow('Forbidden');
        await expect(registered(workspaceFunctions.listInvites)._handler(viewer.ctx, {
            workspace_id: 'workspace-1',
        })).rejects.toThrow('Forbidden');
        await expect(registered(workspaceFunctions.revokeInvite)._handler(viewer.ctx, {
            workspace_id: 'workspace-1',
            invite_id: 'invite-1',
        })).rejects.toThrow('Forbidden');

        const owner = makeDirectCtx({
            identity: {
                subject: 'owner-subject',
                issuer: 'https://clerk.example.test',
            },
            userId: 'owner-user',
            role: 'owner',
            invite: {
                workspace_id: 'workspace-1',
                status: 'pending',
            },
        });
        await expect(registered(workspaceFunctions.createInvite)._handler(owner.ctx, {
            workspace_id: 'workspace-1',
            email: 'INVITEE@example.test',
            role: 'editor',
            token_hash: 'hash',
            expires_at: Date.now() + 60_000,
        })).resolves.toEqual({ invite_id: 'auth_invites-1' });
        expect(owner.inserted).toContainEqual({
            table: 'auth_invites',
            value: expect.objectContaining({
                email: 'invitee@example.test',
                role: 'editor',
                invited_by_user_id: 'owner-user',
            }),
        });
        await expect(registered(workspaceFunctions.listInvites)._handler(owner.ctx, {
            workspace_id: 'workspace-1',
        })).resolves.toEqual([]);
        await expect(registered(workspaceFunctions.revokeInvite)._handler(owner.ctx, {
            workspace_id: 'workspace-1',
            invite_id: 'invite-1',
        })).resolves.toEqual({ ok: true });
        expect(owner.ctx.db.patch).toHaveBeenCalledWith(
            'invite-1',
            expect.objectContaining({ status: 'revoked' })
        );
    });

    it('lets viewers pull but rejects viewer pushes and keeps GC internal', async () => {
        const viewer = makeDirectCtx({
            identity: {
                subject: 'viewer-subject',
                issuer: 'https://clerk.example.test',
            },
            userId: 'viewer-user',
            role: 'viewer',
        });

        await expect(registered(syncFunctions.pull)._handler(viewer.ctx, {
            workspace_id: 'workspace-1',
            cursor: 0,
            limit: 10,
        })).resolves.toMatchObject({ changes: [] });
        await expect(registered(syncFunctions.push)._handler(viewer.ctx, {
            workspace_id: 'workspace-1',
            ops: [],
        })).rejects.toThrow('Forbidden');

        for (const fn of [syncFunctions.gcTombstones, syncFunctions.gcChangeLog]) {
            expect(registered(fn).isInternal).toBe(true);
            expect(registered(fn).isPublic).not.toBe(true);
        }
    });

    it('wires the authorization guards into every affected public template', () => {
        const templateRoot = fileURLToPath(new URL('../../../templates/convex/', import.meta.url));
        const users = readFileSync(`${templateRoot}/users.ts`, 'utf8');
        const workspaces = readFileSync(`${templateRoot}/workspaces.ts`, 'utf8');
        const sync = readFileSync(`${templateRoot}/sync.ts`, 'utf8');
        const backgroundJobs = readFileSync(`${templateRoot}/backgroundJobs.ts`, 'utf8');
        const notifications = readFileSync(`${templateRoot}/notifications.ts`, 'utf8');
        const rateLimits = readFileSync(`${templateRoot}/rateLimits.ts`, 'utf8');
        const webhooks = readFileSync(`${templateRoot}/webhooks.ts`, 'utf8');

        expect(users).toContain('await requireCallerSubject(');
        expect(users).toContain('await requireCallerUserId(');
        expect(users.match(/= internalQuery\(\{/g)?.length).toBe(2);
        expect(workspaces).toContain('export const resolveSession = internalQuery({');
        expect(workspaces).toContain('export const listInvitesInternal = internalQuery({');
        expect(workspaces.match(/requireWorkspaceRole\(/g)?.length).toBeGreaterThanOrEqual(3);
        expect(workspaces).toContain('await requireInviteAcceptance(ctx, email)');
        expect(workspaces).not.toContain('invited_by_user_id: v.id');
        expect(workspaces).not.toContain('accepted_user_id: v.id');
        expect(sync).toContain(
            'const callerUserId = await requireSyncWriteAccess(ctx, args.workspace_id)'
        );
        expect(sync).toContain('scopeNotificationWrite(');
        expect(sync).toContain('isChangeVisibleToUser(');
        expect(sync).toContain('export const gcTombstones = internalMutation({');
        expect(sync).toContain('export const gcChangeLog = internalMutation({');
        expect(backgroundJobs.match(/= internal(?:Mutation|Query)\(\{/g)?.length).toBe(9);
        expect(backgroundJobs).not.toContain("args.user_id !== '*'");
        expect(notifications.match(/= internal(?:Mutation|Query)\(\{/g)?.length).toBe(3);
        expect(rateLimits.match(/= internal(?:Mutation|Query)\(\{/g)?.length).toBe(3);
        expect(webhooks.match(/= internal(?:Mutation|Query)\(\{/g)?.length).toBe(20);
    });

    it('ships the guarded templates in the generated package asset', () => {
        const packedPath = fileURLToPath(
            new URL('../../../templates/convex.pack.json.gz', import.meta.url)
        );
        const payload = JSON.parse(gunzipSync(readFileSync(packedPath)).toString('utf8')) as {
            files: Record<string, string>;
        };

        expect(payload.files['authz.ts']).toContain('requireInviteAcceptance');
        expect(payload.files['users.ts']).toContain('= internalQuery({');
        expect(payload.files['workspaces.ts']).toContain('listInvitesInternal = internalQuery({');
        expect(payload.files['sync.ts']).toContain('gcTombstones = internalMutation({');
        expect(payload.files['backgroundJobs.ts']).not.toContain("args.user_id !== '*'");
        expect(payload.files['backgroundJobs.ts'].match(/= internal(?:Mutation|Query)\(\{/g)?.length).toBe(9);
        expect(payload.files['notifications.ts'].match(/= internal(?:Mutation|Query)\(\{/g)?.length).toBe(3);
        expect(payload.files['rateLimits.ts'].match(/= internal(?:Mutation|Query)\(\{/g)?.length).toBe(3);
        expect(payload.files['webhooks.ts'].match(/= internal(?:Mutation|Query)\(\{/g)?.length).toBe(20);
    });
});
