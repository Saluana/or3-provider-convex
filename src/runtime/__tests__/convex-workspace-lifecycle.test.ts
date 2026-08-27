import { describe, expect, it, vi } from 'vitest';
import { readFileSync } from 'node:fs';

vi.mock('../../../templates/convex/_generated/server', () => ({
    internalMutation: (definition: any) => ({ ...definition, _handler: definition.handler }),
    internalQuery: (definition: any) => ({ ...definition, _handler: definition.handler }),
    mutation: (definition: any) => ({ ...definition, _handler: definition.handler }),
    query: (definition: any) => ({ ...definition, _handler: definition.handler }),
}));

import * as workspaceFunctions from '../../../templates/convex/workspaces';
import * as syncFunctions from '../../../templates/convex/sync';

type Handler = (ctx: any, args: any) => Promise<any>;
const handler = (fn: unknown): Handler => (fn as { _handler: Handler })._handler;

type Row = Record<string, any> & { _id: string };

function makeFixture(options: {
    deleted?: boolean;
    includeActiveWorkspace?: boolean;
    activeWorkspaceId?: string;
} = {}) {
    const deleted = options.deleted ?? false;
    const includeActiveWorkspace = options.includeActiveWorkspace ?? false;
    const activeWorkspaceId = options.activeWorkspaceId ?? 'ws-deleted';
    const workspaces: Row[] = [
        {
            _id: 'ws-deleted',
            name: 'Deleted Workspace',
            owner_user_id: 'user-1',
            created_at: 1,
            deleted,
            ...(deleted ? { deleted_at: 2 } : {}),
        },
    ];
    const memberships: Row[] = [{
        _id: 'member-deleted',
        workspace_id: 'ws-deleted',
        user_id: 'user-1',
        role: 'owner',
        created_at: 1,
    }];
    if (includeActiveWorkspace) {
        workspaces.push({
            _id: 'ws-active',
            name: 'Active Workspace',
            owner_user_id: 'user-1',
            created_at: 3,
            deleted: false,
        });
        memberships.push({
            _id: 'member-active',
            workspace_id: 'ws-active',
            user_id: 'user-1',
            role: 'editor',
            created_at: 3,
        });
    }

    const tables: Record<string, Row[]> = {
        auth_accounts: [{
            _id: 'account-1',
            provider: 'clerk',
            provider_user_id: 'subject-1',
            user_id: 'user-1',
        }],
        users: [{
            _id: 'user-1',
            email: 'owner@example.test',
            active_workspace_id: activeWorkspaceId,
        }],
        workspaces,
        workspace_members: memberships,
        admin_users: [{ _id: 'admin-1', user_id: 'user-1', created_at: 1 }],
        server_version_counter: [],
        auth_invites: [],
    };
    let nextWorkspace = 1;

    const makeQuery = (table: string) => {
        let predicates: Array<(row: Row) => boolean> = [];
        let direction: 'asc' | 'desc' = 'asc';
        const builder: any = {
            withIndex: (_index: string, build: (q: any) => unknown) => {
                const chain: any = {
                    eq: (field: string, value: unknown) => {
                        predicates.push((row) => row[field] === value);
                        return chain;
                    },
                    gt: (field: string, value: unknown) => {
                        predicates.push((row) => row[field] > (value as number));
                        return chain;
                    },
                };
                build(chain);
                return builder;
            },
            filter: () => builder,
            order: (nextDirection: 'asc' | 'desc') => {
                direction = nextDirection;
                return builder;
            },
            first: async () => builder.materialize()[0] ?? null,
            collect: async () => builder.materialize(),
            take: async (limit: number) => builder.materialize().slice(0, limit),
            materialize: () => {
                const rows = (tables[table] ?? []).filter((row) => predicates.every((predicate) => predicate(row)));
                return rows.sort((left, right) =>
                    direction === 'asc'
                        ? (left.created_at ?? 0) - (right.created_at ?? 0)
                        : (right.created_at ?? 0) - (left.created_at ?? 0)
                );
            },
        };
        return builder;
    };

    const ctx = {
        auth: {
            getUserIdentity: async () => ({
                issuer: 'https://clerk.example.test',
                subject: 'subject-1',
                email: 'owner@example.test',
            }),
        },
        db: {
            query: (table: string) => makeQuery(table),
            get: async (id: string) => Object.values(tables).flat().find((row) => row._id === id) ?? null,
            insert: async (table: string, value: Omit<Row, '_id'>) => {
                const id = table === 'workspaces' ? `ws-new-${nextWorkspace++}` : `${table}-new`;
                tables[table] ??= [];
                tables[table].push({ _id: id, ...value });
                return id;
            },
            patch: async (id: string, value: Record<string, unknown>) => {
                const row = Object.values(tables).flat().find((candidate) => candidate._id === id);
                if (!row) throw new Error(`Missing row ${id}`);
                for (const [key, next] of Object.entries(value)) {
                    if (next === undefined) delete row[key];
                    else row[key] = next;
                }
            },
        },
    } as any;

    return { ctx, tables };
}

describe('Convex workspace lifecycle authorization', () => {
    it('lists active member workspaces but hides soft-deleted ones', async () => {
        const active = makeFixture({ deleted: false });
        await expect(handler(workspaceFunctions.listMyWorkspaces)(active.ctx, {}))
            .resolves.toEqual([expect.objectContaining({ _id: 'ws-deleted', is_active: true })]);

        const deleted = makeFixture({ deleted: true });
        await expect(handler(workspaceFunctions.listMyWorkspaces)(deleted.ctx, {})).resolves.toEqual([]);
    });

    it('never resolves or selects a deleted workspace, and falls back to an active member', async () => {
        const deleted = makeFixture({ deleted: true });
        await expect(handler(workspaceFunctions.resolveSession)(deleted.ctx, {
            provider: 'clerk', provider_user_id: 'subject-1',
        })).resolves.toBeNull();
        await expect(handler(workspaceFunctions.setActive)(deleted.ctx, {
            workspace_id: 'ws-deleted',
        })).rejects.toThrow('Forbidden');

        const fallback = makeFixture({ deleted: true, includeActiveWorkspace: true, activeWorkspaceId: 'ws-deleted' });
        await expect(handler(workspaceFunctions.ensure)(fallback.ctx, {
            provider: 'clerk', provider_user_id: 'subject-1',
            email: 'owner@example.test',
        })).resolves.toMatchObject({ id: 'ws-active' });
    });

    it('does not enumerate invites for a deleted workspace', async () => {
        const fixture = makeFixture({ deleted: true });
        fixture.tables.auth_invites.push({
            _id: 'invite-1',
            workspace_id: 'ws-deleted',
            email: 'owner@example.test',
            token_hash: 'token-hash',
            role: 'viewer',
            status: 'pending',
            expires_at: Date.now() + 60_000,
            created_at: 1,
        });

        await expect(handler(workspaceFunctions.validateInviteInternal)(fixture.ctx, {
            workspace_id: 'ws-deleted',
            email: 'owner@example.test',
            token_hash: 'token-hash',
        })).resolves.toEqual({ ok: false, reason: 'not_found' });
        await expect(handler(workspaceFunctions.consumeInvite)(fixture.ctx, {
            workspace_id: 'ws-deleted',
            email: 'owner@example.test',
            token_hash: 'token-hash',
        })).resolves.toEqual({ ok: false, reason: 'not_found' });
    });

    it('keeps privileged deleted-workspace inspection and restore available', async () => {
        const fixture = makeFixture({ deleted: true });
        const adminSource = readFileSync(new URL('../../../templates/convex/admin.ts', import.meta.url), 'utf8');
        expect(adminSource).toContain('if (!include_deleted)');
        expect(adminSource).toContain('export const getWorkspace = query({');
        expect(adminSource).toContain('export const restoreWorkspace = mutation({');
        expect(adminSource).toContain('deleted: false');
        await expect(handler(workspaceFunctions.listMyWorkspaces)(fixture.ctx, {}))
            .resolves.toEqual([]);
    });

    it('denies sync read access for a deleted workspace through the central role guard', async () => {
        const fixture = makeFixture({ deleted: true });
        await expect(handler(syncFunctions.pull)(fixture.ctx, {
            workspace_id: 'ws-deleted', cursor: 0, limit: 1,
        })).rejects.toThrow('Forbidden');
    });
});
