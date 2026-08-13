/**
 * Server-authored sync writes that must mint UUID op_ids, allocate
 * server_version, and append change_log the same way client push does.
 */
import type { MutationCtx } from './_generated/server';
import type { Id, TableNames } from './_generated/dataModel';

const UUID_RE =
    /^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;

const TABLE_INDEX: Record<
    ServerAuthoredTable,
    { table: TableNames; indexName: string; pkField: 'id' | 'hash' }
> = {
    notifications: {
        table: 'notifications',
        indexName: 'by_workspace_id',
        pkField: 'id',
    },
    file_meta: {
        table: 'file_meta',
        indexName: 'by_workspace_hash',
        pkField: 'hash',
    },
    kv: { table: 'kv', indexName: 'by_workspace_id', pkField: 'id' },
};

export type ServerAuthoredTable = 'notifications' | 'file_meta' | 'kv';

export function isSyncUuid(value: string): boolean {
    return UUID_RE.test(value);
}

const nowSec = (): number => Math.floor(Date.now() / 1000);

async function allocateServerVersion(
    ctx: MutationCtx,
    workspaceId: Id<'workspaces'>
): Promise<number> {
    const existing = await ctx.db
        .query('server_version_counter')
        .withIndex('by_workspace', (q) => q.eq('workspace_id', workspaceId))
        .first();
    if (existing) {
        const next = existing.value + 1;
        await ctx.db.patch(existing._id, { value: next });
        return next;
    }
    await ctx.db.insert('server_version_counter', {
        workspace_id: workspaceId,
        value: 1,
    });
    return 1;
}

function stripReserved(payload: Record<string, unknown>): Record<string, unknown> {
    const next = { ...payload };
    delete next._id;
    delete next._creationTime;
    delete next.workspace_id;
    return next;
}

export async function applyServerAuthoredOp(
    ctx: MutationCtx,
    workspaceId: Id<'workspaces'>,
    op: {
        table: ServerAuthoredTable;
        operation: 'put' | 'delete';
        pk: string;
        payload?: Record<string, unknown>;
    }
): Promise<{ opId: string; serverVersion: number }> {
    const tableInfo = TABLE_INDEX[op.table];
    const now = nowSec();
    const opId = crypto.randomUUID();
    const hlc = `${now}:server:${opId.slice(0, 8)}`;
    const serverVersion = await allocateServerVersion(ctx, workspaceId);
    const payload = stripReserved(op.payload ?? {});

    type IndexQueryBuilder = {
        eq: (field: string, value: unknown) => IndexQueryBuilder;
    };
    type ConvexDoc = {
        _id: Id<TableNames>;
    } & Record<string, unknown>;
    type QueryByIndex = {
        withIndex: (
            index: string,
            cb: (q: IndexQueryBuilder) => IndexQueryBuilder
        ) => { first: () => Promise<ConvexDoc | null> };
    };

    const existing = await (ctx.db.query(tableInfo.table) as unknown as QueryByIndex)
        .withIndex(tableInfo.indexName, (q) =>
            q.eq('workspace_id', workspaceId).eq(tableInfo.pkField, op.pk)
        )
        .first();

    const stamps = {
        clock: now,
        hlc,
        op_id: opId,
        server_version: serverVersion,
        updated_at: now,
    };

    if (op.operation === 'delete') {
        if (existing) {
            await ctx.db.patch(existing._id, {
                deleted: true,
                deleted_at: now,
                ...stamps,
            });
        } else {
            await ctx.db.insert(tableInfo.table, {
                workspace_id: workspaceId,
                [tableInfo.pkField]: op.pk,
                deleted: true,
                deleted_at: now,
                created_at: now,
                ...stamps,
                ...payload,
            } as never);
        }
        const tombstone = await ctx.db
            .query('tombstones')
            .withIndex('by_workspace_table_pk', (q) =>
                q
                    .eq('workspace_id', workspaceId)
                    .eq('table_name', op.table)
                    .eq('pk', op.pk)
            )
            .first();
        if (tombstone) {
            await ctx.db.patch(tombstone._id, {
                deleted_at: now,
                server_deleted_at: now,
                clock: now,
                hlc,
                op_id: opId,
                server_version: serverVersion,
            });
        } else {
            await ctx.db.insert('tombstones', {
                workspace_id: workspaceId,
                table_name: op.table,
                pk: op.pk,
                deleted_at: now,
                server_deleted_at: now,
                clock: now,
                hlc,
                op_id: opId,
                server_version: serverVersion,
                created_at: now,
            });
        }
    } else if (existing) {
        await ctx.db.patch(existing._id, {
            ...payload,
            deleted: false,
            deleted_at: undefined,
            ...stamps,
        });
    } else {
        await ctx.db.insert(tableInfo.table, {
            workspace_id: workspaceId,
            [tableInfo.pkField]: op.pk,
            deleted: false,
            created_at: now,
            ...payload,
            ...stamps,
        } as never);
    }

    await ctx.db.insert('change_log', {
        workspace_id: workspaceId,
        server_version: serverVersion,
        table_name: op.table,
        pk: op.pk,
        op: op.operation,
        payload: op.operation === 'put' ? { ...payload, ...stamps, deleted: false } : undefined,
        clock: now,
        hlc,
        device_id: 'server',
        op_id: opId,
        created_at: now,
    });

    return { opId, serverVersion };
}
