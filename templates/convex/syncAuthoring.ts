/**
 * Server-authored sync writes that must mint UUID op_ids, allocate
 * server_version, and append change_log the same way client push does.
 */
import type { MutationCtx } from './_generated/server';
import type { Id, TableNames } from './_generated/dataModel';
import { recordMaterializedPreimage, compareSnapshotRevisions, legacySnapshotRevision } from './snapshot';
import { recordRetainedStorage } from './storageUsage';

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

/** Metadata must sort after its base without consuming a semantic clock tick. */
function nextStorageMetadataHlc(value: string): string {
    if (value.length < 512) return `${value}~`;
    for (let index = Math.min(value.length, 512) - 1; index >= 0; index -= 1) {
        const unit = value.charCodeAt(index);
        // Do not leave the first half of a Unicode surrogate pair in a prefix.
        if (unit >= 0xdc00 && unit <= 0xdfff && index > 0 &&
            value.charCodeAt(index - 1) >= 0xd800 && value.charCodeAt(index - 1) <= 0xdbff) continue;
        if (unit === 0xffff) continue;
        const next = unit + 1 >= 0xd800 && unit + 1 <= 0xdfff ? 0xe000 : unit + 1;
        return value.slice(0, index) + String.fromCharCode(next);
    }
    throw new Error('Cannot advance storage metadata revision');
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
    let hlc = `${now}:server:${opId.slice(0, 8)}`;
    const serverVersion = await allocateServerVersion(ctx, workspaceId);
    const payload = stripReserved(op.payload ?? {});
    if (op.table === 'file_meta' && op.operation === 'put') await recordRetainedStorage(ctx, workspaceId, { ...payload, hash: op.pk });

    type IndexQueryBuilder = {
        eq: (field: string, value: unknown) => IndexQueryBuilder;
    };
    type ConvexDoc = {
        _id: Id<TableNames>;
        clock?: number;
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
    const tombstone = await ctx.db
        .query('tombstones')
        .withIndex('by_workspace_table_pk', (q) =>
            q.eq('workspace_id', workspaceId)
                .eq('table_name', op.table)
                .eq('pk', op.pk)
        )
        .first();
    const storedRevisions = [existing, tombstone].filter((value): value is NonNullable<typeof value> => Boolean(value))
        .map(value => legacySnapshotRevision(op.table, op.pk, value)).sort(compareSnapshotRevisions);
    const base = storedRevisions.at(-1);
    const storageCompletion = op.table === 'file_meta' && op.operation === 'put';
    // Byte completion cannot outrank a delete captured from the same base.
    // A new metadata row starts at zero; every client mutation advances it.
    const clock = storageCompletion ? base?.clock ?? 0 : Math.max(now, (base?.clock ?? 0) + 1);
    if (storageCompletion && base) hlc = nextStorageMetadataHlc(base.hlc);
    if (!Number.isSafeInteger(clock)) throw new Error('Cannot advance record revision');

    // Native uploads and server-authored writes participate in the same frozen
    // snapshots as client sync pushes. Keep the old state before replacing it.
    if (existing) await recordMaterializedPreimage(ctx, workspaceId, op.table, op.pk, existing, serverVersion - 1);

    const stamps = {
        clock,
        hlc,
        op_id: opId,
        server_version: serverVersion,
        updated_at: now,
    };
    // Storage completion proves bytes, not an intention to restore a file.
    const preserveDeletion = op.table === 'file_meta' && (existing?.deleted === true ||
        Boolean(tombstone && (!existing || compareSnapshotRevisions(
            legacySnapshotRevision(op.table, op.pk, tombstone),
            legacySnapshotRevision(op.table, op.pk, existing)) >= 0)));
    const deletedAt = preserveDeletion ? (typeof existing?.deleted_at === 'number' ? existing.deleted_at :
        tombstone?.server_deleted_at ?? tombstone?.deleted_at ?? now) : undefined;
    const canonicalPayload: Record<string, unknown> = {
        ...stripReserved(existing ?? {}),
        ...payload,
        [tableInfo.pkField]: op.pk,
        created_at: existing?.created_at ?? payload.created_at ?? now,
        deleted: preserveDeletion,
        ...stamps,
    };
    if (preserveDeletion) canonicalPayload.deleted_at = deletedAt;
    else delete canonicalPayload.deleted_at;

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
                ...payload,
                ...stamps,
            } as never);
        }
        if (tombstone) {
            await ctx.db.patch(tombstone._id, {
                deleted_at: now,
                server_deleted_at: now,
                clock,
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
                clock,
                hlc,
                op_id: opId,
                server_version: serverVersion,
                created_at: now,
            });
        }
    } else if (existing) {
        await ctx.db.patch(existing._id, {
            ...canonicalPayload,
            deleted_at: deletedAt,
        });
    } else {
        await ctx.db.insert(tableInfo.table, {
            workspace_id: workspaceId,
            [tableInfo.pkField]: op.pk,
            ...canonicalPayload,
        } as never);
    }

    await ctx.db.insert('change_log', {
        workspace_id: workspaceId,
        server_version: serverVersion,
        table_name: op.table,
        pk: op.pk,
        op: op.operation,
        payload: op.operation === 'put' ? canonicalPayload : undefined,
        clock,
        hlc,
        device_id: 'server',
        op_id: opId,
        created_at: now,
    });

    return { opId, serverVersion };
}
