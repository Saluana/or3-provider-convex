import type { MutationCtx } from './_generated/server';
import type { Id } from './_generated/dataModel';

const normalizeHash = (hash: string) => hash.trim().replace(/^sha256:/i, '').toLowerCase();
async function usage(ctx: MutationCtx, workspaceId: Id<'workspaces'>) {
    let state = await ctx.db.query('storage_usage').withIndex('by_workspace', q => q.eq('workspace_id', workspaceId)).first();
    if (!state) {
        const id = await ctx.db.insert('storage_usage', { workspace_id: workspaceId, used_bytes: 0, ready: false });
        state = (await ctx.db.get(id))!;
    }
    return state;
}

/** Same mutation as metadata admission: never release charges on logical delete. */
export async function recordRetainedStorage(ctx: MutationCtx, workspaceId: Id<'workspaces'>, row: Record<string, unknown>) {
    if (typeof row.hash !== 'string' || row.size_bytes === undefined) return;
    const size = row.size_bytes;
    if (typeof size !== 'number' || !Number.isSafeInteger(size) || size < 0) throw new Error('Invalid retained file size');
    const hash = normalizeHash(row.hash);
    const previous = await ctx.db.query('retained_storage').withIndex('by_workspace_hash', q => q.eq('workspace_id', workspaceId).eq('hash', hash)).first();
    if (previous && previous.size_bytes >= size && previous.metadata_aliases.includes(row.hash)) return;
    const retainedSize = Math.max(size, previous?.size_bytes ?? 0);
    const aliases = [...new Set([...(previous?.metadata_aliases ?? []), row.hash])];
    const state = await usage(ctx, workspaceId);
    const total = state.used_bytes + retainedSize - (previous?.size_bytes ?? 0);
    if (!Number.isSafeInteger(total)) throw new Error('Invalid retained storage total');
    if (previous) await ctx.db.patch(previous._id, { size_bytes: retainedSize, metadata_aliases: aliases });
    else await ctx.db.insert('retained_storage', { workspace_id: workspaceId, hash, size_bytes: retainedSize, metadata_aliases: aliases });
    if (total !== state.used_bytes) await ctx.db.patch(state._id, { used_bytes: total });
}

/** One bounded, resumable upgrade page per call. Mutations serialize concurrent
 * uploads/backfill; digest upserts prevent double-charging rows written meanwhile.
 * Return progress instead of throwing, which would roll back the migration page.
 */
export async function prepareStorageUsage(ctx: MutationCtx, workspaceId: Id<'workspaces'>) {
    let state = await usage(ctx, workspaceId);
    if (!state.ready) {
        const page = await ctx.db.query('file_meta').withIndex('by_workspace_deleted', q => q.eq('workspace_id', workspaceId))
            .paginate({ cursor: state.cursor ?? null, numItems: 200 });
        for (const row of page.page) await recordRetainedStorage(ctx, workspaceId, row);
        await ctx.db.patch(state._id, { cursor: page.isDone ? undefined : page.continueCursor, ready: page.isDone });
        state = (await ctx.db.get(state._id))!;
    }
    return state;
}

/** Only physically collected native bytes may release their charge. Other
 * metadata aliases keep the charge until they too have crossed deletion.
 */
export async function releaseCollectedStorage(ctx: MutationCtx, workspaceId: Id<'workspaces'>, value: string) {
    const hash = normalizeHash(value);
    // Track the actual keys admitted/backfilled rather than guessing the two
    // modern spellings. Legacy casing and duplicate rows must retain charges.
    if (await ctx.db.query('file_meta').withIndex('by_workspace_hash', q => q.eq('workspace_id', workspaceId).eq('hash', value)).first()) return;
    const retained = await ctx.db.query('retained_storage').withIndex('by_workspace_hash', q => q.eq('workspace_id', workspaceId).eq('hash', hash)).first();
    if (!retained) return;
    const aliases = retained.metadata_aliases.filter(alias => alias !== value);
    if (aliases.length) {
        await ctx.db.patch(retained._id, { metadata_aliases: aliases });
        return;
    }
    const state = await usage(ctx, workspaceId);
    await ctx.db.delete(retained._id);
    await ctx.db.patch(state._id, { used_bytes: state.used_bytes - retained.size_bytes });
}
