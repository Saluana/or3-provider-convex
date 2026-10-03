import type { MutationCtx } from './_generated/server';
import type { Id } from './_generated/dataModel';

const normalizeHash = (hash: string) => hash.replace(/^sha256:/i, '').trim().toLowerCase();
async function claim(ctx: MutationCtx, workspaceId: Id<'workspaces'>, hash: string) {
    return ctx.db.query('storage_deletion_claims').withIndex('by_workspace_hash', q =>
        q.eq('workspace_id', workspaceId).eq('hash', normalizeHash(hash))).first();
}
export async function claimStorageDeletion(ctx: MutationCtx, workspaceId: Id<'workspaces'>, hash: string) {
    if (!await claim(ctx, workspaceId, hash)) await ctx.db.insert('storage_deletion_claims', {
        workspace_id: workspaceId, hash: normalizeHash(hash), deleted_at: Math.floor(Date.now() / 1000),
    });
}
/** Call only inside the same mutation that verifies and commits new original bytes. */
export async function releaseStorageDeletion(ctx: MutationCtx, workspaceId: Id<'workspaces'>, hash: string) {
    const existing = await claim(ctx, workspaceId, hash);
    if (existing) await ctx.db.delete(existing._id);
}
export async function assertStorageNotDeleted(ctx: MutationCtx, workspaceId: Id<'workspaces'>,
    table: string, row: Record<string, unknown>) {
    if (row.deleted === true || !['file_meta', 'messages', 'posts'].includes(table)) return;
    let hashes: string[] = [];
    if (table === 'file_meta') hashes = typeof row.hash === 'string' ? [row.hash] : [];
    else if (typeof row.file_hashes === 'string' && row.file_hashes) {
        const parsed: unknown = JSON.parse(row.file_hashes);
        if (!Array.isArray(parsed) || parsed.some(hash => typeof hash !== 'string')) throw new Error('Invalid file references');
        hashes = parsed as string[];
    }
    for (const hash of new Set(hashes)) {
        if (await claim(ctx, workspaceId, hash)) throw new Error('Original bytes were deleted. Re-upload before restoring this file or reference.');
    }
    // A newer upload may release the hash barrier while an offline writer still
    // carries the old storage ID. Legacy rows without a provider are native too,
    // matching storage reads and deletion. Verify the binding before reviving it.
    if (table === 'file_meta' && (!row.storage_provider_id || row.storage_provider_id === 'convex') && typeof row.storage_id === 'string') {
        const object = await ctx.db.system.get(row.storage_id as Id<'_storage'>);
        const hash = typeof row.hash === 'string' ? normalizeHash(row.hash) : '';
        const digest = /^[a-f0-9]{64}$/.test(hash) ? btoa(String.fromCharCode(...hash.match(/../g)!.map(byte => parseInt(byte, 16)))) : '';
        if (!object || object.sha256 !== digest || object.size !== row.size_bytes) {
            throw new Error('Storage object no longer matches this file. Re-upload the original.');
        }
    }
}
