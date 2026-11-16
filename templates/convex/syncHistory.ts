/** Deployment-wide history. Only an explicitly journaled replacement rotates it.
 * All functions here are admin-key-only; public reads fail while reset drains.
 */
import { ConvexError, v } from 'convex/values';
import { internalMutation, internalQuery, type MutationCtx, type QueryCtx } from './_generated/server';
import { newSyncReadBudget, readSyncBatch } from './syncReadBudget';
export const historyArgs = {
    history_incarnation: v.optional(v.union(v.string(), v.null())),
    history_incarnation_capability: v.optional(v.string()),
    unavailable_record_capability: v.optional(v.string()),
};
export function requireHistoryCapability(args: { history_incarnation_capability?: string; unavailable_record_capability?: string }) {
    for (const [actual, required] of [[args.history_incarnation_capability, 'incarnation-v1'], [args.unavailable_record_capability, 'unavailable-v1']]) {
        if (actual !== required) throw new ConvexError({ code: 'OR3_SYNC_UPDATE_REQUIRED', statusCode: 426, requiredCapability: required });
    }
}
export async function readHistory(ctx: QueryCtx | MutationCtx): Promise<string> {
    const row = await ctx.db.query('sync_history').withIndex('by_key', q => q.eq('key', 'history')).unique();
    if (!row || !/^[\x21-\x7e]{1,128}$/.test(row.incarnation)) throw new ConvexError({ code: 'OR3_SYNC_HISTORY_UNSUPPORTED' });
    if (row.phase !== 'ready') throw new ConvexError({ code: 'OR3_SYNC_HISTORY_RESETTING', statusCode: 503 });
    return row.incarnation;
}
export function changedHistory(current: string): never {
    throw new ConvexError({ code: 'OR3_SYNC_HISTORY_CHANGED', statusCode: 409, historyIncarnation: current, requiresSnapshot: true });
}
export async function fenceHistory(ctx: QueryCtx | MutationCtx, expected: string | null | undefined) {
    const current = await readHistory(ctx);
    if (expected !== current) changedHistory(current);
    return current;
}
function uuid(value: string) {
    if (!/^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(value)) throw new Error('Expected a fresh journaled UUID');
}
export const initialize = internalMutation({
    args: { incarnation: v.string() }, handler: async (ctx, args) => {
        uuid(args.incarnation);
        const existing = await ctx.db.query('sync_history').withIndex('by_key', q => q.eq('key', 'history')).unique();
        if (existing) return { incarnation: existing.incarnation, attemptId: existing.attempt_id, ready: existing.phase === 'ready' };
        await ctx.db.insert('sync_history', { key: 'history', incarnation: args.incarnation, phase: 'resetting', attempt_id: args.incarnation, target: args.incarnation });
        return { incarnation: args.incarnation, attemptId: args.incarnation, ready: false };
    },
});
export const beginReplacement = internalMutation({
    args: { attempt_id: v.string(), incarnation: v.string() }, handler: async (ctx, args) => {
        uuid(args.attempt_id); uuid(args.incarnation);
        const row = await ctx.db.query('sync_history').withIndex('by_key', q => q.eq('key', 'history')).unique();
        if (!row) throw new Error('Initialize history before replacement');
        if (row.attempt_id === args.attempt_id) {
            if (row.target !== args.incarnation) throw new Error('Reset attempt conflict');
            return { ready: row.phase === 'ready' };
        }
        if (row.phase !== 'ready' || row.incarnation === args.incarnation) throw new Error('Reset conflict');
        await ctx.db.patch(row._id, { phase: 'resetting', attempt_id: args.attempt_id, target: args.incarnation });
        return { ready: false };
    },
});
export const finishReplacement = internalMutation({
    args: { attempt_id: v.string() }, handler: async (ctx, args) => {
        const row = await ctx.db.query('sync_history').withIndex('by_key', q => q.eq('key', 'history')).unique();
        if (!row || row.attempt_id !== args.attempt_id) throw new Error('Reset attempt conflict');
        if (row.phase === 'ready') return { ready: true, historyIncarnation: row.incarnation };
        const budget = newSyncReadBudget();
        for (const table of ['sync_snapshot_pages', 'sync_snapshot_sessions', 'device_cursors'] as const) {
            const page = await readSyncBatch(ctx.db.query(table), 100, budget);
            for (const item of page.rows) await ctx.db.delete(item._id);
            if (page.hasMore) return { ready: false };
        }
        await ctx.db.patch(row._id, { incarnation: row.target, phase: 'ready' });
        return { ready: true, historyIncarnation: row.target };
    },
});
/** Privileged journal reconciliation; never initializes or changes state. */
export const status = internalQuery({ args: {}, handler: async ctx => {
    const row = await ctx.db.query('sync_history').withIndex('by_key', q => q.eq('key', 'history')).unique();
    return row ? { incarnation: row.incarnation, attemptId: row.attempt_id, target: row.target, phase: row.phase } : null;
} });
