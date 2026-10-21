/** Private host credential storage. All operations are Convex-internal:
 * the trusted host authorizes owners and encrypts credentials before writing.
 * Serializable mutations protect insert-only identity, rotations and evidence. */
import { v } from 'convex/values';
import { internalMutation, internalQuery } from './_generated/server';
import type { Doc } from './_generated/dataModel';

function toConnection(row: Doc<'plugin_connections'>) {
    const { _id, _creationTime, workspace_id, ...connection } = row;
    return { ...connection, workspaceId: workspace_id };
}

export const list = internalQuery({
    args: { ownerUserId: v.string(), workspaceId: v.id('workspaces'), pluginId: v.optional(v.string()) },
    handler: async (ctx, args) => {
        const rows = await ctx.db.query('plugin_connections')
            .withIndex('by_workspace_owner', q => q.eq('workspace_id', args.workspaceId).eq('ownerUserId', args.ownerUserId))
            .collect();
        return rows.filter(row => args.pluginId === undefined || row.pluginId === args.pluginId)
            .sort((a, b) => a.createdAt - b.createdAt || a.id.localeCompare(b.id))
            .map(toConnection);
    },
});

export const get = internalQuery({
    args: { id: v.string() },
    handler: async (ctx, args) => {
        const row = await ctx.db.query('plugin_connections').withIndex('by_connection_id', q => q.eq('id', args.id)).unique();
        return row ? toConnection(row) : null;
    },
});

export const insert = internalMutation({
    args: { connection: v.object({
        id: v.string(), ownerUserId: v.string(), workspaceId: v.id('workspaces'),
        pluginId: v.string(), providerId: v.string(), slotId: v.optional(v.string()),
        label: v.string(), scopes: v.array(v.string()), revision: v.number(),
        secretCiphertext: v.string(), createdAt: v.number(), updatedAt: v.number(),
    }) },
    handler: async (ctx, { connection }) => {
        const existing = await ctx.db.query('plugin_connections').withIndex('by_connection_id', q => q.eq('id', connection.id)).unique();
        if (existing) return false;
        const { workspaceId, ...fields } = connection;
        await ctx.db.insert('plugin_connections', { ...fields, workspace_id: workspaceId });
        return true;
    },
});

export const update = internalMutation({
    args: { update: v.object({
        id: v.string(), expectedRevision: v.number(), revision: v.number(),
        updatedAt: v.number(), secretCiphertext: v.string(),
        scopes: v.optional(v.array(v.string())), label: v.optional(v.string()),
    }) },
    handler: async (ctx, { update }) => {
        const row = await ctx.db.query('plugin_connections').withIndex('by_connection_id', q => q.eq('id', update.id)).unique();
        if (!row || row.revision !== update.expectedRevision) return false;
        await ctx.db.patch(row._id, {
            revision: update.revision, updatedAt: update.updatedAt, secretCiphertext: update.secretCiphertext,
            ...(update.scopes === undefined ? {} : { scopes: update.scopes }),
            ...(update.label === undefined ? {} : { label: update.label }),
        });
        return true;
    },
});

export const remove = internalMutation({
    args: { id: v.string() },
    handler: async (ctx, args) => {
        const connection = await ctx.db.query('plugin_connections').withIndex('by_connection_id', q => q.eq('id', args.id)).unique();
        const evidence = await ctx.db.query('plugin_connection_tests').withIndex('by_connection', q => q.eq('connectionId', args.id)).unique();
        if (evidence) await ctx.db.delete(evidence._id);
        if (connection) await ctx.db.delete(connection._id);
    },
});

export const getTestEvidence = internalQuery({
    args: { connectionId: v.string() },
    handler: async (ctx, args) => {
        const row = await ctx.db.query('plugin_connection_tests').withIndex('by_connection', q => q.eq('connectionId', args.connectionId)).unique();
        if (!row) return null;
        const { _id, _creationTime, workspace_id, ...evidence } = row;
        return evidence;
    },
});

export const setTestEvidence = internalMutation({
    args: { evidence: v.object({
        connectionId: v.string(), revision: v.number(), operationId: v.string(), ok: v.boolean(),
        code: v.optional(v.union(
            v.literal('bad-credentials'), v.literal('insufficient-scope'), v.literal('network-failure'),
            v.literal('rate-limit'), v.literal('provider-outage'), v.literal('deadline-exceeded'),
            v.literal('response-too-large'), v.literal('policy-denied'), v.literal('unknown'),
        )),
        checkedAt: v.number(), detail: v.optional(v.string()),
    }) },
    handler: async (ctx, { evidence }) => {
        const connection = await ctx.db.query('plugin_connections').withIndex('by_connection_id', q => q.eq('id', evidence.connectionId)).unique();
        if (!connection || connection.revision !== evidence.revision) return false;
        const previous = await ctx.db.query('plugin_connection_tests').withIndex('by_connection', q => q.eq('connectionId', evidence.connectionId)).unique();
        if (previous && (previous.revision > evidence.revision ||
            (previous.revision === evidence.revision && previous.checkedAt > evidence.checkedAt))) return false;
        const fields = { ...evidence, workspace_id: connection.workspace_id };
        if (previous) await ctx.db.replace(previous._id, fields);
        else await ctx.db.insert('plugin_connection_tests', fields);
        return true;
    },
});
