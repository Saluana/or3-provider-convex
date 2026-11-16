/** Durable original Convex values, independent of log/snapshot retention. */
import { convexToJson, type Value } from 'convex/values';
import type { MutationCtx } from './_generated/server';
import type { Id } from './_generated/dataModel';
import { SyncChangeSchema, SnapshotItemSchema, TABLE_PAYLOAD_SCHEMAS, syncJsonByteLength, isLosslessSyncIdentifier } from './syncContract/shared/sync/schemas';
export function assertIdentity(table: string, pk: unknown): asserts pk is string {
    if (typeof table !== 'string' || !table.length || typeof pk !== 'string' || !pk.length ||
        !isLosslessSyncIdentifier(table) || !isLosslessSyncIdentifier(pk)) throw new Error('Sync coverage identity failure');
}
export function readDefect(entry: unknown, snapshot: boolean): string | null {
    const parsed = (snapshot ? SnapshotItemSchema : SyncChangeSchema).safeParse(entry);
    const revision = (entry as { revision?: {clock?: number}; stamp?: {clock?: number} })?.revision ?? (entry as {stamp?: {clock?: number}})?.stamp;
    if (revision && !Number.isSafeInteger(revision.clock)) return 'invalid_revision';
    if (!parsed.success) return 'invalid_envelope';
    const item = entry as { tableName: string; payload?: unknown; kind?: string; op?: string; revision?: { hlc: string } ; stamp?: { hlc: string } };
    if (syncJsonByteLength(entry) > 1024 * 1024) return 'oversized_record';
    if (!Object.hasOwn(TABLE_PAYLOAD_SCHEMAS, item.tableName)) return 'unknown_table';
    if (item.payload !== undefined && syncJsonByteLength(item.payload) > 256 * 1024) return 'oversized_payload';
    if (item.kind === 'row' || item.op === 'put') {
        // Legacy message order is validated in a derived view, never rewritten.
        let payload = item.payload;
        if (item.tableName === 'messages' && payload && typeof payload === 'object' && !('order_key' in payload)) {
            payload = { ...payload, order_key: (item.revision ?? item.stamp)?.hlc };
        }
        if (!TABLE_PAYLOAD_SCHEMAS[item.tableName]!.safeParse(payload).success) return 'invalid_payload';
        const row = payload as Record<string, unknown>;
        if (row[item.tableName === 'file_meta' ? 'hash' : 'id'] !== (entry as { pk: string }).pk) return 'payload_identity_mismatch';
        for (const key of ['id', 'hash', 'thread_id', 'project_id', 'user_id', 'document_id', 'root_thread_id', 'summary_message_id']) {
            if (typeof row[key] === 'string' && !isLosslessSyncIdentifier(row[key])) return 'invalid_payload_identifier';
        }
    }
    return null;
}
export async function retainEvidence(ctx: MutationCtx, workspace: Id<'workspaces'>, history: string,
    table: string, pk: string, source: unknown, reason: string) {
    // Convex enforces a 1 MiB maximum source document. Serialize only that bounded
    // original value, preserving bytes/NaN/Int64 via Convex's lossless codec.
    const original = source as Record<string, Value>;
    const bytes = new TextEncoder().encode(JSON.stringify(convexToJson(source as Value)));
    const hash = Array.from(new Uint8Array(await crypto.subtle.digest('SHA-256', bytes))).map(b => b.toString(16).padStart(2, '0')).join('');
    const existing = await ctx.db.query('sync_unavailable_records').withIndex('by_source', q =>
        q.eq('workspace_id', workspace).eq('history_incarnation', history).eq('source_key', hash)).unique();
    if (existing) return String(existing._id);
    const { _id, _creationTime, ...fields } = original;
    const sourceDocument = await ctx.db.insert('sync_unavailable_sources', fields);
    const id = await ctx.db.insert('sync_unavailable_records', {
        source_document_id: sourceDocument,
        ...(typeof _creationTime === 'number' ? { source_creation_time: _creationTime } : {}),
        workspace_id: workspace, history_incarnation: history, source_key: hash,
        table_name: table, pk, source_id: String(original._id ?? ''), reason, status: 'unresolved',
        ...(typeof original.server_version === 'number' ? { source_version: original.server_version } : {}),
        ...(typeof original.op_id === 'string' && original.op_id.length <= 128 ? { source_op_id: original.op_id } : {}),
        ...(typeof original.clock === 'number' ? { source_clock: original.clock } : {}),
        created_at: Math.floor(Date.now() / 1000), chunks: Math.ceil(bytes.length / 32768),
    });
    return String(id);
}
