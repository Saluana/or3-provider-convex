/**
 * @module convex/sync
 *
 * Purpose:
 * Implements the OR3 Cloud sync protocol in Convex.
 * Provides push and pull while retaining complete change history for bootstrap.
 *
 * Behavior:
 * - `push` accepts batched changes and applies LWW conflict resolution
 * - `pull` and `watchChanges` read change logs after a cursor
 * - `updateDeviceCursor` tracks per-device progress for retention
 * - GC entry points are internal and gated by the verified snapshot-v1 contract
 *
 * Constraints:
 * - Read endpoints accept owner/editor/viewer membership
 * - Write endpoints accept owner/editor membership
 * - GC entry points are internal-only and bounded
 * - Wire schema is snake_case and aligns with Dexie
 * - `order_key` and `clock` drive deterministic ordering and conflicts
 *
 * Non-Goals:
 * - Per-table cursors or server-side merge strategies beyond LWW
 * - Schema enforcement beyond lightweight validation in `validatePayload`
 */
import { v } from 'convex/values';
import { mutation, query, internalMutation, type MutationCtx, type QueryCtx } from './_generated/server';
import type { Id, TableNames } from './_generated/dataModel';
import { getPkField } from '../shared/sync/table-metadata';
import { requireWorkspaceRole } from './authz';
import { SYNC_HISTORY_GC_POLICY } from './syncHistoryGcPolicy';
import {
    decodeSnapshotCursor,
    encodeSnapshotCursor,
    normalizeSnapshotTables,
    compareSnapshotRevisions,
    resolveSnapshotWinner,
    type SnapshotCandidate,
    type SnapshotRevision,
} from './snapshot';

const nowSec = (): number => Math.floor(Date.now() / 1000);
const MIN_SYNC_RETENTION_SECONDS = 60 * 60;
const MAX_SYNC_RETENTION_SECONDS = 365 * 24 * 60 * 60;
const MAX_SYNC_GC_BATCH_SIZE = 1000;
const MAX_SYNC_GC_CONTINUATIONS = 1000;

function assertSafeNonnegativeInteger(value: number, name: string): void {
    if (!Number.isSafeInteger(value) || value < 0) {
        throw new Error(`Invalid ${name}`);
    }
}

function validateGcArguments(args: {
    retention_seconds?: number;
    batch_size?: number;
    cursor?: number;
    tombstone_cursor?: number;
    changelog_cursor?: number;
    continuation_count?: number;
}): void {
    if (args.retention_seconds !== undefined && (
        !Number.isSafeInteger(args.retention_seconds) ||
        args.retention_seconds < MIN_SYNC_RETENTION_SECONDS ||
        args.retention_seconds > MAX_SYNC_RETENTION_SECONDS
    )) throw new Error('Invalid retention_seconds');
    if (args.batch_size !== undefined && (
        !Number.isSafeInteger(args.batch_size) ||
        args.batch_size < 1 ||
        args.batch_size > MAX_SYNC_GC_BATCH_SIZE
    )) throw new Error('Invalid batch_size');
    for (const [name, value] of [
        ['cursor', args.cursor],
        ['tombstone_cursor', args.tombstone_cursor],
        ['changelog_cursor', args.changelog_cursor],
        ['continuation_count', args.continuation_count],
    ] as const) {
        if (value !== undefined) assertSafeNonnegativeInteger(value, name);
    }
    if ((args.continuation_count ?? 0) > MAX_SYNC_GC_CONTINUATIONS) {
        throw new Error('Invalid continuation_count');
    }
}

// ============================================================
// CONSTANTS
// ============================================================

/** Maximum ops allowed per push batch */
const MAX_PUSH_OPS = 100;

/** Maximum limit for pull requests */
const MAX_PULL_LIMIT = 500;

/** Maximum materialized records examined by one snapshot page. */
const MAX_SNAPSHOT_PAGE_SIZE = 1000;

/** Hard bound for canonical storage metadata/reference pages. */
const MAX_CANONICAL_STORAGE_PAGE_SIZE = 500;

/** Snapshot page chains expire after one hour. */
const SNAPSHOT_TTL_SECONDS = 60 * 60;

/** Maximum operation ID length in characters */
const MAX_OP_ID_LENGTH = 64;

/** Maximum payload size in bytes (64KB) */
const MAX_PAYLOAD_SIZE_BYTES = 64 * 1024;

const SYNC_READ_ROLES = new Set(['owner', 'editor', 'viewer'] as const);
const SYNC_WRITE_ROLES = new Set(['owner', 'editor'] as const);

// ============================================================
// HELPERS
// ============================================================

/**
 * Internal helper.
 *
 * Purpose:
 * Allocates a contiguous server_version range for a workspace.
 * The return value is the start of the range.
 */
async function allocateServerVersions(
    ctx: MutationCtx,
    workspaceId: Id<'workspaces'>,
    count: number
): Promise<number> {
    if (count <= 0) {
        // Should not happen, but return current version if it does
        const existing = await ctx.db
            .query('server_version_counter')
            .withIndex('by_workspace', (q) => q.eq('workspace_id', workspaceId))
            .first();
        return existing?.value ?? 0;
    }

    const existing = await ctx.db
        .query('server_version_counter')
        .withIndex('by_workspace', (q) => q.eq('workspace_id', workspaceId))
        .first();

    if (existing) {
        const start = existing.value + 1;
        const end = existing.value + count;
        await ctx.db.patch(existing._id, { value: end });
        return start;
    } else {
        await ctx.db.insert('server_version_counter', {
            workspace_id: workspaceId,
            value: count,
        });
        return 1;
    }
}

/**
 * Internal mapping.
 *
 * Purpose:
 * Maps table names to their workspace-scoped index.
 *
 * @remarks
 * `file_meta` uses `hash` as the primary key rather than `id`.
 */
const TABLE_INDEX_MAP: Partial<Record<string, { table: string; indexName: string }>> = {
    threads: { table: 'threads', indexName: 'by_workspace_id' },
    messages: { table: 'messages', indexName: 'by_workspace_id' },
    projects: { table: 'projects', indexName: 'by_workspace_id' },
    posts: { table: 'posts', indexName: 'by_workspace_id' },
    kv: { table: 'kv', indexName: 'by_workspace_id' },
    file_meta: { table: 'file_meta', indexName: 'by_workspace_hash' },
    notifications: { table: 'notifications', indexName: 'by_workspace_id' },
};

type MaterializedDoc = {
    _id: Id<TableNames>;
    _creationTime?: number;
    id?: string;
    hash?: string;
    deleted?: boolean;
    deleted_at?: number;
    clock?: number;
    hlc?: string;
    op_id?: string;
    server_version?: number;
} & Record<string, unknown>;

type TombstoneDoc = {
    _id: Id<'tombstones'>;
    table_name: string;
    pk: string;
    deleted_at: number;
    server_deleted_at?: number;
    clock: number;
    hlc?: string;
    op_id?: string;
    server_version: number;
    created_at?: number;
};

type SnapshotHistoryDoc = {
    table_name: string;
    pk: string;
    server_version: number;
    kind: 'row' | 'tombstone';
    payload?: unknown;
    clock: number;
    hlc: string;
    op_id: string;
    server_deleted_at?: number;
};

type DynamicIndexBuilder = {
    eq: (field: string, value: unknown) => DynamicIndexBuilder;
    gt: (field: string, value: unknown) => DynamicIndexBuilder;
    lte: (field: string, value: unknown) => DynamicIndexBuilder;
};

type CanonicalStorageKind =
    | 'live_metadata'
    | 'reference_edges'
    | 'active_reservations';

type CanonicalStorageCursor = {
    version: 1;
    kind: CanonicalStorageKind;
    hash?: string;
    afterPk: string;
    tableIndex?: number;
    currentPk?: string;
    hashOffset?: number;
};

function normalizeStorageHash(value: string): string {
    return value.replace(/^sha256:/i, '').replace(/^md5:/i, '').trim().toLowerCase();
}

function parseCanonicalFileHashes(value: unknown): string[] {
    if (typeof value !== 'string' || value.length === 0) return [];
    try {
        const parsed = JSON.parse(value) as unknown;
        if (!Array.isArray(parsed)) return [];
        return [...new Set(parsed
            .filter((item): item is string => typeof item === 'string')
            .map(normalizeStorageHash)
            .filter(Boolean))].sort();
    } catch {
        return [];
    }
}

function encodeCanonicalStorageCursor(token: CanonicalStorageCursor): string {
    const asciiJson = encodeURIComponent(JSON.stringify(token));
    return btoa(asciiJson).replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/u, '');
}

function decodeCanonicalStorageCursor(
    value: string | undefined,
    kind: CanonicalStorageKind,
    hash: string | undefined
): CanonicalStorageCursor {
    if (!value) return { version: 1, kind, hash, afterPk: '' };
    if (value.length > 2048) throw new Error('Invalid canonical storage cursor');
    try {
        const base64 = value.replace(/-/g, '+').replace(/_/g, '/');
        const padding = '='.repeat((4 - (base64.length % 4)) % 4);
        const parsed = JSON.parse(decodeURIComponent(atob(base64 + padding))) as Partial<CanonicalStorageCursor>;
        if (
            parsed.version !== 1 ||
            parsed.kind !== kind ||
            parsed.hash !== hash ||
            typeof parsed.afterPk !== 'string' ||
            (parsed.tableIndex !== undefined &&
                (!Number.isInteger(parsed.tableIndex) || parsed.tableIndex < 0 || parsed.tableIndex > 2)) ||
            (parsed.currentPk !== undefined && typeof parsed.currentPk !== 'string') ||
            (parsed.hashOffset !== undefined &&
                (!Number.isInteger(parsed.hashOffset) || parsed.hashOffset < 0))
        ) {
            throw new Error('invalid cursor shape');
        }
        return parsed as CanonicalStorageCursor;
    } catch {
        throw new Error('Invalid canonical storage cursor');
    }
}

function legacySnapshotRevision(
    tableName: string,
    pk: string,
    value: { clock?: number; hlc?: string; op_id?: string }
): SnapshotRevision {
    const clock = Number.isInteger(value.clock) && (value.clock as number) >= 0
        ? (value.clock as number)
        : 0;
    const hlc = typeof value.hlc === 'string' && value.hlc.length > 0
        ? value.hlc
        : `legacy-${clock}`;
    const opId = typeof value.op_id === 'string' && value.op_id.length > 0
        ? value.op_id
        : `legacy:${tableName}:${pk}:${clock}:${hlc}`;
    return { clock, hlc, opId };
}

function completeStoredRevision(
    value: { clock?: number; hlc?: string; op_id?: string }
): SnapshotRevision | null {
    if (
        !Number.isInteger(value.clock) ||
        (value.clock as number) < 0 ||
        typeof value.hlc !== 'string' ||
        value.hlc.length === 0 ||
        typeof value.op_id !== 'string' ||
        value.op_id.length === 0
    ) {
        // Orphan tombstones do not carry an owner, so fail closed rather than
        // expose another user's notification existence.
        return null;
    }
    return { clock: value.clock as number, hlc: value.hlc, opId: value.op_id };
}

function incomingWinsStoredRevision(
    incoming: SnapshotRevision,
    stored: { clock?: number; hlc?: string; op_id?: string }
): boolean {
    const storedClock = Number.isInteger(stored.clock) ? (stored.clock as number) : 0;
    if (incoming.clock !== storedClock) return incoming.clock > storedClock;
    const complete = completeStoredRevision(stored);
    // Equal-clock legacy state is ambiguous. Fail closed until repair proves
    // the originating operation rather than inventing a winner.
    return complete ? compareSnapshotRevisions(incoming, complete) > 0 : false;
}

function snapshotPayloadFromDoc(
    doc: MaterializedDoc,
    pkField: string,
    pk: string,
    revision: SnapshotRevision
): Record<string, unknown> {
    const {
        _id,
        _creationTime,
        workspace_id,
        server_version,
        op_id,
        ...payload
    } = doc;
    void _id;
    void _creationTime;
    void workspace_id;
    void server_version;
    void op_id;
    return {
        ...payload,
        [pkField]: pk,
        clock: revision.clock,
        hlc: revision.hlc,
    };
}

function materializedSnapshotCandidate(
    tableName: string,
    pk: string,
    doc: MaterializedDoc,
    serverVersionOverride?: number
): SnapshotCandidate {
    const revision = legacySnapshotRevision(tableName, pk, doc);
    const serverVersion = serverVersionOverride ?? doc.server_version;
    if (doc.deleted === true) {
        return {
            kind: 'tombstone',
            tableName,
            pk,
            revision,
            serverDeletedAt:
                typeof doc.deleted_at === 'number' && doc.deleted_at >= 0
                    ? Math.floor(doc.deleted_at)
                    : nowSec(),
            serverVersion,
        };
    }
    return {
        kind: 'row',
        tableName,
        pk,
        payload: snapshotPayloadFromDoc(doc, getPkField(tableName), pk, revision),
        revision,
        serverVersion,
    };
}

function tombstoneSnapshotCandidate(
    tableName: string,
    pk: string,
    tombstone: TombstoneDoc
): SnapshotCandidate {
    return {
        kind: 'tombstone',
        tableName,
        pk,
        revision: legacySnapshotRevision(tableName, pk, tombstone),
        serverDeletedAt: Math.max(0, Math.floor(tombstone.server_deleted_at ?? tombstone.deleted_at)),
        serverVersion: tombstone.server_version,
    };
}

function historySnapshotCandidate(history: SnapshotHistoryDoc): SnapshotCandidate {
    const revision = {
        clock: history.clock,
        hlc: history.hlc,
        opId: history.op_id,
    };
    if (history.kind === 'tombstone') {
        return {
            kind: 'tombstone',
            tableName: history.table_name,
            pk: history.pk,
            revision,
            serverDeletedAt: Math.max(0, Math.floor(history.server_deleted_at ?? 0)),
            serverVersion: history.server_version,
        };
    }
    return {
        kind: 'row',
        tableName: history.table_name,
        pk: history.pk,
        payload: history.payload,
        revision,
        serverVersion: history.server_version,
    };
}

async function recordMaterializedPreimage(
    ctx: MutationCtx,
    workspaceId: Id<'workspaces'>,
    tableName: string,
    pk: string,
    doc: MaterializedDoc,
    fallbackServerVersion: number
): Promise<void> {
    const candidate = materializedSnapshotCandidate(
        tableName,
        pk,
        doc,
        doc.server_version ?? fallbackServerVersion
    );
    await ctx.db.insert('sync_record_versions', {
        workspace_id: workspaceId,
        table_name: tableName,
        pk,
        server_version: candidate.serverVersion ?? fallbackServerVersion,
        kind: candidate.kind,
        payload: candidate.kind === 'row' ? candidate.payload : undefined,
        clock: candidate.revision.clock,
        hlc: candidate.revision.hlc,
        op_id: candidate.revision.opId,
        server_deleted_at:
            candidate.kind === 'tombstone' ? candidate.serverDeletedAt : undefined,
    });
}

async function readSnapshotHistory(
    ctx: MutationCtx,
    workspaceId: Id<'workspaces'>,
    tableName: string,
    pk: string,
    highWatermark: number
): Promise<SnapshotCandidate | null> {
    const history = (await ctx.db
        .query('sync_record_versions')
        .withIndex('by_workspace_table_pk_version', (q) =>
            q.eq('workspace_id', workspaceId)
                .eq('table_name', tableName)
                .eq('pk', pk)
                .lte('server_version', highWatermark)
        )
        .order('desc')
        .first()) as SnapshotHistoryDoc | null;
    return history ? historySnapshotCandidate(history) : null;
}

async function readMaterializedSnapshotRows(
    ctx: MutationCtx,
    workspaceId: Id<'workspaces'>,
    tableName: string,
    afterPk: string | null,
    limit: number
): Promise<MaterializedDoc[]> {
    const tableInfo = TABLE_INDEX_MAP[tableName];
    if (!tableInfo) return [];
    const pkField = getPkField(tableName);
    type DynamicQuery = {
        withIndex: (
            indexName: string,
            build: (q: DynamicIndexBuilder) => DynamicIndexBuilder
        ) => { take: (count: number) => Promise<MaterializedDoc[]> };
    };
    return (ctx.db.query(tableInfo.table as TableNames) as unknown as DynamicQuery)
        .withIndex(tableInfo.indexName, (q) => {
            const scoped = q.eq('workspace_id', workspaceId);
            return afterPk === null ? scoped : scoped.gt(pkField, afterPk);
        })
        .take(limit);
}

async function readSnapshotTombstones(
    ctx: MutationCtx,
    workspaceId: Id<'workspaces'>,
    tableName: string,
    afterPk: string | null,
    limit: number
): Promise<TombstoneDoc[]> {
    return (await ctx.db
        .query('tombstones')
        .withIndex('by_workspace_table_pk', (q) => {
            const scoped = q
                .eq('workspace_id', workspaceId)
                .eq('table_name', tableName);
            return afterPk === null ? scoped : scoped.gt('pk', afterPk);
        })
        .take(limit)) as TombstoneDoc[];
}

async function resolveSnapshotKey(
    ctx: MutationCtx,
    workspaceId: Id<'workspaces'>,
    tableName: string,
    pk: string,
    highWatermark: number,
    row: MaterializedDoc | undefined,
    tombstone: TombstoneDoc | undefined,
    callerUserId: Id<'users'>
): Promise<SnapshotCandidate | null> {
    if (
        tableName === 'notifications' &&
        (!row || row.user_id !== String(callerUserId))
    ) {
        // Orphan tombstones do not carry an owner, so fail closed rather than
        // expose another user's notification existence.
        return null;
    }
    const candidates: SnapshotCandidate[] = [];
    const history = await readSnapshotHistory(
        ctx,
        workspaceId,
        tableName,
        pk,
        highWatermark
    );
    if (history) candidates.push(history);
    if (row) candidates.push(materializedSnapshotCandidate(tableName, pk, row));
    if (tombstone) candidates.push(tombstoneSnapshotCandidate(tableName, pk, tombstone));
    return resolveSnapshotWinner(candidates, highWatermark);
}

/**
 * Internal helper.
 *
 * Purpose:
 * Ensures a delete operation is represented by a tombstone with the newest
 * clock value so deletes do not resurrect during sync.
 */
async function upsertTombstone(
    ctx: MutationCtx,
    workspaceId: Id<'workspaces'>,
    op: {
        table_name: string;
        pk: string;
        clock: number;
        hlc: string;
        op_id: string;
    },
    serverVersion: number,
    deletedAt: number,
    preimageVersion: number
): Promise<void> {
    const existing = await ctx.db
        .query('tombstones')
        .withIndex('by_workspace_table_pk', (q) =>
            q.eq('workspace_id', workspaceId)
                .eq('table_name', op.table_name)
                .eq('pk', op.pk)
        )
        .first();

    if (existing && !incomingWinsStoredRevision(
        { clock: op.clock, hlc: op.hlc, opId: op.op_id },
        existing
    )) {
        return;
    }

    if (existing) {
        const previousRevision = legacySnapshotRevision(op.table_name, op.pk, existing);
        await ctx.db.insert('sync_record_versions', {
            workspace_id: workspaceId,
            table_name: op.table_name,
            pk: op.pk,
            server_version: existing.server_version ?? preimageVersion,
            kind: 'tombstone',
            clock: previousRevision.clock,
            hlc: previousRevision.hlc,
            op_id: previousRevision.opId,
            server_deleted_at: existing.server_deleted_at ?? existing.deleted_at,
        });
        await ctx.db.patch(existing._id, {
            deleted_at: deletedAt,
            server_deleted_at: nowSec(),
            clock: op.clock,
            hlc: op.hlc,
            op_id: op.op_id,
            server_version: serverVersion,
        });
        return;
    }

    await ctx.db.insert('tombstones', {
        workspace_id: workspaceId,
        table_name: op.table_name,
        pk: op.pk,
        deleted_at: deletedAt,
        server_deleted_at: nowSec(),
        clock: op.clock,
        hlc: op.hlc,
        op_id: op.op_id,
        server_version: serverVersion,
        created_at: nowSec(),
    });
}

/**
 * Internal helper.
 *
 * Purpose:
 * Removes fields that could reassign documents across workspaces or Convex IDs.
 */
function sanitizePayload(
    tableName: string,
    payload: Record<string, unknown> | undefined
): Record<string, unknown> | undefined {
    if (!payload) return payload;
    const { workspace_id, _id, ...safe } = payload;
    // ref_count is a derived cache, not wire authority. Keeping a server-side
    // zero maintains backwards-compatible row shape while canonical reference
    // queries remain the only source used by quota/GC.
    if (tableName === 'file_meta') delete safe.ref_count;
    return safe;
}

/**
 * Internal helper.
 *
 * Purpose:
 * Lightweight validation for common fields to avoid malformed writes.
 */
function validatePayload(
    tableName: string,
    payload: Record<string, unknown> | undefined
): void {
    if (!payload) return;
    if ('deleted' in payload && typeof payload.deleted !== 'boolean') {
        throw new Error(
            `Invalid payload for ${tableName}: 'deleted' must be boolean`
        );
    }
}

function stableJson(value: unknown): string {
    if (value === undefined) return 'undefined';
    if (value === null || typeof value !== 'object') return JSON.stringify(value);
    if (Array.isArray(value)) return `[${value.map(stableJson).join(',')}]`;
    const record = value as Record<string, unknown>;
    return `{${Object.keys(record).sort().map((key) =>
        `${JSON.stringify(key)}:${stableJson(record[key])}`
    ).join(',')}}`;
}

function operationFingerprint(op: SyncOperation): string {
    return stableJson({
        table_name: op.table_name,
        operation: op.operation,
        pk: op.pk,
        payload: op.payload,
        clock: op.clock,
        hlc: op.hlc,
        device_id: op.device_id,
    });
}

function changeLogFingerprint(change: Record<string, unknown>): string {
    return stableJson({
        table_name: change.table_name,
        operation: change.op,
        pk: change.pk,
        payload: change.payload,
        clock: change.clock,
        hlc: change.hlc,
        device_id: change.device_id,
    });
}

function validateSyncOperation(
    workspaceId: Id<'workspaces'>,
    op: SyncOperation
): string | undefined {
    if (op.op_id.length > MAX_OP_ID_LENGTH) {
        return `op_id too long: ${op.op_id.length} exceeds ${MAX_OP_ID_LENGTH}`;
    }
    if (!TABLE_INDEX_MAP[op.table_name]) return `Invalid table: ${op.table_name}`;

    let serializedPayload: string | undefined;
    try {
        serializedPayload = op.payload === undefined ? undefined : JSON.stringify(op.payload);
    } catch {
        return `Invalid payload for ${op.table_name}: payload is not serializable`;
    }
    if ((serializedPayload?.length ?? 0) > MAX_PAYLOAD_SIZE_BYTES) {
        return `Payload too large for ${op.table_name}: exceeds ${MAX_PAYLOAD_SIZE_BYTES} bytes`;
    }
    if (op.payload !== undefined &&
        (typeof op.payload !== 'object' || op.payload === null || Array.isArray(op.payload))) {
        return `Invalid payload for ${op.table_name}: payload must be an object`;
    }

    const payload = op.payload as Record<string, unknown> | undefined;
    try {
        validatePayload(op.table_name, payload);
    } catch (error) {
        return error instanceof Error ? error.message : String(error);
    }
    if (!payload) return undefined;
    if ('_id' in payload) return `Invalid payload for ${op.table_name}: '_id' is immutable`;
    if ('workspace_id' in payload && String(payload.workspace_id) !== String(workspaceId)) {
        return `Invalid payload for ${op.table_name}: 'workspace_id' is immutable`;
    }
    const pkField = getPkField(op.table_name);
    if (pkField in payload && payload[pkField] !== op.pk) {
        return `Invalid payload for ${op.table_name}: '${pkField}' must match operation pk`;
    }
    return undefined;
}

/**
 * Internal helper.
 *
 * Purpose:
 * Applies a single operation to a data table using LWW conflict resolution.
 *
 * Behavior:
 * - Deletes set `deleted` and track `deleted_at`
 * - Puts update if incoming `clock` is newer or equal
 */
async function applyOpToTable(
    ctx: MutationCtx,
    workspaceId: Id<'workspaces'>,
    op: {
        table_name: string;
        operation: 'put' | 'delete';
        pk: string;
        payload?: unknown;
        clock: number;
        hlc: string;
        op_id: string;
    },
    serverVersion: number,
    preimageVersion: number
): Promise<{ wasExisting: boolean; applied: boolean }> {
    const tableInfo = TABLE_INDEX_MAP[op.table_name];
    if (!tableInfo) {
        console.warn(`Unknown table: ${op.table_name}`);
        return { wasExisting: false, applied: false };
    }

    const { table, indexName } = tableInfo;
    const pkField = getPkField(op.table_name);

    // SECURITY: Strip workspace_id and _id from payload to prevent injection attacks
    const payload = sanitizePayload(
        op.table_name,
        op.payload as Record<string, unknown> | undefined
    );

    validatePayload(op.table_name, payload);
    const payloadCreatedAt =
        typeof payload?.created_at === 'number' ? (payload.created_at as number) : undefined;
    const payloadUpdatedAt =
        typeof payload?.updated_at === 'number' ? (payload.updated_at as number) : undefined;

    // Find existing record
    // Note: Type casts (as any) are necessary because Convex doesn't support
    // fully type-safe dynamic table queries. Table name is validated via TABLE_INDEX_MAP
    // and runtime validation of payloads happens client-side in ConflictResolver.applyPut()
    // using Zod schemas (TABLE_PAYLOAD_SCHEMAS).
    // Future: Consider a type-safe helper with switch statement for each table.
    type IndexQueryBuilder = {
        eq: (field: string, value: unknown) => IndexQueryBuilder;
    };
    type ConvexDoc = {
        _id: Id<TableNames>;
        deleted?: boolean;
        clock?: number;
        hlc?: string;
        op_id?: string;
    } & Record<string, unknown>;
    type QueryByIndex = {
        withIndex: (
            index: string,
            cb: (q: IndexQueryBuilder) => IndexQueryBuilder
        ) => { first: () => Promise<ConvexDoc | null> };
    };

    const typedTable = table as TableNames;
    const existing = await (ctx.db.query(typedTable) as unknown as QueryByIndex)
        .withIndex(indexName, (q) =>
            pkField === 'hash'
                ? q.eq('workspace_id', workspaceId).eq('hash', op.pk)
                : q.eq('workspace_id', workspaceId).eq('id', op.pk)
        )
        .first();
    const existingTombstone = await (ctx.db.query('tombstones') as unknown as QueryByIndex)
        .withIndex('by_workspace_table_pk', (q) =>
            q.eq('workspace_id', workspaceId)
                .eq('table_name', op.table_name)
                .eq('pk', op.pk)
        )
        .first();

    const wasExisting = Boolean(existing);
    let applied = false;

    if (op.operation === 'delete') {
        if (!existing) {
            return { wasExisting, applied };
        }

        const existingClock = existing.clock ?? 0;
        const existingHlc = typeof existing.hlc === 'string' ? existing.hlc : '';
        const shouldApplyDelete = incomingWinsStoredRevision(
            { clock: op.clock, hlc: op.hlc, opId: op.op_id },
            existing
        );
        if (!shouldApplyDelete) {
            return { wasExisting, applied };
        }

        console.debug('[sync] apply delete', {
            table: op.table_name,
            pk: op.pk,
            clock: op.clock,
            existingClock,
            existingHlc,
            incomingHlc: op.hlc,
        });
        await recordMaterializedPreimage(
            ctx,
            workspaceId,
            op.table_name,
            op.pk,
            existing,
            preimageVersion
        );
        await ctx.db.patch(existing._id, {
            deleted: true,
            // Retention age must be server-authored; client timestamps cannot
            // make a live tombstone eligible for premature collection.
            deleted_at: nowSec(),
            updated_at: payloadUpdatedAt ?? nowSec(),
            clock: op.clock,
            hlc: op.hlc,
            op_id: op.op_id,
            server_version: serverVersion,
        });
        applied = true;
    } else {
        // Put operation
        if (existing) {
            const existingClock = existing.clock ?? 0;
            const existingHlc = typeof existing.hlc === 'string' ? existing.hlc : '';
            const shouldApply = incomingWinsStoredRevision(
                { clock: op.clock, hlc: op.hlc, opId: op.op_id },
                existing
            );
            // LWW with deterministic HLC tie-break on equal clocks.
            if (shouldApply) {
                console.debug('[sync] apply put', {
                    table: op.table_name,
                    pk: op.pk,
                    clock: op.clock,
                    existingClock,
                    existingHlc,
                    incomingHlc: op.hlc,
                    applied: true,
                });
                await recordMaterializedPreimage(
                    ctx,
                    workspaceId,
                    op.table_name,
                    op.pk,
                    existing,
                    preimageVersion
                );
                await ctx.db.patch(existing._id, {
                    ...(payload ?? {}),
                    clock: op.clock,
                    hlc: op.hlc,
                    op_id: op.op_id,
                    server_version: serverVersion,
                    updated_at: payloadUpdatedAt ?? nowSec(),
                });
                applied = true;
            } else {
                console.debug('[sync] apply put skipped', {
                    table: op.table_name,
                    pk: op.pk,
                    clock: op.clock,
                    existingClock,
                    existingHlc,
                    incomingHlc: op.hlc,
                    applied: false,
                });
            }
            // else: local wins, no-op
        } else {
            if (existingTombstone && !incomingWinsStoredRevision(
                { clock: op.clock, hlc: op.hlc, opId: op.op_id },
                existingTombstone
            )) {
                return { wasExisting, applied };
            }
            // New record
            const insertPayload: Record<string, unknown> = {
                ...(payload ?? {}),
            };
            if (table === 'file_meta' && insertPayload.ref_count == null) {
                insertPayload.ref_count = 0;
            }
            type InsertableDb = {
                insert: (name: TableNames, doc: Record<string, unknown>) => Promise<Id<TableNames>>;
            };
            await (ctx.db as unknown as InsertableDb).insert(table as TableNames, {
                ...insertPayload,
                workspace_id: workspaceId,
                [pkField]: op.pk,
                clock: op.clock,
                hlc: op.hlc,
                op_id: op.op_id,
                server_version: serverVersion,
                created_at: payloadCreatedAt ?? nowSec(),
                updated_at: payloadUpdatedAt ?? payloadCreatedAt ?? nowSec(),
            });
            applied = true;
        }
    }

    return { wasExisting, applied };
}

/**
 * Internal authorization helper.
 *
 * Purpose:
 * Ensures the caller is authenticated and a member of the workspace.
 */
async function requireSyncReadAccess(
    ctx: MutationCtx | QueryCtx,
    workspaceId: Id<'workspaces'>
): Promise<Id<'users'>> {
    const { userId } = await requireWorkspaceRole(ctx, workspaceId, SYNC_READ_ROLES);
    if (!userId) throw new Error('Unauthorized');
    return userId;
}

async function requireSyncWriteAccess(
    ctx: MutationCtx,
    workspaceId: Id<'workspaces'>
): Promise<Id<'users'>> {
    const { userId } = await requireWorkspaceRole(ctx, workspaceId, SYNC_WRITE_ROLES);
    if (!userId) throw new Error('Unauthorized');
    return userId;
}

type SyncOperation = {
    op_id: string;
    table_name: string;
    operation: 'put' | 'delete';
    pk: string;
    payload?: unknown;
    clock: number;
    hlc: string;
    device_id: string;
};

/** Derives notification ownership from the authenticated sync subject. */
async function scopeNotificationWrite(
    ctx: MutationCtx,
    workspaceId: Id<'workspaces'>,
    callerUserId: Id<'users'>,
    op: SyncOperation
): Promise<SyncOperation> {
    if (op.table_name !== 'notifications') return op;

    const callerId = String(callerUserId);
    const payload =
        typeof op.payload === 'object' && op.payload !== null
            ? (op.payload as Record<string, unknown>)
            : {};
    if (payload.user_id !== undefined && payload.user_id !== callerId) {
        throw new Error('Forbidden: notification owner mismatch');
    }

    const existing = await ctx.db
        .query('notifications')
        .withIndex('by_workspace_id', (q) =>
            q.eq('workspace_id', workspaceId).eq('id', op.pk)
        )
        .first();

    if (existing && existing.user_id !== callerId) {
        throw new Error('Forbidden: notification owner mismatch');
    }
    if (!existing && op.operation === 'delete') {
        throw new Error('Forbidden: notification not owned by caller');
    }

    return {
        ...op,
        payload: {
            ...payload,
            user_id: callerId,
        },
    };
}

function isChangeVisibleToUser(
    change: { table_name: string; payload?: unknown },
    callerUserId: Id<'users'>
): boolean {
    if (change.table_name !== 'notifications') return true;
    if (typeof change.payload !== 'object' || change.payload === null) {
        return false;
    }
    return (change.payload as Record<string, unknown>).user_id === String(callerUserId);
}

// ============================================================
// SYNC MUTATIONS
// ============================================================

/**
 * `sync.push` (mutation)
 *
 * Purpose:
 * Writes a batch of client-side changes into Convex and the change log.
 *
 * Behavior:
 * - Enforces a maximum batch size and payload size
 * - Idempotent by `op_id` and returns existing server versions
 * - Applies LWW conflict resolution per table
 * - Writes `change_log` entries for downstream pulls
 *
 * Constraints:
 * - `op_id` length is capped
 * - Only allowlisted table names are accepted
 * - Viewer memberships cannot push workspace changes
 */
export const push = mutation({
    args: {
        workspace_id: v.id('workspaces'),
        ops: v.array(
            v.object({
                op_id: v.string(),
                table_name: v.string(),
                operation: v.union(v.literal('put'), v.literal('delete')),
                pk: v.string(),
                payload: v.optional(v.any()),
                clock: v.number(),
                hlc: v.string(),
                device_id: v.string(),
            })
        ),
    },
    handler: async (ctx, args) => {
        console.debug('[sync] push', {
            workspace: args.workspace_id,
            ops: args.ops.length,
        });
        // Validate batch size to prevent abuse
        if (args.ops.length > MAX_PUSH_OPS) {
            throw new Error(`Batch size ${args.ops.length} exceeds maximum of ${MAX_PUSH_OPS} ops`);
        }

        // Verify workspace membership
        const callerUserId = await requireSyncWriteAccess(ctx, args.workspace_id);

        type Result = {
            opId: string;
            success: boolean;
            serverVersion?: number;
            error?: string;
            errorCode?:
                | 'VALIDATION_ERROR'
                | 'UNAUTHORIZED'
                | 'CONFLICT'
                | 'NOT_FOUND'
                | 'RATE_LIMITED'
                | 'OVERSIZED'
                | 'NETWORK_ERROR'
                | 'SERVER_ERROR'
                | 'UNKNOWN';
            tableName?: string;
            operation?: 'put' | 'delete';
            payload?: unknown;
            wasExisting?: boolean;
            applied?: boolean;
        };

        const results: Array<Result | undefined> = new Array(args.ops.length);
        const failGroup = (
            indices: number[],
            op: SyncOperation,
            error: string,
            errorCode: NonNullable<Result['errorCode']> = 'VALIDATION_ERROR'
        ) => {
            for (const index of indices) {
                results[index] = {
                    opId: op.op_id,
                    success: false,
                    error,
                    errorCode,
                    tableName: op.table_name,
                    operation: op.operation,
                };
            }
        };

        let latestVersion = 0;
        let batchPreimageVersion = 0;

        // Group by operation ID before allocating versions. Exact repeats share
        // one application/version; conflicting reuse is rejected for every copy.
        const groups = new Map<string, {
            op: SyncOperation;
            fingerprint: string;
            indices: number[];
            conflicting: boolean;
        }>();
        args.ops.forEach((op, index) => {
            const fingerprint = operationFingerprint(op);
            const group = groups.get(op.op_id);
            if (!group) {
                groups.set(op.op_id, { op, fingerprint, indices: [index], conflicting: false });
                return;
            }
            group.indices.push(index);
            if (group.fingerprint !== fingerprint) group.conflicting = true;
        });

        const candidates: Array<{ op: SyncOperation; indices: number[] }> = [];
        for (const group of groups.values()) {
            if (group.conflicting) {
                failGroup(
                    group.indices,
                    group.op,
                    `Conflicting operations reuse op_id ${group.op.op_id}`,
                    'CONFLICT'
                );
                continue;
            }
            const validationError = validateSyncOperation(args.workspace_id, group.op);
            if (validationError) {
                failGroup(group.indices, group.op, validationError);
                continue;
            }
            try {
                const scopedOp = await scopeNotificationWrite(
                    ctx,
                    args.workspace_id,
                    callerUserId,
                    group.op
                );
                candidates.push({ op: scopedOp, indices: group.indices });
            } catch (error) {
                failGroup(
                    group.indices,
                    group.op,
                    error instanceof Error ? error.message : String(error)
                );
            }
        }

        const existingLogs = await Promise.all(
            candidates.map(({ op }) =>
                ctx.db
                    .query('change_log')
                    .withIndex('by_op_id', (q) => q.eq('op_id', op.op_id))
                    .first()
            )
        );
        const newOps: typeof candidates = [];
        candidates.forEach((candidate, index) => {
            const existing = existingLogs[index];
            if (!existing) {
                newOps.push(candidate);
                return;
            }
            if (
                String(existing.workspace_id) !== String(args.workspace_id) ||
                changeLogFingerprint(existing) !== operationFingerprint(candidate.op)
            ) {
                failGroup(
                    candidate.indices,
                    candidate.op,
                    `Conflicting operation reuses processed op_id ${candidate.op.op_id}`,
                    'CONFLICT'
                );
                return;
            }
            latestVersion = Math.max(latestVersion, existing.server_version);
            for (const resultIndex of candidate.indices) {
                results[resultIndex] = {
                    opId: candidate.op.op_id,
                    success: true,
                    serverVersion: existing.server_version,
                };
            }
        });

        if (newOps.length > 0) {
            const startVersion = await allocateServerVersions(ctx, args.workspace_id, newOps.length);
            batchPreimageVersion = startVersion - 1;
            latestVersion = startVersion + newOps.length - 1;

            // Apply unique ops sequentially in allocated server-version order.
            for (let offset = 0; offset < newOps.length; offset += 1) {
                const { op, indices } = newOps[offset]!;
                const serverVersion = startVersion + offset;
                try {
                    console.debug('[sync] push apply', {
                        table: op.table_name,
                        pk: op.pk,
                        op: op.operation,
                        clock: op.clock,
                        opId: op.op_id,
                        serverVersion,
                    });
                    const applyResult = await applyOpToTable(
                        ctx,
                        args.workspace_id,
                        op,
                        serverVersion,
                        batchPreimageVersion
                    );

                    const opPayload =
                        typeof op.payload === 'object' && op.payload !== null
                            ? (op.payload as Record<string, unknown>)
                            : undefined;
                    await ctx.db.insert('change_log', {
                        workspace_id: args.workspace_id,
                        server_version: serverVersion,
                        table_name: op.table_name,
                        pk: op.pk,
                        op: op.operation,
                        payload: opPayload,
                        clock: op.clock,
                        hlc: op.hlc,
                        device_id: op.device_id,
                        op_id: op.op_id,
                        created_at: nowSec(),
                    });

                    if (op.operation === 'delete') {
                        await upsertTombstone(
                            ctx,
                            args.workspace_id,
                            op,
                            serverVersion,
                            nowSec(),
                            batchPreimageVersion
                        );
                    }

                    const success: Result = {
                        opId: op.op_id,
                        serverVersion,
                        success: true,
                        tableName: op.table_name,
                        operation: op.operation,
                        payload: opPayload,
                        wasExisting: applyResult.wasExisting,
                        applied: applyResult.applied,
                    };
                    for (const resultIndex of indices) results[resultIndex] = success;
                } catch (error) {
                    failGroup(indices, op, String(error), 'SERVER_ERROR');
                }
            }
        }

        return {
            results: results.filter((result): result is Result => Boolean(result)),
            serverVersion: latestVersion,
        };
    },
});

/**
 * Bounded, repeatable repair for tombstones created before full revision
 * metadata was persisted. A row is repaired only when its exact delete log is
 * uniquely identifiable; ambiguous or missing history is returned to the
 * operator and left untouched.
 */
export const repairLegacyTombstones = internalMutation({
    args: {
        workspace_id: v.id('workspaces'),
        after_server_version: v.optional(v.number()),
        limit: v.optional(v.number()),
    },
    handler: async (ctx, args) => {
        const limit = Math.max(1, Math.min(500, Math.floor(args.limit ?? 100)));
        const after = Math.max(0, Math.floor(args.after_server_version ?? 0));
        const page = await ctx.db
            .query('tombstones')
            .withIndex('by_workspace_version', (q) =>
                q.eq('workspace_id', args.workspace_id).gt('server_version', after)
            )
            .order('asc')
            .take(limit + 1);
        const rows = page.slice(0, limit);
        const ambiguous: string[] = [];
        const unresolved: string[] = [];
        let repaired = 0;

        for (const tombstone of rows) {
            const needsRevision = !completeStoredRevision(tombstone);
            const needsServerTime = typeof tombstone.server_deleted_at !== 'number';
            if (!needsRevision && !needsServerTime) continue;

            const logs = await ctx.db
                .query('change_log')
                .withIndex('by_workspace_version', (q) =>
                    q.eq('workspace_id', args.workspace_id)
                        .eq('server_version', tombstone.server_version)
                )
                .take(10);
            const matches = logs.filter((log) =>
                log.op === 'delete' &&
                log.table_name === tombstone.table_name &&
                log.pk === tombstone.pk &&
                log.clock === tombstone.clock
            );
            const key = `${tombstone.table_name}:${tombstone.pk}`;
            if (matches.length > 1) {
                ambiguous.push(key);
                continue;
            }
            const match = matches[0];
            if (!match && needsRevision) {
                unresolved.push(key);
                continue;
            }
            await ctx.db.patch(tombstone._id, {
                hlc: match?.hlc ?? tombstone.hlc,
                op_id: match?.op_id ?? tombstone.op_id,
                server_deleted_at:
                    match?.created_at ?? tombstone.created_at ?? tombstone.deleted_at,
            });
            repaired += 1;
        }

        return {
            scanned: rows.length,
            repaired,
            ambiguous,
            unresolved,
            nextServerVersion: rows.at(-1)?.server_version ?? after,
            hasMore: page.length > limit,
        };
    },
});

/**
 * `sync.updateDeviceCursor` (mutation)
 *
 * Purpose:
 * Records the latest server version seen by a device for retention logic.
 */
export const updateDeviceCursor = mutation({
    args: {
        workspace_id: v.id('workspaces'),
        device_id: v.string(),
        last_seen_version: v.number(),
    },
    handler: async (ctx, args) => {
        const callerUserId = await requireSyncReadAccess(ctx, args.workspace_id);
        const deviceId = args.device_id.trim();
        if (!deviceId || deviceId.length > 256) throw new Error('Invalid device_id');
        assertSafeNonnegativeInteger(args.last_seen_version, 'last_seen_version');

        const counter = await ctx.db
            .query('server_version_counter')
            .withIndex('by_workspace', (q) => q.eq('workspace_id', args.workspace_id))
            .first();
        if (args.last_seen_version > (counter?.value ?? 0)) {
            throw new Error('Cursor exceeds workspace version');
        }

        const existing = await ctx.db
            .query('device_cursors')
            .withIndex('by_workspace_device', (q) =>
                q.eq('workspace_id', args.workspace_id).eq('device_id', deviceId)
            )
            .first();

        if (existing) {
            if (existing.owner_user_id && existing.owner_user_id !== callerUserId) {
                throw new Error('Device cursor belongs to another user');
            }
            if (args.last_seen_version < existing.last_seen_version) {
                throw new Error('Device cursor cannot regress');
            }
            await ctx.db.patch(existing._id, {
                owner_user_id: existing.owner_user_id ?? callerUserId,
                last_seen_version: args.last_seen_version,
                updated_at: nowSec(),
            });
        } else {
            await ctx.db.insert('device_cursors', {
                workspace_id: args.workspace_id,
                device_id: deviceId,
                owner_user_id: callerUserId,
                last_seen_version: args.last_seen_version,
                updated_at: nowSec(),
            });
        }
    },
});

/**
 * `sync.snapshot` (mutation)
 *
 * Captures one workspace high-watermark and returns deterministic bounded
 * pages from canonical materialized rows/tombstones. Continuation calls bind
 * to the persisted session and reconstruct pre-watermark state from bounded
 * pre-image lookups if writes occurred after the first page.
 */
export const snapshot = mutation({
    args: {
        workspace_id: v.id('workspaces'),
        page_size: v.number(),
        page_token: v.optional(v.string()),
        tables: v.optional(v.array(v.string())),
    },
    handler: async (ctx, args) => {
        if (
            !Number.isInteger(args.page_size) ||
            args.page_size <= 0 ||
            args.page_size > MAX_SNAPSHOT_PAGE_SIZE
        ) {
            throw new Error(
                `Snapshot page size must be between 1 and ${MAX_SNAPSHOT_PAGE_SIZE}`
            );
        }

        const tables = normalizeSnapshotTables(args.tables, Object.keys(TABLE_INDEX_MAP));
        const callerUserId = await requireSyncReadAccess(ctx, args.workspace_id);

        const now = nowSec();
        let snapshotId: string;
        let highWatermark: number;
        let tableIndex = 0;
        let afterPk: string | null = null;

        if (args.page_token) {
            const cursor = decodeSnapshotCursor(args.page_token);
            let session: {
                workspace_id: Id<'workspaces'>;
                user_id: string;
                high_watermark: number;
                tables: string[];
                expires_at: number;
            } | null = null;
            try {
                session = await ctx.db.get(
                    cursor.snapshotId as Id<'sync_snapshot_sessions'>
                );
            } catch {
                throw new Error('Snapshot session is unavailable');
            }
            if (
                !session ||
                String(session.workspace_id) !== String(args.workspace_id) ||
                session.user_id !== callerUserId ||
                session.expires_at <= now
            ) {
                throw new Error('Snapshot session is unavailable');
            }
            if (JSON.stringify(session.tables) !== JSON.stringify(tables)) {
                throw new Error('Snapshot page token does not match the requested tables');
            }
            if (cursor.tableIndex > session.tables.length) {
                throw new Error('Invalid snapshot page token');
            }
            snapshotId = cursor.snapshotId;
            highWatermark = session.high_watermark;
            tableIndex = cursor.tableIndex;
            afterPk = cursor.afterPk;
        } else {
            const counter = await ctx.db
                .query('server_version_counter')
                .withIndex('by_workspace', (q) =>
                    q.eq('workspace_id', args.workspace_id)
                )
                .first();
            highWatermark = counter?.value ?? 0;
            snapshotId = String(await ctx.db.insert('sync_snapshot_sessions', {
                workspace_id: args.workspace_id,
                user_id: callerUserId,
                high_watermark: highWatermark,
                tables,
                created_at: now,
                expires_at: now + SNAPSHOT_TTL_SECONDS,
            }));
        }

        type PageItem =
            | {
                  kind: 'row';
                  tableName: string;
                  pk: string;
                  payload: unknown;
                  revision: SnapshotRevision;
              }
            | {
                  kind: 'tombstone';
                  tableName: string;
                  pk: string;
                  revision: SnapshotRevision;
                  serverDeletedAt: number;
              };
        const items: PageItem[] = [];
        let examined = 0;
        let stoppedWithinTable = false;

        while (tableIndex < tables.length && examined < args.page_size) {
            const tableName = tables[tableIndex];
            if (!tableName) break;
            const remaining = args.page_size - examined;
            const [rows, tombstones] = await Promise.all([
                readMaterializedSnapshotRows(
                    ctx,
                    args.workspace_id,
                    tableName,
                    afterPk,
                    remaining + 1
                ),
                readSnapshotTombstones(
                    ctx,
                    args.workspace_id,
                    tableName,
                    afterPk,
                    remaining + 1
                ),
            ]);

            const pkField = getPkField(tableName);
            const rowsByPk = new Map<string, MaterializedDoc>();
            for (const row of rows) {
                const pk = row[pkField];
                if (typeof pk === 'string' && pk.length > 0) rowsByPk.set(pk, row);
            }
            const tombstonesByPk = new Map(tombstones.map((row) => [row.pk, row]));
            const orderedPks = [
                ...new Set([...rowsByPk.keys(), ...tombstonesByPk.keys()]),
            ].sort();
            const pagePks = orderedPks.slice(0, remaining);

            for (const pk of pagePks) {
                const winner = await resolveSnapshotKey(
                    ctx,
                    args.workspace_id,
                    tableName,
                    pk,
                    highWatermark,
                    rowsByPk.get(pk),
                    tombstonesByPk.get(pk),
                    callerUserId
                );
                if (!winner) continue;
                if (winner.kind === 'row') {
                    items.push({
                        kind: 'row',
                        tableName: winner.tableName,
                        pk: winner.pk,
                        payload: winner.payload,
                        revision: winner.revision,
                    });
                } else {
                    items.push({
                        kind: 'tombstone',
                        tableName: winner.tableName,
                        pk: winner.pk,
                        revision: winner.revision,
                        serverDeletedAt: winner.serverDeletedAt,
                    });
                }
            }

            examined += pagePks.length;
            if (orderedPks.length > pagePks.length) {
                afterPk = pagePks[pagePks.length - 1] ?? afterPk;
                stoppedWithinTable = true;
                break;
            }

            tableIndex += 1;
            afterPk = null;
        }

        const hasMore = stoppedWithinTable || tableIndex < tables.length;
        const nextPageToken = hasMore
            ? encodeSnapshotCursor({
                  version: 1,
                  snapshotId,
                  tableIndex,
                  afterPk,
              })
            : null;

        return {
            workspaceId: String(args.workspace_id),
            snapshotId,
            highWatermark,
            items,
            nextPageToken,
        };
    },
});

/**
 * `sync.queryCanonicalStorage` (query)
 *
 * Returns a strictly bounded page from canonical materialized storage state.
 * This function never consults `change_log` or other retained operation history.
 */
export const queryCanonicalStorage = query({
    args: {
        workspace_id: v.id('workspaces'),
        kind: v.union(
            v.literal('live_metadata'),
            v.literal('reference_edges'),
            v.literal('active_reservations')
        ),
        page_size: v.number(),
        cursor: v.optional(v.string()),
        hash: v.optional(v.string()),
        now: v.optional(v.number()),
    },
    handler: async (ctx, args) => {
        if (
            !Number.isInteger(args.page_size) ||
            args.page_size <= 0 ||
            args.page_size > MAX_CANONICAL_STORAGE_PAGE_SIZE
        ) {
            throw new Error(
                `Canonical storage page size must be between 1 and ${MAX_CANONICAL_STORAGE_PAGE_SIZE}`
            );
        }
        if (args.now !== undefined && (!Number.isInteger(args.now) || args.now < 0)) {
            throw new Error('Invalid canonical storage time');
        }
        await requireSyncReadAccess(ctx, args.workspace_id);
        const hash = args.hash === undefined ? undefined : normalizeStorageHash(args.hash);
        if (args.hash !== undefined && (!hash || args.hash.length > 256)) {
            throw new Error('Invalid canonical storage hash');
        }
        const cursor = decodeCanonicalStorageCursor(args.cursor, args.kind, hash);

        if (args.kind === 'active_reservations') {
            const now = args.now ?? Math.floor(Date.now() / 1000);
            const afterCreationTime = cursor.afterPk ? Number(cursor.afterPk) : undefined;
            if (afterCreationTime !== undefined && !Number.isFinite(afterCreationTime)) {
                throw new Error('Invalid canonical storage cursor');
            }
            const reservationQuery = ctx.db
                .query('upload_intents')
                .withIndex('by_workspace_status', (q) => {
                    const indexed = q.eq('workspace_id', args.workspace_id).eq('status', 'active');
                    return afterCreationTime === undefined
                        ? indexed
                        : indexed.gt('_creationTime', afterCreationTime);
                });
            const rows = await reservationQuery.take(args.page_size + 1);
            const hasMore = rows.length > args.page_size;
            const scanned = hasMore ? rows.slice(0, args.page_size) : rows;
            const items = scanned.flatMap((row) => {
                if (row.expires_at <= now || row.reserved_bytes <= 0 ||
                    (hash !== undefined && row.hash !== hash)) return [];
                return [{
                    kind: 'reservation' as const,
                    reservationId: String(row._id),
                    hash: row.hash,
                    sizeBytes: row.reserved_bytes,
                    expiresAt: row.expires_at,
                }];
            });
            const last = scanned[scanned.length - 1];
            return {
                items,
                hasMore,
                ...(hasMore && last ? {
                    nextCursor: encodeCanonicalStorageCursor({
                        version: 1, kind: args.kind, hash, afterPk: String(last._creationTime),
                    }),
                } : {}),
            };
        }

        if (args.kind === 'live_metadata') {
            const rows = await ctx.db
                .query('file_meta')
                .withIndex('by_workspace_hash', (q) =>
                    q.eq('workspace_id', args.workspace_id).gt('hash', cursor.afterPk)
                )
                .take(args.page_size + 1);
            const hasMore = rows.length > args.page_size;
            const scanned = hasMore ? rows.slice(0, args.page_size) : rows;
            const items = scanned.flatMap((row) => {
                const normalizedHash = normalizeStorageHash(row.hash);
                if (row.deleted || (hash !== undefined && normalizedHash !== hash)) return [];
                if (!Number.isSafeInteger(row.size_bytes) || row.size_bytes < 0) {
                    throw new Error('Invalid canonical file size');
                }
                return [{
                    kind: 'metadata' as const,
                    hash: normalizedHash,
                    sizeBytes: row.size_bytes,
                    ...(row.storage_id ? { storageId: String(row.storage_id) } : {}),
                    updatedAt: row.updated_at,
                }];
            });
            const last = scanned[scanned.length - 1];
            return {
                items,
                hasMore,
                ...(hasMore && last
                    ? {
                          nextCursor: encodeCanonicalStorageCursor({
                              version: 1,
                              kind: args.kind,
                              hash,
                              afterPk: last.hash,
                          }),
                      }
                    : {}),
            };
        }

        type ReferenceSourceTable = 'messages' | 'posts';
        type ReferenceSourceDoc = {
            id: string;
            deleted?: boolean;
            file_hashes?: string | null;
        };
        type ReferenceSourceQuery = {
            withIndex: (
                indexName: string,
                build: (q: DynamicIndexBuilder) => DynamicIndexBuilder
            ) => {
                first: () => Promise<ReferenceSourceDoc | null>;
                take: (count: number) => Promise<ReferenceSourceDoc[]>;
            };
        };
        const tables: ReferenceSourceTable[] = ['messages', 'posts'];
        let tableIndex = cursor.tableIndex ?? 0;
        let afterPk = cursor.afterPk;
        let currentPk = cursor.currentPk;
        let hashOffset = cursor.hashOffset ?? 0;
        let examined = 0;
        const items: Array<{
            kind: 'reference';
            hash: string;
            sourceTable: ReferenceSourceTable;
            sourceId: string;
        }> = [];

        while (
            tableIndex < tables.length &&
            items.length < args.page_size &&
            examined < args.page_size
        ) {
            const table = tables[tableIndex]!;
            const queryTable = () =>
                ctx.db.query(table as TableNames) as unknown as ReferenceSourceQuery;
            let row: ReferenceSourceDoc | null;
            if (currentPk) {
                row = await queryTable()
                    .withIndex('by_workspace_id', (q) =>
                        q.eq('workspace_id', args.workspace_id).eq('id', currentPk)
                    )
                    .first();
            } else {
                row = (await queryTable()
                    .withIndex('by_workspace_id', (q) =>
                        q.eq('workspace_id', args.workspace_id).gt('id', afterPk)
                    )
                    .take(1))[0] ?? null;
            }

            if (!row) {
                tableIndex += 1;
                afterPk = '';
                currentPk = undefined;
                hashOffset = 0;
                continue;
            }
            examined += 1;
            const hashes = row.deleted
                ? []
                : parseCanonicalFileHashes(row.file_hashes).filter(
                      (item) => hash === undefined || item === hash
                  );
            for (let index = hashOffset; index < hashes.length; index += 1) {
                const edgeHash = hashes[index]!;
                items.push({
                    kind: 'reference',
                    hash: edgeHash,
                    sourceTable: table,
                    sourceId: row.id,
                });
                if (items.length === args.page_size) {
                    const nextOffset = index + 1;
                    return {
                        items,
                        hasMore: true,
                        nextCursor: encodeCanonicalStorageCursor({
                            version: 1,
                            kind: args.kind,
                            hash,
                            afterPk: nextOffset < hashes.length ? afterPk : row.id,
                            tableIndex,
                            ...(nextOffset < hashes.length
                                ? { currentPk: row.id, hashOffset: nextOffset }
                                : {}),
                        }),
                    };
                }
            }
            afterPk = row.id;
            currentPk = undefined;
            hashOffset = 0;
        }

        const hasMore = tableIndex < tables.length;
        return {
            items,
            hasMore,
            ...(hasMore
                ? {
                      nextCursor: encodeCanonicalStorageCursor({
                          version: 1,
                          kind: args.kind,
                          hash,
                          afterPk,
                          tableIndex,
                          ...(currentPk ? { currentPk, hashOffset } : {}),
                      }),
                  }
                : {}),
        };
    },
});

// ============================================================
// SYNC QUERIES
// ============================================================

/**
 * `sync.pull` (query)
 *
 * Purpose:
 * Returns a page of changes after a given server_version cursor.
 *
 * Behavior:
 * - Results are ordered by `server_version` ascending
 * - `tables` can be used to filter specific tables
 */
export const pull = query({
    args: {
        workspace_id: v.id('workspaces'),
        cursor: v.number(),
        limit: v.number(),
        tables: v.optional(v.array(v.string())),
    },
    handler: async (ctx, args) => {
        const callerUserId = await requireSyncReadAccess(ctx, args.workspace_id);

        // Cap limit to prevent abuse
        const limit = Math.min(args.limit, MAX_PULL_LIMIT);

        type ChangeLogRow = {
            server_version: number;
            table_name: string;
            pk: string;
            op: string;
            payload?: unknown;
            clock: number;
            hlc: string;
            device_id: string;
            op_id: string;
        };
        const rawResults = (await ctx.db
            .query('change_log')
            .withIndex('by_workspace_version', (q) =>
                q.eq('workspace_id', args.workspace_id).gt('server_version', args.cursor)
            )
            .order('asc')
            .take(limit + 1)) as ChangeLogRow[];

        const hasMore = rawResults.length > limit;
        const window = hasMore ? rawResults.slice(0, -1) : rawResults;
        const tableFilter: string[] = Array.isArray(args.tables) ? args.tables : [];
        const visibleWindow = window.filter((change) =>
            isChangeVisibleToUser(change, callerUserId)
        );
        const changes =
            tableFilter.length > 0
                ? visibleWindow.filter((c) => tableFilter.includes(c.table_name))
                : visibleWindow;

        const lastChange = window[window.length - 1];
        const nextCursor = lastChange ? lastChange.server_version : args.cursor;

        console.debug('[sync] pull', {
            workspace: args.workspace_id,
            cursor: args.cursor,
            limit,
            returned: changes.length,
            nextCursor,
            hasMore,
        });

        return {
            changes: changes.map((c) => ({
                serverVersion: c.server_version,
                tableName: c.table_name,
                pk: c.pk,
                op: c.op,
                payload: c.payload,
                stamp: {
                    clock: c.clock,
                    hlc: c.hlc,
                    deviceId: c.device_id,
                    opId: c.op_id,
                },
            })),
            nextCursor,
            hasMore,
        };
    },
});

/**
 * `sync.watchChanges` (query)
 *
 * Purpose:
 * Reactive subscription endpoint that re-runs when new changes arrive.
 *
 * Behavior:
 * - Returns changes after an optional cursor
 * - Caller controls the page size via `limit`
 */
export const watchChanges = query({
    args: {
        workspace_id: v.id('workspaces'),
        cursor: v.optional(v.number()),
        limit: v.optional(v.number()),
    },
    handler: async (ctx, args) => {
        const callerUserId = await requireSyncReadAccess(ctx, args.workspace_id);
        const since = args.cursor ?? 0;
        const limit = args.limit ?? 100;

        type ChangeLogRow = {
            server_version: number;
            table_name: string;
            pk: string;
            op: string;
            payload?: unknown;
            clock: number;
            hlc: string;
            device_id: string;
            op_id: string;
        };
        const changes = (await ctx.db
            .query('change_log')
            .withIndex('by_workspace_version', (q) =>
                q.eq('workspace_id', args.workspace_id).gt('server_version', since)
            )
            .order('asc')
            .take(limit)) as ChangeLogRow[];

        const latestChange = changes[changes.length - 1];
        const latestVersion = latestChange ? latestChange.server_version : since;
        const visibleChanges = changes.filter((change) =>
            isChangeVisibleToUser(change, callerUserId)
        );

        console.debug('[sync] watchChanges', {
            workspace: args.workspace_id,
            cursor: since,
            limit,
            returned: visibleChanges.length,
            latestVersion,
        });

        return {
            changes: visibleChanges.map((c) => ({
                serverVersion: c.server_version,
                tableName: c.table_name,
                pk: c.pk,
                op: c.op,
                payload: c.payload,
                stamp: {
                    clock: c.clock,
                    hlc: c.hlc,
                    deviceId: c.device_id,
                    opId: c.op_id,
                },
            })),
            latestVersion,
        };
    },
});

/**
 * `sync.getServerVersion` (query)
 *
 * Purpose:
 * Returns the current server version counter for a workspace.
 */
export const getServerVersion = query({
    args: {
        workspace_id: v.id('workspaces'),
    },
    handler: async (ctx, args) => {
        await requireSyncReadAccess(ctx, args.workspace_id);
        const counter = await ctx.db
            .query('server_version_counter')
            .withIndex('by_workspace', (q) => q.eq('workspace_id', args.workspace_id))
            .first();

        return counter?.value ?? 0;
    },
});

/**
 * `sync.gcTombstones` (internal mutation)
 *
 * Purpose:
 * Deletes one bounded page of old tombstones already acknowledged by every
 * registered device. Fresh devices bootstrap from the materialized snapshot.
 *
 * Behavior:
 * - Is unavailable to public Convex callers
 * - Scans and deletes at most `batch_size` records
 */
export const gcTombstones = internalMutation({
    args: {
        workspace_id: v.id('workspaces'),
        retention_seconds: v.number(),
        batch_size: v.optional(v.number()),
        cursor: v.optional(v.number()),
    },
    handler: async (ctx, args) => {
        validateGcArguments(args);
        const startCursor = args.cursor ?? 0;
        if (!SYNC_HISTORY_GC_POLICY.enabled || !SYNC_HISTORY_GC_POLICY.snapshotBootstrapVerified) {
            return { purged: 0, hasMore: false, nextCursor: startCursor, disabled: true, reason: SYNC_HISTORY_GC_POLICY.reason };
        }
        const batchSize = args.batch_size ?? 100;
        const minCursor = await ctx.db.query('device_cursors')
            .withIndex('by_workspace_version', (q) => q.eq('workspace_id', args.workspace_id))
            .order('asc').first();
        if (!minCursor) return { purged: 0, hasMore: false, nextCursor: startCursor, disabled: false };
        const page = await ctx.db.query('tombstones')
            .withIndex('by_workspace_version', (q) => q.eq('workspace_id', args.workspace_id).gt('server_version', startCursor))
            .order('asc').take(batchSize + 1);
        const scanned = page.slice(0, batchSize);
        const cutoff = nowSec() - args.retention_seconds;
        let purged = 0;
        for (const tombstone of scanned) {
            if (tombstone.server_version <= minCursor.last_seen_version && typeof tombstone.server_deleted_at === 'number' && tombstone.server_deleted_at < cutoff) {
                await ctx.db.delete(tombstone._id);
                purged += 1;
            }
        }
        return { purged, hasMore: page.length > batchSize, nextCursor: scanned.at(-1)?.server_version ?? startCursor, disabled: false };
    },
});

/**
 * `sync.gcChangeLog` (internal mutation)
 *
 * Purpose:
 * Deletes one bounded page of old history already acknowledged by every
 * registered device.
 *
 * Behavior:
 * - Is unavailable to public Convex callers
 * - Returns a disabled result without querying or deleting sync history
 */
export const gcChangeLog = internalMutation({
    args: {
        workspace_id: v.id('workspaces'),
        retention_seconds: v.number(),
        batch_size: v.optional(v.number()),
        cursor: v.optional(v.number()),
    },
    handler: async (ctx, args) => {
        validateGcArguments(args);
        const startCursor = args.cursor ?? 0;
        if (!SYNC_HISTORY_GC_POLICY.enabled || !SYNC_HISTORY_GC_POLICY.snapshotBootstrapVerified) {
            return { purged: 0, hasMore: false, nextCursor: startCursor, disabled: true, reason: SYNC_HISTORY_GC_POLICY.reason };
        }
        const batchSize = args.batch_size ?? 100;
        const minCursor = await ctx.db.query('device_cursors')
            .withIndex('by_workspace_version', (q) => q.eq('workspace_id', args.workspace_id))
            .order('asc').first();
        if (!minCursor) return { purged: 0, hasMore: false, nextCursor: startCursor, disabled: false };
        const page = await ctx.db.query('change_log')
            .withIndex('by_workspace_version', (q) => q.eq('workspace_id', args.workspace_id).gt('server_version', startCursor))
            .order('asc').take(batchSize + 1);
        const scanned = page.slice(0, batchSize);
        const cutoff = nowSec() - args.retention_seconds;
        let purged = 0;
        for (const change of scanned) {
            if (change.server_version <= minCursor.last_seen_version && change.created_at < cutoff) {
                await ctx.db.delete(change._id);
                purged += 1;
            }
        }
        return { purged, hasMore: page.length > batchSize, nextCursor: scanned.at(-1)?.server_version ?? startCursor, disabled: false };
    },
});

// ============================================================
// SCHEDULED GC (Internal)
// ============================================================

/**
 * `sync.runWorkspaceGc` (internal mutation)
 *
 * Purpose:
 * Compatibility entry point for deployments that still have a scheduled call.
 * It performs no queries, deletes, or continuation scheduling.
 */
export const runWorkspaceGc = internalMutation({
    args: {
        workspace_id: v.id('workspaces'),
        retention_seconds: v.optional(v.number()),
        batch_size: v.optional(v.number()),
        tombstone_cursor: v.optional(v.number()),
        changelog_cursor: v.optional(v.number()),
        continuation_count: v.optional(v.number()),
    },
    handler: async (_ctx, args) => {
        validateGcArguments(args);
        const startTombstoneCursor = args.tombstone_cursor ?? 0;
        const startChangelogCursor = args.changelog_cursor ?? 0;
        return {
            purged: 0,
            hasMore: false,
            nextTombstoneCursor: startTombstoneCursor,
            nextChangelogCursor: startChangelogCursor,
            disabled: true,
            reason: SYNC_HISTORY_GC_POLICY.reason,
        };
    },
});

/**
 * `sync.runScheduledGc` (internal mutation)
 *
 * Purpose:
 * Compatibility entry point for existing deployments. It does not inspect
 * workspaces or schedule collection while snapshot bootstrap is unavailable.
 */
export const runScheduledGc = internalMutation({
    args: {},
    handler: async () => {
        return {
            workspacesScheduled: 0,
            disabled: true,
            reason: SYNC_HISTORY_GC_POLICY.reason,
        };
    },
});
