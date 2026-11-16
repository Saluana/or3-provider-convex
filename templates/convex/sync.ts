import { isSyncUuid } from './syncAuthoring';
import { historyArgs, readHistory, fenceHistory, changedHistory, requireHistoryCapability } from './syncHistory';
import { assertIdentity, readDefect, retainEvidence } from './syncUnavailable';
import { PullResponseSchema, SnapshotResponseSchema, PushResultItemSchema } from './syncContract/shared/sync/schemas';
import { assertStorageNotDeleted } from './storageDeletion';
import { hasWorkspaceItemSemantics, requireWorkspaceItemCapability } from './workspaceItemCapability';
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
 * - Server-side merge strategies beyond complete-record LWW
 */
import { ConvexError, v, convexToJson, type Value, type Infer } from 'convex/values';
import { makeFunctionReference } from 'convex/server';
import schema from './schema';
import { recordRetainedStorage } from './storageUsage';
import { mutation, query, internalMutation, internalQuery, type MutationCtx, type QueryCtx } from './_generated/server';
import type { Id, TableNames } from './_generated/dataModel';
import { getPkField } from './tableMetadata';
import { requireActiveWorkspace, requireWorkspaceRole } from './authz';
import { SYNC_HISTORY_GC_POLICY, computePullRetention } from './syncHistoryGcPolicy';
import {
    legacySnapshotRevision,
    materializedSnapshotCandidate,
    recordMaterializedPreimage,
    SNAPSHOT_TTL_SECONDS,
    type MaterializedDoc,
    decodeSnapshotCursor,
    encodeSnapshotCursor,
    normalizeSnapshotTables,
    compareSnapshotRevisions,
    resolveSnapshotWinner,
    type SnapshotCandidate,
    type SnapshotRevision,
} from './snapshot';
import { prepareSyncWrite, nextSyncClock } from './syncContract/shared/sync/prepare_sync_write';
import { compareSyncTuple, syncJsonByteLength } from './syncContract/shared/sync/schemas';
import { normalizeHistoricalSyncPayload } from './snapshot';
import { getConvexSize } from 'convex/values';
import { newSyncReadBudget, readSyncBatch, canReadSyncDocument, type SyncReadBudget } from './syncReadBudget';
import { readRequestUsage } from './requestUsage';
import { parseChatSeek, encodeChatSeek } from './historySeek';

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

/** Maximum payload size in bytes (256KB) */
const MAX_PAYLOAD_SIZE_BYTES = 256 * 1024;

const SYNC_READ_ROLES = new Set(['owner', 'editor', 'viewer'] as const);
const SYNC_WRITE_ROLES: ReadonlySet<'owner' | 'editor' | 'viewer'> = new Set(['owner', 'editor']);

/**
 * Key families that were historically stored in the client-writable `kv`
 * table but are host enforcement state. They now live in the private
 * `host_settings` table, and ordinary sync must not recreate or delete copies
 * that later code could mistake for authority.
 */
const RESERVED_KV_SETTING_KEYS: ReadonlySet<string> = new Set([
    'admin.guest_access.enabled',
]);
const RESERVED_KV_SETTING_PREFIXES: readonly string[] = ['plugins.'];

/**
 * Setup values are private host-managed user data and the host migration
 * copies them verbatim. Sync must not be able to seed a crafted setup document
 * (including revision/operation metadata) before the first host read. Matches
 * `plugin:<id>:setup-values` plus the digest- and operation-scoped variants;
 * unknown `plugin:` keys are not part of the private namespace.
 */
function isReservedSetupValuesKey(key: string): boolean {
    if (!key.startsWith('plugin:')) return false;
    const rest = key.slice('plugin:'.length);
    const separatorIndex = rest.indexOf(':');
    if (separatorIndex <= 0) return false;
    const suffix = rest.slice(separatorIndex + 1);
    return suffix === 'setup-values' || suffix.startsWith('setup-values.');
}

function reservedKvSettingKey(op: SyncOperation): string | null {
    if (op.table_name !== 'kv') return null;
    const payload =
        typeof op.payload === 'object' && op.payload !== null
            ? (op.payload as Record<string, unknown>)
            : undefined;
    const candidates: string[] = [];
    if (typeof payload?.name === 'string') candidates.push(payload.name);
    const separatorIndex = op.pk.indexOf(':');
    if (separatorIndex !== -1) candidates.push(op.pk.slice(separatorIndex + 1));
    for (const candidate of candidates) {
        if (!candidate) continue;
        if (RESERVED_KV_SETTING_KEYS.has(candidate)) return candidate;
        if (RESERVED_KV_SETTING_PREFIXES.some((prefix) => candidate.startsWith(prefix))) {
            return candidate;
        }
        if (isReservedSetupValuesKey(candidate)) return candidate;
    }
    return null;
}

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
        const start = nextSyncClock(existing.value);
        const end = existing.value + count;
        if (!Number.isSafeInteger(end)) throw new Error('Sync version exhausted');
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
    gte: (field: string, value: unknown) => DynamicIndexBuilder;
    lte: (field: string, value: unknown) => DynamicIndexBuilder;
};

type CanonicalStorageKind =
    | 'live_metadata'
    | 'retained_metadata'
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

function tombstoneSnapshotCandidate(
    tableName: string,
    pk: string,
    tombstone: TombstoneDoc
): SnapshotCandidate {
    return {
        source: tombstone,
        kind: 'tombstone',
        tableName,
        pk,
        revision: legacySnapshotRevision(tableName, pk, tombstone),
        serverDeletedAt: tombstone.server_deleted_at ?? tombstone.deleted_at,
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
            source: history,
            tableName: history.table_name,
            pk: history.pk,
            revision,
            ...(history.table_name === 'file_meta' && history.payload ? { payload: history.payload } : {}),
            serverDeletedAt: history.server_deleted_at ?? 0,
            serverVersion: history.server_version,
        };
    }
    return {
        kind: 'row',
        source: history,
        tableName: history.table_name,
        pk: history.pk,
        payload: normalizeHistoricalSyncPayload(history.table_name, history.payload),
        revision,
        serverVersion: history.server_version,
    };
}

async function readSnapshotHistory(
    ctx: MutationCtx,
    workspaceId: Id<'workspaces'>,
    tableName: string,
    pk: string,
    highWatermark: number,
    budget?: SyncReadBudget
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
    if (history && budget) { budget.bytes -= getConvexSize(history as import('convex/values').Value); budget.reads--; }
    return history ? historySnapshotCandidate(history) : null;
}

async function readMaterializedSnapshotRows(
    ctx: MutationCtx,
    workspaceId: Id<'workspaces'>,
    tableName: string,
    afterPk: string | null,
    limit: number,
    budget: SyncReadBudget
) {
    const tableInfo = TABLE_INDEX_MAP[tableName];
    if (!tableInfo) throw new Error('Invalid snapshot table');
    const pkField = getPkField(tableName);
    type DynamicQuery = {
        withIndex: (
            indexName: string,
            build: (q: DynamicIndexBuilder) => DynamicIndexBuilder
        ) => AsyncIterable<MaterializedDoc>;
    };
    // Strict Convex string bounds skip NUL extensions of the boundary key.
    // Seek inclusively and discard only the exact boundary, charging its read.
    return readSyncBatch((ctx.db.query(tableInfo.table as TableNames) as unknown as DynamicQuery)
        .withIndex(tableInfo.indexName, (q) => {
            const scoped = q.eq('workspace_id', workspaceId);
            return afterPk === null ? scoped : scoped.gte(pkField, afterPk);
        })
        , limit, budget, row => afterPk !== null && row[pkField] === afterPk);
}

async function readSnapshotTombstones(
    ctx: MutationCtx,
    workspaceId: Id<'workspaces'>,
    tableName: string,
    afterPk: string | null,
    limit: number,
    budget: SyncReadBudget
) {
    return readSyncBatch(ctx.db
        .query('tombstones')
        .withIndex('by_workspace_table_pk', (q) => {
            const scoped = q
                .eq('workspace_id', workspaceId)
                .eq('table_name', tableName);
            return afterPk === null ? scoped : scoped.gte('pk', afterPk);
        })
        , limit, budget, row => afterPk !== null && row.pk === afterPk);
}

async function resolveSnapshotKey(
    ctx: MutationCtx,
    workspaceId: Id<'workspaces'>,
    tableName: string,
    pk: string,
    highWatermark: number,
    row: MaterializedDoc | undefined,
    tombstone: TombstoneDoc | undefined,
    callerUserId: Id<'users'>,
    retainedFiles = false,
    budget?: SyncReadBudget
): Promise<SnapshotCandidate | null> {
    if (
        tableName === 'notifications' &&
        (!row || row.user_id !== String(callerUserId))
    ) {
        // Unknown ownership is private evidence, never a workspace marker.
        if (row && (typeof row.user_id !== 'string' || !row.user_id.length)) await retainEvidence(ctx, workspaceId, await readHistory(ctx), tableName, pk, row, 'unknown_notification_owner');
        if (!row && tombstone) await retainEvidence(ctx, workspaceId, await readHistory(ctx), tableName, pk, tombstone, 'unknown_notification_owner');
        return null;
    }
    const candidates: SnapshotCandidate[] = [];
    const history = await readSnapshotHistory(
        ctx,
        workspaceId,
        tableName,
        pk,
        highWatermark,
        budget
    );
    if (history) candidates.push(history);
    if (row) candidates.push(materializedSnapshotCandidate(tableName, pk, row));
    if (tombstone) candidates.push(tombstoneSnapshotCandidate(tableName, pk, tombstone));
    for (const candidate of candidates) {
        if (!Number.isSafeInteger(candidate.serverVersion) || (candidate.serverVersion as number) < 0) throw new Error('Snapshot coverage version failure');
    }
    const eligible = candidates.filter(candidate => (candidate.serverVersion ?? 0) <= highWatermark);
    const latestVersion = Math.max(...eligible.map(candidate => candidate.serverVersion ?? 0));
    // An unusable latest revision cannot authorize replay of an older preimage.
    const uncertain = eligible.find(candidate => candidate.serverVersion === latestVersion &&
        (!Number.isSafeInteger(candidate.revision.clock) || candidate.revision.clock < 0 || !candidate.revision.hlc || !candidate.revision.opId));
    const winner = uncertain ?? resolveSnapshotWinner(eligible, highWatermark);
    if (retainedFiles && tableName === 'file_meta' && winner?.kind === 'tombstone') {
        const retained = candidates.find(candidate => candidate.kind === 'tombstone' && candidate.payload &&
            candidate.serverVersion !== undefined && candidate.serverVersion <= highWatermark &&
            compareSnapshotRevisions(candidate.revision, winner.revision) === 0);
        if (retained?.kind === 'tombstone') return {
            source: retained.source, kind: 'row', tableName, pk, payload: retained.payload, revision: winner.revision, serverVersion: winner.serverVersion,
        };
        // Old pre-images may lack retained metadata. Never label a concurrent
        // replacement as the original or silently omit an ambiguous original.
        if (row && (row.server_version ?? 0) > highWatermark) throw new Error('Retained backup metadata is unavailable; retry the backup');
    }
    return winner;
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
            retain_until: nowSec() + SNAPSHOT_TTL_SECONDS,
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
    if (tableName === 'kv' && safe.value === null) delete safe.value;
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

/** Reuse the deployed table validators so admission cannot drift from writes. */
function matchesDocumentValidator(definition: any, value: unknown): boolean {
    switch (definition.type) {
        case 'any': return true;
        case 'null': return value === null;
        case 'string': case 'boolean': return typeof value === definition.type;
        case 'number': return typeof value === 'number' && Number.isFinite(value);
        case 'id': return typeof value === 'string' && value.length > 0;
        case 'literal': return value === definition.value;
        case 'union': return definition.value.some((member: any) => matchesDocumentValidator(member, value));
        case 'array': return Array.isArray(value) && value.every(item => matchesDocumentValidator(definition.value, item));
        case 'object': {
            if (!value || typeof value !== 'object' || Array.isArray(value)) return false;
            const record = value as Record<string, unknown>;
            return Object.keys(record).every(key => key in definition.value) &&
                Object.entries(definition.value).every(([key, field]: [string, any]) =>
                    record[key] === undefined ? field.optional : matchesDocumentValidator(field.fieldType, record[key]));
        }
        default: return false;
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

function prepareOperation(op: SyncOperation): SyncOperation {
    const payload = op.operation === 'put' && op.payload && typeof op.payload === 'object'
        ? { ...op.payload as Record<string, unknown>, clock: op.clock, hlc: op.hlc, op_id: op.op_id }
        : op.payload;
    const prepared = prepareSyncWrite({ tableName: op.table_name, pk: op.pk, operation: op.operation,
        payload, stamp: { clock: op.clock, hlc: op.hlc, opId: op.op_id, deviceId: op.device_id } });
    return { ...op, payload: prepared.payload };
}

function validateSyncOperation(
    workspaceId: Id<'workspaces'>,
    op: SyncOperation
): string | undefined {
    if (!isSyncUuid(op.op_id)) {
        return `op_id must be a UUID`;
    }
    if (!TABLE_INDEX_MAP[op.table_name]) return `Invalid table: ${op.table_name}`;
    if (!Number.isSafeInteger(op.clock) || op.clock < 0) return 'Invalid clock';
    for (const [name, value, maximum] of [['hlc', op.hlc, 512], ['device_id', op.device_id, 256], ['pk', op.pk, 1024]] as const) {
        if (!(name === 'device_id' ? value.trim() : value) || value.length > maximum) return `Invalid ${name}`;
    }
    const reservedKey = reservedKvSettingKey(op);
    if (reservedKey) {
        return `Reserved host setting key: ${reservedKey}`;
    }

    let serializedPayload: string | undefined;
    try {
        serializedPayload = op.payload === undefined ? undefined : JSON.stringify(op.payload);
    } catch {
        return `Invalid payload for ${op.table_name}: payload is not serializable`;
    }
    if (
        serializedPayload !== undefined &&
        new TextEncoder().encode(serializedPayload).byteLength >
            MAX_PAYLOAD_SIZE_BYTES
    ) {
        return `Payload too large for ${op.table_name}: exceeds ${MAX_PAYLOAD_SIZE_BYTES} bytes`;
    }
    if (op.payload !== undefined &&
        (typeof op.payload !== 'object' || op.payload === null || Array.isArray(op.payload))) {
        return `Invalid payload for ${op.table_name}: payload must be an object`;
    }

    const payload = op.payload as Record<string, unknown> | undefined;
    if (op.operation === 'delete') {
        try { prepareOperation(op); }
        catch (error) { return error instanceof Error ? error.message : 'Invalid sync write'; }
        return undefined;
    }
    try {
        validatePayload(op.table_name, payload);
    } catch (error) {
        return error instanceof Error ? error.message : String(error);
    }
    if (!payload && op.operation === 'put') return `Invalid payload for ${op.table_name}: put requires a complete record`;
    if (!payload) {
        try { prepareOperation(op); }
        catch (error) { return error instanceof Error ? error.message : 'Invalid sync write'; }
        return undefined;
    }
    if ('_id' in payload) return `Invalid payload for ${op.table_name}: '_id' is immutable`;
    if ('workspace_id' in payload && String(payload.workspace_id) !== String(workspaceId)) {
        return `Invalid payload for ${op.table_name}: 'workspace_id' is immutable`;
    }
    const pkField = getPkField(op.table_name);
    if (pkField in payload && payload[pkField] !== op.pk) {
        return `Invalid payload for ${op.table_name}: '${pkField}' must match operation pk`;
    }
    try { op.payload = prepareOperation(op).payload; }
    catch (error) { return error instanceof Error ? error.message : 'Invalid sync write'; }
    const tables = schema.tables as unknown as Record<string, { validator: { json: any } }>;
    if (op.operation === 'put') {
        if (op.table_name === 'file_meta' && (!Number.isSafeInteger(payload.size_bytes) || (payload.size_bytes as number) < 0)) {
            return 'Invalid payload for file_meta: size_bytes must be a nonnegative integer';
        }
        const canonical = {
            ...sanitizePayload(op.table_name, op.payload as Record<string, unknown>),
            ...(op.table_name === 'file_meta' ? { ref_count: 0 } : {}),
            workspace_id: workspaceId,
            [pkField]: op.pk,
            clock: op.clock,
            hlc: op.hlc,
            op_id: op.op_id,
        };
        if (!matchesDocumentValidator(tables[op.table_name]!.validator.json, canonical)) {
            return `Invalid payload for ${op.table_name}: record does not match the canonical schema`;
        }
    }
    return undefined;
}

type CurrentWinner = { kind: 'put'; payload: unknown; revision: SnapshotRevision } |
    { kind: 'delete'; revision: SnapshotRevision; serverDeletedAt: number };

async function readPushReceipt(ctx: MutationCtx, opId: string) {
    return ctx.db.query('change_log').withIndex('by_op_id', q => q.eq('op_id', opId)).first();
}
async function readPushSources(ctx: MutationCtx, workspace: Id<'workspaces'>, op: SyncOperation) {
    const info = TABLE_INDEX_MAP[op.table_name]!;
    const row = await (ctx.db.query(info.table as TableNames) as any).withIndex(info.indexName, (q: any) =>
        q.eq('workspace_id', workspace).eq(getPkField(op.table_name), op.pk)).first() as MaterializedDoc | null;
    const tombstone = await ctx.db.query('tombstones').withIndex('by_workspace_table_pk', q =>
        q.eq('workspace_id', workspace).eq('table_name', op.table_name).eq('pk', op.pk)).first();
    return { row, tombstone };
}
function pushStoredDefect(op: SyncOperation, receipt: Awaited<ReturnType<typeof readPushReceipt>>,
    { row, tombstone }: Awaited<ReturnType<typeof readPushSources>>) {
    const fault = (source: unknown, reason: string, hold = true) => ({ source, reason, hold });
    if (op.table_name === 'notifications' && row && !row.user_id) return fault(row, 'unknown_notification_owner');
    if (receipt) {
        const reason = readDefect(toWireChange(receipt), false);
        if (reason) return fault(receipt, reason);
    }
    const candidates = [row ? materializedSnapshotCandidate(op.table_name, op.pk, row) : null,
        tombstone ? tombstoneSnapshotCandidate(op.table_name, op.pk, tombstone) : null]
        .filter((value): value is SnapshotCandidate => value !== null);
    // Invalid explicit ordering cannot authorize replacing a corrupt record.
    for (const candidate of candidates) {
        if (!Number.isSafeInteger(candidate.serverVersion) || (candidate.serverVersion as number) < 0) {
            return fault(candidate.source, 'invalid_server_version');
        }
        const revisionDefect = readDefect({ kind: 'tombstone', tableName: op.table_name, pk: op.pk,
            revision: candidate.revision, serverDeletedAt: 0 }, true);
        if (revisionDefect) return fault(candidate.source, revisionDefect);
    }
    const winner = resolveSnapshotWinner(candidates, Number.MAX_SAFE_INTEGER);
    if (!winner) return receipt ? fault(receipt, 'missing_current_winner') : null;
    const { source, serverVersion: _version, ...wire } = winner;
    const reason = readDefect(wire, true);
    if (!reason) return null;
    const incoming = { clock: op.clock, hlc: op.hlc, opId: op.op_id };
    const replaces = !receipt && (!row || incomingWinsStoredRevision(incoming, row)) &&
        (!tombstone || incomingWinsStoredRevision(incoming, tombstone));
    return fault(source, reason, !replaces);
}

async function currentWinner(ctx: MutationCtx, workspaceId: Id<'workspaces'>, tableName: string, pk: string): Promise<CurrentWinner | undefined> {
    const table = TABLE_INDEX_MAP[tableName]!;
    const row = await (ctx.db.query(table.table as TableNames) as any)
        .withIndex(table.indexName, (q: any) => q.eq('workspace_id', workspaceId).eq(getPkField(tableName), pk)).first();
    const tombstone = await ctx.db.query('tombstones').withIndex('by_workspace_table_pk', q =>
        q.eq('workspace_id', workspaceId).eq('table_name', tableName).eq('pk', pk)).first();
    const candidates: SnapshotCandidate[] = [];
    if (row) candidates.push(materializedSnapshotCandidate(tableName, pk, row));
    if (tombstone) candidates.push(tombstoneSnapshotCandidate(tableName, pk, tombstone));
    const winner = resolveSnapshotWinner(candidates, Number.MAX_SAFE_INTEGER);
    if (!winner) return undefined;
    return winner.kind === 'row' ? { kind: 'put', payload: winner.payload, revision: winner.revision } :
        { kind: 'delete', revision: winner.revision, serverDeletedAt: winner.serverDeletedAt };
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
        device_id: string;
    },
    serverVersion: number,
    preimageVersion: number
): Promise<{ wasExisting: boolean; applied: boolean }> {
    op = prepareOperation(op);
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

    if (op.operation === 'put') validatePayload(op.table_name, payload);
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
            ) && (!existingTombstone || incomingWinsStoredRevision(
                { clock: op.clock, hlc: op.hlc, opId: op.op_id }, existingTombstone));
            // LWW with deterministic HLC tie-break on equal clocks.
            if (shouldApply) {
                // Puts replace the complete record. Obsolete attachment JSON in
                // the stored row is neither retained nor new-write authority.
                await assertStorageNotDeleted(ctx, workspaceId, table, { ...(payload ?? {}), [pkField]: op.pk });
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
                await ctx.db.replace(existing._id, {
                    ...(payload ?? {}),
                    workspace_id: workspaceId,
                    [pkField]: op.pk,
                    ...(table === 'file_meta' ? { ref_count: 0 } : {}),
                    clock: op.clock,
                    hlc: op.hlc,
                    op_id: op.op_id,
                    server_version: serverVersion,
                    updated_at: payloadUpdatedAt ?? nowSec(),
                } as any);
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
            await assertStorageNotDeleted(ctx, workspaceId, table, { ...(payload ?? {}), [pkField]: op.pk });
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

    if (applied && op.table_name === 'file_meta' && op.operation === 'put') {
        await recordRetainedStorage(ctx, workspaceId, { ...payload, hash: op.pk });
    }
    if (applied && (op.table_name === 'threads' || op.table_name === 'messages')) {
        const threadIds = op.table_name === 'threads' ? [op.pk] : [existing?.thread_id, payload?.thread_id];
        for (const threadId of new Set(threadIds)) if (typeof threadId === 'string') {
            const revision = await ctx.db.query('chat_history_revisions').withIndex('by_workspace_thread', (q) =>
                q.eq('workspace_id', workspaceId).eq('thread_id', threadId)).first();
            if (revision) await ctx.db.patch(revision._id, { value: revision.value + 1 });
            else await ctx.db.insert('chat_history_revisions', { workspace_id: workspaceId, thread_id: threadId, value: 1 });
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
function scopeNotificationWrite(
    callerUserId: Id<'users'>,
    op: SyncOperation,
    existing: MaterializedDoc | null
): SyncOperation {
    if (op.table_name !== 'notifications') return op;

    const callerId = String(callerUserId);
    const payload =
        typeof op.payload === 'object' && op.payload !== null
            ? (op.payload as Record<string, unknown>)
            : {};
    if (payload.user_id !== undefined && payload.user_id !== callerId) {
        throw new Error('Forbidden: notification owner mismatch');
    }

    if (existing && existing.user_id && existing.user_id !== callerId) {
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

function toWireChange(c: {
    server_version: number;
    table_name: string;
    pk: string;
    op: string;
    payload?: unknown;
    clock: number;
    hlc: string;
    device_id: string;
    op_id: string;
}) {
    return {
        serverVersion: c.server_version,
        tableName: c.table_name,
        pk: c.pk,
        op: c.op,
        payload: normalizeHistoricalSyncPayload(c.table_name, c.payload),
        stamp: {
            clock: c.clock,
            hlc: c.hlc,
            deviceId: c.device_id,
            opId: c.op_id,
        },
    };
}

async function pullRetentionForWorkspace(
    ctx: QueryCtx,
    workspaceId: Id<'workspaces'>,
    cursor: number
) {
    const oldest = await ctx.db
        .query('change_log')
        .withIndex('by_workspace_version', (q) => q.eq('workspace_id', workspaceId))
        .order('asc')
        .first();
    const counter = await ctx.db
        .query('server_version_counter')
        .withIndex('by_workspace', (q) => q.eq('workspace_id', workspaceId))
        .first();
    assertSafeNonnegativeInteger(cursor, 'pull cursor');
    const head = counter?.value ?? 0;
    assertSafeNonnegativeInteger(head, 'workspace head');
    if (!Number.isSafeInteger(head + 1) || oldest && (!Number.isSafeInteger(oldest.server_version) || oldest.server_version < 1 || oldest.server_version > head)) throw new Error('Sync coverage retention failure');
    return { head, retention: computePullRetention({
        cursor,
        oldestLogVersion: oldest?.server_version ?? null,
        highWatermark: head,
    }) };
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
const pushArgs = {
        workspace_id: v.id('workspaces'),
        ...historyArgs,
        stored_record_capability: v.optional(v.string()),
        workspace_item_capability: v.optional(v.literal('v1')),
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
    };
const pushArgumentValidator = v.object(pushArgs);
export const push = mutation({ args: pushArgs, handler: applyPush });
async function applyPush(ctx: MutationCtx, args: Infer<typeof pushArgumentValidator>) {
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
        requireHistoryCapability(args);
        const historyIncarnation = await fenceHistory(ctx, args.history_incarnation);

        type Result = {
            opId: string;
            success: boolean;
            serverVersion?: number;
            error?: string;
            errorCode?:
                | 'STORED_RECORD_UNAVAILABLE'
                | 'VALIDATION_ERROR'
                | 'UNAUTHORIZED'
                | 'CONFLICT'
                | 'NOT_FOUND'
                | 'RATE_LIMITED'
                | 'OVERSIZED'
                | 'NETWORK_ERROR'
                | 'SERVER_ERROR'
                | 'UNKNOWN';
            evidenceRef?: string;
            tableName?: string;
            operation?: 'put' | 'delete';
            payload?: unknown;
            wasExisting?: boolean;
            applied?: boolean;
            replayed?: boolean;
            winner?: CurrentWinner;
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

        const candidates: Array<{ op: SyncOperation; indices: number[]; fingerprint: string }> = [];
        // A native stored document can be 1 MiB, including a corrupt tombstone.
        // Reserve the worst-case point reads BEFORE each op; charge repeated
        // apply/winner reads and preimages conservatively against the same cap.
        // Keep 2 MiB for bounded authorization/counter/evidence headers.
        let remainingBytes = 14 * 1024 * 1024;
        const existingLogs: Array<Awaited<ReturnType<typeof readPushReceipt>>> = [];
        for (const group of groups.values()) {
            if (group.conflicting) {
                failGroup(group.indices, group.op, 'Conflicting operations reuse op_id', 'CONFLICT');
                continue;
            }
            if (remainingBytes < 12 * 1024 * 1024) {
                failGroup(group.indices, group.op, 'Sync batch storage budget reached; retry this operation', 'RATE_LIMITED');
                continue;
            }
            // Only known tables have canonical storage. Invalid input remains
            // the ordinary per-op validation disposition, never stored evidence.
            if (!Object.hasOwn(TABLE_INDEX_MAP, group.op.table_name)) {
                failGroup(group.indices, group.op, `Invalid table: ${group.op.table_name}`); continue;
            }
            const receipt = await readPushReceipt(ctx, group.op.op_id);
            remainingBytes -= receipt ? getConvexSize(receipt) : 0;
            // Never inspect/retain another workspace's receipt.
            if (receipt && String(receipt.workspace_id) !== String(args.workspace_id)) {
                failGroup(group.indices, group.op, 'Conflicting operation reuses processed op_id', 'CONFLICT'); continue;
            }
            const sources = await readPushSources(ctx, args.workspace_id, group.op);
            if (args.workspace_item_capability !== 'v1' &&
                (hasWorkspaceItemSemantics(group.op.table_name, group.op.payload) || hasWorkspaceItemSemantics(group.op.table_name, sources.row))) {
                requireWorkspaceItemCapability(args.workspace_item_capability);
            }
            remainingBytes -= (sources.row ? 6 * getConvexSize(sources.row as Value) : 0) +
                (sources.tombstone ? 4 * getConvexSize(sources.tombstone) : 0) +
                4 * getConvexSize(group.op as Value);
            // Known foreign ownership is an authorization failure, never an
            // evidence reference or a winner containing another user's data.
            if (group.op.table_name === 'notifications' && sources.row?.user_id && sources.row.user_id !== String(callerUserId)) {
                failGroup(group.indices, group.op, 'Forbidden: notification owner mismatch'); continue;
            }
            const defect = pushStoredDefect(group.op, receipt, sources);
            if (defect) {
                if (args.stored_record_capability !== 'stored-record-v1') {
                    throw new ConvexError({ code: 'OR3_SYNC_UPDATE_REQUIRED', statusCode: 426, requiredCapability: 'stored-record-v1' });
                }
                const sourceRef = await retainEvidence(ctx, args.workspace_id, historyIncarnation,
                    group.op.table_name, group.op.pk, defect.source, defect.reason);
                if (defect.hold) {
                    // The returned reference exports both immutable intent and
                    // its source link. The source itself is copied only once.
                    const evidenceRef = await retainEvidence(ctx, args.workspace_id, historyIncarnation,
                        group.op.table_name, group.op.pk, { intent: group.op, evidence_ref: sourceRef }, 'push_stored_record');
                    for (const index of group.indices) results[index] = { opId: group.op.op_id, success: false,
                        errorCode: 'STORED_RECORD_UNAVAILABLE', evidenceRef, error: 'Stored sync record unavailable; retained for manual recovery' };
                    continue;
                }
            }
            try {
                const scopedOp = scopeNotificationWrite(callerUserId, group.op, sources.row);
                candidates.push({ op: scopedOp, indices: group.indices, fingerprint: operationFingerprint(scopedOp) });
                existingLogs.push(receipt);
            } catch (error) {
                failGroup(group.indices, group.op, error instanceof Error ? error.message : String(error));
            }
        }

        const unprocessedOps: typeof candidates = [];
        for (const [index, candidate] of candidates.entries()) {
            const existing = existingLogs[index];
            if (!existing) {
                unprocessedOps.push(candidate);
                continue;
            }
            if (
                String(existing.workspace_id) !== String(args.workspace_id) ||
                (existing.request_fingerprint ?? changeLogFingerprint(existing)) !== candidate.fingerprint
            ) {
                failGroup(
                    candidate.indices,
                    candidate.op,
                    `Conflicting operation reuses processed op_id ${candidate.op.op_id}`,
                    'CONFLICT'
                );
                continue;
            }
            latestVersion = Math.max(latestVersion, existing.server_version);
            const winner = await currentWinner(ctx, args.workspace_id, candidate.op.table_name, candidate.op.pk);
            for (const resultIndex of candidate.indices) {
                results[resultIndex] = {
                    opId: candidate.op.op_id,
                    success: true,
                    serverVersion: existing.server_version,
                    wasExisting: true,
                    applied: false,
                    replayed: true,
                    ...(winner ? { winner } : {}),
                };
            }
        }

        // Exact receipts remain valid after ownership/lifecycle state changes.
        // Only new operations need dynamic storage admission before allocation.
        const newOps: typeof candidates = [];
        for (const candidate of unprocessedOps) {
            const { op, indices } = candidate;
            try {
                const validationError = validateSyncOperation(args.workspace_id, op);
                if (validationError) { failGroup(indices, op, validationError); continue; }
                if (op.operation === 'put') await assertStorageNotDeleted(ctx, args.workspace_id, op.table_name, {
                    ...sanitizePayload(op.table_name, op.payload as Record<string, unknown>),
                    [getPkField(op.table_name)]: op.pk,
                });
                newOps.push(candidate);
            } catch (error) {
                failGroup(indices, op, error instanceof Error ? error.message : String(error));
            }
        }

        if (newOps.length > 0) {
            const startVersion = await allocateServerVersions(ctx, args.workspace_id, newOps.length);
            batchPreimageVersion = startVersion - 1;
            latestVersion = startVersion + newOps.length - 1;

            // Apply unique ops sequentially in allocated server-version order.
            for (let offset = 0; offset < newOps.length; offset += 1) {
                const { op, indices, fingerprint } = newOps[offset]!;
                const serverVersion = startVersion + offset;
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

                const submittedPayload =
                    typeof op.payload === 'object' && op.payload !== null
                        ? (op.payload as Record<string, unknown>)
                        : undefined;
                const winner = await currentWinner(ctx, args.workspace_id, op.table_name, op.pk);
                const opPayload = applyResult.applied && op.operation === 'put' && winner?.kind === 'put'
                    ? winner.payload : submittedPayload;
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
                    request_fingerprint: fingerprint,
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

                const finalWinner = op.operation === 'delete'
                    ? await currentWinner(ctx, args.workspace_id, op.table_name, op.pk) : winner;
                const success: Result = {
                    opId: op.op_id,
                    serverVersion,
                    success: true,
                    tableName: op.table_name,
                    operation: op.operation,
                    payload: opPayload,
                    wasExisting: applyResult.wasExisting,
                    applied: applyResult.applied,
                    ...(finalWinner ? { winner: finalWinner } : {}),
                };
                for (const resultIndex of indices) results[resultIndex] = success;
                // Unexpected failures deliberately escape: Convex rolls back the
                // counter, canonical rows, pre-images and log as one mutation.
            }
        }

        const response = {
            results: results.filter((result): result is Result => Boolean(result)),
            serverVersion: latestVersion,
            historyIncarnation,
        };
        for (const result of response.results) PushResultItemSchema.parse(result);
        return response;

}

async function requireCanonicalGenerationActor(
    ctx: MutationCtx | QueryCtx,
    workspaceId: Id<'workspaces'>,
    actorUserId: Id<'users'>
): Promise<void> {
    const workspace = await ctx.db.get(workspaceId);
    if (!workspace || workspace.deleted === true) throw new ConvexError({ code: 'FORBIDDEN', message: 'Forbidden' });
    const membership = await ctx.db
        .query('workspace_members')
        .withIndex('by_workspace_user', (q: any) =>
            q.eq('workspace_id', workspaceId).eq('user_id', actorUserId)
        )
        .first();
    if (!membership || !SYNC_WRITE_ROLES.has(membership.role)) {
        throw new ConvexError({ code: 'FORBIDDEN', message: 'Forbidden background history actor' });
    }
}

/** Internal service read: current membership, materialized rows, one snapshot. */
export const readChatHistory = internalQuery({
    args: { workspace_id: v.id('workspaces'), actor_user_id: v.id('users'), query: v.union(
        v.object({ kind: v.literal('thread'), thread_id: v.string() }),
        v.object({ kind: v.literal('messages'), message_ids: v.array(v.string()) }),
        v.object({ kind: v.literal('thread_page'), thread_id: v.string(), limit: v.number(), cursor: v.optional(v.string()) }),
    ) },
    handler: async (ctx, args) => {
        await requireActiveWorkspace(ctx, args.workspace_id);
        const membership = await ctx.db.query('workspace_members').withIndex('by_workspace_user', (q) =>
            q.eq('workspace_id', args.workspace_id).eq('user_id', args.actor_user_id)).first();
        if (!membership || !SYNC_READ_ROLES.has(membership.role)) throw new Error('Forbidden canonical history actor');
        const counter = await ctx.db.query('server_version_counter').withIndex('by_workspace', (q) => q.eq('workspace_id', args.workspace_id)).first();
        const revision = String(counter?.value ?? 0);
        const request = args.query;
        const validId = (id: string) => id.length > 0 && new TextEncoder().encode(id).length <= 200;
        const wire = (row: Record<string, unknown>) => {
            const { _id, _creationTime, workspace_id, server_version, ...record } = row;
            return record;
        };
        if (request.kind === 'thread') {
            if (!validId(request.thread_id)) throw new Error('Invalid canonical thread ID');
            const row = await ctx.db.query('threads').withIndex('by_workspace_id', (q) =>
                q.eq('workspace_id', args.workspace_id).eq('id', request.thread_id)).first();
            const threadRevision = await ctx.db.query('chat_history_revisions').withIndex('by_workspace_thread', (q) =>
                q.eq('workspace_id', args.workspace_id).eq('thread_id', request.thread_id)).first();
            if (!row) return { status: 'ok', project_ownership: 'resolved',
                revision: String(threadRevision?.value ?? 0), thread: undefined };
            if (row.project_id) {
                // Deleted or missing owners release chats, matching the browser
                // resolver even when the delete/detach writes arrive out of order.
                const owner = await ctx.db.query('projects').withIndex('by_workspace_id', (q) =>
                    q.eq('workspace_id', args.workspace_id).eq('id', row.project_id!)).first();
                return { status: 'ok', project_ownership: 'resolved', revision: String(threadRevision?.value ?? 0),
                    thread: { ...wire(row), project_id: owner && !owner.deleted ? owner.id : null } };
            }
            const projects = await ctx.db.query('projects').withIndex('by_workspace', (q) => q.eq('workspace_id', args.workspace_id)).collect();
            const owners = projects.filter(project => {
                if (project.deleted) return false;
                const entries: unknown = typeof project.data === 'string' ? JSON.parse(project.data) : project.data ?? [];
                if (!Array.isArray(entries)) throw new Error('Canonical project membership is unresolved.');
                return entries.some(entry => {
                    if (typeof entry === 'string') return entry.trim() === request.thread_id;
                    if (!entry || typeof entry !== 'object' || Array.isArray(entry)) return false;
                    const kind = typeof entry.kind === 'string' ? entry.kind.trim().toLowerCase() : entry.kind;
                    return (kind === undefined || kind === 'chat') && typeof entry.id === 'string'
                        && entry.id.trim() === request.thread_id;
                });
            });
            return { status: 'ok', project_ownership: owners.length > 1 ? 'conflict' : 'resolved',
                revision: String(threadRevision?.value ?? 0), thread: { ...wire(row), project_id: owners[0]?.id ?? null } };
        }
        if (request.kind === 'messages') {
            if (request.message_ids.length > 100 || request.message_ids.some((id) => !validId(id))) throw new Error('Invalid bounded message query');
            const rows = await Promise.all(request.message_ids.map((id) => ctx.db.query('messages').withIndex('by_workspace_id', (q) =>
                q.eq('workspace_id', args.workspace_id).eq('id', id)).first()));
            return { status: 'ok', revision, messages: rows.filter((row) => row !== null).map((row) => wire(row!)) };
        }
        if (!validId(request.thread_id) || !Number.isSafeInteger(request.limit) || request.limit < 1 || request.limit > 100
            || request.cursor && new TextEncoder().encode(request.cursor).length > 2048) throw new Error('Invalid bounded history page');
        const seek = parseChatSeek(request.cursor, request.thread_id);
        if (seek) {
            const direction = seek.backward ? 'desc' as const : 'asc' as const;
            const base = (q: any) => q.eq('workspace_id', args.workspace_id).eq('thread_id', request.thread_id);
            const parts = seek.key ? [0, 1, 2] : [3];
            const rows: Array<Record<string, unknown> & { index: number; order_key: string; id: string }> = [];
            for (const part of parts) {
                const remaining = request.limit - rows.length; if (!remaining) break;
                const batch = await ctx.db.query('messages').withIndex('by_history_order', (q: any) => {
                    let range = base(q); const key = seek.key;
                    if (!key) return range;
                    const comparison = seek.backward ? 'lt' : 'gt';
                    if (part === 0) return range.eq('index', key[0]).eq('order_key', key[1])[comparison]('id', key[2]);
                    if (part === 1) return range.eq('index', key[0])[comparison]('order_key', key[1]);
                    return range[comparison]('index', key[0]);
                }).order(direction).take(remaining);
                rows.push(...batch);
            }
            const last = rows.at(-1);
            return { status: 'ok', revision, messages: rows.map(wire), next_cursor: rows.length === request.limit && last
                ? encodeChatSeek({ ...seek, key: [last.index, last.order_key, last.id] }) : undefined };
        }
        const page = await ctx.db.query('messages').withIndex('by_history_order', (q) =>
            q.eq('workspace_id', args.workspace_id).eq('thread_id', request.thread_id))
            .paginate({ numItems: request.limit, cursor: request.cursor ?? null });
        return { status: 'ok', revision, messages: page.page.map(wire), next_cursor: page.isDone ? undefined : page.continueCursor };
    },
});

async function generationReceipt(
    ctx: MutationCtx | QueryCtx,
    workspaceId: Id<'workspaces'>,
    generationId: string,
    stage: 'admission' | 'finalization'
) {
    return await ctx.db
        .query('background_generation_receipts')
        .withIndex('by_workspace_generation_stage', (q: any) =>
            q.eq('workspace_id', workspaceId)
                .eq('generation_id', generationId)
                .eq('stage', stage)
        )
        .first();
}

const toolResultParentArgs = {
    workspace_id: v.id('workspaces'), actor_user_id: v.id('users'), generation_id: v.string(),
    message_id: v.string(), thread_id: v.string(), admission_clock: v.number(), fingerprint: v.string(),
};

async function requireToolResultParent(ctx: MutationCtx | QueryCtx, args: {
    workspace_id: Id<'workspaces'>; actor_user_id: Id<'users'>; generation_id: string;
    message_id: string; thread_id: string; admission_clock: number; fingerprint: string;
}): Promise<void> {
    await requireCanonicalGenerationActor(ctx, args.workspace_id, args.actor_user_id);
    const receipt = await generationReceipt(ctx, args.workspace_id, args.generation_id, 'admission');
    const thread = await ctx.db.query('threads').withIndex('by_workspace_id', q =>
        q.eq('workspace_id', args.workspace_id).eq('id', args.thread_id)).first();
    const assistant = await ctx.db.query('messages').withIndex('by_workspace_id', q =>
        q.eq('workspace_id', args.workspace_id).eq('id', args.message_id)).first();
    const data = assistant?.data as { generation_id?: string } | undefined;
    if (!receipt || receipt.outcome !== 'admitted' || receipt.fingerprint !== args.fingerprint || !thread || thread.deleted ||
        !assistant || assistant.deleted || assistant.clock !== args.admission_clock || assistant.thread_id !== args.thread_id ||
        assistant.pending !== true || data?.generation_id !== args.generation_id)
        throw new ConvexError({ code: 'SUPERSEDED', message: 'Tool result parent history is unavailable' });
}

/** Retain immutable tool chunks and publish their sync changes atomically. */
export const retainChatToolResult = internalMutation({
    args: { ...toolResultParentArgs, device_id: v.string(), rows: v.array(v.object({ record: v.any(), op_id: v.string() })) },
    handler: async (ctx, args) => {
        await requireToolResultParent(ctx, args);
        if (!args.rows.length || args.rows.length > 32) throw new ConvexError({ code: 'VALIDATION_ERROR', message: 'Invalid tool chunks' });
        const fresh = [];
        let bytes = 0;
        const ids = new Set<string>();
        for (const entry of args.rows) {
            const row = entry.record;
            if (!row || typeof row.id !== 'string' || !/^tool-result:[a-f0-9]{64}:\d+$/.test(row.id) || ids.has(row.id) ||
                row.name !== `chat.tool-result:${encodeURIComponent(args.thread_id)}:${row.id.slice('tool-result:'.length)}` ||
                typeof row.value !== 'string' || new TextEncoder().encode(row.value).length > 64 * 1024 ||
                row.clock !== 1 || row.deleted !== false || !Number.isSafeInteger(row.created_at) || row.created_at < 0 ||
                !Number.isSafeInteger(row.updated_at) || row.updated_at < 0 || !isSyncUuid(entry.op_id))
                throw new ConvexError({ code: 'VALIDATION_ERROR', message: 'Invalid tool chunk' });
            ids.add(row.id); bytes += new TextEncoder().encode(row.value).length;
            const tombstone = await ctx.db.query('tombstones').withIndex('by_workspace_table_pk', q =>
                q.eq('workspace_id', args.workspace_id).eq('table_name', 'kv').eq('pk', row.id)).first();
            const existing = await ctx.db.query('kv').withIndex('by_workspace_id', q =>
                q.eq('workspace_id', args.workspace_id).eq('id', row.id)).first();
            if (tombstone || existing?.deleted) throw new ConvexError({ code: 'SUPERSEDED', message: 'Tool result was deleted' });
            if (existing) {
                if (existing.clock !== 1 || existing.name !== row.name || existing.value !== row.value)
                    throw new ConvexError({ code: 'CONFLICT', message: 'Conflicting tool result retention' });
            } else fresh.push(entry);
        }
        if (bytes > 256 * 1024) throw new ConvexError({ code: 'VALIDATION_ERROR', message: 'Invalid tool result size' });
        if (!fresh.length) return;
        const start = await allocateServerVersions(ctx, args.workspace_id, fresh.length);
        const hlc = `${Date.now().toString(36).padStart(9, '0')}:000:background`;
        for (const [index, entry] of fresh.entries()) {
            const row = entry.record;
            const payload = { id: row.id, name: row.name, value: row.value, clock: 1, deleted: false,
                created_at: row.created_at, updated_at: row.updated_at, hlc, op_id: entry.op_id };
            await applyOpToTable(ctx, args.workspace_id, { table_name: 'kv', operation: 'put', pk: row.id,
                payload, clock: 1, hlc, op_id: entry.op_id, device_id: args.device_id }, start + index, start - 1);
            await ctx.db.insert('change_log', { workspace_id: args.workspace_id, server_version: start + index,
                table_name: 'kv', pk: row.id, op: 'put', payload, clock: 1, hlc, device_id: args.device_id,
                op_id: entry.op_id, created_at: nowSec() });
        }
    },
});

export const readChatToolResult = internalQuery({
    args: { ...toolResultParentArgs, ids: v.array(v.string()) },
    handler: async (ctx, args) => {
        await requireToolResultParent(ctx, args);
        if (!args.ids.length || args.ids.length > 32) throw new ConvexError({ code: 'VALIDATION_ERROR', message: 'Invalid tool chunks' });
        const parts = [];
        let bytes = 0;
        for (const id of args.ids) {
            const row = await ctx.db.query('kv').withIndex('by_workspace_id', q =>
                q.eq('workspace_id', args.workspace_id).eq('id', id)).first();
            if (!row || row.deleted || row.clock !== 1 || typeof row.value !== 'string' ||
                new TextEncoder().encode(row.value).length > 64 * 1024 ||
                row.name !== `chat.tool-result:${encodeURIComponent(args.thread_id)}:${id.slice('tool-result:'.length)}`)
                throw new ConvexError({ code: 'TOOL_RESULT_UNAVAILABLE', message: 'Tool result is unavailable or corrupt' });
            bytes += new TextEncoder().encode(row.value).length;
            parts.push(row.value);
        }
        if (bytes > 256 * 1024) throw new ConvexError({ code: 'TOOL_RESULT_UNAVAILABLE', message: 'Invalid tool result size' });
        return parts.join('');
    },
});

/** Atomically admits the rows a background generation owns before execution. */
export const admitChatGeneration = internalMutation({
    args: {
        workspace_id: v.id('workspaces'),
        actor_user_id: v.id('users'),
        generation_id: v.string(),
        message_id: v.string(),
        kind: v.union(v.literal('new-turn'), v.literal('continuation')),
        expected_clock: v.optional(v.number()),
        expected_generation_id: v.optional(v.string()),
        fingerprint: v.string(),
        device_id: v.string(),
        records: v.array(v.object({
            table_name: v.union(v.literal('threads'), v.literal('messages')),
            record: v.any(),
            op_id: v.string(),
        })),
    },
    handler: async (ctx, args) => {
        await requireCanonicalGenerationActor(
            ctx,
            args.workspace_id,
            args.actor_user_id
        );
        const receipt = await generationReceipt(
            ctx,
            args.workspace_id,
            args.generation_id,
            'admission'
        );
        if (receipt) {
            if (receipt.fingerprint !== args.fingerprint) {
                throw new ConvexError({ code: 'CONFLICT', message: 'Conflicting background admission replay' });
            }
            return {
                status: 'admitted' as const,
                replayed: true,
                serverVersion: receipt.server_version ?? 0,
            };
        }

        if (args.kind === 'continuation') {
            const current = await ctx.db
                .query('messages')
                .withIndex('by_workspace_id', (q: any) =>
                    q.eq('workspace_id', args.workspace_id).eq('id', args.message_id)
                )
                .first();
            const data = current?.data && typeof current.data === 'object'
                ? current.data as Record<string, unknown>
                : {};
            const expectedClock = args.expected_clock ?? -1;
            const admissionRecord = args.records.at(-1)?.record as
                | Record<string, unknown>
                | undefined;
            const admissionClock = admissionRecord?.clock;
            if (
                current?.deleted ||
                (current && typeof admissionClock === 'number' &&
                    current.clock > admissionClock) ||
                (current && current.clock === admissionClock &&
                    data.generation_id !== args.generation_id) ||
                (current && typeof admissionClock === 'number' &&
                    current.clock > expectedClock && current.clock < admissionClock) ||
                (current &&
                    current.clock === expectedClock &&
                    args.expected_generation_id !== undefined &&
                    data.generation_id !== args.expected_generation_id)
            ) {
                throw new ConvexError({ code: 'CONFLICT', message: 'Conflicting continuation admission' });
            }
        }

        if (args.records.length < 1 || args.records.length > 3) {
            throw new ConvexError({ code: 'VALIDATION_ERROR', message: 'Invalid background history records' });
        }
        const preparedRecords = args.records.map((entry) => {
            const record = entry.record as Record<string, unknown>;
            const pk = typeof record.id === 'string' ? record.id : '';
            const clock = record.clock;
            if (!pk || !Number.isSafeInteger(clock) || (clock as number) < 0) {
                throw new ConvexError({ code: 'VALIDATION_ERROR', message: 'Invalid background history record' });
            }
            const hlc = typeof record.hlc === 'string' && record.hlc
                ? record.hlc
                : `${Date.now().toString(36).padStart(9, '0')}:000:background`;
            const payload: Record<string, unknown> = { ...record, hlc, op_id: entry.op_id };
            // Pending assistants bypass the foreground sync hook that derives
            // message ordering. Use the same HLC key before materializing them.
            if (entry.table_name === 'messages' && !payload.order_key) {
                payload.order_key = hlc;
            }
            const op = { table_name: entry.table_name, operation: 'put' as const, pk, payload,
                clock: clock as number, hlc, op_id: entry.op_id, device_id: args.device_id };
            const validationError = validateSyncOperation(args.workspace_id, op);
            if (validationError) throw new ConvexError({ code: 'VALIDATION_ERROR', message: validationError });
            return { entry, pk, clock: clock as number, hlc, payload: op.payload as Record<string, unknown> };
        });
        const startVersion = await allocateServerVersions(
            ctx,
            args.workspace_id,
            preparedRecords.length
        );
        const preimageVersion = startVersion - 1;
        const hookRows: Array<{ tableName: 'threads' | 'messages'; payload: Record<string, unknown>; wasExisting: boolean; applied: boolean }> = [];
        for (let offset = 0; offset < args.records.length; offset += 1) {
            const { entry, pk, clock, hlc, payload } = preparedRecords[offset]!;
            const serverVersion = startVersion + offset;
            const applyResult = await applyOpToTable(ctx, args.workspace_id, {
                table_name: entry.table_name,
                operation: 'put',
                pk,
                payload,
                clock: clock as number,
                hlc,
                op_id: entry.op_id,
                device_id: args.device_id,
            }, serverVersion, preimageVersion);
            if (entry.table_name === 'threads' || pk !== args.message_id) {
                hookRows.push({ tableName: entry.table_name, payload, ...applyResult });
            }
            await ctx.db.insert('change_log', {
                workspace_id: args.workspace_id,
                server_version: serverVersion,
                table_name: entry.table_name,
                pk,
                op: 'put',
                payload,
                clock: clock as number,
                hlc,
                device_id: args.device_id,
                op_id: entry.op_id,
                created_at: nowSec(),
            });
        }

        const assistant = await ctx.db
            .query('messages')
            .withIndex('by_workspace_id', (q: any) =>
                q.eq('workspace_id', args.workspace_id).eq('id', args.message_id)
            )
            .first();
        const assistantData = assistant?.data && typeof assistant.data === 'object'
            ? assistant.data as Record<string, unknown>
            : {};
        if (!assistant || assistant.deleted === true ||
            assistant.clock !== (args.records.at(-1)?.record as any)?.clock ||
            assistantData.generation_id !== args.generation_id) {
            throw new ConvexError({ code: 'SUPERSEDED', message: 'Background assistant admission was superseded' });
        }
        const serverVersion = startVersion + args.records.length - 1;
        await ctx.db.insert('background_generation_receipts', {
            workspace_id: args.workspace_id,
            generation_id: args.generation_id,
            stage: 'admission',
            fingerprint: args.fingerprint,
            outcome: 'admitted',
            server_version: serverVersion,
            created_at: nowSec(),
        });
        return { status: 'admitted' as const, replayed: false, serverVersion, hookRows };
    },
});

/** Atomically commits a terminal snapshot if this generation still owns the row. */
export const finalizeChatGeneration = internalMutation({
    args: {
        workspace_id: v.id('workspaces'),
        actor_user_id: v.id('users'),
        generation_id: v.string(),
        message_id: v.string(),
        admission_clock: v.number(),
        fingerprint: v.string(),
        device_id: v.string(),
        op_id: v.string(),
        snapshot: v.any(),
    },
    handler: async (ctx, args) => {
        await requireCanonicalGenerationActor(ctx, args.workspace_id, args.actor_user_id);
        const receipt = await generationReceipt(
            ctx,
            args.workspace_id,
            args.generation_id,
            'finalization'
        );
        if (receipt) {
            if (receipt.fingerprint !== args.fingerprint) {
                throw new ConvexError({ code: 'CONFLICT', message: 'Conflicting background finalization replay' });
            }
            return receipt.outcome === 'committed'
                ? { status: 'committed' as const, replayed: true, serverVersion: receipt.server_version ?? 0 }
                : { status: 'superseded' as const, reason: receipt.outcome };
        }

        const current = await ctx.db
            .query('messages')
            .withIndex('by_workspace_id', (q: any) =>
                q.eq('workspace_id', args.workspace_id).eq('id', args.message_id)
            )
            .first();
        const data = current?.data && typeof current.data === 'object'
            ? current.data as Record<string, unknown>
            : {};
        const reason = !current
            ? 'missing'
            : current.deleted
              ? 'deleted'
              : data.generation_id !== args.generation_id
                ? 'newer_generation'
                : current.clock !== args.admission_clock
                  ? 'edited'
                  : null;
        if (reason) {
            await ctx.db.insert('background_generation_receipts', {
                workspace_id: args.workspace_id,
                generation_id: args.generation_id,
                stage: 'finalization',
                fingerprint: args.fingerprint,
                outcome: reason,
                created_at: nowSec(),
            });
            return { status: 'superseded' as const, reason };
        }

        const snapshot = args.snapshot as {
            status: 'complete' | 'error' | 'aborted';
            content: string;
            reasoning: string;
            toolCalls?: unknown[];
            usage?: unknown;
            error?: string;
            completedAt: number;
        };
        if (!['complete', 'error', 'aborted'].includes(snapshot.status) ||
            typeof snapshot.content !== 'string' || typeof snapshot.reasoning !== 'string' ||
            !Number.isFinite(snapshot.completedAt)) {
            throw new ConvexError({ code: 'VALIDATION_ERROR', message: 'Invalid background terminal snapshot' });
        }
        let clock: number;
        try { clock = nextSyncClock(current!.clock); }
        catch { throw new ConvexError({ code: 'VALIDATION_ERROR', message: 'Sync clock exhausted or invalid' }); }
        const hlc = `${Date.now().toString(36).padStart(9, '0')}:000:background`;
        const {
            _id: _currentId,
            _creationTime: _currentCreationTime,
            workspace_id: _currentWorkspaceId,
            server_version: _currentServerVersion,
            error: _oldError,
            ...currentWithoutError
        } = current!;
        const terminalState = snapshot.status === 'complete'
            ? 'complete'
            : snapshot.status === 'aborted' ? 'aborted' : 'failed';
        const usage = readRequestUsage(snapshot.usage);
        let payload = {
            ...currentWithoutError,
            ...(snapshot.error ? { error: snapshot.error } : {}),
            pending: false,
            updated_at: Math.floor(snapshot.completedAt / 1000),
            clock,
            hlc,
            op_id: args.op_id,
            data: {
                ...data,
                ...(usage ? { usage } : {}),
                content: snapshot.content,
                reasoning_text: snapshot.reasoning || null,
                tool_calls: snapshot.toolCalls ?? null,
                generation_state: terminalState,
                background_job_status: snapshot.status,
                background_job_error: snapshot.error ?? null,
                error: snapshot.error ?? null,
            },
        };
        payload = prepareOperation({ table_name: 'messages', pk: args.message_id, operation: 'put',
            payload, clock, hlc, op_id: args.op_id, device_id: args.device_id }).payload as typeof payload;
        const serverVersion = await allocateServerVersions(ctx, args.workspace_id, 1);
        await applyOpToTable(ctx, args.workspace_id, {
            table_name: 'messages', operation: 'put', pk: args.message_id,
            payload, clock, hlc, op_id: args.op_id, device_id: args.device_id,
        }, serverVersion, serverVersion - 1);
        await ctx.db.insert('change_log', {
            workspace_id: args.workspace_id,
            server_version: serverVersion,
            table_name: 'messages',
            pk: args.message_id,
            op: 'put',
            payload,
            clock,
            hlc,
            device_id: args.device_id,
            op_id: args.op_id,
            created_at: nowSec(),
        });
        await ctx.db.insert('background_generation_receipts', {
            workspace_id: args.workspace_id,
            generation_id: args.generation_id,
            stage: 'finalization',
            fingerprint: args.fingerprint,
            outcome: 'committed',
            server_version: serverVersion,
            created_at: nowSec(),
        });
        return { status: 'committed' as const, replayed: false, serverVersion };
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
        ...historyArgs,
        device_id: v.string(),
        last_seen_version: v.number(),
    },
    handler: async (ctx, args) => {
        const callerUserId = await requireSyncReadAccess(ctx, args.workspace_id);
        requireHistoryCapability(args);
        const historyIncarnation = await fenceHistory(ctx, args.history_incarnation);
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
            .withIndex('by_workspace_user_device', (q) =>
                q.eq('workspace_id', args.workspace_id).eq('owner_user_id', callerUserId).eq('device_id', deviceId)
            )
            .first();

        if (existing) {
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
        return { ok: true, historyIncarnation };
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
const snapshotArgs = {
    ...historyArgs,
    workspace_id: v.id('workspaces'),
    workspace_item_capability: v.optional(v.literal('v1')),
    page_size: v.number(),
    page_token: v.optional(v.string()),
    tables: v.optional(v.array(v.string())),
};
export const snapshot = mutation({
    args: snapshotArgs,
    handler: (ctx, args) => captureSnapshot(ctx, args, false),
});
/** Host archives retain file metadata which sync bootstrap sends as tombstones. */
export const backupSnapshot = internalMutation({
    args: snapshotArgs,
    handler: (ctx, args) => captureSnapshot(ctx, args, true),
});

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
type SnapshotPageResponse = {
    historyIncarnation: string;
    unavailable: import('./syncContract/shared/sync/types').SnapshotUnavailableRecord[];
    workspaceId: string;
    snapshotId: string;
    highWatermark: number;
    items: PageItem[];
    nextPageToken: string | null;
};

async function captureSnapshot(ctx: MutationCtx, args: {
    workspace_id: Id<'workspaces'>;
    workspace_item_capability?: 'v1';
    history_incarnation?: string | null;
    history_incarnation_capability?: string;
    unavailable_record_capability?: string;
    page_size: number;
    page_token?: string;
    tables?: string[];
}, retainedFiles: boolean) {
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

    if (!retainedFiles) requireHistoryCapability(args);
    const historyIncarnation = await readHistory(ctx);
    const expected = retainedFiles && args.history_incarnation === undefined
        ? (args.page_token ? decodeSnapshotCursor(args.page_token).historyIncarnation : null) : args.history_incarnation;
    if (!(expected === null && !args.page_token) && expected !== historyIncarnation) changedHistory(historyIncarnation);
    const now = nowSec();
    let snapshotId: string;
    let highWatermark: number;
    let tableIndex = 0;
    let afterPk: string | null = null;
    let expiresAt = now + SNAPSHOT_TTL_SECONDS;

    if (args.page_token) {
        const cursor = decodeSnapshotCursor(args.page_token);
        if (cursor.historyIncarnation !== historyIncarnation) changedHistory(historyIncarnation);
        let session: {
            workspace_id: Id<'workspaces'>;
            user_id: string;
            high_watermark: number;
            tables: string[];
            retained_files?: boolean;
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
        if ((session.retained_files === true) !== retainedFiles) throw new Error('Snapshot page token does not match the requested mode');
        if (cursor.tableIndex > session.tables.length) {
            throw new Error('Invalid snapshot page token');
        }
        snapshotId = cursor.snapshotId;
        highWatermark = session.high_watermark;
        tableIndex = cursor.tableIndex;
        afterPk = cursor.afterPk;
        expiresAt = session.expires_at;
        // Authorization, mode, tables and expiry were checked before replay.
        // A response fits in 2 MiB, hence at most four 512 KiB chunks.
        const cached = await ctx.db.query('sync_snapshot_pages').withIndex('by_page', q =>
            q.eq('session_id', cursor.snapshotId as Id<'sync_snapshot_sessions'>)
                .eq('table_index', tableIndex).eq('after_pk', afterPk).eq('page_size', args.page_size)).take(4);
        if (cached.length) {
            const bytes = new Uint8Array(cached.reduce((total, chunk) => total + chunk.data.byteLength, 0));
            let offset = 0;
            for (const chunk of cached) { bytes.set(new Uint8Array(chunk.data), offset); offset += chunk.data.byteLength; }
            const response = JSON.parse(new TextDecoder().decode(bytes)) as SnapshotPageResponse;
            if (response.items.some((item) => item.kind === 'row' && hasWorkspaceItemSemantics(item.tableName, item.payload))) requireWorkspaceItemCapability(args.workspace_item_capability);
            return response;
        }
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
            retained_files: retainedFiles,
            created_at: now,
            expires_at: expiresAt,
        }));
    }

    const requestPosition = { tableIndex, afterPk };

    const items: PageItem[] = [];
    const unavailable: SnapshotPageResponse['unavailable'] = [];
    const budget = newSyncReadBudget();
    let responseBytes = 0;
    let examined = 0;
    let stoppedWithinTable = false;

    while (tableIndex < tables.length && examined < args.page_size && budget.bytes >= 5 * 1024 * 1024) {
        const tableName = tables[tableIndex];
        if (!tableName) break;
        const remaining = args.page_size - examined;
        const rowPage = await readMaterializedSnapshotRows(
                ctx,
                args.workspace_id,
                tableName,
                afterPk,
                remaining + 1,
                budget
            );
        const tombstonePage = await readSnapshotTombstones(
                ctx,
                args.workspace_id,
                tableName,
                afterPk,
                remaining + 1,
                budget
            );

        const rows = rowPage.rows;
        const tombstones = tombstonePage.rows;
        const pkField = getPkField(tableName);
        const rowsByPk = new Map<string, MaterializedDoc>();
        for (const row of rows) {
            const pk = row[pkField];
            assertIdentity(tableName, pk);
            if (rowsByPk.has(pk)) throw new Error('Duplicate snapshot logical key');
            rowsByPk.set(pk, row);
        }
        for (const row of tombstones) assertIdentity(tableName, row.pk);
        const tombstonesByPk = new Map(tombstones.map((row) => [row.pk, row]));
        if (tombstonesByPk.size !== tombstones.length) throw new Error('Duplicate snapshot logical key');
        const orderedPks = [
            ...new Set([...rowsByPk.keys(), ...tombstonesByPk.keys()]),
        ].sort((a, b) => compareSyncTuple([a], [b]));
        // A truncated source proves coverage only through its last fetched key.
        // Never seek past unseen rows merely because the other source read ahead.
        const frontiers = [rowPage.hasMore ? rows.at(-1)?.[pkField] : undefined,
            tombstonePage.hasMore ? tombstones.at(-1)?.pk : undefined]
            .filter((pk): pk is string => typeof pk === 'string');
        const pagePks = orderedPks.filter(pk => frontiers.every(last => compareSyncTuple([pk], [last]) <= 0)).slice(0, remaining);

        let consumed = 0;
        for (const pk of pagePks) {
            if (!canReadSyncDocument(budget)) break;
            const winner = await resolveSnapshotKey(
                ctx,
                args.workspace_id,
                tableName,
                pk,
                highWatermark,
                rowsByPk.get(pk),
                tombstonesByPk.get(pk),
                callerUserId,
                retainedFiles,
                budget
            );
            assertIdentity(tableName, pk);
            if (!winner) { consumed++; continue; }
            const { serverVersion: _version, source: _source, ...item } = winner;
            const reason = readDefect(item, true);
            // Estimate marker BEFORE retention; do not persist unconsumed entries.
            const marker = { tableName, pk, originalKind: winner.kind, snapshotId, highWatermark,
                evidenceRef: 'x'.repeat(64), reason: reason ?? 'invalid_record' };
            const itemBytes = syncJsonByteLength(reason ? marker : item);
            if (itemBytes > 1536 * 1024) throw new Error('Snapshot identity exceeds coverage byte ceiling');
            if (responseBytes + itemBytes > 1536 * 1024) break;
            responseBytes += itemBytes;
            consumed++;
            if (reason) {
                marker.evidenceRef = await retainEvidence(ctx, args.workspace_id, historyIncarnation, tableName, pk, winner.source, reason);
                unavailable.push(marker);
            } else items.push(item as PageItem);
        }

        examined += consumed;
        if (orderedPks.length > consumed || rowPage.hasMore || tombstonePage.hasMore) {
            afterPk = pagePks[consumed - 1] ?? afterPk;
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
              historyIncarnation,
              tableIndex,
              afterPk,
          })
        : null;

    if (items.some(item => item.kind === 'row' && hasWorkspaceItemSemantics(item.tableName, item.payload))) requireWorkspaceItemCapability(args.workspace_item_capability);
    const response: SnapshotPageResponse = {
        historyIncarnation,
        unavailable,
        workspaceId: String(args.workspace_id),
        snapshotId,
        highWatermark,
        items,
        nextPageToken,
    };
    SnapshotResponseSchema.parse(response);
    if (args.page_token) {
        const bytes = new TextEncoder().encode(JSON.stringify(response));
        if (bytes.byteLength > 2 * 1024 * 1024) throw new Error('Snapshot response exceeds byte ceiling');
        for (let offset = 0, chunk = 0; offset < bytes.byteLength; offset += 512 * 1024, chunk++) {
            await ctx.db.insert('sync_snapshot_pages', {
                workspace_id: args.workspace_id,
                session_id: snapshotId as Id<'sync_snapshot_sessions'>,
                table_index: requestPosition.tableIndex,
                after_pk: requestPosition.afterPk,
                page_size: args.page_size,
                chunk,
                data: bytes.slice(offset, offset + 512 * 1024).buffer,
                expires_at: expiresAt,
            });
        }
    }
    return response;
}

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
            v.literal('retained_metadata'),
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
            const items = [];
            const processedHashes = new Set<string>();
            for (const row of scanned) {
                if (row.expires_at <= now || (hash !== undefined && row.hash !== hash)) continue;
                if (!Number.isSafeInteger(row.expires_at) ||
                    !Number.isSafeInteger(row.size_bytes) || row.size_bytes < 0 ||
                    !/^[0-9a-f]{64}$/.test(row.hash)) {
                    throw new Error('Invalid upload reservation');
                }
                if (row.size_bytes === 0 || processedHashes.has(row.hash)) continue;
                processedHashes.add(row.hash);
                const canonicalFile = await ctx.db.query('file_meta')
                    .withIndex('by_workspace_hash', q => q.eq('workspace_id', args.workspace_id).eq('hash', `sha256:${row.hash}`))
                    .filter(q => q.eq(q.field('deleted'), false)).first();
                const legacyFile = canonicalFile ? null : await ctx.db.query('file_meta')
                    .withIndex('by_workspace_hash', q => q.eq('workspace_id', args.workspace_id).eq('hash', row.hash))
                    .filter(q => q.eq(q.field('deleted'), false)).first();
                if (canonicalFile || legacyFile) continue;
                const group = await ctx.db.query('upload_intents')
                    .withIndex('by_workspace_hash_status_expiry', q => q.eq('workspace_id', args.workspace_id).eq('hash', row.hash).eq('status', 'active').gt('expires_at', now))
                    .take(MAX_CANONICAL_STORAGE_PAGE_SIZE + 1);
                // Bound group reads independently of the outer scan. Refuse to
                // report a partial group, which could silently lose its charge.
                if (group.length > MAX_CANONICAL_STORAGE_PAGE_SIZE) throw new Error('Upload reservation group exceeds the query limit');
                group.sort((a, b) => a._creationTime - b._creationTime || String(a._id).localeCompare(String(b._id)));
                const pending = group.filter(intent => {
                    if (intent.expires_at <= now) return false;
                    if (!Number.isSafeInteger(intent.expires_at) ||
                        !Number.isSafeInteger(intent.size_bytes) || intent.size_bytes < 0) {
                        throw new Error('Invalid upload reservation');
                    }
                    return intent.size_bytes > 0;
                });
                // Every page observes the same current leader for a hash. The
                // leader remains charged even when another intent expires or
                // is cancelled, and later pages never charge it a second time.
                if (pending[0]?._id !== row._id) continue;
                items.push({
                    kind: 'reservation' as const,
                    reservationId: String(row._id),
                    hash: row.hash,
                    sizeBytes: Math.max(...pending.map(intent => intent.size_bytes)),
                    expiresAt: Math.max(...pending.map(intent => intent.expires_at)),
                });
            }
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

        if (args.kind === 'live_metadata' || args.kind === 'retained_metadata') {
            const retained = args.kind === 'retained_metadata';
            const queryByHash = (storedHash: string) => ctx.db
                .query('file_meta')
                .withIndex('by_workspace_hash', (q) =>
                    q.eq('workspace_id', args.workspace_id).eq('hash', storedHash)
                )
                .take(args.page_size + 1);
            let rows = hash !== undefined
                ? await queryByHash(`${hash.length === 32 ? 'md5' : 'sha256'}:${hash}`)
                : await ctx.db
                    .query('file_meta')
                    .withIndex('by_workspace_hash', (q) =>
                        q.eq('workspace_id', args.workspace_id).gt('hash', cursor.afterPk)
                    )
                    .take(args.page_size + 1);
            if (hash !== undefined && rows.length === 0) {
                // Canonical hashes use their algorithm prefix; retain a fallback for older
                // rows that stored the bare digest.
                rows = await queryByHash(hash);
            }
            const hasMore = rows.length > args.page_size;
            const scanned = hasMore ? rows.slice(0, args.page_size) : rows;
            const items = scanned.flatMap((row) => {
                const normalizedHash = normalizeStorageHash(row.hash);
                if (row.deleted !== retained || (hash !== undefined && normalizedHash !== hash)) return [];
                if (!Number.isSafeInteger(row.size_bytes) || row.size_bytes < 0) {
                    throw new Error('Invalid canonical file size');
                }
                return [{
                    kind: retained ? 'retained_metadata' as const : 'metadata' as const,
                    hash: normalizedHash,
                    sizeBytes: row.size_bytes,
                    ...(row.storage_id ? { storageId: String(row.storage_id) } : {}),
                    ...(typeof row.mime_type === 'string' ? { mimeType: row.mime_type } : {}),
                    ...(typeof row.name === 'string' ? { name: row.name } : {}),
                    ...(row.kind === 'image' || row.kind === 'pdf' || row.kind === 'file'
                        ? { fileKind: row.kind }
                        : {}),
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
 * `sync.pullChecked` (mutation)
 *
 * Purpose:
 * Returns a page of changes after a given server_version cursor.
 *
 * Behavior:
 * - Results are ordered by `server_version` ascending
 * - `tables` can be used to filter specific tables
 */
const pullArgs = { workspace_id: v.id('workspaces'), workspace_item_capability: v.optional(v.literal('v1')),
    ...historyArgs, cursor: v.number(), limit: v.number(), tables: v.optional(v.array(v.string())) };
/** Preserve the old query address solely as an explicit client update boundary. */
export const pull = query({ args: pullArgs, handler: async (ctx, args) => {
    await requireSyncReadAccess(ctx, args.workspace_id);
    throw new ConvexError({ code: 'OR3_SYNC_UPDATE_REQUIRED', statusCode: 426, requiredCapability: 'unavailable-v1' });
} });
export const pullChecked = mutation({
    args: pullArgs,
    handler: async (ctx, args) => {
        const caller = await requireSyncReadAccess(ctx, args.workspace_id);
        requireHistoryCapability(args);
        const historyIncarnation = await readHistory(ctx);
        const { head, retention } = await pullRetentionForWorkspace(ctx, args.workspace_id, args.cursor);
        if (args.history_incarnation !== historyIncarnation || retention.requiresSnapshot) return {
            historyIncarnation, changes: [], unavailable: [], nextCursor: args.cursor, hasMore: false,
            ...retention, requiresSnapshot: true,
        };
        if (!Number.isSafeInteger(args.limit) || args.limit < 1) throw new Error('Invalid pull limit');
        const limit = Math.min(args.limit, MAX_PULL_LIMIT);
        const budget = newSyncReadBudget();
        const page = await readSyncBatch(ctx.db.query('change_log').withIndex('by_workspace_version', q =>
            q.eq('workspace_id', args.workspace_id).gt('server_version', args.cursor)).order('asc'), limit + 1, budget);
        let previous = args.cursor;
        // Include metadata lookahead, preventing a duplicated boundary version from disappearing.
        for (const row of page.rows) {
            if (!Number.isSafeInteger(row.server_version) || row.server_version <= previous || row.server_version > head) throw new Error('Sync coverage version failure');
            previous = row.server_version;
        }
        const changes = [], unavailable = [];
        let nextCursor = args.cursor, consumed = 0, bytes = 0;
        for (const row of page.rows.slice(0, limit)) {
            assertIdentity(row.table_name, row.pk);
            if (row.op !== 'put' && row.op !== 'delete') throw new Error('Sync coverage operation failure');
            const wire = toWireChange(row);
            const reason = readDefect(wire, false);
            const visible = isChangeVisibleToUser(row, caller);
            const selected = !args.tables?.length || args.tables.includes(row.table_name);
            const marker = { tableName: row.table_name, pk: row.pk, originalKind: row.op,
                serverVersion: row.server_version, evidenceRef: 'x'.repeat(64), reason: reason ?? 'invalid_record' };
            const size = visible && selected ? syncJsonByteLength(reason ? marker : wire) : 0;
            if (size > 1536 * 1024) throw new Error('Pull identity exceeds coverage byte ceiling');
            if (bytes + size > 1536 * 1024) break;
            bytes += size;
            if (reason) {
                // Even unknown-owner notification defects are retained privately.
                marker.evidenceRef = await retainEvidence(ctx, args.workspace_id, historyIncarnation, row.table_name, row.pk, row, reason);
                if (visible && selected) unavailable.push(marker);
            } else if (visible && selected) changes.push(wire);
            nextCursor = row.server_version; consumed++;
        }
        if (changes.some(change => hasWorkspaceItemSemantics(change.tableName, change.payload))) requireWorkspaceItemCapability(args.workspace_item_capability);
        const response = { historyIncarnation, changes, unavailable, nextCursor,
            hasMore: consumed < page.rows.length || page.hasMore, ...retention };
        PullResponseSchema.parse(response);
        return response;
    },
});
/** Reactive, content-free wakeups. Delivery MUST use the evidence-writing pull mutation. */
export const watchChanges = query({
    args: { workspace_id: v.id('workspaces'), ...historyArgs,
        workspace_item_capability: v.optional(v.literal('v1')), cursor: v.optional(v.number()), limit: v.optional(v.number()), tables: v.optional(v.array(v.string())) },
    handler: async (ctx, args) => {
        await requireSyncReadAccess(ctx, args.workspace_id);
        requireHistoryCapability(args);
        const historyIncarnation = await readHistory(ctx);
        const counter = await ctx.db.query('server_version_counter').withIndex('by_workspace', q => q.eq('workspace_id', args.workspace_id)).first();
        return { historyIncarnation, version: counter?.value ?? 0 };
    },
});

export const getServerVersion = query({
    args: { workspace_id: v.id('workspaces'), ...historyArgs },
    handler: async (ctx, args) => {
        await requireSyncReadAccess(ctx, args.workspace_id);
        requireHistoryCapability(args);
        const historyIncarnation = await fenceHistory(ctx, args.history_incarnation);
        const counter = await ctx.db.query('server_version_counter').withIndex('by_workspace', q => q.eq('workspace_id', args.workspace_id)).first();
        return { historyIncarnation, version: counter?.value ?? 0 };
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
/** Workspace deletion schedules this separately: cached responses can be much
 * larger than the workspace's current records and must not share its deletion
 * transaction. The function remains internal and uses indexed bounded pages. */
export const purgeSnapshotPages = internalMutation({
    args: { workspace_id: v.id('workspaces') },
    handler: async (ctx, args) => {
        const page = await readSyncBatch(ctx.db.query('sync_snapshot_pages')
            .withIndex('by_workspace', q => q.eq('workspace_id', args.workspace_id)), 100, newSyncReadBudget());
        for (const row of page.rows) await ctx.db.delete(row._id);
        if (page.hasMore) await ctx.scheduler.runAfter(0, makeFunctionReference<'mutation'>('sync:purgeSnapshotPages'), args);
    },
});

/** Bounded maintenance. A pre-image can only serve snapshots begun before its
 * write, so its one-hour lease outlives every session that could need it. */
async function collectSyncState(ctx: MutationCtx, workspaceId: Id<'workspaces'>, retentionSeconds: number, batchSize: number, budget: SyncReadBudget) {
    let budgetHasMore = false;
    const read = async <T extends import('convex/values').Value>(query: AsyncIterable<T>) => {
        const page = await readSyncBatch(query, batchSize + 1, budget);
        budgetHasMore ||= page.hasMore;
        return page.rows;
    };
    const now = nowSec();
    const expiredDevices = await read(ctx.db.query('device_cursors').withIndex('by_workspace_updated', q =>
        q.eq('workspace_id', workspaceId).lt('updated_at', now - retentionSeconds)));
    const expiredSessions = await read(ctx.db.query('sync_snapshot_sessions').withIndex('by_workspace_expiry', q =>
        q.eq('workspace_id', workspaceId).lte('expires_at', now)));
    for (const row of [...expiredDevices.slice(0, batchSize), ...expiredSessions.slice(0, batchSize)]) await ctx.db.delete(row._id);

    const expiredPages = await read(ctx.db.query('sync_snapshot_pages').withIndex('by_workspace_expiry', q =>
        q.eq('workspace_id', workspaceId).lte('expires_at', now)));
    for (const row of expiredPages.slice(0, batchSize)) await ctx.db.delete(row._id);

    const active = await ctx.db.query('sync_snapshot_sessions').withIndex('by_workspace_expiry', q =>
        q.eq('workspace_id', workspaceId).gt('expires_at', now)).first();
    let legacyHasMore = false;
    if (active) {
        // Add leases to pre-upgrade history in bounded pages. Extending a legacy
        // lease for one hour is conservative and needs no destructive migration.
        const legacy = await read(ctx.db.query('sync_record_versions').withIndex('by_workspace_expiry', q =>
            q.eq('workspace_id', workspaceId).eq('retain_until', undefined)));
        for (const row of legacy.slice(0, batchSize)) await ctx.db.patch(row._id, { retain_until: now + SNAPSHOT_TTL_SECONDS });
        legacyHasMore = legacy.length > batchSize;
    }
    const history = active
        ? await read(ctx.db.query('sync_record_versions').withIndex('by_workspace_expiry', q =>
            q.eq('workspace_id', workspaceId).gt('retain_until', 0).lte('retain_until', now)))
        : await read(ctx.db.query('sync_record_versions').withIndex('by_workspace_version', q =>
            q.eq('workspace_id', workspaceId)));
    for (const row of history.slice(0, batchSize)) await ctx.db.delete(row._id);
    const minimum = await ctx.db.query('device_cursors').withIndex('by_workspace_version', q =>
        q.eq('workspace_id', workspaceId)).order('asc').first();
    const counter = minimum ? null : await ctx.db.query('server_version_counter').withIndex('by_workspace', q =>
        q.eq('workspace_id', workspaceId)).first();
    return { minimumVersion: minimum?.last_seen_version ?? counter?.value ?? 0,
        hasMore: budgetHasMore || expiredDevices.length > batchSize || expiredSessions.length > batchSize || expiredPages.length > batchSize || history.length > batchSize || legacyHasMore };
}

async function gcTombstonesPage(ctx: MutationCtx, args: { workspace_id: Id<'workspaces'>; retention_seconds: number; batch_size?: number; cursor?: number }, budget = newSyncReadBudget(), state?: Awaited<ReturnType<typeof collectSyncState>>) {
    validateGcArguments(args);
    const startCursor = args.cursor ?? 0;
    if (!SYNC_HISTORY_GC_POLICY.enabled || !SYNC_HISTORY_GC_POLICY.snapshotBootstrapVerified) {
        return { purged: 0, hasMore: false, nextCursor: startCursor, disabled: true, reason: SYNC_HISTORY_GC_POLICY.reason };
    }
    const batchSize = args.batch_size ?? 100;
    state ??= await collectSyncState(ctx, args.workspace_id, args.retention_seconds, batchSize, budget);
    const page = await readSyncBatch(ctx.db.query('tombstones')
        .withIndex('by_workspace_version', (q) => q.eq('workspace_id', args.workspace_id).gt('server_version', startCursor))
        .order('asc'), batchSize + 1, budget);
    const scanned = page.rows.slice(0, batchSize);
    const cutoff = nowSec() - args.retention_seconds;
    let purged = 0;
    for (const tombstone of scanned) {
        if (tombstone.server_version <= state.minimumVersion && typeof tombstone.server_deleted_at === 'number' && tombstone.server_deleted_at < cutoff) {
            await ctx.db.delete(tombstone._id);
            purged += 1;
        }
    }
    return { purged, hasMore: page.rows.length > batchSize || page.hasMore || state.hasMore, nextCursor: scanned.at(-1)?.server_version ?? startCursor, disabled: false };
}

export const gcTombstones = internalMutation({
    args: {
        workspace_id: v.id('workspaces'),
        retention_seconds: v.number(),
        batch_size: v.optional(v.number()),
        cursor: v.optional(v.number()),
    },
    handler: (ctx, args) => gcTombstonesPage(ctx, args),
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
async function gcChangeLogPage(ctx: MutationCtx, args: { workspace_id: Id<'workspaces'>; retention_seconds: number; batch_size?: number; cursor?: number }, budget = newSyncReadBudget(), state?: Awaited<ReturnType<typeof collectSyncState>>) {
    validateGcArguments(args);
    const startCursor = args.cursor ?? 0;
    if (!SYNC_HISTORY_GC_POLICY.enabled || !SYNC_HISTORY_GC_POLICY.snapshotBootstrapVerified) {
        return { purged: 0, hasMore: false, nextCursor: startCursor, disabled: true, reason: SYNC_HISTORY_GC_POLICY.reason };
    }
    const batchSize = args.batch_size ?? 100;
    state ??= await collectSyncState(ctx, args.workspace_id, args.retention_seconds, batchSize, budget);
    const page = await readSyncBatch(ctx.db.query('change_log')
        .withIndex('by_workspace_version', (q) => q.eq('workspace_id', args.workspace_id).gt('server_version', startCursor))
        .order('asc'), batchSize + 1, budget);
    const scanned = page.rows.slice(0, batchSize);
    const cutoff = nowSec() - args.retention_seconds;
    let purged = 0;
    for (const change of scanned) {
        if (change.server_version <= state.minimumVersion && change.created_at < cutoff) {
            await ctx.db.delete(change._id);
            purged += 1;
        }
    }
    return { purged, hasMore: page.rows.length > batchSize || page.hasMore || state.hasMore, nextCursor: scanned.at(-1)?.server_version ?? startCursor, disabled: false };
}

export const gcChangeLog = internalMutation({
    args: {
        workspace_id: v.id('workspaces'),
        retention_seconds: v.number(),
        batch_size: v.optional(v.number()),
        cursor: v.optional(v.number()),
    },
    handler: (ctx, args) => gcChangeLogPage(ctx, args),
});

// ============================================================
// SCHEDULED GC (Internal)
// ============================================================

/**
 * `sync.runWorkspaceGc` (internal mutation)
 *
 * Purpose:
 * Collects one bounded workspace page and schedules continuation separately.
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
    handler: async (ctx, args) => {
        validateGcArguments(args);
        const startTombstoneCursor = args.tombstone_cursor ?? 0;
        const startChangelogCursor = args.changelog_cursor ?? 0;
        if (!SYNC_HISTORY_GC_POLICY.enabled || !SYNC_HISTORY_GC_POLICY.snapshotBootstrapVerified) {
            return {
                purged: 0,
                hasMore: false,
                nextTombstoneCursor: startTombstoneCursor,
                nextChangelogCursor: startChangelogCursor,
                disabled: true,
                reason: SYNC_HISTORY_GC_POLICY.reason,
            };
        }
        let tombstoneCursor = startTombstoneCursor;
        let changelogCursor = startChangelogCursor;
        let purged = 0;
        let continuation = args.continuation_count ?? 0;
        let hasMore = true;
        if (continuation < MAX_SYNC_GC_CONTINUATIONS) {
            const budget = newSyncReadBudget();
            const retentionSeconds = args.retention_seconds ?? MIN_SYNC_RETENTION_SECONDS;
            const state = await collectSyncState(ctx, args.workspace_id, retentionSeconds, args.batch_size ?? 100, budget);
            const tomb = await gcTombstonesPage(ctx, { workspace_id: args.workspace_id,
                retention_seconds: retentionSeconds, batch_size: args.batch_size, cursor: tombstoneCursor }, budget, state);
            const log = await gcChangeLogPage(ctx, { workspace_id: args.workspace_id,
                retention_seconds: retentionSeconds, batch_size: args.batch_size, cursor: changelogCursor }, budget, state);
            purged += tomb.purged + log.purged;
            tombstoneCursor = tomb.nextCursor;
            changelogCursor = log.nextCursor;
            hasMore = tomb.hasMore || log.hasMore;
            continuation += 1;
        }
        if (hasMore && continuation < MAX_SYNC_GC_CONTINUATIONS) {
            await ctx.scheduler.runAfter(0, makeFunctionReference<'mutation'>('sync:runWorkspaceGc'), {
                workspace_id: args.workspace_id,
                retention_seconds: args.retention_seconds,
                batch_size: args.batch_size,
                tombstone_cursor: tombstoneCursor,
                changelog_cursor: changelogCursor,
                continuation_count: continuation,
            });
        }
        return {
            purged,
            hasMore,
            nextTombstoneCursor: tombstoneCursor,
            nextChangelogCursor: changelogCursor,
            disabled: false,
        };
    },
});

/**
 * `sync.runScheduledGc` (internal mutation)
 *
 * Purpose:
 * Schedules bounded collection for every workspace using paginated enumeration.
 */
export const runScheduledGc = internalMutation({
    args: { cursor: v.optional(v.string()) },
    handler: async (ctx, args) => {
        if (!SYNC_HISTORY_GC_POLICY.enabled || !SYNC_HISTORY_GC_POLICY.snapshotBootstrapVerified) {
            return {
                workspacesScheduled: 0,
                disabled: true,
                reason: SYNC_HISTORY_GC_POLICY.reason,
            };
        }
        const workspaces = await ctx.db.query('workspaces').paginate({ numItems: 100, cursor: args.cursor ?? null });
        for (const workspace of workspaces.page) {
            await ctx.scheduler.runAfter(0, makeFunctionReference<'mutation'>('sync:runWorkspaceGc'), { workspace_id: workspace._id });
        }
        if (!workspaces.isDone) {
            await ctx.scheduler.runAfter(0, makeFunctionReference<'mutation'>('sync:runScheduledGc'), { cursor: workspaces.continueCursor });
        }
        return {
            workspacesScheduled: workspaces.page.length,
            disabled: false,
        };
    },
});

/** Content-free internal notification query. The host authenticates and freshly
 * authorizes every SSE delivery; data is read through the ordinary sync routes.
 */
export const watchVersion = internalQuery({
    args: { workspace_id: v.id('workspaces') },
    handler: async (ctx, args) => {
        const counter = await ctx.db.query('server_version_counter')
            .withIndex('by_workspace', q => q.eq('workspace_id', args.workspace_id)).first();
        return { historyIncarnation: await readHistory(ctx), version: counter?.value ?? 0 };
    },
});

/** Privileged operator surfaces. Internal functions require deployment admin auth,
 * plus the impersonated caller's current workspace access. No automatic repair. */
const evidenceArgs = { workspace_id: v.id('workspaces'), evidence_ref: v.string() };
async function authorizedEvidence(ctx: MutationCtx | QueryCtx, workspace: Id<'workspaces'>, ref: string) {
    await requireSyncReadAccess(ctx, workspace);
    const row = await ctx.db.get(ref as Id<'sync_unavailable_records'>);
    if (!row || row.workspace_id !== workspace) throw new Error('Evidence not found');
    return row;
}
export const inspectUnavailable = internalQuery({
    args: { workspace_id: v.id('workspaces'), cursor: v.optional(v.string()) }, handler: async (ctx, args) => {
        await requireSyncReadAccess(ctx, args.workspace_id);
        const page = await ctx.db.query('sync_unavailable_records').withIndex('by_workspace', q => q.eq('workspace_id', args.workspace_id))
            .paginate({ numItems: 25, cursor: args.cursor ?? null });
        return { items: page.page.map(row => ({ evidenceRef: String(row._id), historyIncarnation: row.history_incarnation,
            tableName: row.table_name.slice(0, 128), pk: row.pk.slice(0, 256), reason: row.reason, status: row.status, chunks: row.chunks })),
            nextCursor: page.isDone ? null : page.continueCursor };
    },
});
export const exportUnavailable = internalQuery({
    args: { ...evidenceArgs, chunk: v.number() }, handler: async (ctx, args) => {
        const row = await authorizedEvidence(ctx, args.workspace_id, args.evidence_ref);
        if (!Number.isSafeInteger(args.chunk) || args.chunk < 0 || args.chunk >= row.chunks) throw new Error('Invalid evidence chunk');
        const source = await ctx.db.get(row.source_document_id);
        if (!source) throw new Error('Evidence source missing');
        const { _id, _creationTime, ...fields } = source;
        const original = { ...fields, _id: row.source_id, ...(row.source_creation_time !== undefined ? { _creationTime: row.source_creation_time } : {}) };
        const bytes = new TextEncoder().encode(JSON.stringify(convexToJson(original as Value)));
        return { data: bytes.slice(args.chunk * 32768, (args.chunk + 1) * 32768).buffer, chunks: row.chunks, encoding: 'convex-json-utf8' };
    },
});
export const revalidateUnavailable = internalQuery({
    args: evidenceArgs, handler: async (ctx, args) => {
        const row = await authorizedEvidence(ctx, args.workspace_id, args.evidence_ref);
        // Original defect stays retained; validate only the CURRENT wire record.
        const candidate = await currentEvidenceCandidate(ctx, args.workspace_id, row.table_name, row.pk);
        let currentDefect: string | null = 'missing_current';
        if (candidate) {
            const { source: _source, serverVersion: _version, ...wire } = candidate;
            currentDefect = readDefect(wire, true);
        }
        return { historyIncarnation: await readHistory(ctx), status: row.status, reason: row.reason, currentDefect };
    },
});
async function currentEvidenceCandidate(ctx: MutationCtx | QueryCtx, workspace: Id<'workspaces'>, table: string, pk: string) {
    const info = TABLE_INDEX_MAP[table];
    if (!info) return null;
    const row = await (ctx.db.query(info.table as TableNames) as any).withIndex(info.indexName, (q: any) =>
        q.eq('workspace_id', workspace).eq(getPkField(table), pk)).unique() as MaterializedDoc | null;
    const tombstone = await ctx.db.query('tombstones').withIndex('by_workspace_table_pk', q =>
        q.eq('workspace_id', workspace).eq('table_name', table).eq('pk', pk)).unique();
    const candidates = [row ? materializedSnapshotCandidate(table, pk, row) : null,
        tombstone ? tombstoneSnapshotCandidate(table, pk, tombstone) : null].filter((value): value is SnapshotCandidate => value !== null);
    return resolveSnapshotWinner(candidates, Number.MAX_SAFE_INTEGER);
}
export const repairUnavailable = internalMutation({
    args: { ...evidenceArgs, history_incarnation: v.string(), replacement: v.optional(v.any()), operation: v.optional(v.union(v.literal('put'), v.literal('delete'))) },
    handler: async (ctx, args) => {
        await requireSyncWriteAccess(ctx, args.workspace_id);
        const historyIncarnation = await fenceHistory(ctx, args.history_incarnation);
        const evidence = await authorizedEvidence(ctx, args.workspace_id, args.evidence_ref);
        if (evidence.history_incarnation !== historyIncarnation) changedHistory(historyIncarnation);
        if (evidence.status !== 'unresolved') return { status: evidence.status };
        const current = await currentEvidenceCandidate(ctx, args.workspace_id, evidence.table_name, evidence.pk);
        if (!current) throw new Error('Current canonical state unavailable; explicit offline recovery required');
        if (current.serverVersion !== evidence.source_version || current.revision.clock !== evidence.source_clock ||
            (evidence.source_op_id !== undefined && current.revision.opId !== evidence.source_op_id)) {
            await ctx.db.patch(evidence._id, { status: 'superseded' });
            return { status: 'superseded' };
        }
        const clock = nextSyncClock(current.revision.clock);
        const opId = crypto.randomUUID();
        const hlc = `${Date.now()}:0:repair`;
        const operation = args.operation ?? 'put';
        const replacement = args.replacement;
        const payload = operation === 'put' && replacement && typeof replacement === 'object' && !Array.isArray(replacement)
            ? { ...replacement, [getPkField(evidence.table_name)]: evidence.pk, clock, hlc, op_id: opId } : replacement;
        const result = await applyPush(ctx, { workspace_id: args.workspace_id, history_incarnation: historyIncarnation,
            history_incarnation_capability: 'incarnation-v1', unavailable_record_capability: 'unavailable-v1', stored_record_capability: 'stored-record-v1', workspace_item_capability: 'v1',
            ops: [{ op_id: opId, table_name: evidence.table_name, pk: evidence.pk, operation, payload, clock, hlc, device_id: 'operator-repair' }] });
        if (!result.results[0]?.success || !result.results[0]?.applied) throw new Error('Repair rejected; evidence remains unresolved');
        await ctx.db.patch(evidence._id, { status: 'repaired', repaired_op_id: opId });
        return { status: 'repaired', opId, serverVersion: result.serverVersion, historyIncarnation };
    },
});

export const getHistoryIncarnation = query({
    args: { workspace_id: v.id('workspaces') }, handler: async (ctx, args) => {
        await requireSyncReadAccess(ctx, args.workspace_id);
        return readHistory(ctx);
    },
});
/** Host reader fallback. Verify the actual source, frozen scope and visibility;
 * a caller-supplied key or bytes alone can never authorize a marker. */
export const retainUnavailableRecord = mutation({
    args: { workspace_id: v.id('workspaces'), ...historyArgs, source: v.union(v.literal('pull'), v.literal('snapshot')), record: v.any(), marker: v.any() },
    handler: async (ctx, args) => {
        const caller = await requireSyncReadAccess(ctx, args.workspace_id);
        requireHistoryCapability(args);
        const history = await fenceHistory(ctx, args.history_incarnation);
        const marker = args.marker as Record<string, any>;
        assertIdentity(marker.tableName, marker.pk);
        let candidate: unknown, original: unknown;
        if (args.source === 'pull') {
            if (!Number.isSafeInteger(marker.serverVersion) || marker.serverVersion <= 0) throw new Error('Invalid source version');
            const row = await ctx.db.query('change_log').withIndex('by_workspace_version', q => q.eq('workspace_id', args.workspace_id).eq('server_version', marker.serverVersion)).unique();
            if (!row || !isChangeVisibleToUser(row, caller) || row.table_name !== marker.tableName || row.pk !== marker.pk || row.op !== marker.originalKind) throw new Error('Unavailable source mismatch');
            candidate = toWireChange(row); original = row;
        } else {
            const session = await ctx.db.get(marker.snapshotId as Id<'sync_snapshot_sessions'>);
            if (!session || session.workspace_id !== args.workspace_id || session.user_id !== caller || session.expires_at <= nowSec() || session.high_watermark !== marker.highWatermark || !session.tables.includes(marker.tableName)) throw new Error('Unavailable source mismatch');
            const winner = await currentEvidenceCandidate(ctx, args.workspace_id, marker.tableName, marker.pk);
            // A changed canonical key needs the frozen preimage, never a newly read replacement.
            const selected = !winner || (winner.serverVersion ?? 0) > session.high_watermark
                ? await readSnapshotHistory(ctx, args.workspace_id, marker.tableName, marker.pk, session.high_watermark) : winner;
            if (!selected || selected.kind !== marker.originalKind || (marker.tableName === 'notifications' &&
                (!(selected.source as MaterializedDoc)?.user_id || (selected.source as MaterializedDoc).user_id !== caller))) throw new Error('Unavailable source mismatch');
            const { source, serverVersion: _version, ...wire } = selected;
            candidate = wire; original = source;
        }
        if (stableJson(candidate) !== stableJson(args.record)) throw new Error('Unavailable original mismatch');
        const reason = readDefect(candidate, args.source === 'snapshot');
        if (!reason) throw new Error('Reader/provider validation disagreement; fail closed');
        return { evidenceRef: await retainEvidence(ctx, args.workspace_id, history, marker.tableName, marker.pk, original, reason) };
    },
});
