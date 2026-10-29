/** Snapshot candidates, revision ordering and preimages shared by all sync writers. */
import type { MutationCtx } from './_generated/server';
import type { Id, TableNames } from './_generated/dataModel';
import { getPkField } from './tableMetadata';
const nowSec = (): number => Math.floor(Date.now() / 1000);
export const SNAPSHOT_TTL_SECONDS = 60 * 60;

export type SnapshotRevision = {
    clock: number;
    hlc: string;
    opId: string;
};

export type SnapshotCandidate =
    | {
          kind: 'row';
          tableName: string;
          pk: string;
          payload: unknown;
          revision: SnapshotRevision;
          serverVersion?: number;
      }
    | {
          kind: 'tombstone';
          tableName: string;
          pk: string;
          revision: SnapshotRevision;
          serverDeletedAt: number;
          /** Retained file metadata for server archives; never sync wire data. */
          payload?: unknown;
          serverVersion?: number;
      };

export type SnapshotCursorToken = {
    version: 1;
    snapshotId: string;
    tableIndex: number;
    afterPk: string | null;
};

export function normalizeSnapshotTables(
    requested: readonly string[] | undefined,
    allowed: readonly string[]
): string[] {
    const allowedSet = new Set(allowed);
    const selected = requested && requested.length > 0 ? requested : allowed;
    const normalized = [...new Set(selected)];
    for (const table of normalized) {
        if (!allowedSet.has(table)) {
            throw new Error(`Invalid snapshot table: ${table}`);
        }
    }
    return normalized.sort();
}

export function compareSnapshotRevisions(
    left: SnapshotRevision,
    right: SnapshotRevision
): number {
    if (left.clock !== right.clock) return left.clock - right.clock;
    const hlcOrder = left.hlc < right.hlc ? -1 : left.hlc > right.hlc ? 1 : 0;
    if (hlcOrder !== 0) return hlcOrder;
    return left.opId < right.opId ? -1 : left.opId > right.opId ? 1 : 0;
}

/** Select one canonical logical state at or before the frozen watermark. */
export function resolveSnapshotWinner(
    candidates: readonly SnapshotCandidate[],
    highWatermark: number
): SnapshotCandidate | null {
    let winner: SnapshotCandidate | null = null;
    for (const candidate of candidates) {
        if (typeof candidate.serverVersion !== 'number') continue;
        if (candidate.serverVersion > highWatermark) continue;
        if (!winner) {
            winner = candidate;
            continue;
        }
        const order = compareSnapshotRevisions(candidate.revision, winner.revision);
        if (order > 0 || (order === 0 && candidate.kind === 'tombstone')) {
            winner = candidate;
        }
    }
    return winner;
}

export function encodeSnapshotCursor(token: SnapshotCursorToken): string {
    const asciiJson = encodeURIComponent(JSON.stringify(token));
    return btoa(asciiJson)
        .replace(/\+/g, '-')
        .replace(/\//g, '_')
        .replace(/=+$/u, '');
}

export function decodeSnapshotCursor(value: string): SnapshotCursorToken {
    if (!value || value.length > 4096) throw new Error('Invalid snapshot page token');
    try {
        const base64 = value.replace(/-/g, '+').replace(/_/g, '/');
        const padding = '='.repeat((4 - (base64.length % 4)) % 4);
        const parsed = JSON.parse(decodeURIComponent(atob(base64 + padding))) as Partial<SnapshotCursorToken>;
        if (
            parsed.version !== 1 ||
            typeof parsed.snapshotId !== 'string' ||
            parsed.snapshotId.length === 0 ||
            !Number.isInteger(parsed.tableIndex) ||
            (parsed.tableIndex as number) < 0 ||
            !(parsed.afterPk === null || typeof parsed.afterPk === 'string')
        ) {
            throw new Error('invalid token shape');
        }
        return parsed as SnapshotCursorToken;
    } catch {
        throw new Error('Invalid snapshot page token');
    }
}


export type MaterializedDoc = {
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


export function legacySnapshotRevision(
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

export function materializedSnapshotCandidate(
    tableName: string,
    pk: string,
    doc: MaterializedDoc,
    serverVersionOverride?: number
): SnapshotCandidate {
    const revision = legacySnapshotRevision(tableName, pk, doc);
    // Rows predating versioned sync existed before every current watermark.
    // Keep that baseline when recording their first pre-image as well.
    const serverVersion = serverVersionOverride ?? doc.server_version ?? 0;
    if (doc.deleted === true) {
        return {
            kind: 'tombstone',
            tableName,
            pk,
            revision,
            ...(tableName === 'file_meta' ? { payload: snapshotPayloadFromDoc(doc, getPkField(tableName), pk, revision) } : {}),
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

export async function recordMaterializedPreimage(
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
        doc.server_version ?? 0
    );
    await ctx.db.insert('sync_record_versions', {
        workspace_id: workspaceId,
        table_name: tableName,
        pk,
        server_version: candidate.serverVersion ?? fallbackServerVersion,
        retain_until: nowSec() + SNAPSHOT_TTL_SECONDS,
        kind: candidate.kind,
        payload: candidate.kind === 'row' || tableName === 'file_meta' ? candidate.payload : undefined,
        clock: candidate.revision.clock,
        hlc: candidate.revision.hlc,
        op_id: candidate.revision.opId,
        server_deleted_at:
            candidate.kind === 'tombstone' ? candidate.serverDeletedAt : undefined,
    });
}
