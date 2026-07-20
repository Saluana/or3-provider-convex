/** Pure helpers shared by the Convex snapshot mutation and contract fixtures. */

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
        if (
            typeof candidate.serverVersion === 'number' &&
            candidate.serverVersion > highWatermark
        ) {
            continue;
        }
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
