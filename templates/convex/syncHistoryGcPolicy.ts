/**
 * Sync history retention is deliberately fail-closed.
 *
 * This is not environment-configurable. It may change only after a consistent
 * snapshot-at-high-watermark bootstrap has been implemented and verified.
 */
export const SYNC_HISTORY_GC_POLICY = Object.freeze({
    enabled: true,
    snapshotBootstrapVerified: true,
    reason:
        'Convex history GC is guarded by the verified snapshot-v1 retention contract.',
});

export function computePullRetention(input: {
    cursor: number;
    oldestLogVersion: number | null;
    highWatermark: number;
}): { oldestRetainedVersion: number; requiresSnapshot: boolean } {
    const oldestRetainedVersion =
        input.oldestLogVersion ?? Math.max(0, input.highWatermark) + 1;
    return {
        oldestRetainedVersion,
        requiresSnapshot:
            oldestRetainedVersion > 0 &&
            input.cursor < Math.max(0, oldestRetainedVersion - 1),
    };
}
