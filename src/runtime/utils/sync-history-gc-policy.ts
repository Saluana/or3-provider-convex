/**
 * Runtime safety policy for retained sync history.
 *
 * This cannot be enabled through environment configuration. Provider-side
 * retention may be reintroduced only with verified snapshot bootstrap support.
 */
export const SYNC_HISTORY_GC_POLICY = Object.freeze({
    enabled: true,
    snapshotBootstrapVerified: true,
    reason:
        'Convex history GC is guarded by the verified snapshot bootstrap (snapshot-v1) retention contract.',
});

export function canRunSyncHistoryGc(): boolean {
    return (
        SYNC_HISTORY_GC_POLICY.enabled &&
        SYNC_HISTORY_GC_POLICY.snapshotBootstrapVerified
    );
}
