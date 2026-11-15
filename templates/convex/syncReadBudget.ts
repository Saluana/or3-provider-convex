import { getConvexSize, type Value } from 'convex/values';

// Convex documents are at most 1 MiB. Reserve that space BEFORE asking the
// iterator for another document (the SDK iterator reads one document at a time).
// Leave 6 MiB for authorization, counters, session and other point reads.
const MAX_DOCUMENT_BYTES = 1024 * 1024;
export const newSyncReadBudget = () => ({ bytes: 10 * MAX_DOCUMENT_BYTES, reads: 4000 });
export type SyncReadBudget = ReturnType<typeof newSyncReadBudget>;
export const canReadSyncDocument = (budget: SyncReadBudget) => budget.bytes >= MAX_DOCUMENT_BYTES && budget.reads > 0;

/** Bounds database work before allocation, including any requested lookahead. */
export async function readSyncBatch<T>(query: AsyncIterable<T>, limit: number,
    budget: SyncReadBudget, skip?: (row: T) => boolean): Promise<{ rows: T[]; hasMore: boolean }> {
    const rows: T[] = [];
    let bytes = 0;
    const iterator = query[Symbol.asyncIterator]();
    try {
        while (rows.length < limit && (bytes < MAX_DOCUMENT_BYTES || rows.length === 0) && canReadSyncDocument(budget)) {
            const next = await iterator.next();
            if (next.done) return { rows, hasMore: false };
            const size = getConvexSize(next.value as Value);
            budget.bytes -= size;
            budget.reads--;
            bytes += size;
            if (!skip?.(next.value)) rows.push(next.value);
        }
        // A shorter page may conservatively require one final empty request.
        return { rows, hasMore: true };
    } finally {
        await iterator.return?.();
    }
}
