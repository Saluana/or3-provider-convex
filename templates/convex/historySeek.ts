const PREFIX = 'chat-seek-v1:';
export type ChatSeek = { thread_id: string; backward: boolean; key?: [number, string, string] };
/** Wire-only helper: the host performs scope authorization before supplying a seek. */
export function parseChatSeek(cursor: string | undefined, threadId: string): ChatSeek | undefined {
    if (!cursor?.startsWith(PREFIX)) return undefined;
    if (new TextEncoder().encode(cursor).length > 2048) throw new Error('Canonical seek exceeds its cursor bound');
    const value = JSON.parse(cursor.slice(PREFIX.length)) as ChatSeek;
    if (!value || value.thread_id !== threadId || typeof value.backward !== 'boolean' || value.key !== undefined
        && (!Array.isArray(value.key) || value.key.length !== 3 || !Number.isSafeInteger(value.key[0])
            || typeof value.key[1] !== 'string' || typeof value.key[2] !== 'string')) throw new Error('Invalid canonical seek');
    return value;
}
export function encodeChatSeek(value: ChatSeek): string { const cursor = PREFIX + JSON.stringify(value); parseChatSeek(cursor, value.thread_id); return cursor; }
