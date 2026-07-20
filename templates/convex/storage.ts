/**
 * @module convex/storage
 *
 * Purpose:
 * Manages file metadata and storage URLs for OR3 Cloud.
 * Blob bytes are stored in Convex storage while metadata is stored in `file_meta`.
 *
 * Behavior:
 * - Validates workspace membership for all operations
 * - Generates short-lived upload URLs
 * - Commits metadata and deduplicates by `hash`
 * - Provides read access to stored file URLs
 * - Supports GC of deleted metadata and storage blobs
 *
 * Constraints:
 * - File size is capped at 100MB in `generateUploadUrl`
 * - `hash` is the canonical identifier and must be stable across devices
 *
 * Non-Goals:
 * - Streaming uploads or multipart coordination
 * - Per-file authorization beyond workspace membership
 */

import { v } from 'convex/values';
import { mutation, query, type MutationCtx, type QueryCtx } from './_generated/server';
import type { Id } from './_generated/dataModel';

// ============================================================
// CONSTANTS
// ============================================================

/** Maximum file size in bytes (100MB) */
const MAX_FILE_SIZE_BYTES = 100 * 1024 * 1024;
const UPLOAD_INTENT_TTL_SECONDS = 15 * 60;
const MAX_GC_DELETE_LIMIT = 100;
const MAX_GC_CANDIDATE_SCAN = 500;
const MAX_GC_REFERENCE_ROWS_PER_TABLE = 500;

function normalizeHash(value: string): string {
    return value.replace(/^sha256:/i, '').trim().toLowerCase();
}

function normalizeMime(value: string): string {
    return value.split(';', 1)[0]?.trim().toLowerCase() || '';
}

function hexToBase64(hex: string): string {
    let binary = '';
    for (let i = 0; i < hex.length; i += 2) {
        binary += String.fromCharCode(Number.parseInt(hex.slice(i, i + 2), 16));
    }
    return btoa(binary);
}

function parseSerializedHashes(value: string | null | undefined): string[] | null {
    if (!value) return [];
    try {
        const parsed = JSON.parse(value) as unknown;
        if (!Array.isArray(parsed) || parsed.some((entry) => typeof entry !== 'string')) {
            return null;
        }
        return parsed.map((entry) => normalizeHash(entry as string));
    } catch {
        // Malformed canonical rows are treated conservatively as potentially
        // referenced; GC must never turn corrupt metadata into data loss.
        return null;
    }
}

async function loadCanonicalReferencedHashes(
    ctx: MutationCtx,
    workspaceId: Id<'workspaces'>
): Promise<Set<string> | null> {
    const messages = await ctx.db
        .query('messages')
        .withIndex('by_workspace_id', (q: {
            eq: (field: 'workspace_id', value: Id<'workspaces'>) => unknown;
        }) => q.eq('workspace_id', workspaceId))
        .take(MAX_GC_REFERENCE_ROWS_PER_TABLE + 1);
    if (messages.length > MAX_GC_REFERENCE_ROWS_PER_TABLE) return null;

    const posts = await ctx.db
        .query('posts')
        .withIndex('by_workspace_id', (q: {
            eq: (field: 'workspace_id', value: Id<'workspaces'>) => unknown;
        }) => q.eq('workspace_id', workspaceId))
        .take(MAX_GC_REFERENCE_ROWS_PER_TABLE + 1);
    if (posts.length > MAX_GC_REFERENCE_ROWS_PER_TABLE) return null;
    const hashes = new Set<string>();
    for (const row of [...messages, ...posts]) {
        if (row.deleted) continue;
        const parsed = parseSerializedHashes(row.file_hashes);
        if (parsed === null) return null;
        for (const hash of parsed) hashes.add(hash);
    }
    return hashes;
}

function inferProviderFromIssuer(issuer: string | undefined): string {
    if (!issuer) return 'clerk';
    if (issuer.includes('clerk')) return 'clerk';
    const marker = '/auth/';
    const markerIndex = issuer.lastIndexOf(marker);
    if (markerIndex === -1) return 'clerk';
    const provider = issuer.slice(markerIndex + marker.length).trim();
    return provider || 'clerk';
}

// ============================================================
// HELPERS
// ============================================================

const nowSec = (): number => Math.floor(Date.now() / 1000);

/**
 * Internal authorization helper.
 *
 * Purpose:
 * Ensures the caller is authenticated and a member of the target workspace.
 */
async function verifyWorkspaceMembership(
    ctx: MutationCtx | QueryCtx,
    workspaceId: Id<'workspaces'>
): Promise<Id<'users'>> {
    const identity = await ctx.auth.getUserIdentity();
    if (!identity) {
        throw new Error('Unauthorized: No identity');
    }
    const provider = inferProviderFromIssuer(identity.issuer);

    const authAccount = await ctx.db
        .query('auth_accounts')
        .withIndex('by_provider', (q: any) =>
            q.eq('provider', provider).eq('provider_user_id', identity.subject)
        )
        .first();

    if (!authAccount) {
        throw new Error('Unauthorized: No auth account');
    }

    const membership = await ctx.db
        .query('workspace_members')
        .withIndex('by_workspace_user', (q: any) =>
            q.eq('workspace_id', workspaceId).eq('user_id', authAccount.user_id)
        )
        .first();

    if (!membership) {
        throw new Error('Forbidden: Not a workspace member');
    }

    return authAccount.user_id;
}

/**
 * `storage.generateUploadUrl` (mutation)
 *
 * Purpose:
 * Returns a short-lived upload URL for Convex storage.
 *
 * Constraints:
 * - Enforces a 100MB size cap
 */
export const generateUploadUrl = mutation({
    args: {
        workspace_id: v.id('workspaces'),
        hash: v.string(),
        mime_type: v.string(),
        size_bytes: v.number(),
        workspace_quota_bytes: v.optional(v.number()),
    },
    handler: async (ctx, args) => {
        const userId = await verifyWorkspaceMembership(ctx, args.workspace_id);

        // Enforce file size limit
        if (!Number.isSafeInteger(args.size_bytes) || args.size_bytes < 1) {
            throw new Error('File size must be a positive integer');
        }
        if (args.size_bytes > MAX_FILE_SIZE_BYTES) {
            throw new Error(
                `File size ${args.size_bytes} exceeds maximum allowed size of ${MAX_FILE_SIZE_BYTES} bytes (100MB)`
            );
        }

        const hash = normalizeHash(args.hash);
        if (!/^[0-9a-f]{64}$/.test(hash)) throw new Error('Invalid SHA-256 digest');
        const mimeType = normalizeMime(args.mime_type);
        if (!mimeType) throw new Error('Invalid MIME type');
        if (args.workspace_quota_bytes !== undefined &&
            (!Number.isSafeInteger(args.workspace_quota_bytes) || args.workspace_quota_bytes < 1)) {
            throw new Error('Invalid workspace quota');
        }
        const now = nowSec();
        const liveFiles = await ctx.db.query('file_meta')
            .withIndex('by_workspace_deleted', (q: any) =>
                q.eq('workspace_id', args.workspace_id).eq('deleted', false))
            .collect();
        const activeIntents = await ctx.db.query('upload_intents')
            .withIndex('by_workspace_status', (q: any) =>
                q.eq('workspace_id', args.workspace_id).eq('status', 'active'))
            .collect();
        const alreadyStored = liveFiles.some((file: any) => normalizeHash(file.hash) === hash);
        const alreadyReserved = activeIntents.some((intent: any) =>
            intent.expires_at > now && intent.hash === hash && intent.reserved_bytes > 0);
        const reservedBytes = alreadyStored || alreadyReserved ? 0 : args.size_bytes;
        if (args.workspace_quota_bytes !== undefined) {
            const usedBytes = liveFiles.reduce((total: number, file: any) => total + file.size_bytes, 0);
            const activeReservedBytes = activeIntents.reduce(
                (total: number, intent: any) => total + (intent.expires_at > now ? intent.reserved_bytes : 0), 0);
            if (usedBytes + activeReservedBytes + reservedBytes > args.workspace_quota_bytes) {
                throw new Error('Workspace storage quota exceeded');
            }
        }

        const uploadUrl = await ctx.storage.generateUploadUrl();
        const intentId = await ctx.db.insert('upload_intents', {
            workspace_id: args.workspace_id,
            user_id: userId,
            hash,
            mime_type: mimeType,
            size_bytes: args.size_bytes,
            reserved_bytes: reservedBytes,
            expires_at: now + UPLOAD_INTENT_TTL_SECONDS,
            status: 'active',
            created_at: now,
        });
        return { uploadUrl, intentId, expiresAt: (now + UPLOAD_INTENT_TTL_SECONDS) * 1000 };
    },
});

export const cancelUploadIntent = mutation({
    args: { workspace_id: v.id('workspaces'), intent_id: v.id('upload_intents') },
    handler: async (ctx, args) => {
        const userId = await verifyWorkspaceMembership(ctx, args.workspace_id);
        const intent = await ctx.db.get(args.intent_id);
        if (!intent || intent.workspace_id !== args.workspace_id || intent.user_id !== userId) {
            throw new Error('Upload intent not found');
        }
        if (intent.status !== 'active') throw new Error('Upload intent is not active');
        await ctx.db.patch(intent._id, { status: 'cancelled', cancelled_at: nowSec() });
    },
});

/**
 * `storage.commitUpload` (mutation)
 *
 * Purpose:
 * Persists file metadata and associates a Convex storage object.
 *
 * Behavior:
 * - If a record with the same `hash` exists, updates storage metadata only
 * - Otherwise inserts a new row with `ref_count = 1`
 * - Performs a small dedup sweep to resolve rare race duplicates
 */
export const commitUpload = mutation({
    args: {
        workspace_id: v.id('workspaces'),
        intent_id: v.id('upload_intents'),
        hash: v.string(),
        storage_id: v.id('_storage'),
        storage_provider_id: v.string(),
        mime_type: v.string(),
        size_bytes: v.number(),
        name: v.string(),
        kind: v.union(v.literal('image'), v.literal('pdf')),
        width: v.optional(v.number()),
        height: v.optional(v.number()),
        page_count: v.optional(v.number()),
    },
    handler: async (ctx, args) => {
        const userId = await verifyWorkspaceMembership(ctx, args.workspace_id);
        const intent = await ctx.db.get(args.intent_id);
        const hash = normalizeHash(args.hash);
        const mimeType = normalizeMime(args.mime_type);
        if (!intent || intent.workspace_id !== args.workspace_id || intent.user_id !== userId) {
            throw new Error('Upload intent not found');
        }
        if (intent.status !== 'active') throw new Error('Upload intent already consumed or cancelled');
        if (intent.expires_at <= nowSec()) throw new Error('Upload intent expired');
        if (intent.hash !== hash || intent.size_bytes !== args.size_bytes || intent.mime_type !== mimeType) {
            throw new Error('Upload metadata does not match intent');
        }
        const object = await ctx.db.system.get(args.storage_id);
        if (!object) throw new Error('Uploaded storage object not found');
        if (object.size !== intent.size_bytes || normalizeMime(object.contentType ?? '') !== intent.mime_type) {
            throw new Error('Uploaded object metadata does not match intent');
        }
        if (object.sha256 !== hexToBase64(intent.hash)) {
            throw new Error('Uploaded object digest does not match intent');
        }

        const existing = await ctx.db
            .query('file_meta')
            .withIndex('by_workspace_hash', (q: any) =>
                q.eq('workspace_id', args.workspace_id).eq('hash', args.hash)
            )
            .first();

        if (existing) {
            await ctx.db.patch(existing._id, {
                storage_id: args.storage_id,
                storage_provider_id: args.storage_provider_id,
                updated_at: nowSec(),
            });
            await ctx.db.patch(intent._id, {
                status: 'consumed', storage_id: args.storage_id, consumed_at: nowSec(),
            });
            return;
        }

        const createdId = await ctx.db.insert('file_meta', {
            workspace_id: args.workspace_id,
            hash: args.hash,
            name: args.name,
            mime_type: args.mime_type,
            kind: args.kind,
            size_bytes: args.size_bytes,
            width: args.width,
            height: args.height,
            page_count: args.page_count,
            ref_count: 1,
            storage_id: args.storage_id,
            storage_provider_id: args.storage_provider_id,
            deleted: false,
            created_at: nowSec(),
            updated_at: nowSec(),
            clock: 0,
        });

        // Cleanup any race condition duplicates (should be rare with .first() check above)
        // Use .take() instead of .collect() for safety
        const matches = await ctx.db
            .query('file_meta')
            .withIndex('by_workspace_hash', (q: any) =>
                q.eq('workspace_id', args.workspace_id).eq('hash', args.hash)
            )
            .take(10); // Limit to prevent abuse

        if (matches.length > 1) {
            const sorted = [...matches].sort(
                (a, b) => a._creationTime - b._creationTime
            );
            const keeper = sorted[0];
            if (!keeper) return;
            for (const file of sorted.slice(1)) {
                if (file._id === keeper._id) continue;
                await ctx.db.delete(file._id);
            }
            if (keeper._id !== createdId) {
                await ctx.db.patch(keeper._id, {
                    storage_id: args.storage_id,
                    storage_provider_id: args.storage_provider_id,
                    updated_at: nowSec(),
                });
            }
        }
        await ctx.db.patch(intent._id, {
            status: 'consumed', storage_id: args.storage_id, consumed_at: nowSec(),
        });
    },
});

/**
 * `storage.getFileUrl` (query)
 *
 * Purpose:
 * Retrieves a signed URL for a stored file.
 *
 * Behavior:
 * - Returns `null` when metadata exists but the blob is missing
 */
export const getFileUrl = query({
    args: {
        workspace_id: v.id('workspaces'),
        hash: v.string(),
    },
    handler: async (ctx, args) => {
        await verifyWorkspaceMembership(ctx, args.workspace_id);

        const file = await ctx.db
            .query('file_meta')
            .withIndex('by_workspace_hash', (q: any) =>
                q.eq('workspace_id', args.workspace_id).eq('hash', args.hash)
            )
            .first();

        if (!file?.storage_id) return null;

        const url = await ctx.storage.getUrl(file.storage_id);
        // Handle case where storage object was deleted but metadata remains
        if (!url) {
            return null;
        }
        return { url };
    },
});

/**
 * `storage.gcDeletedFiles` (mutation)
 *
 * Purpose:
 * Deletes storage blobs and metadata for files that are soft-deleted and
 * no longer referenced.
 *
 * Constraints:
 * - Ignores the compatibility-only `ref_count` cache and verifies canonical
 *   materialized message/post references before deleting
 * - Bounds candidate and reference reads independently of dataset size
 * - Uses a caller-provided retention window in seconds
 */
export const gcDeletedFiles = mutation({
    args: {
        workspace_id: v.id('workspaces'),
        retention_seconds: v.number(),
        limit: v.optional(v.number()),
    },
    handler: async (ctx, args) => {
        await verifyWorkspaceMembership(ctx, args.workspace_id);

        if (!Number.isSafeInteger(args.retention_seconds) || args.retention_seconds < 0) {
            throw new Error('Invalid retention window');
        }
        const limit = args.limit ?? 25;
        if (!Number.isSafeInteger(limit) || limit <= 0 || limit > MAX_GC_DELETE_LIMIT) {
            throw new Error(`GC limit must be between 1 and ${MAX_GC_DELETE_LIMIT}`);
        }
        const cutoff = nowSec() - args.retention_seconds;
        let deletedCount = 0;
        const candidateScanLimit = Math.min(
            MAX_GC_CANDIDATE_SCAN,
            Math.max(limit, limit * 4)
        );

        const candidates = await ctx.db
            .query('file_meta')
            .withIndex('by_workspace_deleted', (q: any) =>
                q.eq('workspace_id', args.workspace_id).eq('deleted', true)
            )
            .take(candidateScanLimit);
        const referencedHashes = await loadCanonicalReferencedHashes(ctx, args.workspace_id);
        if (referencedHashes === null) {
            return { deletedCount: 0, scannedCount: candidates.length };
        }

        for (const file of candidates) {
            if (deletedCount >= limit) break;
            if (!file.deleted_at || file.deleted_at > cutoff) continue;
            // A bounded scan that cannot prove absence fails closed. This may
            // defer collection in a very large workspace, but cannot delete a
            // live blob or allocate an unbounded result set.
            if (referencedHashes.has(normalizeHash(file.hash))) continue;

            if (file.storage_id) {
                await ctx.storage.delete(file.storage_id);
            }
            await ctx.db.delete(file._id);
            deletedCount += 1;
        }

        return { deletedCount, scannedCount: candidates.length };
    },
});
