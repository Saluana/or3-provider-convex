/** Provider-owned, user-bound download URLs. Native Convex URLs stay server-only. */
import { createHmac, timingSafeEqual } from 'node:crypto';
import { createError, eventHandler, getQuery, sendStream, setResponseHeader, type H3Event } from 'h3';
import { z } from 'zod';
import { useRuntimeConfig } from '#imports';
import { resolveSessionContext } from '~~/server/auth/session';
import { requireStorageWorkspaceAccess } from '~~/server/utils/storage/workspace-access';
import { resolveDownloadPolicy } from '~~/server/utils/storage/download-policy';
import type { PresignDownloadRequest, PresignDownloadResponse } from '~~/server/storage/gateway/types';
import { convexInternalApi } from '../../utils/convex-api';
import { buildGatewayAdminIdentity, getConvexAdminGatewayClient } from '../utils/convex-gateway';
import { resolveConvexAuthProvider } from '../utils/provider-compat';

const DEFAULT_TTL_MS = 5 * 60_000;
const MAX_TTL_MS = 15 * 60_000;
const TokenSchema = z.object({
    version: z.literal(1),
    workspaceId: z.string().min(1).max(256),
    userId: z.string().min(1).max(256),
    provider: z.string().min(1).max(256),
    providerUserId: z.string().min(1).max(256),
    hash: z.string().regex(/^(?:sha256:[0-9a-f]{64}|md5:[0-9a-f]{32})$/),
    storageId: z.string().min(1).max(256),
    disposition: z.enum(['inline', 'attachment']),
    expiresAt: z.number().int().positive().max(Number.MAX_SAFE_INTEGER),
});
type DownloadToken = z.infer<typeof TokenSchema>;
const NativeFileSchema = z.object({
    url: z.string().url(),
    storageId: z.string().min(1),
    mimeType: z.string().optional(),
    name: z.string().optional(),
    fileKind: z.enum(['image', 'pdf', 'file']).optional(),
});

function getAdminKey(event: H3Event): string {
    const key = useRuntimeConfig(event).sync?.convexAdminKey;
    if (!key) throw createError({ statusCode: 503, statusMessage: 'Convex downloads require the server admin key' });
    return key;
}

function signature(payload: string, key: string): Buffer {
    return createHmac('sha256', key).update('or3-convex-download:v1\0').update(payload).digest();
}

function requireUnexpired(claims: DownloadToken): void {
    if (claims.expiresAt <= Date.now()) {
        throw createError({ statusCode: 403, statusMessage: 'Invalid or expired token' });
    }
}

function verifyToken(token: string, key: string): DownloadToken {
    try {
        if (token.length > 8192 || !/^[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+$/.test(token)) throw new Error('Invalid token');
        const [payload, encodedSignature] = token.split('.') as [string, string];
        const actual = Buffer.from(encodedSignature, 'base64url');
        const expected = signature(payload, key);
        if (actual.length !== expected.length || !timingSafeEqual(actual, expected)) throw new Error('Invalid signature');
        const claims = TokenSchema.parse(JSON.parse(Buffer.from(payload, 'base64url').toString('utf8')));
        requireUnexpired(claims);
        return claims;
    } catch {
        throw createError({ statusCode: 403, statusMessage: 'Invalid or expired token' });
    }
}

async function requireSession(event: H3Event) {
    // A new GET resolves the auth provider's session; signed URLs never replace it.
    const session = await resolveSessionContext(event);
    if (!session?.authenticated || !session.user?.id || !session.provider || !session.providerUserId) {
        throw createError({ statusCode: 401, statusMessage: 'Unauthorized' });
    }
    return session as typeof session & { user: { id: string }; provider: string; providerUserId: string };
}

async function readNativeFile(event: H3Event, session: Awaited<ReturnType<typeof requireSession>>, workspaceId: string, hash: string) {
    const client = getConvexAdminGatewayClient(event, getAdminKey(event), buildGatewayAdminIdentity(
        resolveConvexAuthProvider(session.provider), session.providerUserId,
    ));
    // This is an internal Convex query. Browser/user JWT clients cannot invoke it.
    let result: unknown;
    try {
        result = await client.query(convexInternalApi.storage.getFileUrl, { workspace_id: workspaceId, hash });
    } catch {
        throw createError({ statusCode: 503, statusMessage: 'Convex download authorization unavailable' });
    }
    if (!result) throw createError({ statusCode: 404, statusMessage: 'File not found' });
    const parsed = NativeFileSchema.safeParse(result);
    if (!parsed.success) throw createError({ statusCode: 503, statusMessage: 'Convex download backend must be updated' });
    const url = new URL(parsed.data.url);
    if (!['http:', 'https:'].includes(url.protocol) || url.username || url.password) {
        throw createError({ statusCode: 502, statusMessage: 'Invalid Convex download URL' });
    }
    return parsed.data;
}

export async function createConvexDownloadUrl(event: H3Event, input: PresignDownloadRequest): Promise<PresignDownloadResponse> {
    const session = await requireSession(event);
    const key = getAdminKey(event);
    const ttl = input.expiresInMs ?? DEFAULT_TTL_MS;
    if (!Number.isSafeInteger(ttl) || ttl < 1) throw createError({ statusCode: 400, statusMessage: 'Invalid download expiry' });
    await requireStorageWorkspaceAccess(event, session, input.workspaceId);
    const file = await readNativeFile(event, session, input.workspaceId, input.hash);
    if (input.storageId && input.storageId !== file.storageId) throw createError({ statusCode: 404, statusMessage: 'File not found' });
    await requireStorageWorkspaceAccess(event, session, input.workspaceId);
    const claims = TokenSchema.parse({
        version: 1, workspaceId: input.workspaceId, userId: session.user.id,
        provider: session.provider, providerUserId: session.providerUserId,
        hash: input.hash, storageId: file.storageId,
        disposition: input.disposition === 'attachment' ? 'attachment' : 'inline',
        expiresAt: Date.now() + Math.min(ttl, MAX_TTL_MS),
    });
    const payload = Buffer.from(JSON.stringify(claims)).toString('base64url');
    const token = `${payload}.${signature(payload, key).toString('base64url')}`;
    return { url: `/api/storage/convex/download?token=${token}`, expiresAt: claims.expiresAt, method: 'GET' };
}

export default eventHandler(async event => {
    setResponseHeader(event, 'Cache-Control', 'private, no-store');
    const token = getQuery(event).token;
    if (typeof token !== 'string' || !token) throw createError({ statusCode: 400, statusMessage: 'Missing token' });
    const claims = verifyToken(token, getAdminKey(event));
    const session = await requireSession(event);
    if (claims.userId !== session.user.id || claims.provider !== session.provider || claims.providerUserId !== session.providerUserId) {
        throw createError({ statusCode: 403, statusMessage: 'Invalid token subject' });
    }
    await requireStorageWorkspaceAccess(event, session, claims.workspaceId);
    const file = await readNativeFile(event, session, claims.workspaceId, claims.hash);
    if (file.storageId !== claims.storageId) throw createError({ statusCode: 404, statusMessage: 'File not found' });
    requireUnexpired(claims);

    let upstream: Response;
    try {
        upstream = await fetch(file.url, { redirect: 'error', signal: AbortSignal.timeout(30_000) });
    } catch {
        throw createError({ statusCode: 502, statusMessage: 'Convex download unavailable' });
    }
    try {
        if (!upstream.ok || !upstream.body) {
            throw createError({ statusCode: upstream.status === 404 ? 404 : 502, statusMessage: 'Convex file unavailable' });
        }
        // Revalidate after the awaited fetch before any bytes or response policy
        // cross the gateway. A replaced/deleted object never satisfies an old URL.
        await requireStorageWorkspaceAccess(event, session, claims.workspaceId);
        const current = await readNativeFile(event, session, claims.workspaceId, claims.hash);
        if (current.storageId !== claims.storageId) throw createError({ statusCode: 404, statusMessage: 'File not found' });
        await requireStorageWorkspaceAccess(event, session, claims.workspaceId);
        requireUnexpired(claims);
        const policy = resolveDownloadPolicy({
            fileKind: current.fileKind, mimeType: current.mimeType, filename: current.name,
            requestedDisposition: claims.disposition,
        });
        const filename = encodeURIComponent(policy.filename).replace(/[!'()*]/g, character => `%${character.charCodeAt(0).toString(16).toUpperCase()}`);
        setResponseHeader(event, 'Content-Type', policy.mimeType);
        setResponseHeader(event, 'Content-Disposition', `${policy.disposition}; filename*=UTF-8''${filename}`);
        setResponseHeader(event, 'X-Content-Type-Options', 'nosniff');
    } catch (error) {
        await upstream.body?.cancel().catch(() => undefined);
        throw error;
    }
    return sendStream(event, upstream.body!);
});
