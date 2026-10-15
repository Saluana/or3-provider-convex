/**
 * Failure modes: bearer URLs outlive TTL or cross users; removed membership or
 * changed/deleted canonical blobs still deliver; active MIME/native headers or
 * redirect targets escape to browsers; missing server credentials fail open.
 * The real H3 presign/download boundary owns token, headers, and byte delivery;
 * only the host identity/membership store and Convex/network transport are faked.
 */
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { createApp, createError, eventHandler, getQuery, toWebHandler } from 'h3';
import { ConvexStorageGatewayAdapter } from '../convex-storage-gateway-adapter';
import download from '../convex-download';

const f = vi.hoisted(() => ({
    config: { sync: { convexAdminKey: 'server-secret' } },
    session: {} as any,
    allowed: true,
    file: null as any,
    query: vi.fn(),
    fetch: vi.fn(),
}));
vi.mock('~~/server/auth/token-broker/resolve', () => ({ resolveProviderToken: async () => null }));
vi.mock('#imports', () => ({ useRuntimeConfig: () => f.config }));
vi.mock('~~/server/auth/session', () => ({ resolveSessionContext: async () => f.session }));
vi.mock('~~/server/utils/storage/workspace-access', () => ({
    requireStorageWorkspaceAccess: async () => {
        if (!f.allowed) throw createError({ statusCode: 403, statusMessage: 'Forbidden' });
    },
}));
vi.mock('../../utils/convex-gateway', () => ({
    getConvexAdminGatewayClient: () => ({ query: f.query }),
    getConvexGatewayClient: () => ({ query: f.query }),
    buildGatewayAdminIdentity: (provider: string, subject: string) => ({ provider, subject }),
}));

const HASH = 'sha256:' + 'a'.repeat(64);
const app = createApp()
    .use('/presign', eventHandler(event => new ConvexStorageGatewayAdapter().presignDownload(event, {
        workspaceId: 'ws-1', hash: String(getQuery(event).hash || HASH), storageId: 'blob-1',
        mimeType: 'image/png', filename: 'untrusted.png', disposition: 'inline', expiresInMs: 250,
    })))
    .use('/api/storage/convex/download', download);
const request = toWebHandler(app);
async function issue(hash = HASH) {
    const response = await request(new Request('http://or3.test/presign?hash=' + encodeURIComponent(hash)));
    expect(response.status).toBe(200);
    return response.json() as Promise<{ url: string; expiresAt: number }>;
}
async function get(url: string) {
    return request(new Request(new URL(url, 'http://or3.test')));
}

describe('Convex HTTP download delivery', () => {
    beforeEach(() => {
        f.config.sync.convexAdminKey = 'server-secret';
        f.session = { authenticated: true, user: { id: 'user-1' }, provider: 'basic-auth', providerUserId: 'subject-1' };
        f.allowed = true;
        f.file = { url: 'https://native.test/permanent-blob', storageId: 'blob-1', mimeType: 'text/html', name: '../unsafe\r\n.html', fileKind: 'file' };
        f.query.mockReset().mockImplementation(async () => f.file);
        f.fetch.mockReset().mockImplementation(async () => new Response('<h1>stored bytes</h1>', {
            headers: { 'content-type': 'text/html', 'content-disposition': 'inline', 'location': 'https://native.test/permanent-blob' },
        }));
        vi.stubGlobal('fetch', f.fetch);
    });
    afterEach(() => { vi.unstubAllGlobals(); vi.restoreAllMocks(); });

    it.each([HASH, 'md5:' + 'b'.repeat(32)])('streams %s with current canonical inert headers instead of native headers or caller metadata', async hash => {
        const signed = await issue(hash);
        const response = await get(signed.url);
        expect(response.status).toBe(200);
        expect(await response.text()).toBe('<h1>stored bytes</h1>');
        expect(response.headers.get('content-type')).toBe('application/octet-stream');
        expect(response.headers.get('content-disposition')).toBe("attachment; filename*=UTF-8''.._unsafe__.html");
        expect(response.headers.get('x-content-type-options')).toBe('nosniff');
        expect(response.headers.get('cache-control')).toBe('private, no-store');
        expect(response.headers.get('location')).toBeNull();
        expect(f.fetch).toHaveBeenCalledWith('https://native.test/permanent-blob', expect.objectContaining({ redirect: 'error' }));
        expect(f.query).toHaveBeenLastCalledWith(expect.anything(), { workspace_id: 'ws-1', hash });
    });

    it('uses current safe raster policy and filename when canonical metadata changes before retrieval', async () => {
        const signed = await issue();
        f.file = { ...f.file, mimeType: 'image/png', name: 'current.png', fileKind: 'image' };
        const response = await get(signed.url);
        expect(response.status).toBe(200);
        expect(response.headers.get('content-type')).toBe('image/png');
        expect(response.headers.get('content-disposition')).toBe("inline; filename*=UTF-8''current.png");
    });

    it.each(['missing', 'tampered', 'expired', 'other-user', 'unauthenticated', 'revoked', 'deleted', 'replaced'])(
        'denies %s download authority before requesting bytes', async failure => {
            let { url, expiresAt } = await issue();
            if (failure === 'missing') url = '/api/storage/convex/download';
            if (failure === 'tampered') url += 'x';
            if (failure === 'expired') vi.spyOn(Date, 'now').mockReturnValue(expiresAt);
            if (failure === 'other-user') f.session = { ...f.session, user: { id: 'user-2' } };
            if (failure === 'unauthenticated') f.session = { authenticated: false };
            if (failure === 'revoked') f.allowed = false;
            if (failure === 'deleted') f.file = null;
            if (failure === 'replaced') f.file = { ...f.file, storageId: 'blob-replacement' };
            const response = await get(url);
            expect(response.status).toBe(failure === 'missing' ? 400 : failure === 'unauthenticated' ? 401 : ['deleted', 'replaced'].includes(failure) ? 404 : 403);
            expect(f.fetch).not.toHaveBeenCalled();
        },
    );

    it.each(['revoked', 'replaced', 'expired'])('discards fetched bytes when %s during upstream work', async failure => {
        const signed = await issue();
        f.fetch.mockImplementation(async () => {
            if (failure === 'revoked') f.allowed = false;
            else if (failure === 'replaced') f.file = { ...f.file, storageId: 'blob-replacement' };
            else vi.spyOn(Date, 'now').mockReturnValue(signed.expiresAt);
            return new Response('must never be delivered');
        });
        const response = await get(signed.url);
        expect(response.status).toBe(failure === 'replaced' ? 404 : 403);
        expect(await response.text()).not.toContain('must never be delivered');
    });

    it('rejects upstream redirects and missing blobs without exposing their target', async () => {
        const signed = await issue();
        f.fetch.mockResolvedValueOnce(new Response(null, { status: 302, headers: { location: 'https://native.test/leak' } }));
        const redirect = await get(signed.url);
        expect(redirect.status).toBe(502);
        expect(redirect.headers.get('location')).toBeNull();
        f.fetch.mockResolvedValueOnce(new Response('gone', { status: 404 }));
        expect((await get(signed.url)).status).toBe(404);
    });

    it('requires server-only credentials and bounds requested TTL', async () => {
        const adapter = new ConvexStorageGatewayAdapter();
        const event = { context: {}, node: { req: { headers: {} } } } as any;
        const input = { workspaceId: 'ws-1', hash: HASH, storageId: 'blob-1' };
        const signed = await adapter.presignDownload(event, { ...input, expiresInMs: 86_400_000 });
        expect(signed.expiresAt).toBeLessThanOrEqual(Date.now() + 15 * 60_000);
        await expect(adapter.presignDownload(event, { ...input, expiresInMs: Number.NaN })).rejects.toMatchObject({ statusCode: 400 });
        f.config.sync.convexAdminKey = '';
        await expect(adapter.presignDownload(event, input)).rejects.toMatchObject({ statusCode: 503 });
    });
});
