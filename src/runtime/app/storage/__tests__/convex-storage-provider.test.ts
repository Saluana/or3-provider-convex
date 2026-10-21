/**
 * The registered Convex client must speak the host's current SSR protocol:
 * generic file capability, protected mutations, and scoped deletion. Exercise
 * its real serialized requests through H3 and the host guards. Backend bytes,
 * persistence, auth and quotas have their own stronger provider tests.
 */
import { afterEach, describe, expect, it, vi } from 'vitest';
import { createApp, eventHandler, readBody, toWebHandler } from 'h3';
import { requireFileKindCapability } from '~~/server/utils/storage/file-kind-capability';
import { createConvexStorageProvider } from '../convex-storage-provider';

vi.mock('#imports', () => ({ useRuntimeConfig: () => ({ security: { proxy: {}, allowedOrigins: [] } }) }));
// Load the real host guard at runtime, like the backend-template fixtures. Its
// Nuxt application's generated config types are outside this package's shim.
const { requireCloudMutation } = await import(
    /* @vite-ignore */ new URL('../../../../../../or3-chat/server/utils/security/cloud-mutation.ts', import.meta.url).href
);
const hash = 'sha256:' + 'a'.repeat(64);

describe('Convex browser storage protocol', () => {
    afterEach(() => vi.unstubAllGlobals());

    function transport() {
        const delivered: Array<{ path: string; body: Record<string, unknown> }> = [];
        const request = toWebHandler(createApp().use(eventHandler(async event => {
            const body = await readBody(event);
            // Both are real host guards; check the generic protocol independently
            // of the mutation header so a missing marker fails as HTTP 426.
            if (event.path !== '/api/storage/delete') requireFileKindCapability(body.file_kind_capability);
            requireCloudMutation(event);
            delivered.push({ path: event.path, body });
            return event.path.includes('presign')
                ? { url: '/signed-by-server', expiresAt: 123, intentId: 'intent-1' }
                : { ok: true };
        })));
        vi.stubGlobal('fetch', async (path: string, init: RequestInit) => {
            const outgoing = new Request(new URL(path, 'http://or3.test'), init);
            // Browser-controlled same-origin headers accompany the client request.
            outgoing.headers.set('origin', 'http://or3.test');
            outgoing.headers.set('host', 'or3.test');
            return request(outgoing);
        });
        return delivered;
    }

    it.each(['upload', 'download', 'commit'] as const)('delivers a generic %s using the current host protocol', async operation => {
        const delivered = transport();
        const provider = createConvexStorageProvider();
        if (operation === 'upload') {
            const result = await provider.getPresignedUploadUrl({ workspaceId: 'ws-1', hash, mimeType: 'text/plain', sizeBytes: 12 });
            expect(result.intentId).toBe('intent-1');
        } else if (operation === 'download') {
            const result = await provider.getPresignedDownloadUrl({ workspaceId: 'ws-1', hash, storageId: 'blob-1' });
            expect(result.url).toBe('/signed-by-server');
        } else {
            await provider.commitUpload!({ workspaceId: 'ws-1', hash, storageId: 'blob-1', intentId: 'intent-1',
                meta: { name: 'proof.txt', mimeType: 'text/plain', sizeBytes: 12, kind: 'file' } });
            expect(delivered[0]?.body.storage_provider_id).toBe('convex');
            expect(delivered[0]?.body.intent_id).toBe('intent-1');
        }
        expect(delivered).toHaveLength(1);
        expect(delivered[0]?.body.workspace_id).toBe('ws-1');
        expect(delivered[0]?.body.hash).toBe(hash);
    });

    it('delivers scoped deletion through the protected gateway and reports gateway mode', async () => {
        const delivered = transport();
        const provider = createConvexStorageProvider();
        await provider.deleteObject!({ workspaceId: 'ws-1', hash, storageId: 'blob-1' });
        expect(delivered).toEqual([{ path: '/api/storage/delete', body: { workspace_id: 'ws-1', hash, storage_id: 'blob-1' } }]);
        expect(provider.mode).toBe('gateway');
    });
});
