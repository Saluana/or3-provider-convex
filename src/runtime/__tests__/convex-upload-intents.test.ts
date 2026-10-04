import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

vi.mock('../../../templates/convex/_generated/server', () => ({
  mutation: (definition: any) => ({ ...definition, _handler: definition.handler }),
  query: (definition: any) => ({ ...definition, _handler: definition.handler }),
  internalMutation: (definition: any) => ({ ...definition, _handler: definition.handler }),
}));

import * as syncFunctions from '../../../templates/convex/sync';
import * as storageFunctions from '../../../templates/convex/storage';
import { verifyStorageReferenceContract } from '~~/shared/testing/contracts/storage';

type Handler = (ctx: any, args: any) => Promise<any>;
const handler = (fn: unknown): Handler => (fn as { _handler: Handler })._handler;

function fixture() {
  const tables: Record<string, any[]> = {
    auth_accounts: [{ _id: 'account-1', provider: 'clerk', provider_user_id: 'subject-1', user_id: 'user-1' }],
    workspaces: [{ _id: 'ws-1', name: 'Workspace', owner_user_id: 'user-1', created_at: 1, deleted: false }],
    workspace_members: [{ _id: 'member-1', workspace_id: 'ws-1', user_id: 'user-1', role: 'editor' }],
    file_meta: [],
    upload_intents: [],
    storage_deletion_claims: [],
    messages: [],
    posts: [],
    change_log: [],
    server_version_counter: [],
    tombstones: [],
  };
  const objects = new Map<string, any>();
  const deletedObjects: string[] = [];
  let nextId = 1;
  const ctx = {
    auth: { getUserIdentity: async () => ({ issuer: 'https://clerk.test', subject: 'subject-1' }) },
    storage: {
      generateUploadUrl: async () => 'https://upload.test',
      delete: async (id: string) => { deletedObjects.push(id); objects.delete(id); },
      getUrl: async (id: string) => objects.has(id) ? `https://download.test/${id}` : null,
    },
    db: {
      system: { get: async (id: string) => objects.get(id) ?? null },
      query(table: string) {
        let rows = [...(tables[table] ?? [])];
        const builder: any = {
          withIndex: (_name: string, constrain: (query: any) => unknown) => {
            const constraints: Array<[string, unknown]> = [];
            const query = {
              eq(field: string, value: unknown) {
                constraints.push([field, value]);
                return query;
              },
            };
            constrain(query);
            rows = rows.filter((row) =>
              constraints.every(([field, value]) => row[field] === value)
            );
            return builder;
          },
          filter: () => builder,
          first: async () => rows[0] ?? null,
          collect: async () => [...rows],
          take: async (limit: number) => rows.slice(0, limit),
        };
        return builder;
      },
      get: async (id: string) => Object.values(tables).flat().find((row: any) => row._id === id) ?? null,
      insert: async (table: string, value: any) => {
        const id = `${table}-${nextId++}`;
        if (!tables[table]) tables[table] = [];
        tables[table].push({ _id: id, _creationTime: nextId, ...value });
        return id;
      },
      patch: async (id: string, value: any) => {
        const row = Object.values(tables).flat().find((candidate: any) => candidate._id === id);
        if (!row) throw new Error(`missing row ${id}`);
        Object.assign(row, value);
      },
      delete: async (id: string) => {
        for (const rows of Object.values(tables)) {
          const index = rows.findIndex((row: any) => row._id === id);
          if (index >= 0) rows.splice(index, 1);
        }
      },
    },
  };
  return { ctx, tables, objects, deletedObjects };
}

const HASH = 'a'.repeat(64);
const hashBase64 = Buffer.from(HASH, 'hex').toString('base64');

describe('Convex persisted upload intents', () => {
  beforeEach(() => vi.useFakeTimers().setSystemTime(new Date('2026-01-01T00:00:00Z')));
  afterEach(() => vi.useRealTimers());

  it('only returns URLs for live committed metadata', async () => {
    const f = fixture();
    const getFileUrl = handler(storageFunctions.getFileUrl);
    f.tables.file_meta.push({
      _id: 'file-deleted', workspace_id: 'ws-1', hash: `sha256:${HASH}`,
      deleted: true, storage_id: 'blob-deleted',
    });
    f.objects.set('blob-deleted', { size: 1 });

    await expect(getFileUrl(f.ctx, { workspace_id: 'ws-1', hash: `sha256:${HASH}` }))
      .resolves.toBeNull();
  });

  it('keeps valid prefixed and legacy bare SHA-256 metadata readable', async () => {
    const prefixed = fixture();
    const getFileUrl = handler(storageFunctions.getFileUrl);
    prefixed.tables.file_meta.push({
      _id: 'file-live', workspace_id: 'ws-1', hash: `sha256:${HASH}`,
      deleted: false, storage_id: 'blob-live',
    });
    prefixed.objects.set('blob-live', { size: 1 });
    await expect(getFileUrl(prefixed.ctx, { workspace_id: 'ws-1', hash: HASH }))
      .resolves.toEqual({ url: 'https://download.test/blob-live' });

    const bare = fixture();
    bare.tables.file_meta.push({
      _id: 'file-bare', workspace_id: 'ws-1', hash: HASH,
      deleted: false, storage_id: 'blob-bare',
    });
    bare.objects.set('blob-bare', { size: 1 });
    await expect(getFileUrl(bare.ctx, { workspace_id: 'ws-1', hash: `sha256:${HASH}` }))
      .resolves.toEqual({ url: 'https://download.test/blob-bare' });
  });

  it('denies storage access for a soft-deleted workspace', async () => {
    const f = fixture();
    f.tables.workspaces[0].deleted = true;
    const generate = handler(storageFunctions.generateUploadUrl);
    const getFileUrl = handler(storageFunctions.getFileUrl);
    const gc = handler(storageFunctions.gcDeletedFiles);

    await expect(generate(f.ctx, {
      workspace_id: 'ws-1', hash: HASH, mime_type: 'image/png', size_bytes: 10,
    })).rejects.toThrow('Forbidden');
    await expect(getFileUrl(f.ctx, {
      workspace_id: 'ws-1', hash: `sha256:${HASH}`,
    })).rejects.toThrow('Forbidden');
    await expect(gc(f.ctx, {
      workspace_id: 'ws-1', retention_seconds: 0, limit: 1,
    })).rejects.toThrow('Forbidden');
  });

  it('executes the shared canonical reference contract', async () => {
    const f = fixture();
    const gc = handler(storageFunctions.gcDeletedFiles);
    const logicalToHash = { live: HASH, orphan: 'b'.repeat(64) } as const;
    await verifyStorageReferenceContract({
      name: 'convex',
      async put(logical) {
        f.tables.file_meta.push({
          _id: `file-${logical}`, workspace_id: 'ws-1', hash: logicalToHash[logical as keyof typeof logicalToHash],
          deleted: true, deleted_at: Math.floor(Date.now() / 1000) - 10_000,
          storage_id: `blob-${logical}`,
        });
      },
      async reference(logical) {
        f.tables.messages.push({
          _id: `message-${logical}`, workspace_id: 'ws-1', deleted: false,
          file_hashes: JSON.stringify([`sha256:${logicalToHash[logical as keyof typeof logicalToHash]}`]),
        });
      },
      async collect() {
        await gc(f.ctx, { workspace_id: 'ws-1', retention_seconds: 3600, limit: 10 });
        return f.deletedObjects.map((id) => id.replace('blob-', ''));
      },
    });
  });

  it.each(['convex', ''])('keeps deletion claims and rejects stale native IDs after re-upload with provider %j', async (storageProviderId) => {
    const f = fixture();
    const hash = 'sha256:' + HASH;
    f.tables.file_meta.push({ _id: 'removed-original', workspace_id: 'ws-1', hash, name: 'Original', kind: 'file',
      mime_type: 'text/plain', size_bytes: 1, ref_count: 0, clock: 1, deleted: true, storage_provider_id: storageProviderId, storage_id: 'old-blob' });
    f.objects.set('old-blob', { size: 1, sha256: hashBase64, contentType: 'text/plain' });
    await handler(storageFunctions.deleteObject)(f.ctx, { workspace_id: 'ws-1', hash });
    expect(f.tables.storage_deletion_claims).toContainEqual(expect.objectContaining({ hash: HASH, workspace_id: 'ws-1' }));
    const push = handler(syncFunctions.push);
    const operation = (table_name: string, pk: string, payload: any) => ({ op_id: crypto.randomUUID(), table_name, pk, payload,
      operation: 'put', clock: 2_000_000_000, hlc: '2000000000:0:stale', device_id: 'stale-device' });
    const reference = () => operation('posts', 'stale-document', { id: 'stale-document', post_type: 'doc', title: 'Stale reference',
      content: '{}', file_hashes: JSON.stringify([hash]), deleted: false });
    const metadata = () => operation('file_meta', hash, { hash, name: 'Stale original', mime_type: 'text/plain', kind: 'file', size_bytes: 1,
      storage_id: 'old-blob', storage_provider_id: storageProviderId, deleted: false });
    for (const op of [reference(), metadata()]) {
      const receipt = await push(f.ctx, { workspace_id: 'ws-1', workspace_item_capability: 'v1', ops: [op] });
      expect(receipt.results[0]).toMatchObject({ success: false });
      expect(f.tables.posts).toHaveLength(0);
      expect(f.tables.file_meta).toHaveLength(0);
    }
    const { intentId } = await handler(storageFunctions.generateUploadUrl)(f.ctx, { workspace_id: 'ws-1', hash,
      mime_type: 'text/plain', size_bytes: 1, workspace_quota_bytes: 100 });
    f.objects.set('new-blob', { size: 1, sha256: hashBase64, contentType: 'text/plain' });
    await handler(storageFunctions.commitUpload)(f.ctx, { workspace_id: 'ws-1', hash, intent_id: intentId,
      storage_id: 'new-blob', storage_provider_id: storageProviderId, name: 'Original', mime_type: 'text/plain', size_bytes: 1, kind: 'file' });
    expect(f.tables.storage_deletion_claims).toHaveLength(0);
    expect((await push(f.ctx, { workspace_id: 'ws-1', workspace_item_capability: 'v1', ops: [reference()] })).results[0]).toMatchObject({ success: true });
    expect((await push(f.ctx, { workspace_id: 'ws-1', workspace_item_capability: 'v1', ops: [metadata()] })).results[0]).toMatchObject({ success: false });
    expect(f.tables.file_meta[0].storage_id).toBe('new-blob');
  });

  it('refuses deletion of live metadata even when it has no reference edges', async () => {
    const f = fixture();
    f.tables.file_meta.push({ _id: 'live-original', workspace_id: 'ws-1', hash: HASH, deleted: false, storage_id: 'live-blob' });
    await expect(handler(storageFunctions.deleteObject)(f.ctx, { workspace_id: 'ws-1', hash: HASH })).rejects.toThrow(/retained|live/i);
    expect(f.deletedObjects).toEqual([]);
  });

  it('deletes an unreferenced object idempotently and removes its metadata', async () => {
    const f = fixture();
    f.tables.file_meta.push({
      _id: 'file-delete', workspace_id: 'ws-1', hash: HASH,
      deleted: true, deleted_at: Math.floor(Date.now() / 1000) - 10_000,
      storage_id: 'blob-delete',
    });
    const remove = handler(storageFunctions.deleteObject);

    await expect(remove(f.ctx, {
      workspace_id: 'ws-1', hash: HASH, storage_id: 'blob-delete',
    })).resolves.toEqual({ deleted: true });
    await expect(remove(f.ctx, {
      workspace_id: 'ws-1', hash: HASH, storage_id: 'blob-delete',
    })).resolves.toEqual({ deleted: false });

    expect(f.deletedObjects).toEqual(['blob-delete']);
    expect(f.tables.file_meta).toEqual([]);
    expect(f.tables.change_log).toEqual([
      expect.objectContaining({
        table_name: 'file_meta',
        pk: HASH,
        op: 'delete',
        op_id: expect.stringMatching(
          /^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i
        ),
      }),
    ]);
  });

  it('rejects mismatched storage ids and canonical live references', async () => {
    const f = fixture();
    f.tables.file_meta.push({
      _id: 'file-live', workspace_id: 'ws-1', hash: HASH,
      storage_id: 'blob-live',
    });
    const remove = handler(storageFunctions.deleteObject);

    await expect(remove(f.ctx, {
      workspace_id: 'ws-1', hash: HASH, storage_id: 'other-blob',
    })).rejects.toThrow('storage_id does not match');

    f.tables.messages.push({
      _id: 'message-live', workspace_id: 'ws-1', deleted: false,
      file_hashes: JSON.stringify([`sha256:${HASH}`]),
    });
    await expect(remove(f.ctx, {
      workspace_id: 'ws-1', hash: HASH, storage_id: 'blob-live',
    })).rejects.toThrow('referenced file');
    expect(f.deletedObjects).toEqual([]);
    expect(f.tables.file_meta).toHaveLength(1);
  });

  it('atomically reserves quota and ignores expired reservations', async () => {
    const f = fixture();
    const generate = handler(storageFunctions.generateUploadUrl);
    const first = await generate(f.ctx, {
      workspace_id: 'ws-1', hash: HASH, mime_type: 'image/png', size_bytes: 60,
      workspace_quota_bytes: 100,
    });
    expect(first.intentId).toBeTruthy();
    await expect(generate(f.ctx, {
      workspace_id: 'ws-1', hash: 'b'.repeat(64), mime_type: 'image/png', size_bytes: 50,
      workspace_quota_bytes: 100,
    })).rejects.toThrow('quota exceeded');

    f.tables.upload_intents[0].expires_at = Math.floor(Date.now() / 1000) - 1;
    await expect(generate(f.ctx, {
      workspace_id: 'ws-1', hash: 'b'.repeat(64), mime_type: 'image/png', size_bytes: 50,
      workspace_quota_bytes: 100,
    })).resolves.toMatchObject({ uploadUrl: 'https://upload.test' });
  });

  it('accepts zero-byte generic file intents', async () => {
    const f = fixture();
    const generate = handler(storageFunctions.generateUploadUrl);
    const result = await generate(f.ctx, {
      workspace_id: 'ws-1', hash: HASH, mime_type: 'application/octet-stream', size_bytes: 0,
      workspace_quota_bytes: 1,
    });

    expect(result).toMatchObject({ uploadUrl: 'https://upload.test' });
    expect(f.tables.upload_intents[0]).toMatchObject({
      hash: HASH,
      mime_type: 'application/octet-stream',
      size_bytes: 0,
      reserved_bytes: 0,
      status: 'active',
    });
  });

  it('binds commit to subject, workspace, object bytes and consumes exactly once', async () => {
    const f = fixture();
    const generate = handler(storageFunctions.generateUploadUrl);
    const commit = handler(storageFunctions.commitUpload);
    const { intentId } = await generate(f.ctx, {
      workspace_id: 'ws-1', hash: HASH, mime_type: 'image/png', size_bytes: 10,
    });
    f.objects.set('storage-good', { size: 10, contentType: 'image/png', sha256: hashBase64 });
    f.objects.set('storage-bad', { size: 11, contentType: 'image/png', sha256: hashBase64 });
    const input = {
      workspace_id: 'ws-1', intent_id: intentId, hash: `sha256:${HASH}`,
      storage_id: 'storage-good', storage_provider_id: 'convex', mime_type: 'image/png',
      size_bytes: 10, name: 'a.png', kind: 'image',
    };

    await expect(commit(f.ctx, { ...input, storage_id: 'storage-bad' }))
      .rejects.toThrow('metadata does not match intent');
    await expect(commit(f.ctx, { ...input, workspace_id: 'ws-2' }))
      .rejects.toThrow();
    await expect(commit(f.ctx, input)).resolves.toBeUndefined();
    expect(f.tables.upload_intents[0]).toMatchObject({ status: 'consumed', storage_id: 'storage-good' });
    expect(f.tables.change_log).toEqual([
      expect.objectContaining({
        table_name: 'file_meta',
        pk: `sha256:${HASH}`,
        op: 'put',
        op_id: expect.stringMatching(
          /^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i
        ),
      }),
    ]);
    await expect(commit(f.ctx, input)).rejects.toThrow('already consumed');
  });

  it('commits zero-byte generic files with the file kind', async () => {
    const f = fixture();
    const generate = handler(storageFunctions.generateUploadUrl);
    const commit = handler(storageFunctions.commitUpload);
    const { intentId } = await generate(f.ctx, {
      workspace_id: 'ws-1', hash: HASH, mime_type: 'application/octet-stream', size_bytes: 0,
    });
    f.objects.set('storage-empty', {
      size: 0,
      contentType: 'application/octet-stream',
      sha256: hashBase64,
    });

    await expect(commit(f.ctx, {
      workspace_id: 'ws-1', intent_id: intentId, hash: `sha256:${HASH}`,
      storage_id: 'storage-empty', storage_provider_id: 'convex',
      mime_type: 'application/octet-stream', size_bytes: 0,
      name: 'empty.bin', kind: 'file',
    })).resolves.toBeUndefined();
    expect(f.tables.file_meta[0]).toMatchObject({
      kind: 'file',
      mime_type: 'application/octet-stream',
      size_bytes: 0,
    });
  });

  it('rejects expiry and cancellation before attaching an object', async () => {
    const f = fixture();
    const generate = handler(storageFunctions.generateUploadUrl);
    const cancel = handler(storageFunctions.cancelUploadIntent);
    const commit = handler(storageFunctions.commitUpload);
    const first = await generate(f.ctx, {
      workspace_id: 'ws-1', hash: HASH, mime_type: 'image/png', size_bytes: 10,
    });
    await cancel(f.ctx, { workspace_id: 'ws-1', intent_id: first.intentId });
    f.objects.set('storage-good', { size: 10, contentType: 'image/png', sha256: hashBase64 });
    await expect(commit(f.ctx, {
      workspace_id: 'ws-1', intent_id: first.intentId, hash: HASH, storage_id: 'storage-good',
      storage_provider_id: 'convex', mime_type: 'image/png', size_bytes: 10,
      name: 'a.png', kind: 'image',
    })).rejects.toThrow('consumed or cancelled');

    const second = await generate(f.ctx, {
      workspace_id: 'ws-1', hash: 'b'.repeat(64), mime_type: 'image/png', size_bytes: 10,
    });
    f.tables.upload_intents.find((row: any) => row._id === second.intentId).expires_at = 0;
    await expect(commit(f.ctx, {
      workspace_id: 'ws-1', intent_id: second.intentId, hash: 'b'.repeat(64), storage_id: 'storage-good',
      storage_provider_id: 'convex', mime_type: 'image/png', size_bytes: 10,
      name: 'b.png', kind: 'image',
    })).rejects.toThrow('expired');
  });

  it('GC ignores ref_count authority and preserves canonical references', async () => {
    const f = fixture();
    const gc = handler(storageFunctions.gcDeletedFiles);
    const deletedAt = Math.floor(Date.now() / 1000) - 10_000;
    f.tables.file_meta.push(
      {
        _id: 'file-live', workspace_id: 'ws-1', hash: HASH,
        deleted: true, deleted_at: deletedAt, ref_count: 0, storage_id: 'blob-live',
      },
      {
        _id: 'file-orphan', workspace_id: 'ws-1', hash: 'b'.repeat(64),
        deleted: true, deleted_at: deletedAt, ref_count: 999, storage_id: 'blob-orphan',
      },
    );
    f.tables.messages.push({
      _id: 'message-1', workspace_id: 'ws-1', deleted: false,
      file_hashes: JSON.stringify([`sha256:${HASH}`]),
    });

    await expect(gc(f.ctx, {
      workspace_id: 'ws-1', retention_seconds: 3600, limit: 2,
    })).resolves.toEqual({ deletedCount: 1, scannedCount: 2 });
    expect(f.tables.file_meta.map((row: any) => row._id)).toEqual(['file-live']);
    expect(f.deletedObjects).toEqual(['blob-orphan']);
  });

  it('bounds Convex GC candidate reads before applying the delete limit', async () => {
    const f = fixture();
    const gc = handler(storageFunctions.gcDeletedFiles);
    const deletedAt = Math.floor(Date.now() / 1000) - 10_000;
    for (let index = 0; index < 600; index += 1) {
      f.tables.file_meta.push({
        _id: `file-${index}`,
        workspace_id: 'ws-1',
        hash: index.toString(16).padStart(64, '0'),
        deleted: true,
        deleted_at: deletedAt,
        ref_count: 1,
      });
    }

    await expect(gc(f.ctx, {
      workspace_id: 'ws-1', retention_seconds: 3600, limit: 2,
    })).resolves.toEqual({ deletedCount: 2, scannedCount: 8 });
    expect(f.tables.file_meta).toHaveLength(598);
  });

  it('fails closed when canonical reference rows exceed the bounded page', async () => {
    const f = fixture();
    const gc = handler(storageFunctions.gcDeletedFiles);
    f.tables.file_meta.push({
      _id: 'file-candidate', workspace_id: 'ws-1', hash: HASH,
      deleted: true, deleted_at: Math.floor(Date.now() / 1000) - 10_000,
      ref_count: 0, storage_id: 'blob-candidate',
    });
    for (let index = 0; index < 501; index += 1) {
      f.tables.messages.push({
        _id: `message-${index}`, workspace_id: 'ws-1', deleted: false, file_hashes: null,
      });
    }

    await expect(gc(f.ctx, {
      workspace_id: 'ws-1', retention_seconds: 3600, limit: 1,
    })).resolves.toEqual({ deletedCount: 0, scannedCount: 1 });
    expect(f.tables.file_meta).toHaveLength(1);
    expect(f.deletedObjects).toEqual([]);
  });
});
