import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

vi.mock('../../../templates/convex/_generated/server', () => ({
  mutation: (definition: any) => ({ ...definition, _handler: definition.handler }),
  query: (definition: any) => ({ ...definition, _handler: definition.handler }),
}));

import * as storageFunctions from '../../../templates/convex/storage';
import { verifyStorageReferenceContract } from '~~/shared/testing/contracts/storage';

type Handler = (ctx: any, args: any) => Promise<any>;
const handler = (fn: unknown): Handler => (fn as { _handler: Handler })._handler;

function fixture() {
  const tables: Record<string, any[]> = {
    auth_accounts: [{ _id: 'account-1', provider: 'clerk', provider_user_id: 'subject-1', user_id: 'user-1' }],
    workspace_members: [{ _id: 'member-1', workspace_id: 'ws-1', user_id: 'user-1', role: 'editor' }],
    file_meta: [],
    upload_intents: [],
    messages: [],
    posts: [],
  };
  const objects = new Map<string, any>();
  const deletedObjects: string[] = [];
  let nextId = 1;
  const ctx = {
    auth: { getUserIdentity: async () => ({ issuer: 'https://clerk.test', subject: 'subject-1' }) },
    storage: {
      generateUploadUrl: async () => 'https://upload.test',
      delete: async (id: string) => { deletedObjects.push(id); },
    },
    db: {
      system: { get: async (id: string) => objects.get(id) ?? null },
      query(table: string) {
        const builder: any = {
          withIndex: () => builder,
          filter: () => builder,
          first: async () => tables[table]?.[0] ?? null,
          collect: async () => [...(tables[table] ?? [])],
          take: async (limit: number) => (tables[table] ?? []).slice(0, limit),
        };
        return builder;
      },
      get: async (id: string) => Object.values(tables).flat().find((row: any) => row._id === id) ?? null,
      insert: async (table: string, value: any) => {
        const id = `${table}-${nextId++}`;
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
    await expect(commit(f.ctx, input)).rejects.toThrow('already consumed');
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
