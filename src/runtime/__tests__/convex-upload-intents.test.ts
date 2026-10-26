import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

vi.mock('../../../templates/convex/_generated/server', () => ({
  mutation: (definition: any) => ({ ...definition, _handler: definition.handler }),
  query: (definition: any) => ({ ...definition, _handler: definition.handler }),
  internalMutation: (definition: any) => ({ ...definition, _handler: definition.handler }),
}));

import * as syncFunctions from '../../../templates/convex/sync';
import * as backgroundFunctions from '../../../templates/convex/backgroundJobs';
import * as storageFunctions from '../../../templates/convex/storage';

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
            const constraints: Array<(row: any) => boolean> = [];
            const query = {
              gt(field: string, value: number) { constraints.push(row => row[field] > value); return query; },
              eq(field: string, value: unknown) {
                constraints.push(row => row[field] === value);
                return query;
              },
            };
            constrain(query);
            rows = rows.filter((row) =>
              constraints.every(matches => matches(row))
            );
            return builder;
          },
          filter: (build: (q: any) => (row: any) => boolean) => {
            const predicate = build({ field: (key: string) => (row: any) => row[key],
              neq: (field: (row: any) => unknown, value: unknown) => (row: any) => field(row) !== value });
            rows = rows.filter(predicate); return builder;
          },
          first: async () => rows[0] ?? null,
          collect: async () => [...rows],
          paginate: async ({ cursor, numItems }: any) => {
            const offset = cursor ? Number(cursor) : 0;
            return { page: rows.slice(offset, offset + numItems), isDone: offset + numItems >= rows.length, continueCursor: String(offset + numItems) };
          },
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

// Collection scenarios start with originals committed by the production
// upload handlers, including their ownership receipt, rather than treating
// client-authored metadata as proof that bytes belong to the workspace.
async function commitFixtureOriginal(f: ReturnType<typeof fixture>, row: any) {
  const deleted = row.deleted; const deletedAt = row.deleted_at;
  row.size_bytes ??= 1; row.mime_type ??= 'text/plain'; row.name ??= 'Original';
  row.kind ??= 'file'; row.ref_count ??= 0;
  const digest = row.hash.replace(/^sha256:/i, '').toLowerCase();
  f.objects.set(row.storage_id, { size: row.size_bytes, contentType: row.mime_type,
    sha256: Buffer.from(digest, 'hex').toString('base64') });
  const input = { workspace_id: row.workspace_id, hash: row.hash, mime_type: row.mime_type, size_bytes: row.size_bytes };
  const grant = await handler(storageFunctions.generateUploadUrl)(f.ctx, input);
  await handler(storageFunctions.commitUpload)(f.ctx, { ...input, intent_id: grant.intentId,
    storage_id: row.storage_id, storage_provider_id: row.storage_provider_id ?? 'convex', name: row.name, kind: row.kind });
  await f.ctx.db.patch(row._id, { deleted, ...(deletedAt !== undefined ? { deleted_at: deletedAt } : {}) });
}

describe('Convex persisted upload intents', () => {
  beforeEach(() => vi.useFakeTimers().setSystemTime(new Date('2026-01-01T00:00:00Z')));
  afterEach(() => vi.useRealTimers());

  // Exercise receipt creation at its real owner, then attack through each
  // metadata/commit/deletion boundary. The storage stub only records effects.
  it('binds native objects to their committed workspace across sync, commit, delete and GC', async () => {
    const f = fixture();
    const hash = `sha256:${HASH}`;
    f.objects.set('owned-blob', { size: 3, sha256: hashBase64, contentType: 'text/plain' });
    const grant = await handler(storageFunctions.generateUploadUrl)(f.ctx, {
      workspace_id: 'ws-1', hash, mime_type: 'text/plain', size_bytes: 3,
    });
    const commit = { workspace_id: 'ws-1', hash, intent_id: grant.intentId, storage_id: 'owned-blob',
      storage_provider_id: 'convex', name: 'Original', mime_type: 'text/plain', size_bytes: 3, kind: 'file' };
    await handler(storageFunctions.commitUpload)(f.ctx, commit);
    await handler(storageFunctions.commitUpload)(f.ctx, commit);
    const original = f.tables.file_meta[0];
    f.tables.workspace_members[0].role = 'viewer';
    f.tables.workspaces.push({ _id: 'ws-2', deleted: false });
    f.tables.workspace_members.push({ _id: 'member-2', workspace_id: 'ws-2', user_id: 'user-1', role: 'owner' });
    const alias = { hash, name: 'Alias', mime_type: 'text/plain', kind: 'file', size_bytes: 3,
      storage_id: 'owned-blob', storage_provider_id: 'convex', deleted: true };
    const pushed = await handler(syncFunctions.push)(f.ctx, { workspace_id: 'ws-2', ops: [{
      op_id: crypto.randomUUID(), table_name: 'file_meta', pk: hash, operation: 'put', payload: alias,
      clock: 2_000_000_000, hlc: '2000000000:0:alias', device_id: 'alias-device',
    }] });
    expect(pushed.results[0]).toMatchObject({ success: false });
    const otherGrant = await handler(storageFunctions.generateUploadUrl)(f.ctx, {
      workspace_id: 'ws-2', hash, mime_type: 'text/plain', size_bytes: 3,
    });
    await expect(handler(storageFunctions.commitUpload)(f.ctx, { ...commit,
      workspace_id: 'ws-2', intent_id: otherGrant.intentId })).rejects.toThrow(/ownership|another workspace|committed/i);
    // Even poisoned metadata already persisted before an upgrade cannot grant deletion.
    f.tables.file_meta.push({ ...original, ...alias, _id: 'poisoned-alias', workspace_id: 'ws-2', deleted_at: 1 });
    await expect(handler(storageFunctions.deleteObject)(f.ctx, { workspace_id: 'ws-2', hash }))
      .rejects.toThrow(/ownership|another workspace|committed/i);
    await expect(handler(storageFunctions.gcDeletedFiles)(f.ctx, { workspace_id: 'ws-2', retention_seconds: 0 }))
      .resolves.toMatchObject({ deletedCount: 0 });
    expect(f.deletedObjects).toEqual([]);
    expect(f.objects.has('owned-blob')).toBe(true);
    f.tables.workspace_members[0].role = 'editor'; original.deleted = true;
    await expect(handler(storageFunctions.deleteObject)(f.ctx, { workspace_id: 'ws-1', hash }))
      .resolves.toEqual({ deleted: true });
    expect(f.deletedObjects).toEqual(['owned-blob']);
  });

  // Immutable system storage metadata, rather than client file_meta, owns proof.
  it.each([true, false])('attests only the actual native storage checksum (%s)', async valid => {
    const f = fixture();
    f.tables.file_meta.push({ _id: 'proof-file', workspace_id: 'ws-1', hash: `sha256:${HASH}`,
      deleted: false, storage_id: 'proof-blob', size_bytes: 3 });
    f.objects.set('proof-blob', { size: 3, sha256: valid ? hashBase64 : 'corrupt-checksum' });
    const result = await handler(storageFunctions.getFileUrl)(f.ctx, {
      workspace_id: 'ws-1', hash: `sha256:${HASH}`, verify_content: true,
    });
    expect(result.verifiedContent).toEqual(valid ? { hash: `sha256:${HASH}`, sizeBytes: 3 } : undefined);
  });

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
      .resolves.toMatchObject({ url: 'https://download.test/blob-live', storageId: 'blob-live' });

    const bare = fixture();
    bare.tables.file_meta.push({
      _id: 'file-bare', workspace_id: 'ws-1', hash: HASH,
      deleted: false, storage_id: 'blob-bare',
    });
    bare.objects.set('blob-bare', { size: 1 });
    await expect(getFileUrl(bare.ctx, { workspace_id: 'ws-1', hash: `sha256:${HASH}` }))
      .resolves.toMatchObject({ url: 'https://download.test/blob-bare', storageId: 'blob-bare' });
  });

  // Backup failures: Trash gets omitted, snapshot IDs select other objects,
  // uncommitted uploads become readable, revoked members retain access, or a
  // read mutates/deletes the retained original. Ordinary downloads stay denied.
  it.each(['ok', 'wrong-workspace', 'wrong-hash', 'replaced', 'missing-object', 'uncommitted', 'other-provider', 'revoked', 'deleted-workspace'])(
    'reads retained backup originals with exact current authority: %s', async failure => {
      const f = fixture();
      const file = { _id: 'trash', workspace_id: 'ws-1', hash: `sha256:${HASH}`,
        deleted: true, storage_id: 'blob-trash', storage_provider_id: 'convex' };
      f.tables.file_meta.push(file);
      f.objects.set('blob-trash', { size: 1 });
      const input = { workspace_id: 'ws-1', hash: file.hash, storage_id: file.storage_id };
      if (failure === 'wrong-workspace') file.workspace_id = 'ws-other';
      if (failure === 'wrong-hash') input.hash = 'sha256:' + 'b'.repeat(64);
      if (failure === 'replaced') file.storage_id = 'blob-new';
      if (failure === 'missing-object') f.objects.clear();
      if (failure === 'uncommitted') f.tables.file_meta.length = 0;
      if (failure === 'other-provider') file.storage_provider_id = 's3';
      if (failure === 'revoked') f.tables.workspace_members.length = 0;
      if (failure === 'deleted-workspace') f.tables.workspaces[0].deleted = true;
      const before = JSON.stringify(f.tables);
      const result = handler(storageFunctions.getBackupFileUrl)(f.ctx, input);
      if (['revoked', 'deleted-workspace'].includes(failure)) await expect(result).rejects.toThrow('Forbidden');
      else if (failure === 'ok') await expect(result).resolves.toMatchObject({ storageId: 'blob-trash', url: 'https://download.test/blob-trash' });
      else await expect(result).resolves.toBeNull();
      expect(JSON.stringify(f.tables)).toBe(before);
      expect(f.deletedObjects).toEqual([]);
    },
  );

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

  it('keeps referenced content while collecting an older orphan', async () => {
    const f = fixture();
    for (const [logical, hash] of [['live', HASH], ['orphan', 'b'.repeat(64)]]) {
      f.tables.file_meta.push({ _id: `file-${logical}`, workspace_id: 'ws-1', hash,
        deleted: true, deleted_at: Math.floor(Date.now() / 1000) - 10_000, storage_id: `blob-${logical}` });
    }
    for (const row of [...f.tables.file_meta]) await commitFixtureOriginal(f, row);
    f.tables.messages.push({ _id: 'message-live', workspace_id: 'ws-1', deleted: false,
      file_hashes: JSON.stringify([`sha256:${HASH}`]) });
    await handler(storageFunctions.gcDeletedFiles)(f.ctx, { workspace_id: 'ws-1', retention_seconds: 3600, limit: 10 });
    expect(f.deletedObjects).toEqual(['blob-orphan']);
  });

  it.each(['convex', ''])('keeps deletion claims and rejects stale native IDs after re-upload with provider %j', async (storageProviderId) => {
    const f = fixture();
    const hash = 'sha256:' + HASH;
    f.tables.file_meta.push({ _id: 'removed-original', workspace_id: 'ws-1', hash, name: 'Original', kind: 'file',
      mime_type: 'text/plain', size_bytes: 1, ref_count: 0, clock: 1, deleted: true, storage_provider_id: storageProviderId, storage_id: 'old-blob' });
    f.objects.set('old-blob', { size: 1, sha256: hashBase64, contentType: 'text/plain' });
    for (const row of [...f.tables.file_meta]) await commitFixtureOriginal(f, row);
    await handler(storageFunctions.deleteObject)(f.ctx, { workspace_id: 'ws-1', hash });
    expect(f.tables.storage_deletion_claims).toContainEqual(expect.objectContaining({ hash: HASH, workspace_id: 'ws-1' }));
    const push = handler(syncFunctions.push);
    const operation = (table_name: string, pk: string, payload: any) => ({ op_id: crypto.randomUUID(), table_name, pk, payload,
      operation: 'put', clock: 2_000_000_000, hlc: '2000000000:0:stale', device_id: 'stale-device' });
    const reference = () => operation('posts', 'stale-document', { id: 'stale-document', post_type: 'doc', title: 'Stale reference',
      content: '{}', file_hashes: JSON.stringify([hash]), deleted: false, created_at: 1, updated_at: 1 });
    const metadata = () => operation('file_meta', hash, { hash, name: 'Stale original', mime_type: 'text/plain', kind: 'file', size_bytes: 1,
      storage_id: 'old-blob', storage_provider_id: storageProviderId, deleted: false, created_at: 1, updated_at: 1 });
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
    for (const row of [...f.tables.file_meta]) await commitFixtureOriginal(f, row);
    f.tables.change_log.length = 0;
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

  // Cancellation/status reads must not deserialize a growing response. The
  // mirror is written by actual lifecycle mutations and remains owner-scoped.
  it('reads job controls without loading the response and preserves cancellation', async () => {
    const f = fixture();
    const created = await handler(backgroundFunctions.create)(f.ctx, { user_id: 'user-1', thread_id: 't1', message_id: 'm1',
      model: 'test', max_concurrent_jobs: 5, max_concurrent_jobs_per_user: 2, initial_content: 'x'.repeat(200_000) });
    const jobId = created.jobId;
    const get = f.ctx.db.get;
    f.ctx.db.get = async id => { if (id === jobId) throw new Error('Read growing response'); return get(id); };
    await expect(handler(backgroundFunctions.getControl)(f.ctx, { job_id: jobId, user_id: 'user-1' }))
      .resolves.toMatchObject({ status: 'streaming', attempts: 0, revision: 0 });
    await expect(handler(backgroundFunctions.getControl)(f.ctx, { job_id: jobId, user_id: 'foreign' })).resolves.toBeNull();
    await expect(handler(backgroundFunctions.checkAborted)(f.ctx, { job_id: jobId })).resolves.toBe(false);
    f.ctx.db.get = get;
    await handler(backgroundFunctions.update)(f.ctx, { job_id: jobId, content_chunk: 'new progress', chunks_received: 1 });
    f.ctx.db.get = async id => { if (id === jobId) throw new Error('Read growing response'); return get(id); };
    await expect(handler(backgroundFunctions.getControl)(f.ctx, { job_id: jobId, user_id: 'user-1' }))
      .resolves.toMatchObject({ status: 'streaming', revision: 1 });
    f.ctx.db.get = get;
    await handler(backgroundFunctions.abort)(f.ctx, { job_id: jobId, user_id: 'user-1' });
    f.ctx.db.get = async id => { if (id === jobId) throw new Error('Read growing response'); return get(id); };
    await expect(handler(backgroundFunctions.getControl)(f.ctx, { job_id: jobId, user_id: 'user-1' })).resolves.toMatchObject({ status: 'aborted' });
    await expect(handler(backgroundFunctions.checkAborted)(f.ctx, { job_id: jobId })).resolves.toBe(true);
  });

  // Historical Trash must count, initialization must make bounded progress,
  // and subsequent uploads must not enumerate the file catalog again.
  it('backfills retained quota once and admits subsequent uploads without catalog scans', async () => {
    const f = fixture();
    for (let index = 0; index < 601; index++) f.tables.file_meta.push({ _id: `old-${index}`,
      workspace_id: 'ws-1', hash: `sha256:${index.toString(16).padStart(64, '0')}`, deleted: true, size_bytes: 1 });
    const generate = handler(storageFunctions.generateUploadUrl);
    const input = { workspace_id: 'ws-1', hash: HASH, mime_type: 'image/png', size_bytes: 1, workspace_quota_bytes: 602 };
    let result: any;
    for (let attempt = 0; attempt < 10; attempt++) {
      result = await generate(f.ctx, input);
      if (result.uploadUrl) break;
    }
    expect(result.uploadUrl).toBe('https://upload.test');
    const query = f.ctx.db.query.bind(f.ctx.db);
    f.ctx.db.query = (table: string) => {
      if (table === 'file_meta') throw new Error('Repeated catalog scan');
      return query(table);
    };
    await expect(generate(f.ctx, { ...input, hash: 'b'.repeat(64) })).rejects.toThrow('quota exceeded');
    await expect(generate(f.ctx, { ...input, hash: `sha256:${'0'.repeat(64)}` })).resolves.toMatchObject({ uploadUrl: 'https://upload.test' });
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

  it('reserves size increases above retained metadata and keeps duplicate holds charged', async () => {
    const f = fixture();
    f.tables.file_meta.push({ _id: 'legacy-file', workspace_id: 'ws-1', hash: HASH, size_bytes: 10, deleted: true });
    const generate = handler(storageFunctions.generateUploadUrl);
    const input = { workspace_id: 'ws-1', hash: HASH, mime_type: 'image/png', size_bytes: 60, workspace_quota_bytes: 50 };
    await expect(generate(f.ctx, input)).rejects.toThrow('quota exceeded');
    await generate(f.ctx, { ...input, workspace_quota_bytes: 100 });
    await generate(f.ctx, { ...input, workspace_quota_bytes: 100 });
    await expect(generate(f.ctx, { ...input, hash: 'b'.repeat(64), size_bytes: 41, workspace_quota_bytes: 100 })).rejects.toThrow('quota exceeded');
  });

  it.each([`sha256:${HASH}`, `SHA256:${HASH.toUpperCase()}`])('releases physically deleted quota only after the last metadata alias is removed: %s', async alias => {
    const f = fixture();
    f.tables.file_meta.push(...[HASH, alias].map((hash, index) => ({
      _id: `old-${index}`, workspace_id: 'ws-1', hash, size_bytes: 60,
      deleted: true, storage_id: `old-blob-${index}`, deleted_at: 1,
    })));
    for (const row of [...f.tables.file_meta]) await commitFixtureOriginal(f, row);
    const generate = handler(storageFunctions.generateUploadUrl);
    const input = { workspace_id: 'ws-1', hash: 'b'.repeat(64), mime_type: 'image/png', size_bytes: 60, workspace_quota_bytes: 100 };
    await expect(generate(f.ctx, input)).rejects.toThrow('quota exceeded');
    const remove = handler(storageFunctions.deleteObject);
    await remove(f.ctx, { workspace_id: 'ws-1', hash: HASH });
    await expect(generate(f.ctx, input)).rejects.toThrow('quota exceeded');
    await remove(f.ctx, { workspace_id: 'ws-1', hash: alias });
    await expect(generate(f.ctx, input)).resolves.toMatchObject({ uploadUrl: 'https://upload.test' });
  });

  it('promotes verified upload size into retained quota when historical metadata understated it', async () => {
    const f = fixture();
    f.tables.file_meta.push({ _id: 'legacy-file', workspace_id: 'ws-1', hash: HASH, size_bytes: 10,
      deleted: true, name: 'Original', mime_type: 'image/png', kind: 'image', ref_count: 0 });
    const generate = handler(storageFunctions.generateUploadUrl);
    const input = { workspace_id: 'ws-1', hash: HASH, mime_type: 'image/png', size_bytes: 60, workspace_quota_bytes: 100 };
    const intent = await generate(f.ctx, input);
    f.objects.set('verified-original', { size: 60, contentType: 'image/png', sha256: hashBase64 });
    await handler(storageFunctions.commitUpload)(f.ctx, { ...input, intent_id: intent.intentId,
      storage_id: 'verified-original', storage_provider_id: 'convex', name: 'Original', kind: 'image' });
    await expect(generate(f.ctx, { ...input, hash: 'b'.repeat(64), size_bytes: 41 })).rejects.toThrow('quota exceeded');
    expect(f.tables.file_meta[0].size_bytes).toBe(60);
    await expect(generate(f.ctx, { ...input, hash: 'b'.repeat(64), size_bytes: 40 })).resolves.toMatchObject({ uploadUrl: 'https://upload.test' });
  });

  it('keeps aliases synced directly into Trash charged after quota initialization', async () => {
    const f = fixture();
    const generate = handler(storageFunctions.generateUploadUrl);
    await generate(f.ctx, { workspace_id: 'ws-1', hash: 'b'.repeat(64), mime_type: 'image/png', size_bytes: 1, workspace_quota_bytes: 100 });
    const alias = `SHA256:${HASH.toUpperCase()}`;
    const result = await handler(syncFunctions.push)(f.ctx, { workspace_id: 'ws-1', ops: [{
      table_name: 'file_meta', operation: 'put', pk: alias, clock: 1, hlc: '1:0:test',
      device_id: 'test', op_id: crypto.randomUUID(), payload: { hash: alias, name: 'Retained',
        mime_type: 'image/png', kind: 'image', size_bytes: 60, ref_count: 0, deleted: true, created_at: 1, updated_at: 1 },
    }] });
    expect(result.results[0]).toMatchObject({ success: true });
    await expect(generate(f.ctx, { workspace_id: 'ws-1', hash: 'c'.repeat(64), mime_type: 'image/png',
      size_bytes: 40, workspace_quota_bytes: 100 })).rejects.toThrow('quota exceeded');
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

  it('binds commit to subject, workspace and object bytes', async () => {
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
    for (const row of [...f.tables.file_meta]) await commitFixtureOriginal(f, row);
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
