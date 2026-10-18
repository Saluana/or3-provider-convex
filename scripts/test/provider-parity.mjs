#!/usr/bin/env bun
// Provider contracts against a disposable local Convex backend. This script
// never accepts a deployment URL, and writes a repeatable JSON receipt.
// Failure modes: durable limits weaken on configuration/outage; malformed
// limits grant requests; connection writes clobber ownership/rotations; stale
// tests revive a rotated credential; workspace removal retains secrets; deleted
// metadata is confused with references/live bytes or loses its paging scope;
// duplicate upload intents lose quota after cancellation/expiry, and legacy
// MD5 metadata becomes invisible at the canonical storage boundary.
// Backup failures: retained Trash bytes are omitted, snapshot identity widens
// access, browser clients obtain permanent URLs, or reading changes an original
// or unfinished upload. Qualify the real host archive with deployed Convex code.
// Restore must beat native upload revisions and reject unapplied metadata;
// Native writes must advance prior live/deleted revisions, keep new snapshots
// current, and retain the frozen Trash preimage for continuations.
import assert from 'node:assert/strict';
import { cp, mkdir, mkdtemp, rm, symlink, writeFile } from 'node:fs/promises';
import { createHash, randomBytes } from 'node:crypto';
import { resolve, join } from 'node:path';
import { tmpdir } from 'node:os';
import { fileURLToPath } from 'node:url';
import { mock } from 'bun:test';

const provider = resolve(fileURLToPath(new URL('../..', import.meta.url)));
const [binary, host, evidence] = process.argv.slice(2).map(p => resolve(p));
assert(binary?.startsWith(join(process.env.HOME, '.cache/convex/binaries/')) && binary.endsWith('/convex-local-backend'));
assert(host && evidence, 'Usage: bun provider-parity.mjs <cached-binary> <host-source> <evidence-dir>');
const root = await mkdtemp(join(tmpdir(), 'or3-convex-parity-'));
await mkdir(evidence, { recursive: true });
await cp(join(provider, 'templates/convex'), join(root, 'convex'), { recursive: true });
await cp(join(host, 'shared'), join(root, 'shared'), { recursive: true });
await symlink(join(provider, 'node_modules'), join(root, 'node_modules'));
await writeFile(join(root, 'package.json'), JSON.stringify({ private: true, type: 'module', dependencies: { convex: '*' } }));
await writeFile(join(root, 'convex/parityProbe.ts'), `
import { internalMutation } from './_generated/server';
import { v } from 'convex/values';
export const seed = internalMutation({args:{},handler:async(ctx)=>{
 const user=await ctx.db.insert('users',{created_at:1,normalized_email:''});
 await ctx.db.insert('auth_accounts',{user_id:user,provider:'basic-auth',provider_user_id:'parity-owner',created_at:1});
 const workspace=await ctx.db.insert('workspaces',{name:'Parity workspace',owner_user_id:user,created_at:1});
 await ctx.db.insert('workspace_members',{workspace_id:workspace,user_id:user,role:'owner',created_at:1});
 for(let i=1;i<=4;i++)await ctx.db.insert('file_meta',{workspace_id:workspace,hash:'sha256:'+String(i).repeat(64),name:'file-'+i,mime_type:'text/plain',kind:'file',size_bytes:i*10,ref_count:0,deleted:i!==2,created_at:1,updated_at:i,clock:1});
 return {workspace,user};
}});
export const quotaWorkspace = internalMutation({args:{user_id:v.id('users')},handler:async(ctx,args)=>{
 const workspace=await ctx.db.insert('workspaces',{name:'Isolated quota workspace',owner_user_id:args.user_id,created_at:1});
 await ctx.db.insert('workspace_members',{workspace_id:workspace,user_id:args.user_id,role:'owner',created_at:1});
 return workspace;
}});
export const expireIntent = internalMutation({args:{intent_id:v.id('upload_intents'),expires_at:v.optional(v.number())},handler:async(ctx,args)=>{
 await ctx.db.patch(args.intent_id,{expires_at:args.expires_at??Math.floor(Date.now()/1000)-1});
}});
export const seedMd5 = internalMutation({args:{workspace_id:v.id('workspaces'),storage_id:v.id('_storage')},handler:async(ctx,args)=>{
 for(const deleted of [false,true])await ctx.db.insert('file_meta',{workspace_id:args.workspace_id,hash:'md5:'+(deleted?'e':'d').repeat(32),name:'legacy.bin',mime_type:'application/octet-stream',kind:'file',size_bytes:100,storage_id:args.storage_id,storage_provider_id:'convex',ref_count:0,deleted,created_at:1,updated_at:1,clock:1});
}});
export const trashOriginal = internalMutation({args:{workspace_id:v.id('workspaces'),hash:v.string()},handler:async(ctx,args)=>{
 const row=await ctx.db.query('file_meta').withIndex('by_workspace_hash',q=>q.eq('workspace_id',args.workspace_id).eq('hash',args.hash)).unique();
 if(!row)throw new Error('Missing fixture original');
 await ctx.db.patch(row._id,{deleted:true,deleted_at:Math.floor(Date.now()/1000)});
}});
export const originalState = internalMutation({args:{workspace_id:v.id('workspaces'),intent_id:v.id('upload_intents'),storage_id:v.id('_storage')},handler:async(ctx,args)=>({
 files:await ctx.db.query('file_meta').withIndex('by_workspace_hash',q=>q.eq('workspace_id',args.workspace_id)).collect(),
 intent:await ctx.db.get(args.intent_id),object:await ctx.db.system.get(args.storage_id),
})});

`);
const instance = 'or3-parity-' + randomBytes(8).toString('hex');
const secret = randomBytes(32).toString('hex');
const keygen = Bun.spawn([binary, 'keygen', 'admin-key', '--instance-name', instance, '--instance-secret', secret], { stdout: 'pipe', stderr: 'pipe' });
const adminKey = (await new Response(keygen.stdout).text()).trim();
assert.equal(await keygen.exited, 0);
function port() { const server = Bun.serve({ hostname: '127.0.0.1', port: 0, fetch: () => new Response() }); const value = server.port; server.stop(true); return value; }
const apiPort = port(), sitePort = port(), url = `http://127.0.0.1:${apiPort}`;
const environmentPath = join(root, 'isolated.env');
await writeFile(environmentPath, `CONVEX_SELF_HOSTED_URL=${url}\nCONVEX_SELF_HOSTED_ADMIN_KEY=${adminKey}\n`, { mode: 0o600 });
const config = { sync: { convexUrl: url, convexAdminKey: adminKey }, public: { sync: { convexUrl: url } } };
mock.module(join(provider, 'src/shims/imports.ts'), () => ({ useRuntimeConfig: () => config }));
// Supply the host's configured admission policy without booting its Nuxt app.
// The provider adapter, HTTP transport, validators and backend handlers are real.
mock.module(join(host, 'server/utils/background-jobs/store.ts'), () => ({ getJobConfig: () => ({ maxConcurrentJobs: 20, maxConcurrentJobsPerUser: 5 }) }));
const checks = [];
async function check(name, operation) {
    try { await operation(); checks.push({ name, passed: true }); console.log('PASS ' + name); }
    catch (error) { const message = String(error).replaceAll(adminKey, '[redacted]'); checks.push({ name, passed: false, error: message }); console.error('FAIL ' + name + ': ' + message); }
}
let backend;
try {
    backend = Bun.spawn([binary, '--interface', '127.0.0.1', '--port', String(apiPort), '--site-proxy-port', String(sitePort), '--instance-name', instance, '--instance-secret', secret, '--disable-beacon', '--local-storage', join(root, 'storage'), join(root, 'backend.sqlite3')], { cwd: root, stdout: Bun.file(join(root, 'backend.log')), stderr: Bun.file(join(root, 'backend-errors.log')) });
    for (let i = 0; i < 100; i++) { try { if ((await fetch(url + '/version')).ok) break; } catch {} if (i === 99) throw Error('Disposable backend failed to start'); await Bun.sleep(100); }
    for (const args of [['env', 'set', 'CLERK_ISSUER_URL=https://parity.example.test'], ['deploy', '--yes', '--typecheck', 'enable']]) {
        const child = Bun.spawn([process.execPath, join(root, 'node_modules/convex/bin/main.js'), ...args, '--env-file', environmentPath], { cwd: root, env: { ...process.env, CI: 'true', CONVEX_DEPLOY_KEY: '', CONVEX_DEPLOYMENT: '' }, stdout: 'pipe', stderr: 'pipe' });
        const [stdout, stderr, code] = await Promise.all([new Response(child.stdout).text(), new Response(child.stderr).text(), child.exited]);
        assert.equal(code, 0, (stdout + stderr).replaceAll(adminKey, '[redacted]'));
    }
    const { ConvexHttpClient } = await import(join(provider, 'node_modules/convex/dist/esm/browser/index.js'));
    const client = new ConvexHttpClient(url);
    client.setAdminAuth(adminKey, { subject: 'parity-owner', issuer: 'https://or3.ai/auth/basic-auth', or3_server: true });
    const { workspace, user } = await client.mutation('parityProbe:seed', {});
    const { ConvexRateLimitProvider } = await import(join(provider, 'src/runtime/server/rate-limit/convex-provider.ts'));
    const limits = new ConvexRateLimitProvider();
    const policy = { windowMs: 60_000, maxRequests: 2 };
    await check('durable rate limits share one atomic allowance between instances', async () => {
        const other = new ConvexRateLimitProvider();
        const results = await Promise.all(Array.from({ length: 8 }, (_, i) => (i % 2 ? other : limits).checkAndRecord('parity-race', policy)));
        assert.equal(results.filter(value => value.allowed).length, 2);
        assert.equal((await other.getStats('parity-race', policy)).remaining, 0);
    });
    await check('invalid rate policies never grant requests or mutate the durable counter', async () => {
        for (const invalid of [{ ...policy, maxRequests: 0 }, { ...policy, windowMs: -1 }, { ...policy, maxRequests: 1.5 }, { ...policy, windowMs: Number.MAX_SAFE_INTEGER + 1 }]) {
            await assert.rejects(new ConvexRateLimitProvider().checkAndRecord('invalid-policy', invalid), /configuration/i);
            await assert.rejects(client.mutation('rateLimits:checkAndRecord', { key: 'invalid-policy', ...invalid }), /configuration/i);
        }
        assert.equal((await limits.getStats('invalid-policy', policy)).remaining, 2);
    });
    await check('configured durable rate limits refuse missing credentials', async () => {
        config.sync.convexAdminKey = '';
        try { await assert.rejects(new ConvexRateLimitProvider().checkAndRecord('missing-key', policy), /admin key/i); }
        finally { config.sync.convexAdminKey = adminKey; }
    });
    await check('admission recovery finds the durable job for its exact owner and idempotency key', async () => {
        const { convexJobProvider } = await import(join(provider, 'src/runtime/server/background-jobs/convex-provider.ts'));
        const input = { user_id: user, thread_id: 'parity-thread', message_id: 'parity-message', model: 'test-model', idempotency_key: 'parity-admission', max_concurrent_jobs: 20, max_concurrent_jobs_per_user: 5 };
        const { jobId } = await client.mutation('backgroundJobs:create', input);
        const { jobId: foreignId } = await client.mutation('backgroundJobs:create', { ...input, user_id: 'other-owner' });
        assert.equal((await convexJobProvider.findJobByIdempotencyKey(input.idempotency_key, user)).id, jobId);
        assert.equal((await convexJobProvider.findJobByIdempotencyKey(input.idempotency_key, 'other-owner')).id, foreignId);
        assert.equal(await convexJobProvider.findJobByIdempotencyKey(input.idempotency_key, 'unrelated'), null);
        assert.equal(await convexJobProvider.findJobByIdempotencyKey('absent', user), null);
    });
    await check('retained metadata pages exclude live data, bind their query kind and retain exact sizes', async () => {
        const first = await client.query('sync:queryCanonicalStorage', { workspace_id: workspace, kind: 'retained_metadata', page_size: 2 });
        assert.deepEqual(first.items.map(row => [row.kind, row.hash, row.sizeBytes]), [['retained_metadata', '1'.repeat(64), 10]]);
        assert.equal(first.hasMore, true);
        const next = await client.query('sync:queryCanonicalStorage', { workspace_id: workspace, kind: 'retained_metadata', page_size: 2, cursor: first.nextCursor });
        assert.deepEqual(next.items.map(row => row.sizeBytes), [30, 40]);
        assert.equal(next.hasMore, false);
        await assert.rejects(client.query('sync:queryCanonicalStorage', { workspace_id: workspace, kind: 'live_metadata', page_size: 2, cursor: first.nextCursor }), /cursor/i);
        await assert.rejects(client.query('sync:queryCanonicalStorage', { workspace_id: workspace, kind: 'retained_metadata', page_size: 9999 }), /page size/i);
    });
    let nativeReceipt;
    await check('an exact native upload commit can retry without authoring another write, while altered receipts are rejected', async () => {
        const bytes = new TextEncoder().encode('native retry proof');
        const hash = 'sha256:' + createHash('sha256').update(bytes).digest('hex');
        const intent = await client.mutation('storage:generateUploadUrl', { workspace_id: workspace, hash, mime_type: 'text/plain', size_bytes: bytes.length });
        const uploaded = await fetch(intent.uploadUrl, { method: 'POST', headers: { 'Content-Type': 'text/plain' }, body: bytes });
        assert.equal(uploaded.status, 200);
        const { storageId } = await uploaded.json();
        const commit = { workspace_id: workspace, intent_id: intent.intentId, hash, storage_id: storageId, storage_provider_id: 'convex', mime_type: 'text/plain', size_bytes: bytes.length, name: 'proof.txt', kind: 'file' };
        await client.mutation('storage:commitUpload', commit);
        nativeReceipt = { hash, intentId: intent.intentId, storageId, commit };
        const publicClient = new ConvexHttpClient(url);
        await assert.rejects(publicClient.query('storage:getFileUrl', { workspace_id: workspace, hash }), /Could not find public function|internal function/i);
        const download = await client.query('storage:getFileUrl', { workspace_id: workspace, hash });
        assert.equal(download.storageId, storageId);
        assert.equal(download.mimeType, 'text/plain');
        assert.equal(download.name, 'proof.txt');
        assert.equal(download.fileKind, 'file');
        assert.equal(new URL(download.url).protocol, 'http:');
        const before = await client.query('sync:getServerVersion', { workspace_id: workspace });
        await client.mutation('storage:commitUpload', commit);
        assert.deepEqual(await client.query('sync:getServerVersion', { workspace_id: workspace }), before);
        for (const change of [{ hash: 'f'.repeat(64) }, { size_bytes: bytes.length + 1 }, { mime_type: 'text/html' }, { storage_provider_id: 's3' }]) {
            await assert.rejects(client.mutation('storage:commitUpload', { ...commit, ...change }), /intent|metadata|receipt/i);
        }
    });
    await check('complete host backups include retained Convex Trash and preserve unfinished uploads', async () => {
        const workspaceId = await client.mutation('parityProbe:quotaWorkspace', { user_id: user });
        const bytes = new TextEncoder().encode('The only retained Trash original.');
        const hash = 'sha256:' + createHash('sha256').update(bytes).digest('hex');
        const intent = await client.mutation('storage:generateUploadUrl', { workspace_id: workspaceId, hash, mime_type: 'text/plain', size_bytes: bytes.length });
        const { storageId } = await (await fetch(intent.uploadUrl, { method: 'POST', headers: { 'Content-Type': 'text/plain' }, body: bytes })).json();
        await client.mutation('storage:commitUpload', { workspace_id: workspaceId, intent_id: intent.intentId, hash, storage_id: storageId, storage_provider_id: 'convex', mime_type: 'text/plain', size_bytes: bytes.length, name: 'retained.txt', kind: 'file' });
        await client.mutation('parityProbe:trashOriginal', { workspace_id: workspaceId, hash });
        const pendingBytes = new TextEncoder().encode('Still waiting for commit.');
        const pendingHash = 'sha256:' + createHash('sha256').update(pendingBytes).digest('hex');
        const pending = await client.mutation('storage:generateUploadUrl', { workspace_id: workspaceId, hash: pendingHash, mime_type: 'text/plain', size_bytes: pendingBytes.length });
        const pendingObject = await (await fetch(pending.uploadUrl, { method: 'POST', headers: { 'Content-Type': 'text/plain' }, body: pendingBytes })).json();
        const stateArgs = { workspace_id: workspaceId, intent_id: pending.intentId, storage_id: pendingObject.storageId };
        const before = await client.mutation('parityProbe:originalState', stateArgs);
        assert.equal(await client.query('storage:getFileUrl', { workspace_id: workspaceId, hash }), null);
        const backupArgs = { workspace_id: workspaceId, hash, storage_id: storageId };
        for (const change of [{ workspace_id: workspace }, { hash: pendingHash }, { storage_id: pendingObject.storageId }]) {
            assert.equal(await client.query('storage:getBackupFileUrl', { ...backupArgs, ...change }), null);
        }
        const outsider = new ConvexHttpClient(url);
        outsider.setAdminAuth(adminKey, { subject: 'not-a-member', issuer: 'https://or3.ai/auth/basic-auth', or3_server: true });
        await assert.rejects(outsider.query('storage:getBackupFileUrl', backupArgs), /Unauthorized|Forbidden|not found/i);
        await assert.rejects(new ConvexHttpClient(url).query('storage:getBackupFileUrl', backupArgs), /public function|internal function/i);

        const session = { authenticated: true, user: { id: user }, provider: 'basic-auth', providerUserId: 'parity-owner', workspace: { id: workspaceId, name: 'Trash backup' }, role: 'owner' };
        mock.module(join(host, 'server/auth/session.ts'), () => ({ resolveSessionContext: async () => session }));
        mock.module(join(host, 'server/auth/token-broker/resolve.ts'), () => ({ resolveProviderToken: async () => null }));
        mock.module(join(host, 'server/api/workspaces/_helpers.ts'), () => ({ requireWorkspaceSession: async () => session }));
        mock.module(join(host, 'server/utils/storage/workspace-access.ts'), () => ({ requireStorageWorkspaceAccess: async () => {} }));
        mock.module(join(host, 'server/utils/sync/is-sync-enabled.ts'), () => ({ isSyncEnabled: () => true }));
        mock.module(join(host, 'server/utils/sync/rate-limiter.ts'), () => ({ checkSyncRateLimit: () => ({ allowed: true }), recordSyncRequest: () => {} }));
        const { ConvexStorageGatewayAdapter } = await import(join(provider, 'src/runtime/server/storage/convex-storage-gateway-adapter.ts'));
        const { ConvexSyncGatewayAdapter } = await import(join(provider, 'src/runtime/server/sync/convex-sync-gateway-adapter.ts'));
        const storage = new ConvexStorageGatewayAdapter();
        const sync = new ConvexSyncGatewayAdapter();
        let selectedStorage = storage, selectedSync = sync;
        mock.module(join(host, 'server/storage/gateway/registry.ts'), () => ({ getActiveStorageGatewayAdapter: () => selectedStorage }));
        mock.module(join(host, 'server/sync/gateway/registry.ts'), () => ({ getActiveSyncGatewayAdapter: () => selectedSync }));
        const { default: backup } = await import(join(host, 'server/api/workspaces/backup.get.ts'));
        const { createApp, toWebHandler } = await import('h3');
        const { scanCloudBackup } = await import(join(host, 'shared/storage/workspace-archive.ts'));
        const request = () => toWebHandler(createApp().use('/backup', backup))(new Request(`http://or3.test/backup?workspaceId=${workspaceId}`));
        // An older runtime must not silently export a snapshot which omits Trash.
        selectedSync = { id: 'convex', snapshot: sync.snapshot.bind(sync) };
        assert.equal((await request()).status, 503);
        selectedSync = sync;
        selectedStorage = { id: 'convex', presignDownload: storage.presignDownload.bind(storage) };
        assert.equal((await request()).status, 503);
        selectedStorage = storage;
        const response = await request();
        assert.equal(response.status, 200);
        const archiveBytes = await response.arrayBuffer();
        const found = [];
        await scanCloudBackup(new Blob([archiveBytes]), async frame => {
            if (frame.header.type === 'file') found.push({ hash: frame.header.hash, bytes: await frame.body.text() });
            if (frame.header.type === 'row' && frame.header.table === 'file_meta') assert.equal((await frame.body.json()).deleted, true);
        });
        assert.deepEqual(found, [{ hash, bytes: new TextDecoder().decode(bytes) }]);
        assert.deepEqual(await client.mutation('parityProbe:originalState', stateArgs), before);
        const retained = await client.query('storage:getBackupFileUrl', backupArgs);
        assert.equal(await (await fetch(retained.url)).text(), new TextDecoder().decode(bytes));
        // Run the real host restore orchestration against the deployed adapter.
        // Only browser session/workspace selection and direct-upload transport
        // are supplied here; LWW, snapshot history and stored state are real.
        const databases = new Map([[workspaceId, {}]]);
        const workspaceApi = {
            create: async () => { const id = await client.mutation('parityProbe:quotaWorkspace', { user_id: user }); databases.set(id, {}); return { id }; },
            setActive: async ({ id }) => { session.workspace = { id, name: 'Restore target' }; },
            update: async () => {},
        };
        mock.module(join(host, 'app/db/client.ts'), () => ({ getActiveWorkspaceId: () => session.workspace.id, getDb: () => databases.get(session.workspace.id) }));
        mock.module(join(host, 'app/composables/auth/useSessionContext.ts'), () => ({ getCachedSessionContext: () => session }));
        mock.module(join(host, 'app/core/workspace/gateway-workspace-api.ts'), () => ({ createGatewayWorkspaceApi: () => workspaceApi }));
        let rejectMetadata = false;
        mock.module(join(host, 'app/core/sync/providers/gateway-sync-provider.ts'), () => ({ createGatewaySyncProvider: () => ({
            snapshot: input => sync.snapshot({}, input),
            push: async input => { const result = await sync.push({}, input); return rejectMetadata ? { ...result, results: result.results.map(item => ({ ...item, applied: false })) } : result; },
        }) }));
        mock.module(join(host, 'app/core/sync/hlc.ts'), () => ({ generateHLC: () => `${String(Date.now()).padStart(15, '0')}:000000:restore`, getDeviceId: () => 'restore-probe' }));
        mock.module(join(host, 'app/core/storage/file-policy.ts'), () => ({ loadFilePolicy: async () => ({ maxUploadBytes: 1024 * 1024 }) }));
        mock.module(join(host, 'app/composables/admin/useAdminExtensions.ts'), () => ({ ADMIN_HEADERS: { 'x-or3-admin-intent': 'admin' } }));
        mock.module(join(host, 'app/utils/errors.ts'), () => ({ reportError: () => {}, err: code => new Error(code) }));
        mock.module(join(host, 'app/core/storage/provider-registry.ts'), () => ({ getActiveStorageProvider: () => ({ commitUpload: true }) }));
        mock.module(join(host, 'app/core/storage/transfer-queue.ts'), () => ({ getStorageTransferQueue: () => ({
            getWorkspaceId: () => session.workspace.id,
            uploadDirect: async (row, body, context) => {
                const intent = await client.mutation('storage:generateUploadUrl', { workspace_id: context.workspaceId, hash: row.hash, mime_type: row.mime_type, size_bytes: body.size });
                const { storageId } = await (await fetch(intent.uploadUrl, { method: 'POST', headers: { 'Content-Type': row.mime_type }, body })).json();
                await client.mutation('storage:commitUpload', { workspace_id: context.workspaceId, intent_id: intent.intentId, hash: row.hash, storage_id: storageId, storage_provider_id: 'convex', mime_type: row.mime_type, size_bytes: body.size, name: row.name, kind: row.kind });
                return { ...row, storage_id: storageId, storage_provider_id: 'convex' };
            },
        }) }));
        const { restoreCloudWorkspace } = await import(join(host, 'app/utils/cloud-workspace-backup.ts'));
        await restoreCloudWorkspace(new Blob([archiveBytes]), { refresh: async () => {} }, () => {});
        const restoredWorkspace = session.workspace.id;
        const restored = await client.mutation('sync:backupSnapshot', { workspace_id: restoredWorkspace, tables: ['file_meta'], page_size: 100 });
        assert.equal(restored.items.length, 1);
        assert.equal(restored.items[0].payload.deleted, true, 'Restore resurrected Trash');
        assert.notEqual(restored.items[0].payload.storage_id, storageId);
        const restoredDownload = await client.query('storage:getBackupFileUrl', { workspace_id: restoredWorkspace, hash, storage_id: restored.items[0].payload.storage_id });
        assert.equal(await (await fetch(restoredDownload.url)).text(), new TextDecoder().decode(bytes));
        assert.deepEqual(await client.mutation('parityProbe:originalState', stateArgs), before);
        rejectMetadata = true;
        await assert.rejects(restoreCloudWorkspace(new Blob([archiveBytes]), { refresh: async () => {} }, () => {}), /did not finish/);
        assert.equal(session.workspace.id, restoredWorkspace);
        await writeFile(join(evidence, 'trash-restore.json'), JSON.stringify({ passed: true, restoredWorkspace, trashPreserved: true, restoredBytesVerified: true, sourceAndPendingUploadKept: true, unappliedMetadataRejected: true }, null, 2) + '\n');
        await writeFile(join(evidence, 'trash-backup.json'), JSON.stringify({ passed: true, fileHash: hash, archiveSha256: createHash('sha256').update(new Uint8Array(archiveBytes)).digest('hex'), trashIncluded: true, originalKept: true, pendingUploadKept: true, publicAccessDenied: true, snapshotMismatchesDenied: true }, null, 2) + '\n');
    });
    await check('native reuploads advance synced revisions and preserve frozen Trash originals', async () => {
        const workspaceId = await client.mutation('parityProbe:quotaWorkspace', { user_id: user });
        const upload = async text => {
            const bytes = new TextEncoder().encode(text), hash = 'sha256:' + createHash('sha256').update(bytes).digest('hex');
            const intent = await client.mutation('storage:generateUploadUrl', { workspace_id: workspaceId, hash, mime_type: 'text/plain', size_bytes: bytes.length });
            const { storageId } = await (await fetch(intent.uploadUrl, { method: 'POST', headers: { 'Content-Type': 'text/plain' }, body: bytes })).json();
            await client.mutation('storage:commitUpload', { workspace_id: workspaceId, intent_id: intent.intentId, hash, storage_id: storageId, storage_provider_id: 'convex', mime_type: 'text/plain', size_bytes: bytes.length, name: 'original.txt', kind: 'file' });
            return { hash, storageId, text };
        };
        const originals = [await upload('First retained original'), await upload('Second retained original')].sort((a, b) => a.hash.localeCompare(b.hash));
        const deleteClock = Math.floor(Date.now() / 1000) + 60;
        const trash = await client.mutation('sync:push', { workspace_id: workspaceId, ops: originals.map(original => ({
            op_id: crypto.randomUUID(), table_name: 'file_meta', operation: 'delete', pk: original.hash,
            clock: deleteClock, hlc: `${Date.now()}:revision-probe`, device_id: 'revision-probe',
        })) });
        assert(trash.results.every(result => result.success && result.applied));
        const first = await client.mutation('sync:backupSnapshot', { workspace_id: workspaceId, tables: ['file_meta'], page_size: 1 });
        const originalUrl = await client.query('storage:getBackupFileUrl', { workspace_id: workspaceId, hash: originals[1].hash, storage_id: originals[1].storageId });
        let replacement = await upload(originals[1].text);
        const next = await client.mutation('sync:backupSnapshot', { workspace_id: workspaceId, tables: ['file_meta'], page_size: 1, page_token: first.nextPageToken });
        assert.equal(next.highWatermark, first.highWatermark);
        assert.equal(next.nextPageToken, null);
        const rows = [...first.items, ...next.items];
        assert.deepEqual(rows.map(row => row.pk), originals.map(original => original.hash));
        for (let index = 0; index < rows.length; index++) {
            assert.equal(rows[index].payload.deleted, true);
            assert.equal(rows[index].payload.storage_id, originals[index].storageId);
            if (index === 1) assert.equal(await (await fetch(originalUrl.url)).text(), originals[index].text);
        }
        let lastClock = deleteClock;
        // Fresh snapshots must select the canonical replacement, including
        // rapid consecutive commits that can share the same wall-clock second.
        for (let attempt = 0; attempt < 3; attempt++) {
            const current = await client.mutation('sync:backupSnapshot', { workspace_id: workspaceId, tables: ['file_meta'], page_size: 100 });
            const row = current.items.find(item => item.pk === replacement.hash);
            assert.equal(row?.payload.storage_id, replacement.storageId, 'Fresh backup selected an obsolete original');
            assert.equal(row.payload.deleted, false);
            assert(row.revision.clock > lastClock, 'Native revision did not advance past the prior revision');
            lastClock = row.revision.clock;
            const file = await client.query('storage:getBackupFileUrl', { workspace_id: workspaceId, hash: replacement.hash, storage_id: row.payload.storage_id });
            assert.equal(await (await fetch(file.url)).text(), replacement.text);
            if (attempt < 2) replacement = await upload(replacement.text);
        }
        // Permanent deletion leaves a tombstone after removing materialized
        // metadata. A later verified reupload must also advance that revision.
        const opId = crypto.randomUUID();
        const deleted = await client.mutation('sync:push', { workspace_id: workspaceId, ops: [{
            op_id: opId, table_name: 'file_meta', operation: 'delete', pk: replacement.hash,
            clock: lastClock + 1, hlc: `${Date.now()}:revision-probe`, device_id: 'revision-probe',
        }] });
        assert(deleted.results[0].success && deleted.results[0].applied);
        await client.mutation('storage:deleteObject', { workspace_id: workspaceId, hash: replacement.hash, storage_id: replacement.storageId });
        const purged = await client.mutation('sync:snapshot', { workspace_id: workspaceId, tables: ['file_meta'], page_size: 100 });
        const tombstone = purged.items.find(item => item.pk === replacement.hash);
        assert.equal(tombstone.kind, 'tombstone');
        replacement = await upload(replacement.text);
        const restored = await client.mutation('sync:backupSnapshot', { workspace_id: workspaceId, tables: ['file_meta'], page_size: 100 });
        const restoredRow = restored.items.find(item => item.pk === replacement.hash);
        assert.equal(restoredRow?.payload.storage_id, replacement.storageId);
        assert(restoredRow.revision.clock > tombstone.revision.clock);
        assert.equal(restoredRow.payload.deleted, false);
        const restoredFile = await client.query('storage:getBackupFileUrl', { workspace_id: workspaceId, hash: replacement.hash, storage_id: replacement.storageId });
        assert.equal(await (await fetch(restoredFile.url)).text(), replacement.text);
        assert.equal(await (await fetch(originalUrl.url)).text(), originals[1].text);
        await writeFile(join(evidence, 'concurrent-backup.json'), JSON.stringify({ passed: true, files: rows.length, frozenWatermark: first.highWatermark,
            trashAndOriginalBytesPreserved: true, freshOriginalVerified: true, nativeRevisionsAdvance: true, tombstoneReuploadVerified: true }, null, 2) + '\n');
    });
    for (const [name, args] of [
        ['generateUploadUrl', { workspace_id: workspace, hash: nativeReceipt?.hash, mime_type: 'text/plain', size_bytes: 18 }],
        ['commitUpload', nativeReceipt?.commit],
        ['cancelUploadIntent', { workspace_id: workspace, intent_id: nativeReceipt?.intentId }],
        ['deleteObject', { workspace_id: workspace, hash: nativeReceipt?.hash, storage_id: nativeReceipt?.storageId }],
        ['gcDeletedFiles', { workspace_id: workspace, retention_seconds: 0 }],
    ]) {
        await check('native storage:' + name + ' is unavailable to public browser clients', async () => {
            assert(nativeReceipt, 'Native upload fixture must be committed');
            const publicClient = new ConvexHttpClient(url);
            await assert.rejects(publicClient.mutation('storage:' + name, args), /Could not find public function|internal function/i);
        });
    }
    for (const mode of ['cancelled', 'expired', 'larger duplicate', 'expiry order']) {
        await check('same-hash quota survives a ' + mode + ' intent and exempts committed live content', async () => {
            const quotaWorkspace = await client.mutation('parityProbe:quotaWorkspace', { user_id: user });
            const bytes = new TextEncoder().encode('q'.repeat(100));
            const hash = 'sha256:' + createHash('sha256').update(bytes).digest('hex');
            const generate = (digest, size) => client.mutation('storage:generateUploadUrl', {
                workspace_id: quotaWorkspace, hash: digest, mime_type: 'text/plain', size_bytes: size, workspace_quota_bytes: 100,
            });
            const first = await generate(hash, mode === 'larger duplicate' ? 1 : 100);
            const duplicate = await generate(hash, 100);
            const reservations = async () => {
                const items = [];
                let cursor;
                do {
                    const page = await client.query('sync:queryCanonicalStorage', { workspace_id: quotaWorkspace, kind: 'active_reservations', page_size: mode === 'expiry order' ? 500 : 1, ...(cursor ? { cursor } : {}) });
                    items.push(...page.items);
                    cursor = page.hasMore ? page.nextCursor : undefined;
                    assert(!page.hasMore || cursor, 'Reservation continuation must advance');
                } while (cursor);
                return items;
            };
            if (mode === 'cancelled') await client.mutation('storage:cancelUploadIntent', { workspace_id: quotaWorkspace, intent_id: first.intentId });
            if (mode === 'expired') await client.mutation('parityProbe:expireIntent', { intent_id: first.intentId });
            if (mode === 'expiry order') await client.mutation('parityProbe:expireIntent', { intent_id: first.intentId, expires_at: Math.floor(Date.now() / 1000) + 1200 });
            const items = await reservations();
            if (mode === 'expiry order') assert.equal(items[0]?.reservationId, first.intentId, 'Creation-order leader survives different expiry order');
            const competitor = await generate('f'.repeat(64), 100).then(() => ({ admitted: true }), () => ({ admitted: false }));
            assert.deepEqual({ reservationBytes: items.reduce((sum, item) => sum + item.sizeBytes, 0), reservations: items.length, competitorAdmitted: competitor.admitted },
                { reservationBytes: 100, reservations: 1, competitorAdmitted: false });
            const uploaded = await fetch(duplicate.uploadUrl, { method: 'POST', headers: { 'Content-Type': 'text/plain' }, body: bytes });
            assert.equal(uploaded.status, 200);
            const { storageId } = await uploaded.json();
            await client.mutation('storage:commitUpload', { workspace_id: quotaWorkspace, intent_id: duplicate.intentId, hash, storage_id: storageId, storage_provider_id: 'convex', mime_type: 'text/plain', size_bytes: 100, name: 'quota.txt', kind: 'file' });
            await generate(hash, 100); // Live bytes occupy quota once, including new same-hash intents.
            assert.deepEqual(await reservations(), []);
            await assert.rejects(generate('f'.repeat(64), 1), /quota/i);
        });
    }
    await check('filtered canonical live and retained metadata preserve supported prefixed MD5 hashes', async () => {
        const md5Workspace = await client.mutation('parityProbe:quotaWorkspace', { user_id: user });
        await client.mutation('parityProbe:seedMd5', { workspace_id: md5Workspace, storage_id: nativeReceipt.storageId });
        for (const [kind, hash] of [['live_metadata', 'md5:' + 'd'.repeat(32)], ['retained_metadata', 'md5:' + 'e'.repeat(32)]]) {
            const page = await client.query('sync:queryCanonicalStorage', { workspace_id: md5Workspace, kind, hash, page_size: 1 });
            assert.equal(page.items.length, 1);
            assert.equal(page.items[0].hash, hash.slice(4));
            assert.equal(page.items[0].storageId, nativeReceipt.storageId);
        }
        const download = await client.query('storage:getFileUrl', { workspace_id: md5Workspace, hash: 'md5:' + 'd'.repeat(32) });
        assert.equal(download.storageId, nativeReceipt.storageId);
    });
    await check('plugin connections persist with owner scoping, insert-only identity, revision CAS and stale-evidence rejection', async () => {
        const { createConvexPluginConnectionStore } = await import(join(provider, 'src/runtime/server/plugin-connections/convex-connection-store.ts'));
        const first = createConvexPluginConnectionStore(), second = createConvexPluginConnectionStore();
        const connection = { id: 'parity-connection', ownerUserId: user, workspaceId: workspace, pluginId: 'plugin-1', providerId: 'service-1', slotId: 'primary', label: 'Saved key', scopes: ['read'], revision: 1, secretCiphertext: 'encrypted-only', createdAt: 1, updatedAt: 1 };
        assert.equal((await Promise.all([first.insert(connection), second.insert({ ...connection, ownerUserId: 'foreign', secretCiphertext: 'foreign' })])).filter(Boolean).length, 1);
        // The winner is intentionally not assumed; ownership must never change afterward.
        const stored = await first.get(connection.id);
        assert(stored);
        assert.equal(await second.insert({ ...connection, ownerUserId: 'overwrite' }), false);
        assert.deepEqual(await first.get(connection.id), stored);
        assert.deepEqual(await first.list({ ownerUserId: 'unrelated', workspaceId: workspace }), []);
        assert.deepEqual(await first.list({ ownerUserId: stored.ownerUserId, workspaceId: workspace, pluginId: 'different' }), []);
        assert.equal((await second.list({ ownerUserId: stored.ownerUserId, workspaceId: workspace })).length, 1);
        const update = { id: connection.id, expectedRevision: 1, revision: 2, updatedAt: 2, secretCiphertext: 'rotated' };
        assert.equal((await Promise.all([first.update(update), second.update({ ...update, secretCiphertext: 'other-rotation' })])).filter(Boolean).length, 1);
        assert.equal((await first.get(connection.id)).ownerUserId, stored.ownerUserId);
        assert.equal(await first.setTestEvidence({ connectionId: connection.id, revision: 1, operationId: 'read', ok: true, checkedAt: 5 }), false);
        const evidence = { connectionId: connection.id, revision: 2, operationId: 'read', ok: true, checkedAt: 5 };
        assert.equal(await first.setTestEvidence(evidence), true);
        assert.equal(await second.setTestEvidence({ ...evidence, ok: false, checkedAt: 4 }), false);
        assert.deepEqual(await second.getTestEvidence(connection.id), evidence);
        const publicClient = new ConvexHttpClient(url);
        await assert.rejects(publicClient.query('pluginConnections:get', { id: connection.id }), /internal|public function|not found/i);
        await first.delete(connection.id);
        assert.equal(await second.get(connection.id), null);
        assert.equal(await second.getTestEvidence(connection.id), null);
        assert.equal(await second.setTestEvidence(evidence), false);
        await first.insert(connection);
        await first.setTestEvidence({ ...evidence, revision: 1 });
        await client.mutation('workspaces:remove', { workspace_id: workspace });
        assert.equal(await second.get(connection.id), null);
        assert.equal(await second.getTestEvidence(connection.id), null);
    });
    await check('durable rate limits fail closed after the backend becomes unavailable', async () => {
        backend.kill(); await backend.exited; backend = undefined;
        await assert.rejects(limits.checkAndRecord('parity-race', policy));
        await assert.rejects(limits.getStats('parity-race', policy));
    });
} finally {
    if (backend) { backend.kill(); await backend.exited; }
    await rm(environmentPath, { force: true });
    await writeFile(join(evidence, 'provider-parity.json'), JSON.stringify({ passed: checks.length > 0 && checks.every(check => check.passed), root, checks }, null, 2) + '\n');
    console.log('Evidence: ' + join(evidence, 'provider-parity.json'));
}
assert(checks.length > 0 && checks.every(check => check.passed), 'Provider parity qualification failed');
