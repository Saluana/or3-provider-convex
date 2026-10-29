#!/usr/bin/env bun
// Failure modes: incomplete server events poison pull; legacy rows disappear at
// snapshot recovery; stale/replayed writes report the wrong winner; malformed
// public input consumes versions; put retains omitted fields; devices pin GC;
// expired snapshots retain bodies forever. This runs the shipped handlers against
// a fresh official loopback Convex backend, including its validators and rollback.
import assert from 'node:assert/strict';
import { cp, mkdir, mkdtemp, writeFile } from 'node:fs/promises';
import { randomBytes } from 'node:crypto';
import { tmpdir } from 'node:os';
import { resolve, join } from 'node:path';
import { ConvexHttpClient } from 'convex/browser';
import { anyApi } from 'convex/server';

const binary = resolve(process.argv[2] ?? '');
assert(binary.startsWith(join(process.env.HOME, '.cache/convex/binaries/')) && binary.endsWith('/convex-local-backend'), 'Pass an official cached Convex backend binary');
const provider = resolve(import.meta.dir, '../..');
const root = await mkdtemp(join(tmpdir(), 'or3-sync-hardening-'));
const evidence = resolve(process.argv[3] ?? join(root, 'evidence'));
await mkdir(evidence, { recursive: true });
await cp(join(provider, 'templates/convex'), join(root, 'convex'), { recursive: true });
await cp(join(provider, '../or3-chat/shared'), join(root, 'shared'), { recursive: true });
// Symlink this provider's dependencies only; no install or operator env is read.
await Bun.write(join(root, 'package.json'), JSON.stringify({ private: true, type: 'module', dependencies: { convex: '1.48.0' } }));
await import('node:fs/promises').then(({ symlink }) => symlink(join(provider, 'node_modules'), join(root, 'node_modules')));
await writeFile(join(root, 'convex/auth.config.ts'), 'export default { providers: [] };\n');
await writeFile(join(root, 'convex/syncProbe.ts'), `
import { internalMutation, internalQuery } from './_generated/server';
import { v } from 'convex/values';
import * as sync from './sync';
import * as notifications from './notifications';
import * as storage from './storage';
import { applyServerAuthoredOp } from './syncAuthoring';
const actor=(ctx:any,subject:string)=>({...ctx,auth:{getUserIdentity:async()=>({subject,issuer:'https://clerk.example.test'})}});
export const seed=internalMutation({args:{},handler:async(ctx)=>{
 const user=await ctx.db.insert('users',{created_at:1});
 const other=await ctx.db.insert('users',{created_at:1});
 const workspace=await ctx.db.insert('workspaces',{name:'Sync regression',owner_user_id:user,created_at:1});
 for(const [id,suffix] of [[user,'a'],[other,'b']] as const){await ctx.db.insert('auth_accounts',{user_id:id,provider:'clerk',provider_user_id:workspace+suffix,created_at:1});await ctx.db.insert('workspace_members',{workspace_id:workspace,user_id:id,role:'editor',created_at:1});}
 await ctx.db.insert('server_version_counter',{workspace_id:workspace,value:5});
 const row=await ctx.db.insert('kv',{workspace_id:workspace,id:'a',name:'setting',value:'old',deleted:false,created_at:1,updated_at:1,clock:5,hlc:'5:0:dev',op_id:'00000000-0000-4000-a000-000000000005',server_version:5});
 await ctx.db.insert('kv',{workspace_id:workspace,id:'z',name:'legacy',value:'retained',deleted:false,created_at:1,updated_at:1,clock:1,hlc:'1:0:dev'});
 return {workspace,user,other,row};
}});
export const invoke=internalMutation({args:{workspace:v.id('workspaces'),name:v.string(),input:v.any(),actor:v.optional(v.string()),fail_after:v.optional(v.boolean()),fail_write:v.optional(v.boolean())},handler:async(ctx,args)=>{
 const context=actor(ctx,args.workspace+(args.actor??'a'));
 if(args.fail_write)context.db=new Proxy(ctx.db,{get(target,key){if(key==='insert')return async(table:any,value:any)=>{if(table==='change_log')throw Error('Injected change-log write failure');return target.insert(table,value);};return Reflect.get(target,key);}});
 const handlers:any={...sync,createNotification:notifications.create,markNotificationRead:notifications.markRead,generateUploadUrl:storage.generateUploadUrl,commitUpload:storage.commitUpload,deleteObject:storage.deleteObject};
 const result=await handlers[args.name]._handler(context,{workspace_id:args.workspace,...args.input});
 if(args.fail_after)throw Error('Injected crash before transaction commit');
 return result;
}});
export const authored=internalMutation({args:{workspace:v.id('workspaces'),op:v.any()},handler:(ctx,args)=>applyServerAuthoredOp(ctx,args.workspace,args.op)});
export const patch=internalMutation({args:{id:v.string(),value:v.any()},handler:(ctx,args)=>ctx.db.patch(args.id as any,args.value)});
export const insert=internalMutation({args:{table:v.string(),value:v.any()},handler:(ctx,args)=>ctx.db.insert(args.table as any,args.value)});
export const seedExpiredSessions=internalMutation({args:{workspace:v.id('workspaces')},handler:async(ctx,args)=>{for(let i=0;i<205;i++)await ctx.db.insert('sync_snapshot_sessions',{workspace_id:args.workspace,user_id:'expired',high_watermark:1,tables:['kv'],created_at:1,expires_at:2});}});
export const gcPage=internalMutation({args:{workspace_id:v.id('workspaces'),retention_seconds:v.number(),batch_size:v.number()},handler:async(ctx,args)=>{
 const result=await (sync.runWorkspaceGc as any)._handler(ctx,args);
 const remaining=(await ctx.db.query('sync_snapshot_sessions').withIndex('by_workspace',(q)=>q.eq('workspace_id',args.workspace_id)).collect()).length;
 return {result,remaining};
}});
export const read=internalQuery({args:{workspace:v.id('workspaces')},handler:async(ctx,args)=>{
 const result:any={};for(const name of ['kv','notifications','file_meta','upload_intents','change_log','server_version_counter','device_cursors','sync_snapshot_sessions','sync_record_versions'])result[name]=(await ctx.db.query(name as any).collect()).filter((row:any)=>row.workspace_id===args.workspace);return result;
}});
`);
const instance = 'sync-hardening-' + randomBytes(8).toString('hex');
const secret = randomBytes(32).toString('hex');
const keygen = Bun.spawn([binary, 'keygen', 'admin-key', '--instance-name', instance, '--instance-secret', secret], { stdout: 'pipe', stderr: 'pipe' });
const adminKey = (await new Response(keygen.stdout).text()).trim();
assert.equal(await keygen.exited, 0);
function port() { const server = Bun.serve({ hostname: '127.0.0.1', port: 0, fetch: () => new Response('probe') }); const value = server.port; server.stop(true); return value; }
const apiPort = port(), sitePort = port(), url = 'http://127.0.0.1:' + apiPort;
const envPath = join(root, 'isolated.env');
await writeFile(envPath, 'CONVEX_SELF_HOSTED_URL=' + url + '\nCONVEX_SELF_HOSTED_ADMIN_KEY=' + adminKey + '\n', { mode: 0o600 });
const env = { ...process.env, CI: 'true', CONVEX_DEPLOY_KEY: '', CONVEX_DEPLOYMENT: '', CONVEX_SELF_HOSTED_URL: url, CONVEX_SELF_HOSTED_ADMIN_KEY: adminKey };
const client = new ConvexHttpClient(url, { logger: false }); client.setAdminAuth(adminKey);
const invoke = (fixture, name, input = {}, extra = {}) => client.mutation(anyApi.syncProbe.invoke, { workspace: fixture.workspace, name, input, ...extra }, { skipQueue: true });
const read = (fixture) => client.query(anyApi.syncProbe.read, { workspace: fixture.workspace });
const op = (overrides = {}) => ({ op_id: crypto.randomUUID(), table_name: 'kv', operation: 'put', pk: 'a', payload: { id: 'a', name: 'setting', deleted: false, created_at: 1, updated_at: 2, clock: 6 }, clock: 6, hlc: '6:0:dev', device_id: 'device', ...overrides });
let backend;
const checks = [], failures = [];
async function check(name, run) { try { await run(); checks.push(name); console.log('PASS ' + name); } catch (error) { failures.push({ name, error: String(error) }); console.error('FAIL ' + name + ': ' + error); } }
function start() { return Bun.spawn([binary, '--interface', '127.0.0.1', '--port', String(apiPort), '--site-proxy-port', String(sitePort), '--instance-name', instance, '--instance-secret', secret, '--disable-beacon', '--local-storage', join(root, 'storage'), join(root, 'backend.sqlite3')], { cwd: root, stdout: Bun.file(join(root, 'backend.log')), stderr: Bun.file(join(root, 'backend-errors.log')) }); }
async function ready() { for (let i = 0; i < 100; i++) { try { if ((await fetch(url + '/version')).ok) return; } catch {} await Bun.sleep(100); } throw Error('Disposable backend did not start'); }
try {
 backend = start(); await ready();
 const deploy = Bun.spawn([process.execPath, join(provider, 'node_modules/convex/bin/main.js'), 'deploy', '--yes', '--typecheck', 'enable', '--env-file', envPath], { cwd: root, env, stdout: Bun.file(join(root, 'deploy.log')), stderr: Bun.file(join(root, 'deploy-errors.log')) });
 assert.equal(await deploy.exited, 0, 'Sandbox deployment failed: ' + root);
 await check('real schema rejects an unknown canonical field', async () => {
  const f=await client.mutation(anyApi.syncProbe.seed,{});
  await assert.rejects(client.mutation(anyApi.syncProbe.patch,{id:f.row,value:{unknown_field:true}}), /validator|schema|extra field/i);
 });
 await check('server-authored creation and update emit complete canonical payloads', async () => {
  const f=await client.mutation(anyApi.syncProbe.seed,{});
  const id=await invoke(f,'createNotification',{user_id:f.user,type:'info',title:'Ready'});
  await invoke(f,'markNotificationRead',{notification_id:id});
  const state=await read(f);
  assert.equal(state.notifications.length,1);assert.equal(state.change_log.length,2);
  assert.equal(state.notifications[0].id,id);assert.equal(state.notifications[0].title,'Ready');
  assert.equal(state.notifications[0].user_id,f.user);assert.ok(state.notifications[0].read_at>0);
  for(const event of state.change_log){assert.equal(event.payload.created_at,state.notifications[0].created_at);assert.equal(event.payload.id,id);assert.equal(event.payload.deleted,false);assert.equal(event.payload.clock,event.clock);}
 });
 await check('legacy snapshot recovery is complete and frozen across concurrent writes', async () => {
  const f=await client.mutation(anyApi.syncProbe.seed,{});
  const initial=await invoke(f,'snapshot',{tables:['kv'],page_size:10});assert.equal(initial.items.find(row=>row.pk==='z')?.payload.value,'retained');
  const first=await invoke(f,'snapshot',{tables:['kv'],page_size:1});
  await invoke(f,'push',{ops:[op({pk:'z',payload:{id:'z',name:'legacy',value:'newer',deleted:false,created_at:1,updated_at:2,clock:6}})]});
  const second=await invoke(f,'snapshot',{tables:['kv'],page_size:1,page_token:first.nextPageToken});
  assert.equal(second.highWatermark,5);assert.equal(second.items[0]?.payload.value,'retained');
  const fresh=await invoke(f,'snapshot',{tables:['kv'],page_size:10});assert.equal(fresh.items.find(row=>row.pk==='z')?.payload.value,'newer');
 });
 await check('stale and replayed writes report the current live or deleted winner', async () => {
  const f=await client.mutation(anyApi.syncProbe.seed,{});const stale=op({clock:4,hlc:'4:0:dev'});
  const first=await invoke(f,'push',{ops:[stale]});assert.equal(first.results[0].applied,false);assert.equal(first.results[0].winner.payload.value,'old');
  const replay=await invoke(f,'push',{ops:[stale]});assert.equal(replay.results[0].replayed,true);assert.equal(replay.results[0].applied,false);
  await invoke(f,'push',{ops:[op({operation:'delete',clock:8,hlc:'8:0:dev',payload:undefined})]});
  const later=await invoke(f,'push',{ops:[stale]});assert.equal(later.results[0].winner.kind,'delete');assert.equal(later.results[0].winner.revision.clock,8);
 });
 await check('public admission rejects corrupt stamps and schema-invalid payloads without allocating versions', async () => {
  const f=await client.mutation(anyApi.syncProbe.seed,{});
  const invalid=[op({clock:-1}),op({clock:1.1}),op({hlc:''}),op({device_id:' '}),op({payload:{id:'a',name:7}}),op({payload:{id:'a',name:'x',unknown:true}})];
  const result=await invoke(f,'push',{ops:invalid});assert(result.results.every(row=>!row.success&&row.errorCode==='VALIDATION_ERROR'));
  const state=await read(f);assert.equal(state.server_version_counter[0].value,5);assert.equal(state.change_log.length,0);
 });
 await check('complete puts remove omitted optional fields and unexpected failures roll back', async () => {
  const f=await client.mutation(anyApi.syncProbe.seed,{});
  await invoke(f,'push',{ops:[op()]});assert.equal((await read(f)).kv.find(row=>row.id==='a').value,undefined);
  await assert.rejects(invoke(f,'push',{ops:[op({clock:9,hlc:'9:0:dev'})]},{fail_after:true}),/Injected crash/);
  const state=await read(f);assert.equal(state.server_version_counter[0].value,6);assert.equal(state.kv.find(row=>row.id==='a').clock,6);
 });
 await check('unexpected storage failure aborts every version and materialized write in its mutation', async () => {
  const f=await client.mutation(anyApi.syncProbe.seed,{});
  await assert.rejects(invoke(f,'push',{ops:[op()]},{fail_write:true}),/Injected change-log write failure/);
  const state=await read(f);assert.equal(state.server_version_counter[0].value,5);assert.equal(state.kv.find(row=>row.id==='a').value,'old');assert.equal(state.sync_record_versions.length,0);
 });
 await check('late native upload preserves deletion until an explicit canonical restore', async () => {
  const f=await client.mutation(anyApi.syncProbe.seed,{});const bytes=new TextEncoder().encode('late upload');const hash=new Bun.CryptoHasher('sha256').update(bytes).digest('hex');
  const intent=await invoke(f,'generateUploadUrl',{hash,mime_type:'text/plain',size_bytes:bytes.length});
  const uploaded=await fetch(intent.uploadUrl,{method:'POST',headers:{'Content-Type':'text/plain'},body:bytes});assert(uploaded.ok);const {storageId}=await uploaded.json();
  await client.mutation(anyApi.syncProbe.insert,{table:'file_meta',value:{workspace_id:f.workspace,hash,name:'file',mime_type:'text/plain',kind:'file',size_bytes:bytes.length,ref_count:0,deleted:true,deleted_at:1,created_at:1,updated_at:1,clock:7,hlc:'7:0:dev',op_id:crypto.randomUUID(),server_version:5}});
  await invoke(f,'commitUpload',{intent_id:intent.intentId,hash,storage_id:storageId,storage_provider_id:'convex',mime_type:'text/plain',size_bytes:bytes.length,name:'file',kind:'file'});
  const state=await read(f);assert.equal(state.file_meta[0].deleted,true);assert.equal(state.upload_intents[0].status,'consumed');
  const peer=await invoke(f,'pull',{cursor:5,limit:20});assert.equal(peer.changes[0].payload.deleted,true);assert.equal(peer.changes[0].payload.storage_id,storageId);
  const {_id,_creationTime,workspace_id,server_version,...payload}=state.file_meta[0];const clock=payload.clock+1;
  const restoredOp=op({table_name:'file_meta',pk:hash,clock,hlc:clock+':0:restore',payload:{...payload,deleted:false,clock,deleted_at:undefined}});
  const restore=await invoke(f,'push',{ops:[restoredOp]});
  assert.equal(restore.results[0].applied,true);assert.equal((await read(f)).file_meta[0].deleted,false);
  const foreign=await client.mutation(anyApi.syncProbe.seed,{});
  const rejected=await invoke(foreign,'push',{ops:[op({table_name:'file_meta',pk:hash,clock,hlc:clock+':0:foreign',payload:{...payload,deleted:false,clock,deleted_at:undefined}})]});
  assert.equal(rejected.results[0].success,false);assert.equal(rejected.results[0].errorCode,'VALIDATION_ERROR');
  assert.equal((await read(foreign)).server_version_counter[0].value,5);
  await invoke(f,'push',{ops:[op({table_name:'file_meta',pk:hash,operation:'delete',payload:undefined,clock:clock+1,hlc:'delete:later'})]});
  await invoke(f,'deleteObject',{hash});
  const replay=await invoke(f,'push',{ops:[restoredOp]});assert.equal(replay.results[0].success,true);assert.equal(replay.results[0].replayed,true);assert.equal(replay.results[0].winner.kind,'delete');
 });
 await check('two users sharing one browser device acknowledge independently', async () => {
  const f=await client.mutation(anyApi.syncProbe.seed,{});
  await invoke(f,'updateDeviceCursor',{device_id:'same-browser',last_seen_version:4});
  await invoke(f,'updateDeviceCursor',{device_id:'same-browser',last_seen_version:3},{actor:'b'});
  assert.equal((await read(f)).device_cursors.length,2);
 });
 await check('byte completion cannot outrank a queued semantic deletion from the same base', async () => {
  const f=await client.mutation(anyApi.syncProbe.seed,{});const bytes=new TextEncoder().encode('concurrent deletion');const hash=new Bun.CryptoHasher('sha256').update(bytes).digest('hex');
  const complete=async()=>{
   const intent=await invoke(f,'generateUploadUrl',{hash,mime_type:'text/plain',size_bytes:bytes.length});
   const uploaded=await fetch(intent.uploadUrl,{method:'POST',headers:{'Content-Type':'text/plain'},body:bytes});assert(uploaded.ok);const {storageId}=await uploaded.json();
   await invoke(f,'commitUpload',{intent_id:intent.intentId,hash,storage_id:storageId,storage_provider_id:'convex',mime_type:'text/plain',size_bytes:bytes.length,name:'file',kind:'file'});
  };
  // A fresh local metadata row starts at clock zero; deletion was captured
  // while its first byte upload was in flight and is delivered afterward.
  await complete();
  let deletion=await invoke(f,'push',{ops:[op({table_name:'file_meta',pk:hash,operation:'delete',payload:undefined,clock:1,hlc:'0:tab:delete'})]});
  assert.equal(deletion.results[0].applied,true);assert.equal((await read(f)).file_meta[0].deleted,true);
  const {_id,_creationTime,workspace_id,server_version,...payload}=(await read(f)).file_meta[0];
  await invoke(f,'push',{ops:[op({table_name:'file_meta',pk:hash,clock:2,hlc:'0:tab:restore',payload:{...payload,clock:2,deleted:false,deleted_at:undefined}})]});
  // The same ordering must remain safe for replacement bytes of a live row.
  await complete();
  deletion=await invoke(f,'push',{ops:[op({table_name:'file_meta',pk:hash,operation:'delete',payload:undefined,clock:3,hlc:'0:tab:delete-again'})]});
  assert.equal(deletion.results[0].applied,true);assert.equal((await read(f)).file_meta[0].deleted,true);
 });
 await check('retirement collects expired cursor/session/history while active snapshot remains recoverable', async () => {
  const f=await client.mutation(anyApi.syncProbe.seed,{});const old=Math.floor(Date.now()/1000)-7200;
  await invoke(f,'updateDeviceCursor',{device_id:'abandoned',last_seen_version:0});
  const cursor=(await read(f)).device_cursors[0];await client.mutation(anyApi.syncProbe.patch,{id:cursor._id,value:{updated_at:old}});
  const first=await invoke(f,'snapshot',{tables:['kv'],page_size:1});
  await invoke(f,'push',{ops:[op({pk:'z',payload:{id:'z',name:'legacy',value:'new',deleted:false,created_at:1,updated_at:2,clock:6}})]});
  await invoke(f,'runWorkspaceGc',{retention_seconds:3600,batch_size:20});
  assert.equal((await read(f)).device_cursors.length,0);
  const second=await invoke(f,'snapshot',{tables:['kv'],page_size:1,page_token:first.nextPageToken});assert.equal(second.items[0]?.payload.value,'retained');
  const state=await read(f);for(const session of state.sync_snapshot_sessions)await client.mutation(anyApi.syncProbe.patch,{id:session._id,value:{expires_at:old}});
  await invoke(f,'runWorkspaceGc',{retention_seconds:3600,batch_size:20});
  const after=await read(f);assert.equal(after.sync_snapshot_sessions.length,0);assert.equal(after.sync_record_versions.length,0);
  await assert.rejects(invoke(f,'snapshot',{tables:['kv'],page_size:1,page_token:first.nextPageToken}),/unavailable/);
 });
 await check('large GC backlog yields between bounded workspace transactions', async () => {
  const f=await client.mutation(anyApi.syncProbe.seed,{});await client.mutation(anyApi.syncProbe.seedExpiredSessions,{workspace:f.workspace});
  const page=await client.mutation(anyApi.syncProbe.gcPage,{workspace_id:f.workspace,retention_seconds:3600,batch_size:20});
  assert.equal(page.result.hasMore,true);
  // Observe committed progress inside the transaction, before the real scheduled
  // continuation can race this read. Each of the two collectors is bounded by 20.
  assert.ok(page.remaining>=165 && page.remaining<205,'GC must make bounded persisted progress');
  for(let attempt=0;attempt<100 && (await read(f)).sync_snapshot_sessions.length>0;attempt++)await Bun.sleep(100);
  assert.equal((await read(f)).sync_snapshot_sessions.length,0,'Scheduled continuation must drain the backlog');
 });
 await check('concurrent tab retries allocate once and converge on the full revision winner', async () => {
  const f=await client.mutation(anyApi.syncProbe.seed,{});
  const write=op();const replies=await Promise.all([invoke(f,'push',{ops:[write]}),invoke(f,'push',{ops:[write]})]);
  assert.equal(replies[0].results[0].serverVersion,6);assert.equal(replies[1].results[0].serverVersion,6);assert.equal((await read(f)).change_log.length,1);
  const lower=op({clock:8,hlc:'8:0:tab',op_id:'00000000-0000-4000-a000-000000000008',payload:{id:'a',name:'setting',value:'loser',deleted:false,created_at:1,updated_at:8,clock:8}});
  const higher={...lower,op_id:'00000000-0000-4000-a000-000000000009',payload:{...lower.payload,value:'winner'}};
  await Promise.all([invoke(f,'push',{ops:[higher]}),invoke(f,'push',{ops:[lower]})]);
  assert.equal((await read(f)).kv.find(row=>row.id==='a').value,'winner');
  const retry=await invoke(f,'push',{ops:[lower]});assert.equal(retry.results[0].winner.payload.value,'winner');assert.equal(retry.results[0].applied,false);
 });
 await check('acknowledged operations survive backend crash and exact receipt replay', async () => {
  const f=await client.mutation(anyApi.syncProbe.seed,{});const write=op();await invoke(f,'push',{ops:[write]});
  backend.kill('SIGKILL');await backend.exited;
  await assert.rejects(read(f));
  backend=start();await ready();
  const replay=await invoke(f,'push',{ops:[write]});assert.equal(replay.results[0].serverVersion,6);assert.equal((await read(f)).change_log.length,1);
 });
} finally {
 backend?.kill();if(backend)await backend.exited;
 await writeFile(join(evidence,'sync-hardening.json'),JSON.stringify({root,url,checks,failures},null,2)+'\n');
}
assert.equal(failures.length,0,'Sync regressions failed; evidence '+join(evidence,'sync-hardening.json'));
console.log('Evidence '+join(evidence,'sync-hardening.json'));
