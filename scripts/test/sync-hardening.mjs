#!/usr/bin/env bun
// Failure modes: incomplete server events poison pull; legacy rows disappear at
// snapshot recovery; stale/replayed writes report the wrong winner; malformed
// public input consumes versions; put retains omitted fields; devices pin GC;
// expired snapshots retain bodies forever. This runs the shipped handlers against
// a fresh official loopback Convex backend, including its validators and rollback.
import assert from 'node:assert/strict';
import { cp, mkdir, mkdtemp, writeFile, readFile, rm } from 'node:fs/promises';
import { randomBytes, createHash } from 'node:crypto';
import { gunzipSync } from 'node:zlib';
import { tmpdir } from 'node:os';
import { resolve, join } from 'node:path';
import { ConvexHttpClient } from 'convex/browser';
import { anyApi } from 'convex/server';
const host = resolve(process.env.OR3_CANONICAL_HOST_ROOT || new URL('../../../or3-chat', import.meta.url).pathname);
const { prepareToolResultStorage, readToolResultStorage } = await import(join(host, 'shared/chat/tool-result-artifact.ts'));
const { TABLE_PAYLOAD_SCHEMAS, compareSyncTuple, SnapshotResponseSchema, PullResponseSchema, PushResultSchema } = await import(join(host, 'shared/sync/schemas.ts'));

const binary = resolve(process.argv[2] ?? '');
assert(binary.startsWith(join(process.env.HOME, '.cache/convex/binaries/')) && binary.endsWith('/convex-local-backend'), 'Pass an official cached Convex backend binary');
const provider = resolve(import.meta.dir, '../..');
const root = await mkdtemp(join(tmpdir(), 'or3-sync-hardening-'));
const evidence = resolve(process.argv[3] ?? join(root, 'evidence'));
await mkdir(evidence, { recursive: true });
await cp(resolve(process.env.OR3_CONVEX_TEMPLATE_ROOT || join(provider, 'templates/convex')), join(root, 'convex'), { recursive: true });
await cp(join(host, 'shared'), join(root, 'shared'), { recursive: true });
// Symlink this provider's dependencies only; no install or operator env is read.
await Bun.write(join(root, 'package.json'), JSON.stringify({ private: true, type: 'module', dependencies: { convex: '1.48.0' } }));
await import('node:fs/promises').then(({ symlink }) => symlink(join(provider, 'node_modules'), join(root, 'node_modules')));
await writeFile(join(root, 'convex/auth.config.ts'), 'export default { providers: [] };\n');
await writeFile(join(root, 'convex/syncProbe.ts'), `
import { internalMutation, internalQuery } from './_generated/server';
import { v, getConvexSize } from 'convex/values';
import * as sync from './sync';
import * as notifications from './notifications';
import * as storage from './storage';
import * as workspaces from './workspaces';
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
export const invoke=internalMutation({args:{workspace:v.id('workspaces'),name:v.string(),input:v.any(),actor:v.optional(v.string()),fail_after:v.optional(v.boolean()),fail_write:v.optional(v.boolean()),fail_evidence:v.optional(v.boolean())},handler:async(ctx,args)=>{
 const context=actor(ctx,args.workspace+(args.actor??'a'));
 const history='getHistoryIncarnation' in sync ? await (ctx.db as any).query('sync_history').first() : null;
 args.input={history_incarnation:history?.incarnation,history_incarnation_capability:'incarnation-v1',unavailable_record_capability:'unavailable-v1',...args.input};
 let bytes=0, reads=0, written=0;
 const measured=(value:any)=>{if(value){bytes+=getConvexSize(value);reads++;}return value;};
 const queryProxy=(target:any):any=>new Proxy(target,{get(t,key){
  if(key===Symbol.asyncIterator)return async function*(){for await(const row of t)yield measured(row);};
  if(['first','unique'].includes(String(key)))return async(...a:any[])=>measured(await t[key](...a));
  if(['take','collect'].includes(String(key)))return async(...a:any[])=>{const rows=await t[key](...a);rows.forEach(measured);return rows;};
  if(['withIndex','order','filter'].includes(String(key)))return(...a:any[])=>queryProxy(t[key](...a));
  const value=t[key];return typeof value==='function'?value.bind(t):value;
 }});
 context.db=new Proxy(ctx.db,{get(t,key){
  if(key==='query')return(...a:any[])=>queryProxy((t.query as any)(...a));
  if(key==='get')return async(...a:any[])=>measured(await (t.get as any)(...a));
  if(['insert','patch','replace'].includes(String(key)))return async(...a:any[])=>{written+=getConvexSize(a.at(-1));return (t as any)[key](...a);};
  const value=(t as any)[key];return typeof value==='function'?value.bind(t):value;
 }});
 if(args.fail_write)context.db=new Proxy(ctx.db,{get(target,key){if(key==='insert')return async(table:any,value:any)=>{if(table==='change_log')throw Error('Injected change-log write failure');return target.insert(table,value);};return Reflect.get(target,key);}});
 if(args.fail_evidence)context.db=new Proxy(context.db,{get(target,key){if(key==='insert')return async(table:any,value:any)=>{if(table==='sync_unavailable_records')throw Error('Injected evidence write failure');return target.insert(table,value);};return Reflect.get(target,key);}});
 const handlers:any={...sync,pull:(sync as any).pullChecked??sync.pull,createNotification:notifications.create,markNotificationRead:notifications.markRead,generateUploadUrl:storage.generateUploadUrl,commitUpload:storage.commitUpload,deleteObject:storage.deleteObject};
 const result=await handlers[args.name]._handler(context,{...Object.fromEntries(Object.entries(args.input).filter(([key])=>key!=='__budget')),workspace_id:args.workspace});
 if(args.fail_after)throw Error('Injected crash before transaction commit');
 return args.input.__budget ? {result,bytes,reads,written} : result;
}});
export const authored=internalMutation({args:{workspace:v.id('workspaces'),op:v.any()},handler:(ctx,args)=>applyServerAuthoredOp(ctx,args.workspace,args.op)});
export const patch=internalMutation({args:{id:v.string(),value:v.any()},handler:(ctx,args)=>ctx.db.patch(args.id as any,args.value)});
export const insert=internalMutation({args:{table:v.string(),value:v.any()},handler:(ctx,args)=>ctx.db.insert(args.table as any,args.value)});
export const poisonReceipt=internalMutation({args:{id:v.id('change_log')},handler:(ctx,args)=>ctx.db.patch(args.id,{request_fingerprint:undefined,payload:{value:1n}})});
export const dropKey=internalMutation({args:{workspace:v.id('workspaces'),pk:v.string()},handler:async(ctx,args)=>{
 for(const row of await ctx.db.query('tombstones').withIndex('by_workspace_table_pk',q=>q.eq('workspace_id',args.workspace).eq('table_name','kv').eq('pk',args.pk)).collect())await ctx.db.delete(row._id);
}});
export const removeOwnedFixture=internalMutation({args:{workspace:v.id('workspaces')},handler:async(ctx,args)=>{
 const owner=await ctx.db.get(args.workspace);
 const member=await ctx.db.query('workspace_members').withIndex('by_workspace_user',q=>q.eq('workspace_id',args.workspace).eq('user_id',owner!.owner_user_id)).first();
 await ctx.db.patch(member!._id,{role:'owner'});
 return (workspaces.remove as any)._handler(actor(ctx,args.workspace+'a'),{workspace_id:args.workspace});
}});
export const upgradeState=internalQuery({args:{workspace:v.id('workspaces')},handler:async(ctx,args)=>{
 const result:any={};for(const table of ['threads','notifications','change_log','server_version_counter'])result[table]=await (ctx.db as any).query(table).filter((q:any)=>q.eq(q.field('workspace_id'),args.workspace)).collect();return result;
}});
export const seedExpiredSessions=internalMutation({args:{workspace:v.id('workspaces')},handler:async(ctx,args)=>{for(let i=0;i<205;i++)await ctx.db.insert('sync_snapshot_sessions',{workspace_id:args.workspace,user_id:'expired',high_watermark:1,tables:['kv'],created_at:1,expires_at:2});}});
export const gcPage=internalMutation({args:{workspace_id:v.id('workspaces'),retention_seconds:v.number(),batch_size:v.number()},handler:async(ctx,args)=>{
 const result=await (sync.runWorkspaceGc as any)._handler(ctx,args);
 const remaining=(await ctx.db.query('sync_snapshot_sessions').withIndex('by_workspace',(q)=>q.eq('workspace_id',args.workspace_id)).collect()).length;
 return {result,remaining};
}});
export const original=internalQuery({args:{id:v.string()},handler:(ctx,args)=>ctx.db.get(args.id as any)});
export const jobCount=internalQuery({args:{user:v.string()},handler:async(ctx,args)=>(await ctx.db.query('background_jobs').withIndex('by_user',q=>q.eq('user_id',args.user)).collect()).length});
export const age=internalMutation({args:{workspace:v.id('workspaces'),version:v.number(),old:v.number()},handler:async(ctx,args)=>{const row=await ctx.db.query('change_log').withIndex('by_workspace_version',q=>q.eq('workspace_id',args.workspace).eq('server_version',args.version)).first();if(row)await ctx.db.patch(row._id,{created_at:args.old});}});
// Age fixture leases in bounded transactions, including large response chunks.
export const expire=internalMutation({args:{workspace:v.id('workspaces'),old:v.number()},handler:async(ctx,args)=>{let count=0;for(const table of ['sync_snapshot_sessions','sync_snapshot_pages'] as const)for(const row of await (ctx.db as any).query(table).withIndex('by_workspace_expiry',(q:any)=>q.eq('workspace_id',args.workspace).gt('expires_at',args.old)).take(1)){await ctx.db.patch(row._id,{expires_at:args.old});count++;}return count;}});
export const read=internalQuery({args:{workspace:v.id('workspaces')},handler:async(ctx,args)=>{
 const result:any={};for(const name of ['kv','notifications','file_meta','upload_intents','change_log','server_version_counter','device_cursors','sync_snapshot_sessions','sync_snapshot_pages','sync_record_versions'])result[name]=await ctx.db.query(name as any).withIndex(({file_meta:'by_workspace_hash',kv:'by_workspace_id',device_cursors:'by_workspace_version',upload_intents:'by_workspace_status',change_log:'by_workspace_version',sync_record_versions:'by_workspace_version'} as any)[name]??'by_workspace',(q:any)=>q.eq('workspace_id',args.workspace)).collect();return result;
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
async function check(name, run) { if(process.env.OR3_SYNC_TEST_FILTER && !name.includes(process.env.OR3_SYNC_TEST_FILTER))return; try { await run(); checks.push(name); console.log('PASS ' + name); } catch (error) { failures.push({ name, error: String(error) }); console.error('FAIL ' + name + ': ' + error); } }
function start() { return Bun.spawn([binary, '--interface', '127.0.0.1', '--port', String(apiPort), '--site-proxy-port', String(sitePort), '--instance-name', instance, '--instance-secret', secret, '--disable-beacon', '--local-storage', join(root, 'storage'), join(root, 'backend.sqlite3')], { cwd: root, stdout: Bun.file(join(root, 'backend.log')), stderr: Bun.file(join(root, 'backend-errors.log')) }); }
async function ready() { for (let i = 0; i < 100; i++) { try { if ((await fetch(url + '/version')).ok) return; } catch {} await Bun.sleep(100); } throw Error('Disposable backend did not start'); }
try {
 backend = start(); await ready();
 const deploy = Bun.spawn([process.execPath, join(provider, 'node_modules/convex/bin/main.js'), 'deploy', '--yes', '--typecheck', 'enable', '--env-file', envPath], { cwd: root, env, stdout: Bun.file(join(root, 'deploy.log')), stderr: Bun.file(join(root, 'deploy-errors.log')) });
 assert.equal(await deploy.exited, 0, 'Sandbox deployment failed: ' + root);
 const initialized=await client.mutation(anyApi.syncHistory.initialize,{incarnation:crypto.randomUUID()});
 if(!initialized.ready)for(let i=0;i<100;i++){if((await client.mutation(anyApi.syncHistory.finishReplacement,{attempt_id:initialized.incarnation})).ready)break;}

 if(process.env.OR3_CONVEX_UPGRADE_PACK)await check('review: published 0.0.26 receipts and rows survive deployment upgrade', async () => {
  const candidate=join(root,'candidate');await cp(join(root,'convex'),candidate,{recursive:true});
  const packed=await readFile(process.env.OR3_CONVEX_UPGRADE_PACK);
  assert.equal(createHash('sha256').update(packed).digest('hex'),'d2897f162d47b492870f9ca23ce7329f934dd9efac9b8c0c7825d46da6aa8713','Use the exact published 0.0.26 template pack');
  const published=JSON.parse(gunzipSync(packed));
  const probe=await readFile(join(root,'convex/syncProbe.ts'));
  const deployTemplates=async(label)=>{
   const child=Bun.spawn([process.execPath,join(provider,'node_modules/convex/bin/main.js'),'deploy','--yes','--typecheck','enable','--env-file',envPath],{cwd:root,env,stdout:Bun.file(join(root,label+'.log')),stderr:Bun.file(join(root,label+'-errors.log'))});assert.equal(await child.exited,0,'Upgrade deployment failed: '+root);
  };
  let f,writes,before;
  try {
   await rm(join(root,'convex'),{recursive:true});
   for(const [name,source] of Object.entries(published.files)){await mkdir(join(root,'convex',name,'..'),{recursive:true});await writeFile(join(root,'convex',name),source);}
   await writeFile(join(root,'convex/auth.config.ts'),'export default { providers: [] };\n');await writeFile(join(root,'convex/syncProbe.ts'),probe);
   await deployTemplates('published');
   f=await client.mutation(anyApi.syncProbe.seed,{});
   writes=[
    op({table_name:'threads',pk:'old-thread',payload:{id:'old-thread',status:'active',pinned:false,forked:false,fork_reason:null,deleted:false,created_at:1,updated_at:2,clock:6}}),
    op({table_name:'notifications',pk:'old-note',payload:{id:'old-note',user_id:f.user,type:'info',title:'old',actions:[{legacy_action:'open'},'legacy-string',null],deleted:false,created_at:1,updated_at:2,clock:6}}),
    op({pk:'deleted',operation:'delete',payload:{updated_at:123,value:'keep'}}),
   ];
   for(const write of writes){const result=await invoke(f,'push',{ops:[write]});assert.equal(result.results[0].success,true,JSON.stringify(result));}
   before=await client.query(anyApi.syncProbe.upgradeState,{workspace:f.workspace});
  } finally {
   await rm(join(root,'convex'),{recursive:true});await cp(candidate,join(root,'convex'),{recursive:true});await deployTemplates('candidate-upgrade');
  }
  for(const write of writes){const replay=await invoke(f,'push',{ops:[write]});assert.equal(replay.results[0].replayed,true,JSON.stringify(replay));}
  const after=await client.query(anyApi.syncProbe.upgradeState,{workspace:f.workspace});
  assert.deepEqual(after,before,'Lost-ACK replay must not mutate original receipts, revisions or rows');
  const page=await invoke(f,'pull',{cursor:5,limit:100});PullResponseSchema.parse(page);for(const change of page.changes)if(change.op==='put')TABLE_PAYLOAD_SCHEMAS[change.tableName].parse(change.payload);
  const snapshot=await invoke(f,'snapshot',{tables:['threads','notifications'],page_size:100});SnapshotResponseSchema.parse(snapshot);for(const item of snapshot.items)if(item.kind==='row')TABLE_PAYLOAD_SCHEMAS[item.tableName].parse(item.payload);
  assert.equal(await invoke(f,'markNotificationRead',{notification_id:'old-note'}),true);
  assert.deepEqual((await client.query(anyApi.syncProbe.upgradeState,{workspace:f.workspace})).notifications[0].actions,writes[1].payload.actions);
  await writeFile(join(evidence,'published-upgrade.json'),JSON.stringify({version:'0.0.26',templateSha256:createHash('sha256').update(packed).digest('hex'),operations:writes.map(write=>write.op_id),fingerprints:before.change_log.map(row=>createHash('sha256').update(row.request_fingerprint).digest('hex')),unchangedAfterReplay:true}));
 });
 await check('C2 journal: real snapshot import rotates twice and reconciles an interrupted reset', async () => {
  const f=await client.mutation(anyApi.syncProbe.seed,{});
  const initial=await invoke(f,'snapshot',{tables:['kv'],page_size:1});
  await invoke(f,'updateDeviceCursor',{device_id:'archived',last_seen_version:5});
  const backup=join(root,'replacement.zip');
  const cli=async(args,label)=>{const process=Bun.spawn(['node',join(provider,'node_modules/convex/bin/main.js'),...args,'--env-file',envPath],{cwd:root,env,stdout:Bun.file(join(root,label+'.log')),stderr:Bun.file(join(root,label+'-errors.log'))});assert.equal(await process.exited,0,label+' failed: '+root);};
  await cli(['export','--path',backup],'export');
  const backupSha=createHash('sha256').update(await readFile(backup)).digest('hex');
  const contract=await client.query(anyApi.or3Backend.version,{});
  const identities=[];
  const runJournal=async(command,path)=>{const process=Bun.spawn(['node',join(provider,'scripts/history.mjs'),command,path],{cwd:root,env:{...env,OR3_CONVEX_BACKUP_SHA256:backupSha,OR3_CONVEX_TEMPLATE_DIGEST:contract.digest},stdout:'pipe',stderr:'pipe'});const out=await new Response(process.stdout).text(),err=await new Response(process.stderr).text();assert.equal(await process.exited,0,out+err);};
  for(let iteration=0;iteration<2;iteration++){
   const journal=join(root,'external-reset-'+iteration+'.json');await runJournal('prepare',journal);
   // Preserve the authenticated archive. This official local backend misreads
   // retained_storage/generated_schema as a _storage object in whole-zip import.
   // Import its exact documents.jsonl members through the supported table API.
   const extraction=Bun.spawn(['unzip','-o','-q',backup,'-d',join(root,'restore-members')],{stdout:'ignore',stderr:'pipe'});assert.equal(await extraction.exited,0);
   const members=await (await import('node:fs/promises')).readdir(join(root,'restore-members'));
   for(const table of members)if(!table.startsWith('_')&&table!=='README.md')await cli(['import','--table',table,'--replace','--yes',join(root,'restore-members',table,'documents.jsonl')],'import-'+iteration+'-'+table);
   const prepared=JSON.parse(await readFile(journal,'utf8'));
   if(iteration===1)await client.mutation(anyApi.syncHistory.beginReplacement,{attempt_id:prepared.attemptId,incarnation:prepared.incarnation});
   await runJournal('complete',journal);await runJournal('complete',journal);
   const receipt=JSON.parse(await readFile(journal,'utf8'));assert.equal(receipt.phase,'ready');identities.push(receipt.incarnation);
   const state=await read(f);assert.equal(state.server_version_counter[0].value,5);assert.equal(state.sync_snapshot_sessions.length,0);assert.equal(state.device_cursors.length,0);
   const reset=await invoke(f,'pull',{history_incarnation:initial.historyIncarnation,cursor:5,limit:10});assert.equal(reset.requiresSnapshot,true);assert.equal(reset.historyIncarnation,receipt.incarnation);
  }
  assert.notEqual(identities[0],identities[1]);
  await writeFile(join(evidence,'c2-journal.json'),JSON.stringify({backupSha,initial:initial.historyIncarnation,identities,interruptedResetReconciled:true,exactTemplateDigest:contract.digest}));
 });
 await check('C2 unavailable: poison retained before coverage, all-invalid snapshot completes and repair publishes', async () => {
  const f=await client.mutation(anyApi.syncProbe.seed,{});
  const source={workspace_id:f.workspace,server_version:5,table_name:'kv',pk:'a',op:'put',payload:{id:'a',name:'setting',value:'original',clock:5,created_at:1,updated_at:1},clock:5,hlc:'5:0:dev',device_id:'device',op_id:'background-tool:legacy',created_at:1};
  await client.mutation(anyApi.syncProbe.insert,{table:'change_log',value:source});
  const page=await invoke(f,'pull',{cursor:4,limit:1});PullResponseSchema.parse(page);
  assert.equal(page.changes.length,0);assert.equal(page.unavailable.length,1);assert.equal(page.nextCursor,5);
  const ref=page.unavailable[0].evidenceRef;
  assert.equal((await invoke(f,'pull',{cursor:4,limit:1})).unavailable[0].evidenceRef,ref);
  const inspected=await invoke(f,'inspectUnavailable',{});assert.equal(inspected.items.length,1);
  const exported=await invoke(f,'exportUnavailable',{evidence_ref:ref,chunk:0});assert.equal(JSON.parse(new TextDecoder().decode(exported.data)).op_id,source.op_id);
  await client.mutation(anyApi.syncProbe.patch,{id:f.row,value:{value:'x'.repeat(300*1024),op_id:source.op_id}});
  let token,markers=0,rows=0;
  do {const snapshot=await invoke(f,'snapshot',{tables:['kv'],page_size:1,page_token:token});SnapshotResponseSchema.parse(snapshot);markers+=snapshot.unavailable.length;rows+=snapshot.items.length;token=snapshot.nextPageToken;}while(token);
  assert.equal(markers,1);assert.equal(rows,1);
  const history=page.historyIncarnation;
  const repaired=await invoke(f,'repairUnavailable',{history_incarnation:history,evidence_ref:ref,replacement:{id:'a',name:'setting',value:'repaired',deleted:false,created_at:1,updated_at:2,clock:6}});
  assert.equal(repaired.status,'repaired');assert.match(repaired.opId,/^[0-9a-f-]{36}$/);
  assert.equal((await invoke(f,'revalidateUnavailable',{evidence_ref:ref})).currentDefect,null);
  const delivered=await invoke(f,'pull',{cursor:5,limit:10});assert.equal(delivered.changes[0].payload.value,'repaired');assert(delivered.changes[0].stamp.clock>5);
  await writeFile(join(evidence,'c2-unavailable.json'),JSON.stringify({page,ref,repaired,delivered}));
 });
 for(const kind of ['winner','losing-delete','replay-winner','revision','version','receipt','missing-winner','ownership']) await check('RC2 push: '+kind+' holds independently and preserves healthy siblings', async () => {
  const receipts=[];
   const f=await client.mutation(anyApi.syncProbe.seed,{});
   let bad=op({clock:1,hlc:'1:0:dev'});
   if(kind==='losing-delete')bad=op({clock:1,hlc:'1:0:dev',operation:'delete'});
   if(kind==='replay-winner')await invoke(f,'push',{ops:[bad]});
   if(['winner','losing-delete','replay-winner'].includes(kind))await client.mutation(anyApi.syncProbe.patch,{id:f.row,value:{value:'x'.repeat(300*1024)}});
   if(kind==='version')await client.mutation(anyApi.syncProbe.patch,{id:f.row,value:{server_version:Number.MAX_SAFE_INTEGER+1}});
   if(kind==='revision')await client.mutation(anyApi.syncProbe.patch,{id:f.row,value:{clock:-1}});
   if(kind==='receipt'||kind==='missing-winner'){
    await invoke(f,'push',{ops:[bad]});
    if(kind==='receipt'){
     const log=(await read(f)).change_log[0];await client.mutation(anyApi.syncProbe.poisonReceipt,{id:log._id});
    } else {
     bad=op({pk:'absent',operation:'delete'});await invoke(f,'push',{ops:[bad]});
     // Receipt exists without any authoritative canonical/tombstone winner.
     await client.mutation(anyApi.syncProbe.dropKey,{workspace:f.workspace,pk:'absent'});
    }
   }
   if(kind==='ownership'){
    bad=op({table_name:'notifications',pk:'bad-owner',operation:'delete'});
    await client.mutation(anyApi.syncProbe.insert,{table:'notifications',value:{workspace_id:f.workspace,id:'bad-owner',user_id:'',type:'info',title:'private',created_at:1,updated_at:1,deleted:false,clock:5}});
   }
   const before=await read(f);
   const healthy=op({pk:'healthy',payload:{id:'healthy',name:'healthy',deleted:false,clock:6,created_at:1,updated_at:2}});
   await assert.rejects(invoke(f,'push',{ops:[healthy,bad]}),e=>e.data?.code==='OR3_SYNC_UPDATE_REQUIRED'&&e.data?.requiredCapability==='stored-record-v1');
   assert.deepEqual(await read(f),before);
   await assert.rejects(invoke(f,'push',{stored_record_capability:'stored-record-v1',ops:[healthy,bad]},{fail_evidence:true}),/Injected evidence write failure/);
   assert.deepEqual(await read(f),before);
   const result=await invoke(f,'push',{stored_record_capability:'stored-record-v1',ops:[healthy,bad]});PushResultSchema.parse(result);
   assert.equal(result.results[0].success,true);const held=result.results[1];assert.equal(held.errorCode,'STORED_RECORD_UNAVAILABLE');assert.equal(held.success,false);assert(held.evidenceRef);assert.equal(held.serverVersion,undefined);assert.equal(held.winner,undefined);
   const exported=await invoke(f,'exportUnavailable',{evidence_ref:held.evidenceRef,chunk:0});const original=JSON.parse(new TextDecoder().decode(exported.data));
   assert.deepEqual(original.intent,bad);assert(original.evidence_ref);
   const again=await invoke(f,'push',{stored_record_capability:'stored-record-v1',ops:[bad]});assert.equal(again.results[0].evidenceRef,held.evidenceRef);
   assert.equal((await read(f)).server_version_counter[0].value,before.server_version_counter[0].value+1);
   receipts.push({kind,result,original});
  await writeFile(join(evidence,'rc2-push-'+kind+'.json'),JSON.stringify(receipts,null,2));
 });
 await check('RC2 push replacement: stale stored attachment JSON cannot abort a complete-record put', async () => {
  const f=await client.mutation(anyApi.syncProbe.seed,{});
  const payload={id:'attachment-message',thread_id:'thread',role:'user',index:0,order_key:'a',clock:5,deleted:false,created_at:1,updated_at:1};
  await client.mutation(anyApi.syncProbe.insert,{table:'messages',value:{...payload,workspace_id:f.workspace,file_hashes:'{broken',hlc:'5:0:dev',op_id:crypto.randomUUID(),server_version:5}});
  const replacement=op({table_name:'messages',pk:payload.id,payload:{...payload,clock:6}}),healthy=op({pk:'healthy',payload:{id:'healthy',name:'ok',clock:6,deleted:false,created_at:1,updated_at:1}});
  const result=await invoke(f,'push',{stored_record_capability:'stored-record-v1',ops:[healthy,replacement]});
  assert(result.results.every(r=>r.success&&r.applied));assert.equal(result.serverVersion,7);
  const page=await invoke(f,'pull',{cursor:5,limit:10});assert.equal(page.changes[1].payload.file_hashes,undefined);
  await writeFile(join(evidence,'rc2-attachment-replacement.json'),JSON.stringify({result,page}));
 });
 await check('RC2 push bounds: near-limit stored sources make bounded progress with stable holds', async () => {
  const f=await client.mutation(anyApi.syncProbe.seed,{}),ops=[],metrics=[];
  for(let i=0;i<18;i++){
   const pk='poison-'+i;
   await client.mutation(anyApi.syncProbe.insert,{table:'kv',value:{workspace_id:f.workspace,id:pk,name:'bad',value:'x'.repeat(950*1024),clock:5,hlc:'5:0:dev',op_id:crypto.randomUUID(),deleted:false,created_at:1,updated_at:1}});
   ops.push(op({pk,clock:1,payload:{id:pk,name:'intent',clock:1,deleted:false,created_at:1,updated_at:1}}));await Bun.sleep(750);
  }
  let remaining=ops;
  for(let i=0;i<30&&remaining.length;i++){
   const measured=await invoke(f,'push',{stored_record_capability:'stored-record-v1',ops:remaining,__budget:true});metrics.push(measured);
   assert(measured.bytes<16*1024*1024);assert(measured.written<16*1024*1024);PushResultSchema.parse(measured.result);
   const held=measured.result.results.filter(r=>r.errorCode==='STORED_RECORD_UNAVAILABLE');assert(held.length>0);
   remaining=remaining.filter(op=>!held.some(r=>r.opId===op.op_id));await Bun.sleep(750);
  }
  assert.equal(remaining.length,0);assert.equal((await client.query(anyApi.syncProbe.upgradeState,{workspace:f.workspace})).server_version_counter[0].value,5);
  await writeFile(join(evidence,'rc2-push-budgets.json'),JSON.stringify(metrics,null,2));
 });
 await check('C2 incarnation: stale pushes and ACKs never mutate replacement and old snapshots reject', async () => {
  const f=await client.mutation(anyApi.syncProbe.seed,{});
  const lost=op();await invoke(f,'push',{ops:[lost]});
  const poison={workspace_id:f.workspace,server_version:5,table_name:'kv',pk:'legacy',op:'put',payload:'bad',clock:5,hlc:'5:0:dev',device_id:'device',op_id:'legacy',created_at:1};
  await client.mutation(anyApi.syncProbe.insert,{table:'change_log',value:poison});
  const oldEvidence=(await invoke(f,'pull',{cursor:4,limit:1})).unavailable[0].evidenceRef;
  const oldExport=await invoke(f,'exportUnavailable',{evidence_ref:oldEvidence,chunk:0});
  const before=await read(f);
  await assert.rejects(invoke(f,'push',{history_incarnation:'discarded-history',ops:[op()]}),/OR3_SYNC_HISTORY_CHANGED/);
  assert.deepEqual(await read(f),before);
  const initial=await invoke(f,'snapshot',{tables:['kv'],page_size:1});
  const old=initial.historyIncarnation;
  const next=crypto.randomUUID();const attempt=crypto.randomUUID();
  await client.mutation(anyApi.syncHistory.beginReplacement,{attempt_id:attempt,incarnation:next});
  await assert.rejects(invoke(f,'push',{ops:[op()]}),/OR3_SYNC_HISTORY/);
  for(let i=0;i<100;i++){const reset=await client.mutation(anyApi.syncHistory.finishReplacement,{attempt_id:attempt});if(reset.ready)break;assert(i<99);}
  await assert.rejects(invoke(f,'push',{history_incarnation:old,ops:[lost]}),/OR3_SYNC_HISTORY_CHANGED/);
  assert.deepEqual(await invoke(f,'exportUnavailable',{evidence_ref:oldEvidence,chunk:0}),oldExport);
  const newEvidence=(await invoke(f,'pull',{cursor:4,limit:1})).unavailable[0].evidenceRef;assert.notEqual(newEvidence,oldEvidence);
  await assert.rejects(invoke(f,'repairUnavailable',{history_incarnation:next,evidence_ref:oldEvidence,replacement:{}}),/OR3_SYNC_HISTORY_CHANGED/);
  assert.equal((await invoke(f,'push',{ops:[op({clock:7,hlc:'7:0:dev',payload:{id:'a',name:'new-history',clock:7,deleted:false,created_at:1,updated_at:2}})]})).results[0].success,true);
  for(const cursor of [0,5,6,120]){const page=await invoke(f,'pull',{history_incarnation:old,cursor,limit:1});assert.equal(page.historyIncarnation,next);assert.equal(page.requiresSnapshot,true);assert.equal(page.changes.length,0);assert.equal(page.nextCursor,cursor);}
  await assert.rejects(invoke(f,'updateDeviceCursor',{history_incarnation:old,device_id:'old',last_seen_version:5}),/OR3_SYNC_HISTORY_CHANGED/);
  await assert.rejects(invoke(f,'snapshot',{history_incarnation:next,page_size:1,tables:['kv'],page_token:initial.nextPageToken}),/OR3_SYNC_HISTORY_CHANGED/);
  assert.equal((await read(f)).device_cursors.length,0);assert.equal((await read(f)).sync_snapshot_sessions.length,0);
  await writeFile(join(evidence,'c2-incarnation.json'),JSON.stringify({old,next,attempt,unchangedAfterStalePush:true}));
 });
 await check('C2 containment: variants, private notifications, evidence GC survival and old-client boundary', async () => {
  const f=await client.mutation(anyApi.syncProbe.seed,{});
  const base={workspace_id:f.workspace,table_name:'kv',op:'put',clock:5,hlc:'5:0:dev',device_id:'device',op_id:crypto.randomUUID(),created_at:1};
  const variants=[{payload:'{broken'}, {payload:{id:'bad',name:7}}, {table_name:'unknown',payload:{}}, {hlc:'x'.repeat(513),payload:{}}, {payload:{id:'bad',name:'oversize',value:'x'.repeat(300*1024)}}, {op:'delete',clock:-1}];
  for(let index=0;index<variants.length;index++)await client.mutation(anyApi.syncProbe.insert,{table:'change_log',value:{...base,server_version:index+6,pk:'bad-'+index,...variants[index]}});
  const counter=(await read(f)).server_version_counter[0];await client.mutation(anyApi.syncProbe.patch,{id:counter._id,value:{value:20}});
  let cursor=5,refs=[];
  for(let index=0;index<variants.length;index++){
   const page=await invoke(f,'pull',{cursor,limit:1});PullResponseSchema.parse(page);assert.equal(page.changes.length,0);assert.equal(page.unavailable.length,1);assert(page.nextCursor>cursor);cursor=page.nextCursor;refs.push(page.unavailable[0].evidenceRef);
  }
  for(const [index,owner]of [[0,undefined],[1,f.other]])await client.mutation(anyApi.syncProbe.insert,{table:'change_log',value:{...base,server_version:12+index,pk:'private-'+index,table_name:'notifications',payload:{...(owner?{user_id:owner}:{}),actions:{bad:true}}}});
  const privatePage=await invoke(f,'pull',{cursor:11,limit:10});assert.equal(privatePage.changes.length,0);assert.equal(privatePage.unavailable.length,0);assert.equal(privatePage.nextCursor,13);
  const inspect=await invoke(f,'inspectUnavailable',{});assert.equal(inspect.items.length,8);
  const original=await invoke(f,'exportUnavailable',{evidence_ref:refs[0],chunk:0});
  // Source GC and session expiry must not delete independent evidence.
  for(let version=6;version<=13;version++)await client.mutation(anyApi.syncProbe.age,{workspace:f.workspace,version,old:1});
  await invoke(f,'updateDeviceCursor',{device_id:'gc',last_seen_version:20});
  await invoke(f,'runWorkspaceGc',{retention_seconds:3600,batch_size:100});
  assert.deepEqual(await invoke(f,'exportUnavailable',{evidence_ref:refs[0],chunk:0}),original);
  for(const [name,input]of [['pull',{cursor:0,limit:1}],['snapshot',{page_size:1}],['push',{ops:[]}],['updateDeviceCursor',{device_id:'old',last_seen_version:0}],['watchChanges',{}]])await assert.rejects(invoke(f,name,{...input,history_incarnation_capability:'old'}),error=>error.data?.code==='OR3_SYNC_UPDATE_REQUIRED'&&error.data?.statusCode===426);
  await writeFile(join(evidence,'c2-containment.json'),JSON.stringify({variants:variants.length,refs,privatePage,evidenceSurvivedGc:true,oldClientEndpoints:5}));
 });
 await check('C2 legacy query: old pull clients receive an explicit structured update boundary', async () => {
  const f=await client.mutation(anyApi.syncProbe.seed,{});
  const oldClient=new ConvexHttpClient(url,{logger:false});oldClient.setAdminAuth(adminKey,{subject:f.workspace+'a',issuer:'https://clerk.example.test'});
  await assert.rejects(oldClient.query(anyApi.sync.pull,{workspace_id:f.workspace,cursor:0,limit:1}),error=>error.data?.code==='OR3_SYNC_UPDATE_REQUIRED'&&error.data?.statusCode===426);
 });
 await check('C2 bounds: near-limit native evidence survives lossless chunk export below transaction limits', async () => {
  const f=await client.mutation(anyApi.syncProbe.seed,{});
  const value='\0'.repeat(950*1024);
  await client.mutation(anyApi.syncProbe.insert,{table:'change_log',value:{workspace_id:f.workspace,server_version:5,table_name:'kv',pk:'large-poison',op:'put',payload:{id:'large-poison',value,bytes:new Uint8Array([0,255]).buffer,int:123n,nonfinite:NaN},clock:5,hlc:'5:0:dev',device_id:'dev',op_id:crypto.randomUUID(),created_at:Math.floor(Date.now()/1000)}});
  const measured=await invoke(f,'pull',{cursor:4,limit:1,__budget:true});const page=measured.result;
  PullResponseSchema.parse(page);assert.equal(page.nextCursor,5);assert.equal(page.unavailable.length,1);
  assert(measured.bytes<16*1024*1024);assert(measured.written<16*1024*1024);
  const ref=page.unavailable[0].evidenceRef;
  const parts=[];let chunks;
  for(let chunk=0;chunks===undefined||chunk<chunks;chunk++){
   const part=await invoke(f,'exportUnavailable',{evidence_ref:ref,chunk});chunks=part.chunks;
   assert(part.data.byteLength<=32768);parts.push(new Uint8Array(part.data));
  }
  const serialized=Buffer.concat(parts);const {jsonToConvex}=await import('convex/values');
  const original=jsonToConvex(JSON.parse(serialized.toString('utf8')));
  assert.equal(original.payload.value,value);assert.deepEqual(new Uint8Array(original.payload.bytes),new Uint8Array([0,255]));assert.equal(original.payload.int,123n);assert(Number.isNaN(original.payload.nonfinite));
  assert.equal((await invoke(f,'pull',{cursor:4,limit:1})).unavailable[0].evidenceRef,ref);
  await writeFile(join(evidence,'c2-native-evidence-budgets.json'),JSON.stringify({readBytes:measured.bytes,writtenBytes:measured.written,exportBytes:serialized.byteLength,chunks,sourceStringBytes:value.length,losslessValues:true}));
 });
 await check('C2 mixed: healthy siblings survive poison and failed evidence cannot advance coverage', async () => {
  const f=await client.mutation(anyApi.syncProbe.seed,{});
  const writes=[op({pk:'before'}),op({pk:'poison'}),op({pk:'after'})];
  for(const write of writes){write.payload.id=write.pk;assert.equal((await invoke(f,'push',{ops:[write]})).results[0].success,true);}
  const state=await read(f);const middle=state.change_log.find(row=>row.pk==='poison');await client.mutation(anyApi.syncProbe.patch,{id:middle._id,value:{op_id:'invalid'}});
  await assert.rejects(invoke(f,'pull',{cursor:5,limit:3},{fail_evidence:true}),/Injected evidence write failure/);
  const page=await invoke(f,'pull',{cursor:5,limit:3});PullResponseSchema.parse(page);assert.deepEqual(page.changes.map(row=>row.pk),['before','after']);assert.deepEqual(page.unavailable.map(row=>row.pk),['poison']);assert.equal(page.nextCursor,8);
  await assert.rejects(invoke(f,'retainUnavailableRecord',{history_incarnation:page.historyIncarnation,source:'pull',record:page.changes[0],marker:{tableName:'kv',pk:'forged',originalKind:'put',serverVersion:6,reason:'invalid'}}),/source mismatch/);
  const healthy=await invoke(f,'push',{ops:[op({pk:'poison',clock:7,hlc:'7:0:dev',payload:{id:'poison',name:'healthy',clock:7,deleted:false,created_at:1,updated_at:2}})]});assert.equal(healthy.results[0].success,true);
  const repair=await invoke(f,'repairUnavailable',{history_incarnation:page.historyIncarnation,evidence_ref:page.unavailable[0].evidenceRef,replacement:{}});assert.equal(repair.status,'superseded');
  assert.equal((await read(f)).server_version_counter[0].value,9);
 });
 await check('C2 snapshot: corrupt deletion revision is unavailable on the final page', async () => {
  const f=await client.mutation(anyApi.syncProbe.seed,{});
  await client.mutation(anyApi.syncProbe.insert,{table:'tombstones',value:{workspace_id:f.workspace,table_name:'kv',pk:'zz',deleted_at:-1,server_deleted_at:-1,clock:-1,hlc:'',op_id:'',server_version:5,created_at:1}});
  let token,final;
  do{final=await invoke(f,'snapshot',{tables:['kv'],page_size:1,page_token:token});token=final.nextPageToken;}while(token);
  assert.equal(final.items.length,0);assert.equal(final.unavailable.length,1);assert.equal(final.unavailable[0].pk,'zz');assert.equal(final.unavailable[0].originalKind,'tombstone');
  const original=await invoke(f,'exportUnavailable',{evidence_ref:final.unavailable[0].evidenceRef,chunk:0});assert.equal(JSON.parse(new TextDecoder().decode(original.data)).clock,-1);
 });
 await check('review: workspace removal drains persisted snapshot pages', async () => {
  const f=await client.mutation(anyApi.syncProbe.seed,{});
  const first=await invoke(f,'snapshot',{tables:['kv'],page_size:1});
  await invoke(f,'snapshot',{tables:['kv'],page_size:1,page_token:first.nextPageToken});
  assert.equal((await read(f)).sync_snapshot_pages.length,1);
  await client.mutation(anyApi.syncProbe.removeOwnedFixture,{workspace:f.workspace});
  for(let i=0;i<30 && (await read(f)).sync_snapshot_pages.length;i++)await Bun.sleep(100);
  assert.equal((await read(f)).sync_snapshot_pages.length,0);
  await assert.rejects(invoke(f,'snapshot',{tables:['kv'],page_size:1,page_token:first.nextPageToken}),/Unauthorized|Forbidden|Workspace|workspace/);
 });
 await check('review: identical snapshot tokens replay identical pages after concurrent writes', async () => {
  const f=await client.mutation(anyApi.syncProbe.seed,{});
  const first=await invoke(f,'snapshot',{tables:['kv'],page_size:1});
  const request={tables:['kv'],page_size:1,page_token:first.nextPageToken};
  const second=await invoke(f,'snapshot',request);
  assert.equal(second.items[0].pk,'z');
  for(const id of ['b','z']){const result=await invoke(f,'push',{ops:[op({pk:id,payload:{id,name:'setting',value:'x'.repeat(240*1024),deleted:false,clock:6,created_at:1,updated_at:2}})]});assert.equal(result.results[0].success,true,JSON.stringify(result));}
  assert.deepEqual(await invoke(f,'snapshot',request),second);
  await assert.rejects(invoke(f,'snapshot',request,{actor:'b'}),/unavailable/);
  await writeFile(join(evidence,'snapshot-retry.json'),JSON.stringify({first,second,retry:await invoke(f,'snapshot',request)}));
 });
 await check('70 large KV rows pull, snapshot and combined GC advance below transaction limits', async () => {
  const f=await client.mutation(anyApi.syncProbe.seed,{});
  const budgets=[];
  const measure=async(name,input)=>{let result;try{result=await invoke(f,name,{...input,__budget:true});}catch(error){throw new Error(name+' after '+budgets.length+' measured calls: '+String(error));}budgets.push({name,bytes:result.bytes,reads:result.reads,written:result.written});assert(result.bytes<16*1024*1024);assert(result.written<16*1024*1024);return result.result;};
  for(let i=0;i<70;i++){await Bun.sleep(750);const id='large-'+String(i).padStart(3,'0');const write=op({pk:id,payload:{id,name:'large',value:'x'.repeat(240*1024),clock:6,created_at:1,updated_at:2,deleted:false}});assert.equal((await invoke(f,'push',{ops:[write]})).results[0].success,true);}
  let cursor=5,count=0;
  for(let i=0;i<100;i++){const page=await measure('pull',{cursor,limit:300});PullResponseSchema.parse(page);count+=page.changes.length;if(!page.hasMore)break;assert(page.nextCursor>cursor);cursor=page.nextCursor;}
  assert.equal(count,70);
  let token,countSnapshot=0;
  for(let i=0;i<100;i++){if(token)await Bun.sleep(750);const request={tables:['kv'],page_size:300,page_token:token};const page=await measure('snapshot',request);if(token)assert.deepEqual(await measure('snapshot',request),page);SnapshotResponseSchema.parse(page);for(const item of page.items)if(item.kind==='row')TABLE_PAYLOAD_SCHEMAS.kv.parse(item.payload);countSnapshot+=page.items.length;if(!page.nextPageToken)break;assert.notEqual(page.nextPageToken,token);token=page.nextPageToken;}
  assert.equal(countSnapshot,72);
  // Overwrite the same records: each transaction creates a large preimage.
  for(let i=0;i<70;i++){await Bun.sleep(750);const id='large-'+String(i).padStart(3,'0');assert.equal((await invoke(f,'push',{ops:[op({pk:id,clock:7,hlc:'7:0:dev',payload:{id,name:'large',value:'y'.repeat(240*1024),clock:7,created_at:1,updated_at:3,deleted:false}})]})).results[0].success,true);}
  const old=Math.floor(Date.now()/1000)-7200;
  // Patch one indexed page at a time; never use the unbounded fixture reader.
  for(let cursor=5;;){const page=await invoke(f,'pull',{cursor,limit:1});for(const change of page.changes)await client.mutation(anyApi.syncProbe.age,{workspace:f.workspace,version:change.serverVersion,old});if(!page.hasMore)break;cursor=page.nextCursor;}
  for(let i=0;i<1000;i++){await Bun.sleep(750);if(!await client.mutation(anyApi.syncProbe.expire,{workspace:f.workspace,old}))break;assert(i<999,'Fixture expiry must drain');}
  let gc;
  for(let i=0;i<200;i++){gc=await measure('runWorkspaceGc',{retention_seconds:3600,batch_size:100,continuation_count:999});if(!gc.hasMore)break;}
  assert.equal(gc.hasMore,false);
  const empty=await invoke(f,'pull',{cursor:0,limit:300});assert.equal(empty.changes.length,0);assert.equal(empty.requiresSnapshot,true);
  await writeFile(join(evidence,'large-record-budgets.json'),JSON.stringify(budgets,null,2));
 });
 await check('public push rejects null fork reason atomically', async () => {
  const f=await client.mutation(anyApi.syncProbe.seed,{});
  const payload={id:'thread',status:'active',pinned:false,forked:false,deleted:false,created_at:1,updated_at:1,clock:1,fork_reason:null};
  const invalid=await invoke(f,'push',{ops:[op({table_name:'threads',pk:'thread',clock:1,hlc:'1:0:dev',payload})]});assert.equal(invalid.results[0].success,false);
  const state=await read(f);assert.equal(state.server_version_counter[0].value,5);assert.equal(state.change_log.length,0);
 });
 await check('notification authoring rejects non-array actions atomically', async () => {
  const f=await client.mutation(anyApi.syncProbe.seed,{});
  await assert.rejects(invoke(f,'createNotification',{user_id:f.user,type:'info',title:'bad',actions:{}}),/Invalid payload/);
  assert.equal((await read(f)).server_version_counter[0].value,5);
 });
 await check('background timestamp rejection precedes writes and finalizer clock overflow is atomic', async () => {
  const f=await client.mutation(anyApi.syncProbe.seed,{});const record={id:'assistant',thread_id:'thread',role:'assistant',index:1,order_key:'a',pending:true,deleted:false,clock:1,data:{generation_id:'generation'}};
  const admission={actor_user_id:f.user,generation_id:'generation',message_id:'assistant',kind:'new-turn',fingerprint:'admission',device_id:'background:generation',records:[{table_name:'messages',op_id:crypto.randomUUID(),record}]};
  await assert.rejects(invoke(f,'admitChatGeneration',admission),/Invalid payload/);
  assert.equal((await read(f)).server_version_counter[0].value,5);
 });
 await check('background finalizer rejects safe-integer exhaustion before receipt or allocation', async () => {
  const f=await client.mutation(anyApi.syncProbe.seed,{});const record={id:'assistant',thread_id:'thread',role:'assistant',index:1,order_key:'a',pending:true,deleted:false,clock:Number.MAX_SAFE_INTEGER,created_at:1,updated_at:1,data:{generation_id:'generation'}};
  await invoke(f,'admitChatGeneration',{actor_user_id:f.user,generation_id:'generation',message_id:'assistant',kind:'new-turn',fingerprint:'admission',device_id:'background:generation',records:[{table_name:'messages',op_id:crypto.randomUUID(),record}]});
  await assert.rejects(invoke(f,'finalizeChatGeneration',{actor_user_id:f.user,generation_id:'generation',message_id:'assistant',admission_clock:record.clock,fingerprint:'final',device_id:'background:generation',op_id:crypto.randomUUID(),snapshot:{status:'complete',content:'ok',reasoning:'',completedAt:2000}}),/clock|revision/i);
  assert.equal((await read(f)).server_version_counter[0].value,6);
 });
 await check('legacy null fork reason reads losslessly without changing stored revision', async () => {
  const f=await client.mutation(anyApi.syncProbe.seed,{});
  const thread={workspace_id:f.workspace,id:'legacy',status:'active',pinned:false,forked:false,fork_reason:null,deleted:false,created_at:1,updated_at:1,clock:1,hlc:'1:0:dev',op_id:crypto.randomUUID(),server_version:5};
  const stored=await client.mutation(anyApi.syncProbe.insert,{table:'threads',value:thread});
  await client.mutation(anyApi.syncProbe.insert,{table:'change_log',value:{workspace_id:f.workspace,server_version:5,table_name:'threads',pk:'legacy',op:'put',payload:Object.fromEntries(Object.entries(thread).filter(([key])=>key!=='workspace_id')),clock:1,hlc:'1:0:dev',device_id:'dev',op_id:thread.op_id,created_at:1}});
  const pull=await invoke(f,'pull',{cursor:4,limit:300});TABLE_PAYLOAD_SCHEMAS.threads.parse(pull.changes[0].payload);assert.equal(pull.changes[0].payload.fork_reason,undefined);
  const page=await invoke(f,'snapshot',{tables:['threads'],page_size:100});TABLE_PAYLOAD_SCHEMAS.threads.parse(page.items[0].payload);assert.equal(page.items[0].payload.fork_reason,undefined);assert.equal(page.items[0].revision.opId,thread.op_id);
  const original=await client.query(anyApi.syncProbe.original,{id:stored});assert.equal(original.fork_reason,null);assert.equal(original.op_id,thread.op_id);
 });
 await check('new background jobs reject malformed canonical admission before durable acceptance', async () => {
  const f=await client.mutation(anyApi.syncProbe.seed,{});
  const record={id:'assistant',thread_id:'thread',role:'assistant',index:1,order_key:'a',pending:true,deleted:false,clock:1,data:{generation_id:'generation'}};
  const history={version:1,kind:'continuation',admissionId:'admission',generationId:'generation',workspaceId:f.workspace,threadId:'thread',messageId:'assistant',expectedAssistant:{clock:0},assistantMessage:record};
  const input={user_id:f.user,thread_id:'thread',message_id:'assistant',model:'test',kind:'chat',generation_id:'generation',idempotency_key:'admission',history_phase:'admission_pending',sync_provider_id:'convex',execution:{version:1,workspaceId:f.workspace,body:{},history},max_concurrent_jobs:20,max_concurrent_jobs_per_user:5};
  await assert.rejects(client.mutation(anyApi.backgroundJobs.create,input),/Invalid payload/);
  assert.equal(await client.query(anyApi.syncProbe.jobCount,{user:f.user}),0);
  record.created_at=1;record.updated_at=1;
  history.generationId='g'.repeat(246);
  await assert.rejects(client.mutation(anyApi.backgroundJobs.create,input),/Invalid sync stamp/);
  assert.equal(await client.query(anyApi.syncProbe.jobCount,{user:f.user}),0);
  history.generationId='generation';assert.ok((await client.mutation(anyApi.backgroundJobs.create,input)).jobId);
 });
 await check('UTF-8 merge and long-key snapshot continuation match the canonical reader', async () => {
  const f=await client.mutation(anyApi.syncProbe.seed,{});
  const keys=['a\0b','a\0',':'.repeat(1024),'漢'.repeat(1024),'\u{10000}','\ue000'];
  for(const id of keys)assert.equal((await invoke(f,'push',{ops:[op({pk:id,payload:{id,name:'unicode',clock:6,created_at:1,updated_at:1,deleted:false}})]})).results[0].success,true);
  await client.mutation(anyApi.syncProbe.insert,{table:'tombstones',value:{workspace_id:f.workspace,table_name:'kv',pk:'\ue001',clock:1,hlc:'1:0:dev',op_id:crypto.randomUUID(),server_version:1,deleted_at:1,server_deleted_at:1,created_at:1}});
  const tuples=[];let token;
  for(let i=0;i<30;i++){const page=await invoke(f,'snapshot',{tables:['kv'],page_size:2,page_token:token});SnapshotResponseSchema.parse(page);tuples.push(...page.items.map(row=>[row.tableName,row.pk,row.kind]));if(!page.nextPageToken)break;assert.notEqual(page.nextPageToken,token);token=page.nextPageToken;}
  await writeFile(join(evidence,'ordering-tuples.json'),JSON.stringify(tuples));
  assert.deepEqual(tuples,[...tuples].sort(compareSyncTuple));assert.equal(tuples.length,9,JSON.stringify(tuples.map(row=>[row[0],row[1].slice(0,12),row[2]])));
 });
 await check('server-authored delete placeholder preserves envelope identity and deletion', async () => {
  const f=await client.mutation(anyApi.syncProbe.seed,{});
  await client.mutation(anyApi.syncProbe.authored,{workspace:f.workspace,op:{table:'kv',operation:'delete',pk:'wanted',payload:{id:'different',name:'setting',deleted:false,created_at:1}}});
  const state=await read(f);assert.equal(state.kv.find(row=>row.id==='wanted')?.deleted,true);assert.equal(state.kv.some(row=>row.id==='different'),false);
 });
 await check('server-authored wire payload remains within the canonical byte ceiling at the boundary', async () => {
  const f=await client.mutation(anyApi.syncProbe.seed,{});const now=Math.floor(Date.now()/1000);
  const shape={id:'a'.repeat(36),user_id:f.user,type:'info',title:'boundary',body:'',created_at:now,deleted:false,clock:now,hlc:now+':server:'+'a'.repeat(8),op_id:'a'.repeat(36),updated_at:now};
  const body='x'.repeat(256*1024-new TextEncoder().encode(JSON.stringify(shape)).length);
  await invoke(f,'createNotification',{user_id:f.user,type:'info',title:'boundary',body});
  const state=await read(f);assert(new TextEncoder().encode(JSON.stringify(state.change_log[0].payload)).length<=256*1024);
  assert.equal(state.notifications[0].server_version,state.change_log[0].server_version);
 });
 await check('canonical normalization preserves raw receipt fingerprints and exact delete payloads', async () => {
  const f=await client.mutation(anyApi.syncProbe.seed,{});
  const write=op({payload:{id:'a',name:'setting',deleted:false,createdAt:1,updatedAt:2,clock:6}});
  const first=await invoke(f,'push',{ops:[write]});assert.equal(first.results[0].success,true);
  const replay=await invoke(f,'push',{ops:[write]});assert.equal(replay.results[0].replayed,true);assert.equal(replay.results[0].serverVersion,first.results[0].serverVersion);
  const state=await read(f);assert.deepEqual(JSON.parse(state.change_log[0].request_fingerprint).payload,write.payload);assert.equal(state.change_log[0].payload.created_at,1);assert.equal(state.change_log[0].payload.createdAt,undefined);
  const deletion=op({operation:'delete',clock:7,hlc:'7:0:dev',payload:{updatedAt:123,custom:{keep:true}}});
  assert.equal((await invoke(f,'push',{ops:[deletion]})).results[0].success,true);
  assert.equal((await invoke(f,'push',{ops:[deletion]})).results[0].replayed,true);
  assert.deepEqual((await read(f)).change_log[1].payload,deletion.payload);
 });
 await check('shipped direct and gateway readers qualify against this deployed backend', async () => {
  const f=await client.mutation(anyApi.syncProbe.seed,{});
  for(const id of ['a\0b',':'.repeat(1024),'漢'.repeat(1024),'\ue000','\u{10000}'])assert.equal((await invoke(f,'push',{ops:[op({pk:id,payload:{id,name:'reader',clock:6,created_at:1,updated_at:1,deleted:false}})]})).results[0].success,true);
  const qualificationFile=join(root,'reader-qualification.json');
  await writeFile(qualificationFile,JSON.stringify({url,adminKey,workspace:f.workspace}),{mode:0o600});
  const child=Bun.spawn(['node',join(provider,'node_modules/vitest/vitest.mjs'),'run','src/runtime/server/sync/__tests__/convex-sync-gateway-adapter.test.ts','-t','qualifies direct and gateway readers against the deployed backend'],{cwd:provider,env:{...process.env,OR3_CANONICAL_HOST_ROOT:host,OR3_CONVEX_QUALIFICATION_FILE:qualificationFile},stdout:'pipe',stderr:'pipe'});
  const [stdout,stderr,code]=await Promise.all([new Response(child.stdout).text(),new Response(child.stderr).text(),child.exited]);
  assert(!(stdout+stderr).includes(adminKey),'Reader qualification output leaked credentials');
  await writeFile(join(evidence,'direct-gateway-readers.log'),stdout+stderr);assert.equal(code,0,'Reader qualification failed; see direct-gateway-readers.log');
 });
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
  const state=await read(f);for(const session of [...state.sync_snapshot_sessions,...state.sync_snapshot_pages])await client.mutation(anyApi.syncProbe.patch,{id:session._id,value:{expires_at:old}});
  await invoke(f,'runWorkspaceGc',{retention_seconds:3600,batch_size:20});
  const after=await read(f);assert.equal(after.sync_snapshot_sessions.length,0);assert.equal(after.sync_record_versions.length,0);assert.equal(after.sync_snapshot_pages.length,0);
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
 await check('tool chunks survive rollback, retry, sync and backend restart without duplicating versions', async () => {
  const f=await client.mutation(anyApi.syncProbe.seed,{});
  const generation='artifact-generation', thread='artifact-thread', message='artifact-assistant';
  await invoke(f,'admitChatGeneration',{actor_user_id:f.user,generation_id:generation,message_id:message,kind:'new-turn',
   fingerprint:'artifact-admission',device_id:'background:'+generation,records:[
    {table_name:'threads',op_id:crypto.randomUUID(),record:{id:thread,clock:1,status:'active',forked:false,pinned:false,deleted:false,created_at:1,updated_at:1}},
    {table_name:'messages',op_id:crypto.randomUUID(),record:{id:'artifact-user',thread_id:thread,role:'user',clock:1,index:0,order_key:'a',deleted:false,created_at:1,updated_at:1,data:{content:'research'}}},
    {table_name:'messages',op_id:crypto.randomUUID(),record:{id:message,thread_id:thread,role:'assistant',clock:1,index:1,order_key:'b',pending:true,deleted:false,created_at:1,updated_at:1,data:{generation_id:generation}}},
   ]});
  const parent={workspace_id:f.workspace,actor_user_id:f.user,generation_id:generation,message_id:message,thread_id:thread,admission_clock:1,fingerprint:'artifact-admission'};
  const content='x'.repeat(24*1024);
  const prepared=await Promise.all(Array.from({length:9},(_,i)=>prepareToolResultStorage(content,{threadId:thread,parentAssistantId:message,callId:'read-'+i})));
  assert(prepared.every(item=>item.rows.length===1));
  const input={...parent,device_id:'background:'+generation,rows:prepared.flatMap(item=>item.rows.map(record=>({record,op_id:crypto.randomUUID()})))};
  const before=(await read(f)).server_version_counter[0].value;
  await assert.rejects(invoke(f,'retainChatToolResult',input,{fail_after:true}),/Injected crash/);
  assert.equal((await read(f)).server_version_counter[0].value,before);
  assert.equal((await read(f)).kv.filter(row=>row.id.startsWith('tool-result:')).length,0);
  await invoke(f,'retainChatToolResult',input);
  await invoke(f,'retainChatToolResult',input);
  assert.equal((await read(f)).server_version_counter[0].value,before+9);
  backend.kill('SIGKILL');await backend.exited;backend=start();await ready();
  await invoke(f,'retainChatToolResult',input);
  const pulled=await invoke(f,'pull',{cursor:before,limit:100});
  assert.equal(pulled.changes.length,9);
  const chunks=new Map(pulled.changes.map(change=>{
   assert.match(change.stamp.opId,/^[a-f0-9]{8}-[a-f0-9]{4}-4[a-f0-9]{3}-[89ab][a-f0-9]{3}-[a-f0-9]{12}$/);
   return [change.pk,change.payload];
  }));
  for(const item of prepared){
   assert.equal(await readToolResultStorage(item.result,async id=>chunks.get(id)),content);
   const ids=item.rows.map(row=>row.id);
   assert.equal(await client.query(anyApi.sync.readChatToolResult,{...parent,ids}),content);
  }
  assert.equal((await read(f)).server_version_counter[0].value,before+9);
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
