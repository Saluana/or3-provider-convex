#!/usr/bin/env bun
// Real, disposable Convex upgrade proof. Never accepts a deployed backend URL.
import assert from 'node:assert/strict';
import { cp, mkdir, mkdtemp, readFile, writeFile, symlink, rm, readdir } from 'node:fs/promises';
import { randomBytes } from 'node:crypto';
import { gunzipSync } from 'node:zlib';
import { resolve, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { tmpdir } from 'node:os';
import { mock } from 'bun:test';

const [binary, host, basePack, evidence, qualifiedProvider] = process.argv.slice(2).map((p) => resolve(p));
assert(binary?.startsWith(join(process.env.HOME, '.cache/convex/binaries/')) && binary.endsWith('/convex-local-backend'));
assert(host && basePack && evidence, 'Usage: bun backend-lifecycle.mjs <cached-binary> <host-source> <base-pack> <evidence-dir>');
const signupOnly = process.argv[7] === 'signup';
const provider = qualifiedProvider ?? resolve(fileURLToPath(new URL('../..', import.meta.url)));
const root = await mkdtemp(join(tmpdir(), 'or3-convex-upgrade-'));
await mkdir(evidence, { recursive: true });
await cp(join(host, 'shared'), join(root, 'shared'), { recursive: true });
const dependencies = {};
for (const name of ['convex', 'typescript', '@types/node']) {
    dependencies[name] = JSON.parse(await readFile(join(host, 'node_modules', name, 'package.json'), 'utf8')).version;
}
await writeFile(join(root, 'package.json'), JSON.stringify({ private: true, type: 'module', dependencies }));
const install = Bun.spawn([process.execPath, 'install', '--ignore-scripts'], { cwd: root, stdout: 'pipe', stderr: 'pipe' });
const [installOutput, installErrors, installCode] = await Promise.all([new Response(install.stdout).text(), new Response(install.stderr).text(), install.exited]);
assert.equal(installCode, 0, installOutput + installErrors);
const old = JSON.parse(gunzipSync(await readFile(basePack)).toString());
for (const [name, contents] of Object.entries(old.files)) {
    const target = join(root, 'convex', name);
    await mkdir(resolve(target, '..'), { recursive: true });
    await writeFile(target, contents);
}
if (!signupOnly && old.files['or3Backend.ts']) {
    // Current published installations include the manifest written by their
    // initializer. Exercise that owner rather than inventing an install receipt.
    const { initializeTemplates } = await import(join(resolve(basePack, '../..'), 'scripts/backend.mjs'));
    initializeTemplates(root);
}
// A real extra function/table-independent persistent record, preserved by deploy.
await writeFile(join(root, 'convex', 'upgradeProbe.ts'), `
import { internalMutation, internalQuery } from './_generated/server';
import { v } from 'convex/values';
${signupOnly ? `import { acceptSignupInviteAndProvisionUser as signup } from './workspaces';
export const rollbackSignup = internalMutation({args:{input:v.any()},handler:async(ctx,args)=>{await (signup as any)._handler(ctx,args.input);throw Error('Injected post-provision failure');}});
export const snapshotSignup = internalQuery({args:{},handler:async(ctx)=>({users:await ctx.db.query('users').collect(), accounts:await ctx.db.query('auth_accounts').collect(),members:await ctx.db.query('workspace_members').collect()})});
export const expireSignupHistory = internalMutation({args:{},handler:async(ctx)=>{for(const row of await ctx.db.query('signup_invites').take(200)) await ctx.db.patch(row._id,{expires_at:1,retain_until:1});}});` : ''}
${signupOnly ? `export const seedLegacyUsers = internalMutation({args:{offset:v.number(),count:v.number()},handler:async(ctx,args)=>{for(let i=args.offset;i<args.offset+args.count;i++) await ctx.db.insert('users',{email:' Legacy-'+i+'@Example.Test ',created_at:1});}});` : ''}
export const put = internalMutation({args:{},handler:async(ctx)=>{const owner=await ctx.db.insert('users',{created_at:1});return ctx.db.insert('workspaces',{name:'Upgrade persistence probe',owner_user_id:owner,created_at:1});}});
export const read = internalQuery({args:{},handler:async(ctx)=>ctx.db.query('workspaces').collect()});
`);
const instance = 'or3-upgrade-' + randomBytes(8).toString('hex');
const secret = randomBytes(32).toString('hex');
const keygen = Bun.spawn([binary, 'keygen', 'admin-key', '--instance-name', instance, '--instance-secret', secret], { stdout: 'pipe', stderr: 'pipe' });
const adminKey = (await new Response(keygen.stdout).text()).trim();
assert.equal(await keygen.exited, 0);
function port() { const s = Bun.serve({ hostname: '127.0.0.1', port: 0, fetch: () => new Response('probe') }); const p = s.port; s.stop(true); return p; }
const apiPort = port(), sitePort = port(), url = `http://127.0.0.1:${apiPort}`;
const env = { ...process.env, CI: 'true', CONVEX_DEPLOY_KEY: '', CONVEX_DEPLOYMENT: '', CONVEX_SELF_HOSTED_URL: url, CONVEX_SELF_HOSTED_ADMIN_KEY: adminKey, VITE_CONVEX_URL: url, OR3_CONVEX_ALLOW_INSECURE_HTTP: 'true' };
const envPath = join(root, 'isolated.env');
await writeFile(envPath, `CONVEX_SELF_HOSTED_URL=${url}\nCONVEX_SELF_HOSTED_ADMIN_KEY=${adminKey}\n`, { mode: 0o600 });
const cli = join(provider, 'scripts/init.mjs');
const convex = join(root, 'node_modules/convex/bin/main.js');
let backend;
function start() { return Bun.spawn([binary, '--interface', '127.0.0.1', '--port', String(apiPort), '--site-proxy-port', String(sitePort), '--instance-name', instance, '--instance-secret', secret, '--disable-beacon', '--local-storage', join(root, 'storage'), join(root, 'backend.sqlite3')], { cwd: root, stdout: Bun.file(join(root, 'backend.log')), stderr: Bun.file(join(root, 'backend-errors.log')) }); }
async function ready() { for (let i = 0; i < 100; i++) { try { if ((await fetch(url + '/version')).ok) return; } catch {} await Bun.sleep(100); } throw Error('Disposable backend did not become ready'); }
async function run(args, override = {}) {
    const child = Bun.spawn([process.execPath, ...args], { cwd: root, env: { ...env, ...override }, stdout: 'pipe', stderr: 'pipe' });
    const [stdout, stderr, code] = await Promise.all([new Response(child.stdout).text(), new Response(child.stderr).text(), child.exited]);
    const output = stdout + stderr;
    assert(!output.includes(adminKey), 'Output leaked the deployment credential');
    return { code, output };
}
async function convexRun(...args) { const result = await run([convex, ...args, '--env-file', envPath]); assert.equal(result.code, 0, result.output); return result; }
async function assertOriginalMarker() {
    const source = await readFile(join(root, 'convex/or3Backend.ts'), 'utf8').catch(error => {
        if (error.code === 'ENOENT') return undefined;
        throw error;
    });
    assert.equal(source, old.files['or3Backend.ts']);
}
const checks = [];
async function check(name, operation) { await operation(); checks.push(name); console.log('PASS ' + name); }
try {
    backend = start(); await ready();
    await convexRun('env', 'set', 'CLERK_ISSUER_URL=https://upgrade.example.test');
    await convexRun('deploy', '--yes', '--typecheck', 'enable');
    await convexRun('run', 'upgradeProbe:put', '{}');
    const before = JSON.parse((await convexRun('run', 'upgradeProbe:read', '{}')).output.trim());
    assert.equal(before.length, 1);
    if (signupOnly) {
    // Supply only Nuxt configuration; the adapter and Convex transport stay real.
    mock.module(join(provider, 'src/shims/imports.ts'), () => ({
        useRuntimeConfig: () => ({ auth: { provider: 'basic-auth' }, sync: { convexUrl: url, convexAdminKey: adminKey } }),
    }));
    const { ConvexAuthWorkspaceStore } = await import(join(provider, 'src/runtime/server/auth/convex-auth-workspace-store.ts'));
    await check('signup rollback, conflict, recipient-only ownership, active-preserving replay, accept/revoke, bounded retention and pagination', async () => {
        const { ConvexHttpClient } = await import(join(host, 'node_modules/convex/dist/esm/browser/index.js'));
        const client = new ConvexHttpClient(url);
        const subject = 'signup-conflict-recipient', email = 'signup-conflict@example.test';
        client.setAdminAuth(adminKey, { subject, issuer: 'https://or3.ai/auth/basic-auth', or3_server: true, email });
        await client.mutation('workspaces:backfillSignupInviteEmails', {});
        const rollbackId = crypto.randomUUID();
        await client.mutation('workspaces:createSignupInvite', {id:rollbackId,email,token_hash:rollbackId,expires_at:Date.now()+3600_000,created_by_admin:'test-admin'});
        const snapshotBefore = await client.query('upgradeProbe:snapshotSignup', {});
        await assert.rejects(client.mutation('upgradeProbe:rollbackSignup',{input:{invite_id:rollbackId,token_hash:rollbackId,email,provider:'basic-auth',provider_user_id:subject}}), /Injected post-provision failure/);
        assert.deepEqual(await client.query('upgradeProbe:snapshotSignup', {}),snapshotBefore);
        assert.deepEqual(await client.query('workspaces:validateSignupInvite',{id:rollbackId,email,token_hash:rollbackId}),{ok:true});
        const id = crypto.randomUUID(), hash = crypto.randomUUID();
        await client.mutation('workspaces:createSignupInvite', { id, email, token_hash: hash, expires_at: Date.now()+3600_000, created_by_admin: 'test-admin' });
        const args = { invite_id: id, token_hash: hash, email, provider: 'basic-auth', provider_user_id: subject };
        const results = await Promise.all(Array.from({length: 8}, () => client.mutation('workspaces:acceptSignupInviteAndProvisionUser', args)));
        assert.equal(results.filter(r => r.ok && r.created).length, 1);
        assert.equal(new Set(results.map(r => r.workspace_id)).size, 1);
        const rows = JSON.parse((await convexRun('run', 'upgradeProbe:read', '{}')).output.trim());
        const privateRow = rows.find(w => w._id === results[0].workspace_id);
        assert.equal(privateRow.owner_user_id, results[0].user_id);
        assert.equal(privateRow.name, 'My Workspace');
        assert.equal(rows.length, before.length+1);
        const snapshot = await client.query('upgradeProbe:snapshotSignup', {});
        assert.deepEqual(snapshot.members.filter(m=>m.workspace_id===results[0].workspace_id).map(m=>({user_id:m.user_id,role:m.role})),[{user_id:results[0].user_id,role:'owner'}]);
        const other = await client.mutation('workspaces:create',{name:'Other selection'});
        await client.mutation('workspaces:setActive',{workspace_id:other});
        await client.mutation('workspaces:acceptSignupInviteAndProvisionUser',args);
        assert.equal((await client.query('upgradeProbe:snapshotSignup', {})).users.find(u=>u._id===results[0].user_id).active_workspace_id,other);
        const used = await client.mutation('workspaces:revokeSignupInvite', {id});
        assert.deepEqual(used, {ok:false,reason:'already_used'});
        const second = crypto.randomUUID();
        await client.mutation('workspaces:createSignupInvite', { id: second, email, token_hash: second, expires_at: Date.now()+3600_000, created_by_admin:'test-admin' });
        client.setAdminAuth(adminKey, {subject:'different-canonical-identity',issuer:'https://or3.ai/auth/basic-auth',or3_server:true,email});
        assert.deepEqual(await client.mutation('workspaces:acceptSignupInviteAndProvisionUser', {...args, provider_user_id:'different-canonical-identity', invite_id:second, token_hash:second}), {ok:false,reason:'account_exists'});
        assert.equal(JSON.parse((await convexRun('run','upgradeProbe:read','{}')).output.trim()).length,rows.length+1);
        const raced = crypto.randomUUID();
        const raceSubject = 'race-subject', raceEmail = 'race@example.test';
        client.setAdminAuth(adminKey,{subject:raceSubject,issuer:'https://or3.ai/auth/basic-auth',or3_server:true,email:raceEmail});
        await client.mutation('workspaces:createSignupInvite',{id:raced,email:raceEmail,token_hash:raced,expires_at:Date.now()+3600_000,created_by_admin:'test-admin'});
        const [accepted,revoked] = await Promise.all([
            client.mutation('workspaces:acceptSignupInviteAndProvisionUser',{invite_id:raced,token_hash:raced,email:raceEmail,provider:'basic-auth',provider_user_id:raceSubject}),
            client.mutation('workspaces:revokeSignupInvite',{id:raced}),
        ]);
        assert.notEqual(accepted.ok,revoked.ok);
        if(accepted.ok) assert.equal(revoked.reason,'already_used'); else assert.equal(accepted.reason,'revoked');
        for(let n=0;n<105;n++) await client.mutation('workspaces:createSignupInvite',{id:'history-'+n,email:raceEmail,token_hash:'history-hash-'+n,expires_at:Date.now()+3600_000,created_by_admin:'test-admin'});
        const history = await client.mutation('workspaces:listSignupInvites',{limit:2});
        assert.equal(history.invites.length,2); assert(history.next_cursor); assert(!('token_hash' in history.invites[0]));
        // Failure modes: wrapped cursor errors become 500, valid continuation
        // repeats a page, or unrelated pagination failures are mislabeled 400.
        await check('signup history HTTP cursor errors are 400, valid pages advance, and other failures remain server errors', async () => {
            const store = new ConvexAuthWorkspaceStore();
            // Supply an authorized store without booting Nuxt's admin session;
            // the host route, provider adapter, and backend run unchanged.
            mock.module(join(host, 'server/admin/signup-invites.ts'), () => ({
                requireSignupInviteStore: async () => ({ store }),
            }));
            const { default: handler } = await import(join(host, 'server/api/admin/signup-invites/list.get.ts'));
            const { createApp, toWebHandler } = await import(join(host, 'node_modules/h3/dist/index.mjs'));
            const server = Bun.serve({ hostname: '127.0.0.1', port: 0, fetch: toWebHandler(createApp().use(handler)) });
            const endpoint = `http://127.0.0.1:${server.port}/api/admin/signup-invites/list?limit=2`;
            try {
                for (const cursor of ['not-a-convex-cursor', '00'.repeat(64), history.next_cursor.slice(0, -16)]) {
                    const invalid = await fetch(endpoint + '&cursor=' + encodeURIComponent(cursor));
                    assert.equal(invalid.status, 400);
                    assert.equal((await invalid.json()).statusMessage, 'Invalid invite cursor');
                }
                const firstResponse = await fetch(endpoint);
                assert.equal(firstResponse.status, 200);
                const first = await firstResponse.json();
                assert.equal(first.invites.length, 2);
                assert(first.next_cursor);
                const nextResponse = await fetch(endpoint + '&cursor=' + encodeURIComponent(first.next_cursor));
                assert.equal(nextResponse.status, 200);
                const next = await nextResponse.json();
                assert.equal(next.invites.length, 2);
                assert(next.invites.every(invite => first.invites.every(previous => previous.id !== invite.id)));
                // A non-cursor database pagination error must not be recast as
                // a cursor error. HTTP validates limit before calling the store.
                await assert.rejects(store.listSignupInvites({ limit: NaN }), error => {
                    assert.notEqual(error.statusCode, 400);
                    assert.doesNotMatch(error.message, /Invalid invite cursor/);
                    return true;
                });
            } finally {
                server.stop(true);
            }
        });
        await client.mutation('upgradeProbe:expireSignupHistory',{});
        const cleaned = await client.mutation('workspaces:listSignupInvites',{limit:100});
        assert.equal(cleaned.invites.length,9); // 109 initial invitations minus the bounded 100-row sweep

    });

        await check('signup uses a bounded, resumable legacy email migration and indexed uniqueness above the transaction read limit', async () => {
            const { ConvexHttpClient } = await import(join(host, 'node_modules/convex/dist/esm/browser/index.js'));
            const client = new ConvexHttpClient(url);
            const email = 'large-signup@example.test', subject = 'large-signup';
            client.setAdminAuth(adminKey, { subject, issuer: 'https://or3.ai/auth/basic-auth', or3_server: true, email });
            // More users than one Convex transaction can read. The mixed-case
            // duplicate at the end also guards against a partial-backfill bypass.
            for (let offset = 0; offset < 33_000; offset += 1000) {
                await client.mutation('upgradeProbe:seedLegacyUsers', { offset, count: 1000 });
                await Bun.sleep(100); // Keep fixture insertion below the local backend's write-rate limit.
            }
            const id = crypto.randomUUID();
            await client.mutation('workspaces:createSignupInvite', { id, email, token_hash: id, expires_at: Date.now() + 3600_000, created_by_admin: 'test-admin' });
            const input = { invite_id: id, token_hash: id, email, provider: 'basic-auth', provider_user_id: subject };
            await assert.rejects(client.mutation('workspaces:acceptSignupInviteAndProvisionUser', input), /Signup email index requires backfill/);
            const first = await client.mutation('workspaces:backfillSignupInviteEmails', {});
            assert.equal(first.done, false);
            assert.equal(first.updated, 100);
            await assert.rejects(client.mutation('workspaces:acceptSignupInviteAndProvisionUser', input), /Signup email index requires backfill/);
            // Exercise the production adapter's automatic resume against the
            // real backend. Only Nuxt's runtime configuration is supplied here.
            const resumed = new ConvexAuthWorkspaceStore();
            const accepted = await resumed.acceptSignupInviteAndProvisionUser(input);
            assert.equal(accepted.ok, true);
            assert.equal(accepted.created, true);
            assert.deepEqual(await client.mutation('workspaces:backfillSignupInviteEmails', {}), { done: true, updated: 0 });
            const duplicateEmail = 'legacy-32999@example.test', duplicateId = crypto.randomUUID();
            await resumed.createSignupInvite({ id: duplicateId, email: duplicateEmail, token_hash: duplicateId, expires_at: Date.now() + 3600_000, created_by_admin: 'test-admin' });
            assert.deepEqual(await resumed.acceptSignupInviteAndProvisionUser({ ...input, invite_id: duplicateId, token_hash: duplicateId, email: duplicateEmail, provider_user_id: 'legacy-duplicate' }), { ok: false, reason: 'account_exists' });
        });

        await writeFile(join(evidence, 'signup.json'), JSON.stringify({ passed: true, checks }, null, 2)+'\n');
        console.log('Evidence: '+join(evidence,'signup.json'));
    } else {
    await check('missing deployment credential refuses before any source mutation', async () => {
        const result = await run([cli, 'deploy'], { CONVEX_SELF_HOSTED_ADMIN_KEY: '' });
        assert.notEqual(result.code, 0); assert.match(result.output, /credential|CONVEX_DEPLOY_KEY/i);
        await assertOriginalMarker();
    });
    await check('custom edits block before any scaffold change', async () => {
        const schemaPath = join(root, 'convex/schema.ts');
        const schema = await readFile(schemaPath, 'utf8');
        await writeFile(schemaPath, schema + '\n// User customization\n');
        const result = await run([cli, 'deploy']);
        assert.notEqual(result.code, 0);
        assert.match(result.output, /customized|conflict/i);
        await assertOriginalMarker();
        await writeFile(schemaPath, schema);
    });
    await check('a template symlink cannot overwrite a file outside the scaffold', async () => {
        const schemaPath = join(root, 'convex/schema.ts'), outside = join(root, 'outside-schema.ts');
        const contents = await readFile(schemaPath);
        await rm(schemaPath); await writeFile(outside, contents); await symlink(outside, schemaPath);
        try {
            const result = await run([cli, 'deploy']); assert.notEqual(result.code, 0); assert.match(result.output, /symbolic link/i);
            assert.deepEqual(await readFile(outside), contents);
            await assertOriginalMarker();
        } finally { await rm(schemaPath); await writeFile(schemaPath, contents); }
    });
    await check('credential destination mismatch refuses before mutation', async () => {
        const result = await run([cli, 'deploy'], { CONVEX_SELF_HOSTED_URL: `http://127.0.0.1:${sitePort}` });
        assert.notEqual(result.code, 0); assert.match(result.output, /match|target/i);
        await assertOriginalMarker();
    });
    await check('upgrade adopts untouched previous scaffold and verifies the real backend', async () => {
        const result = await run([cli, 'deploy'], { CONVEX_DEPLOY_KEY: adminKey, CONVEX_SELF_HOSTED_ADMIN_KEY: 'separate-runtime-auth-key' });
        assert.equal(result.code, 0, result.output); assert.match(result.output, /verified/i);
        assert.deepEqual(JSON.parse((await convexRun('run', 'upgradeProbe:read', '{}')).output.trim()), before);
    });
    const marker = JSON.parse((await convexRun('run', 'or3Backend:version', '{}')).output.trim());
    await check('concurrent source starts cannot own the same backend update', async () => {
        process.kill(backend.pid, 'SIGSTOP');
        const first = run([cli, 'deploy']);
        try {
            for (let i = 0; i < 100; i++) { try { await readFile(join(root, '.or3/convex-update.lock/owner.json')); break; } catch { await Bun.sleep(10); } }
            const second = await run([cli, 'deploy']); assert.notEqual(second.code, 0); assert.match(second.output, /already running/i);
        } finally { process.kill(backend.pid, 'SIGCONT'); }
        assert.equal((await first).code, 0);
    });
    await check('an interrupted owner lease is recovered without changing data', async () => {
        await mkdir(join(root, '.or3/convex-update.lock'));
        await writeFile(join(root, '.or3/convex-update.lock/owner.json'), '{"pid":2147483647}');
        const result = await run([cli, 'deploy']); assert.equal(result.code, 0, result.output);
        assert.deepEqual(JSON.parse((await convexRun('run', 'upgradeProbe:read', '{}')).output.trim()), before);
    });
    await check('invalid new functions fail deployment without erasing existing records', async () => {
        const markerPath = join(root, 'convex/or3Backend.ts');
        const source = await readFile(markerPath, 'utf8');
        await writeFile(markerPath, source.replace(marker.providerVersion, '0.0.1').replace(marker.digest, 'e'.repeat(64)));
        await convexRun('deploy', '--yes', '--typecheck', 'enable');
        await writeFile(markerPath, source);
        const invalid = join(root, 'convex/invalidUpgrade.ts');
        await writeFile(invalid, 'const bad: number = "invalid"; export { bad };\n');
        const result = await run([cli, 'deploy']); assert.notEqual(result.code, 0); assert.match(result.output, /deployment failed/i);
        assert.deepEqual(JSON.parse((await convexRun('run', 'upgradeProbe:read', '{}')).output.trim()), before);
        await rm(invalid);
        const interrupted = Bun.spawn([process.execPath, cli, 'deploy'], { cwd: root, env, stdout: 'pipe', stderr: 'pipe' });
        let observed = false;
        for (let i = 0; i < 200; i++) {
            if ((await readdir(join(root, '.or3'))).some((name) => name.startsWith('convex-deploy-') && name.endsWith('.env'))) { observed = true; break; }
            await Bun.sleep(10);
        }
        assert(observed, 'Did not reach the real Convex CLI deployment boundary');
        process.kill(interrupted.pid, 'SIGTERM');
        const [stdout, stderr, code] = await Promise.all([new Response(interrupted.stdout).text(), new Response(interrupted.stderr).text(), interrupted.exited]);
        assert.notEqual(code, 0, stdout + stderr);
        assert(!((stdout + stderr).includes(adminKey)), 'Interrupted output leaked a credential');
        assert(!(await readdir(join(root, '.or3'))).some((name) => name.startsWith('convex-deploy-') || name === 'convex-update.lock'), 'Interrupted deploy retained an active child lease or secret environment file');
        checks.push('interrupted deployment stops its child and preserves retryable source');
        const retried = await run([cli, 'deploy']); assert.equal(retried.code, 0, retried.output);
    });
    await check('restart skips deployment and preserves generated declarations', async () => {
        const generated = await readFile(join(root, 'convex/_generated/api.d.ts'));
        const result = await run([cli, 'deploy']); assert.equal(result.code, 0, result.output); assert.match(result.output, /already|matches/i);
        assert.deepEqual(await readFile(join(root, 'convex/_generated/api.d.ts')), generated);
    });
    await check('backend restart retains marker and data', async () => {
        backend.kill(); await backend.exited; backend = start(); await ready();
        assert.deepEqual(JSON.parse((await convexRun('run', 'or3Backend:version', '{}')).output.trim()), marker);
        assert.deepEqual(JSON.parse((await convexRun('run', 'upgradeProbe:read', '{}')).output.trim()), before);
        const result = await run([cli, 'deploy']); assert.equal(result.code, 0, result.output); assert.match(result.output, /already|matches/i);
    });
    await check('older source refuses a newer incompatible backend', async () => {
        const markerPath = join(root, 'convex/or3Backend.ts');
        const source = await readFile(markerPath, 'utf8');
        await writeFile(markerPath, source.replace(marker.providerVersion, '99.0.0').replace(marker.digest, 'f'.repeat(64)));
        await convexRun('deploy', '--yes', '--typecheck', 'enable');
        const result = await run([cli, 'deploy']); assert.notEqual(result.code, 0); assert.match(result.output, /newer|downgrade/i);
        await writeFile(markerPath, source); await convexRun('deploy', '--yes', '--typecheck', 'enable');
    });
    await check('unavailable backend never becomes permission to deploy', async () => {
        backend.kill(); await backend.exited; backend = undefined;
        const result = await run([cli, 'deploy']); assert.notEqual(result.code, 0); assert.match(result.output, /reach|fetch|connect/i);
    });
    await writeFile(join(evidence, 'lifecycle.json'), JSON.stringify({ passed: true, provider, root, url, marker, checks, retainedRecords: before.length }, null, 2) + '\n');
    console.log('Evidence: ' + join(evidence, 'lifecycle.json'));
    }
} finally {
    if (backend) { backend.kill(); await backend.exited; }
    await rm(envPath, { force: true });
}
