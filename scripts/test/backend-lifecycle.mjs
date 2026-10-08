#!/usr/bin/env bun
// Real, disposable Convex upgrade proof. Never accepts a deployed backend URL.
import assert from 'node:assert/strict';
import { cp, mkdir, mkdtemp, readFile, writeFile, symlink, rm, readdir } from 'node:fs/promises';
import { randomBytes } from 'node:crypto';
import { gunzipSync } from 'node:zlib';
import { resolve, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { tmpdir } from 'node:os';

const [binary, host, basePack, evidence, qualifiedProvider] = process.argv.slice(2).map((p) => resolve(p));
assert(binary?.startsWith(join(process.env.HOME, '.cache/convex/binaries/')) && binary.endsWith('/convex-local-backend'));
assert(host && basePack && evidence, 'Usage: bun backend-lifecycle.mjs <cached-binary> <host-source> <base-pack> <evidence-dir>');
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
// A real extra function/table-independent persistent record, preserved by deploy.
await writeFile(join(root, 'convex', 'upgradeProbe.ts'), `
import { internalMutation, internalQuery } from './_generated/server';
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
const checks = [];
async function check(name, operation) { await operation(); checks.push(name); console.log('PASS ' + name); }
try {
    backend = start(); await ready();
    await convexRun('env', 'set', 'CLERK_ISSUER_URL=https://upgrade.example.test');
    await convexRun('deploy', '--yes', '--typecheck', 'enable');
    await convexRun('run', 'upgradeProbe:put', '{}');
    const before = JSON.parse((await convexRun('run', 'upgradeProbe:read', '{}')).output.trim());
    assert.equal(before.length, 1);
    await check('missing deployment credential refuses before any source mutation', async () => {
        const result = await run([cli, 'deploy'], { CONVEX_SELF_HOSTED_ADMIN_KEY: '' });
        assert.notEqual(result.code, 0); assert.match(result.output, /credential|CONVEX_DEPLOY_KEY/i);
        await assert.rejects(readFile(join(root, 'convex/or3Backend.ts')));
    });
    await check('custom edits block before any scaffold change', async () => {
        const schemaPath = join(root, 'convex/schema.ts');
        const schema = await readFile(schemaPath, 'utf8');
        await writeFile(schemaPath, schema + '\n// User customization\n');
        const result = await run([cli, 'deploy']);
        assert.notEqual(result.code, 0);
        assert.match(result.output, /customized|conflict/i);
        await assert.rejects(readFile(join(root, 'convex/or3Backend.ts')));
        await writeFile(schemaPath, schema);
    });
    await check('a template symlink cannot overwrite a file outside the scaffold', async () => {
        const schemaPath = join(root, 'convex/schema.ts'), outside = join(root, 'outside-schema.ts');
        const contents = await readFile(schemaPath);
        await rm(schemaPath); await writeFile(outside, contents); await symlink(outside, schemaPath);
        try {
            const result = await run([cli, 'deploy']); assert.notEqual(result.code, 0); assert.match(result.output, /symbolic link/i);
            assert.deepEqual(await readFile(outside), contents);
            await assert.rejects(readFile(join(root, 'convex/or3Backend.ts')));
        } finally { await rm(schemaPath); await writeFile(schemaPath, contents); }
    });
    await check('credential destination mismatch refuses before mutation', async () => {
        const result = await run([cli, 'deploy'], { CONVEX_SELF_HOSTED_URL: `http://127.0.0.1:${sitePort}` });
        assert.notEqual(result.code, 0); assert.match(result.output, /match|target/i);
        await assert.rejects(readFile(join(root, 'convex/or3Backend.ts')));
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
} finally {
    if (backend) { backend.kill(); await backend.exited; }
    await rm(envPath, { force: true });
}
