import { createHash, randomUUID } from 'node:crypto';
import { spawn } from 'node:child_process';
import { existsSync, lstatSync, mkdirSync, readFileSync, renameSync, rmSync, writeFileSync, cpSync } from 'node:fs';
import { dirname, join, resolve } from 'node:path';
import { createRequire } from 'node:module';
import { fileURLToPath } from 'node:url';
import { gunzipSync } from 'node:zlib';

const packageRoot = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const hash = (value) => createHash('sha256').update(value).digest('hex');
const contract = JSON.parse(readFileSync(join(packageRoot, 'templates/convex.contract.json'), 'utf8'));

function destination(root, name) {
    if (name.startsWith('/') || name.includes('\\') || name.split('/').some((part) => !part || part === '.' || part === '..')) throw Error('Unsafe Convex template path.');
    let path = root;
    for (const part of name.split('/')) {
        path = join(path, part);
        if (existsSync(path) && lstatSync(path).isSymbolicLink()) throw Error(`Convex update refuses symbolic link: ${name}`);
    }
    return path;
}

function atomicWrite(path, value) {
    mkdirSync(dirname(path), { recursive: true, mode: 0o700 });
    const temporary = path + '.' + randomUUID() + '.tmp';
    writeFileSync(temporary, value, { mode: 0o600 });
    renameSync(temporary, path);
}

function updateTemplates(root, { force = false, create = false } = {}) {
    const functionsRoot = destination(root, 'convex');
    const configPath = destination(root, 'convex.json');
    if (existsSync(configPath)) {
        const config = JSON.parse(readFileSync(configPath, 'utf8'));
        if (config.functions && !['convex', 'convex/'].includes(config.functions)) throw Error('Automatic Convex updates require the standard convex/ functions directory.');
    }
    const manifestPath = destination(root, '.or3/convex-templates.json');
    const previous = JSON.parse(readFileSync(join(packageRoot, 'templates/convex.previous.json'), 'utf8'));
    const manifest = existsSync(manifestPath) ? JSON.parse(readFileSync(manifestPath, 'utf8')) : previous;
    if (!manifest.files || typeof manifest.files !== 'object') throw Error('Invalid Convex template manifest; no files were changed.');
    const { files } = JSON.parse(gunzipSync(readFileSync(join(packageRoot, 'templates/convex.pack.json.gz'))).toString());
    const owned = Object.fromEntries(Object.entries(files).filter(([name]) => !name.startsWith('_generated/')));
    const writes = [], removals = [], conflicts = [];
    for (const [name, contents] of Object.entries(owned)) {
        if (typeof contents !== 'string') throw Error('Invalid Convex template content.');
        const path = destination(functionsRoot, name);
        const current = existsSync(path) ? readFileSync(path, 'utf8') : null;
        if (current === contents) continue;
        if (!force && current !== null && (create || hash(current) !== manifest.files[name])) conflicts.push(name);
        else if (!force && current === null && manifest.files[name] && existsSync(manifestPath)) conflicts.push(name + ' (deleted locally)');
        else writes.push([path, contents]);
    }
    for (const name of Object.keys(manifest.files)) {
        if (Object.hasOwn(owned, name)) continue;
        const path = destination(functionsRoot, name);
        if (!existsSync(path)) continue;
        if (!force && hash(readFileSync(path)) !== manifest.files[name]) conflicts.push(name);
        else removals.push(path);
    }
    if (conflicts.length) throw Error(`Convex templates are customized or conflict: ${conflicts.join(', ')}. No files were changed. Merge the provider templates deliberately before retrying; custom functions are preserved.`);
    let backup;
    if ((writes.length || removals.length) && existsSync(functionsRoot)) {
        backup = destination(root, '.or3/convex-backups/' + randomUUID());
        mkdirSync(backup, { recursive: true, mode: 0o700 });
        cpSync(functionsRoot, join(backup, 'convex'), { recursive: true, dereference: false });
        if (existsSync(manifestPath)) cpSync(manifestPath, join(backup, 'convex-templates.json'));
    }
    // The previous hash manifest remains until all writes complete. A retry can
    // recognize both already-updated files and untouched previous files.
    for (const [path, contents] of writes) atomicWrite(path, contents);
    for (const path of removals) rmSync(path);
    atomicWrite(manifestPath, JSON.stringify({ ...contract, files: Object.fromEntries(Object.entries(owned).map(([name, value]) => [name, hash(value)])) }, null, 2) + '\n');
    return { changed: writes.length + removals.length, backup };
}

function normalizeUrl(value) {
    const url = new URL(value);
    if (url.username || url.password || url.pathname !== '/' || url.search || url.hash || !['http:', 'https:'].includes(url.protocol)) throw Error('Convex URL must be a deployment origin without credentials or a path.');
    if (url.protocol === 'http:' && process.env.OR3_CONVEX_ALLOW_INSECURE_HTTP !== 'true') throw Error('HTTP Convex requires OR3_CONVEX_ALLOW_INSECURE_HTTP=true.');
    return url.origin;
}

async function readBackend(url) {
    const response = await fetch(url + '/api/query', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ path: 'or3Backend:version', args: {}, format: 'json' }), signal: AbortSignal.timeout(10_000) });
    const result = await response.json();
    if (result.status === 'success' && result.value && /^\d+\.\d+\.\d+$/.test(result.value.providerVersion) && /^[a-f0-9]{64}$/.test(result.value.digest)) return result.value;
    // Only the real Convex missing-function response permits a first upgrade.
    // Network errors, authorization errors, and arbitrary 404s never do.
    if (result.status === 'error' && /Could not find public function.*or3Backend:version/i.test(result.errorMessage ?? '')) return null;
    throw Error('Could not verify the Convex backend contract; no deployment was attempted.');
}

function compareVersions(left, right) {
    const a = left.split('.').map(Number), b = right.split('.').map(Number);
    for (let i = 0; i < 3; i++) if (a[i] !== b[i]) return a[i] - b[i];
    return 0;
}

async function withLease(root, operation) {
    const lock = destination(root, '.or3/convex-update.lock');
    mkdirSync(dirname(lock), { recursive: true, mode: 0o700 });
    try { mkdirSync(lock, { mode: 0o700 }); }
    catch (error) {
        if (error.code !== 'EEXIST') throw error;
        let owner;
        try { owner = JSON.parse(readFileSync(join(lock, 'owner.json'), 'utf8')); } catch { throw Error('Convex update lease is incomplete; inspect .or3/convex-update.lock before retrying.'); }
        if (!Number.isSafeInteger(owner.pid) || owner.pid <= 0) throw Error('Invalid Convex update lease.');
        try { process.kill(owner.pid, 0); throw Error('A Convex update is already running for this checkout. Retry after it finishes.'); }
        catch (probe) {
            if (probe.code !== 'ESRCH') throw probe;
            const abandoned = lock + '.' + randomUUID() + '.abandoned';
            renameSync(lock, abandoned); rmSync(abandoned, { recursive: true }); mkdirSync(lock, { mode: 0o700 });
        }
    }
    writeFileSync(join(lock, 'owner.json'), JSON.stringify({ pid: process.pid }), { mode: 0o600 });
    try { return await operation(); } finally { rmSync(lock, { recursive: true, force: true }); }
}

export function initializeTemplates(root, options) {
    return withLease(root, () => updateTemplates(root, options));
}

async function deploy(root, url, key) {
    const requireFromProvider = createRequire(import.meta.url);
    const cli = join(dirname(requireFromProvider.resolve('convex/package.json')), 'bin/main.js');
    const envPath = destination(root, '.or3/convex-deploy-' + randomUUID() + '.env');
    atomicWrite(envPath, `CONVEX_SELF_HOSTED_URL=${url}\nCONVEX_SELF_HOSTED_ADMIN_KEY=${key}\n`);
    const env = { ...process.env, CONVEX_DEPLOYMENT: '', CONVEX_DEPLOY_KEY: '', CONVEX_SELF_HOSTED_URL: url, CONVEX_SELF_HOSTED_ADMIN_KEY: key, CI: 'true' };
    try {
        await new Promise((accept, reject) => {
            const child = spawn(process.versions.bun ? 'node' : process.execPath, [cli, 'deploy', '--yes', '--typecheck', 'enable', '--env-file', envPath], { cwd: root, env, stdio: ['ignore', 'pipe', 'pipe'] });
            let output = '';
            const capture = (chunk) => { output = (output + String(chunk)).slice(-1024 * 1024); };
            child.stdout.on('data', capture); child.stderr.on('data', capture);
            let killTimer;
            const terminate = () => {
                child.kill('SIGTERM');
                killTimer ??= setTimeout(() => child.kill('SIGKILL'), 5_000);
            };
            process.once('SIGINT', terminate); process.once('SIGTERM', terminate);
            const timer = setTimeout(terminate, 180_000);
            const cleanup = () => {
                clearTimeout(timer); clearTimeout(killTimer);
                process.removeListener('SIGINT', terminate); process.removeListener('SIGTERM', terminate);
            };
            child.once('error', (error) => { cleanup(); reject(error); });
            child.once('exit', (code) => {
                cleanup();
                const safe = output.split(key).join('[redacted]');
                if (code === 0) { console.log(safe.trim()); accept(); }
                else reject(Error(`Convex deployment failed (${code ?? 'terminated'}). Backend readiness was not recorded; retry after fixing the error.\n${safe}`));
            });
        });
    } finally { rmSync(envPath, { force: true }); }
}

export async function ensureBackend(root) {
    const url = normalizeUrl(process.env.VITE_CONVEX_URL ?? process.env.CONVEX_SELF_HOSTED_URL ?? '');
    if (process.env.CONVEX_SELF_HOSTED_URL && normalizeUrl(process.env.CONVEX_SELF_HOSTED_URL) !== url) throw Error('Convex credential target must match VITE_CONVEX_URL.');
    return withLease(root, async () => {
        const current = await readBackend(url);
        if (current?.digest === contract.digest) { console.log('Convex backend already matches this provider. No deployment needed.'); return; }
        if (current && compareVersions(current.providerVersion, contract.providerVersion) >= 0) throw Error('Convex backend is newer or differs at the same immutable provider version. Refusing an automatic downgrade.');
        const cloudKey = process.env.CONVEX_DEPLOY_KEY?.trim();
        const adminKey = process.env.CONVEX_SELF_HOSTED_ADMIN_KEY?.trim();
        const key = cloudKey || adminKey;
        if (!key || /[\r\n]/.test(key)) throw Error('Set CONVEX_DEPLOY_KEY (Convex Cloud) or CONVEX_SELF_HOSTED_ADMIN_KEY once to enable automatic backend updates.');
        const updated = updateTemplates(root);
        if (updated.backup) console.log('Previous Convex source preserved at ' + updated.backup);
        console.log('Deploying the backend bundled with or3-provider-convex@' + contract.providerVersion + '…');
        await deploy(root, url, key);
        const deployed = await readBackend(url);
        if (deployed?.digest !== contract.digest) throw Error('Convex deployment finished but the required backend contract did not verify. The app must not start.');
        console.log('Convex backend deployed and verified.');
    });
}
