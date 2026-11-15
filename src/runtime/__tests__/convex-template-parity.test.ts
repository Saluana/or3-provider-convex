import { it, expect } from 'vitest';
import { cpSync, mkdtempSync, mkdirSync, readFileSync, writeFileSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join, resolve } from 'node:path';
import { execFileSync, spawnSync } from 'node:child_process';
import { createHash } from 'node:crypto';

it('rejects a weaker bundled validator even when its generated manifest hash is updated', () => {
    const root = mkdtempSync(join(tmpdir(), 'convex-parity-'));
    const provider = resolve(import.meta.dirname, '../../..');
    const host = resolve(provider, process.env.OR3_CANONICAL_HOST_ROOT || '../or3-chat');
    try {
        cpSync(join(provider, 'templates'), join(root, 'templates'), { recursive: true });
        mkdirSync(join(root, 'scripts'));
        mkdirSync(join(root, 'src/runtime'), { recursive: true });
        cpSync(join(provider, 'package.json'), join(root, 'package.json'));
        cpSync(join(provider, 'scripts/build-template-pack.mjs'), join(root, 'scripts/build-template-pack.mjs'));
        const manifestPath = join(root, 'templates/convex/syncContract/source.json');
        const manifest = JSON.parse(readFileSync(manifestPath, 'utf8'));
        for (const file of Object.keys(manifest.files)) {
            const target = join(root, 'host', file);
            mkdirSync(dirname(target), { recursive: true });
            writeFileSync(target, execFileSync('git', ['show', `${manifest.hostCommit}:${file}`], { cwd: host }));
        }
        const check = () => spawnSync(process.execPath, [join(root, 'scripts/build-template-pack.mjs'), '--check-host', join(root, 'host')], { encoding: 'utf8' });
        const healthy = check();
        expect(healthy.status, healthy.stderr || String(healthy.error)).toBe(0);
        const validator = join(root, 'templates/convex/syncContract/shared/sync/prepare_sync_write.ts');
        const original = readFileSync(validator, 'utf8');
        const weaker = original.replace('pk.length > 1024', 'pk.length > 2048');
        expect(weaker).not.toBe(original);
        writeFileSync(validator, weaker);
        manifest.files['shared/sync/prepare-sync-write.ts'].generated = createHash('sha256').update(weaker).digest('hex');
        writeFileSync(manifestPath, JSON.stringify(manifest));
        const result = check();
        expect(result.status).not.toBe(0);
        expect(result.stderr).toContain('Generated host source mismatch');
    } finally {
        rmSync(root, { recursive: true, force: true });
    }
});
