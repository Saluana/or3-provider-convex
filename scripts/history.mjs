#!/usr/bin/env node
/** Explicit isolated-candidate reset. Journal MUST live outside restored data. */
import { readFile, writeFile, rename, open } from 'node:fs/promises';
import { resolve, dirname } from 'node:path';
import { randomUUID } from 'node:crypto';
import { ConvexHttpClient } from 'convex/browser';
const [command, journalFile] = process.argv.slice(2);
if (!['prepare', 'complete'].includes(command) || !journalFile) throw Error('Usage: node scripts/history.mjs prepare|complete /external/reset-journal.json');
const path = resolve(journalFile);
async function persist(value) {
    const temporary = path + '.tmp';
    await writeFile(temporary, JSON.stringify(value, null, 2) + '\n', { mode: 0o600 });
    const file = await open(temporary, 'r'); try { await file.sync(); } finally { await file.close(); }
    await rename(temporary, path);
    const directory = await open(dirname(path), 'r'); try { await directory.sync(); } finally { await directory.close(); }
}
if (command === 'prepare') {
    const journal = { attemptId: randomUUID(), incarnation: randomUUID(), phase: 'prepared',
        target: process.env.CONVEX_SELF_HOSTED_URL ?? process.env.CONVEX_URL,
        backupSha256: process.env.OR3_CONVEX_BACKUP_SHA256, templateDigest: process.env.OR3_CONVEX_TEMPLATE_DIGEST };
    if (!journal.target || !/^[a-f0-9]{64}$/.test(journal.backupSha256 ?? '') || !/^[a-f0-9]{64}$/.test(journal.templateDigest ?? '')) throw Error('Set exact target URL, OR3_CONVEX_BACKUP_SHA256 and OR3_CONVEX_TEMPLATE_DIGEST');
    // Never silently replace an old journal; each extraction gets a new path.
    await writeFile(path, JSON.stringify(journal, null, 2) + '\n', { mode: 0o600, flag: 'wx' });
    await persist(journal);
    console.log('Prepared isolated replacement; import verified backup before complete.');
} else {
    const journal = JSON.parse(await readFile(path, 'utf8'));
    const url = process.env.CONVEX_SELF_HOSTED_URL ?? process.env.CONVEX_URL;
    const key = process.env.CONVEX_SELF_HOSTED_ADMIN_KEY ?? process.env.CONVEX_DEPLOY_KEY;
    if (!key || url !== journal.target || !['prepared', 'resetting', 'ready'].includes(journal.phase)) throw Error('Journal/credential target mismatch');
    const client = new ConvexHttpClient(url, { logger: false }); client.setAdminAuth(key);
    const contract = await client.query('or3Backend:version', {});
    if (contract.digest !== journal.templateDigest) throw Error('Exact target template mismatch');
    if (journal.phase === 'ready') {
        const state = await client.query('syncHistory:status', {});
        if (state?.phase !== 'ready' || state.incarnation !== journal.incarnation) throw Error('Completed candidate changed; prepare a fresh journal before extraction');
        console.log('Verified previously completed candidate; no rotation.');
        process.exit(0);
    }
    journal.phase = 'resetting'; await persist(journal);
    const initialized = await client.mutation('syncHistory:initialize', { incarnation: journal.incarnation });
    // A newly initialized candidate drains its pre-contract state under its own attempt ID.
    if (!initialized.ready) {
        for (let i = 0; ; i++) { if (i === 10000) throw Error('Reset did not drain'); if ((await client.mutation('syncHistory:finishReplacement', { attempt_id: initialized.attemptId })).ready) break; }
    }
    // Initialization itself is the replacement reset when the archive was pre-contract.
    if (initialized.incarnation !== journal.incarnation) {
        await client.mutation('syncHistory:beginReplacement', { attempt_id: journal.attemptId, incarnation: journal.incarnation });
        for (let i = 0; ; i++) { if (i === 10000) throw Error('Reset did not drain'); const result = await client.mutation('syncHistory:finishReplacement', { attempt_id: journal.attemptId }); if (result.ready) { if (result.historyIncarnation !== journal.incarnation) throw Error('Reset verification failed'); break; } }
    }
    journal.phase = 'ready'; await persist(journal);
    console.log('Verified replacement reset; candidate remains isolated until operator release.');
}
