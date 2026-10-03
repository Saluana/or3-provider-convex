import { defineConfig, mergeConfig } from 'vitest/config';
import { execFileSync } from 'node:child_process';
import { readFileSync } from 'node:fs';
import path from 'node:path';
import config from './vitest.config';

const host = path.resolve(import.meta.dirname, '../../manual-context-compaction');
const files = ['shared/chat/history-reader', 'shared/chat/background-history', 'shared/chat/compaction'];
for (const file of files) {
    if (!execFileSync('git', ['show', `HEAD:${file}.ts`], { cwd: host }).equals(readFileSync(path.join(host, `${file}.ts`))))
        throw new Error(`Uncommitted canonical contract: ${file}`);
}
const lane = mergeConfig(config, defineConfig({ resolve: { alias: files.map((file) => ({
    find: `~~/${file}`, replacement: path.join(host, `${file}.ts`),
})) } }));
lane.test = { ...lane.test, include: ['src/runtime/__tests__/convex-snapshot-contract.test.ts'] };
export default lane;
