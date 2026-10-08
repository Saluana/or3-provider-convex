#!/usr/bin/env node
import { resolve } from 'node:path';
import { ensureBackend, initializeTemplates } from './backend.mjs';

try {
    const [command = 'init', ...flags] = process.argv.slice(2);
    if (command === 'deploy' && flags.length === 0) {
        await ensureBackend(process.cwd());
    } else if (command === 'init' && flags.every((flag) => ['--update', '--force'].includes(flag))) {
        if (flags.includes('--force') && flags.includes('--update')) throw Error('Use --update or --force, not both.');
        const result = await initializeTemplates(resolve(process.cwd()), { force: flags.includes('--force'), create: !flags.includes('--update') && !flags.includes('--force') });
        console.log(`Convex scaffold updated (${result.changed} files).`);
        if (result.backup) console.log('Previous source preserved at ' + result.backup);
        console.log('Run or3-provider-convex deploy to deploy and verify the backend.');
    } else {
        throw Error('Usage: or3-provider-convex init [--update|--force] | deploy');
    }
} catch (error) {
    console.error(error instanceof Error ? error.message : String(error));
    process.exitCode = 1;
}
