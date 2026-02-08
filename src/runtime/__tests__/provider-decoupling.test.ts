import { readdirSync, readFileSync } from 'node:fs';
import { join } from 'node:path';
import { describe, expect, it } from 'vitest';

function collectTypeScriptFiles(dir: string): string[] {
    const entries = readdirSync(dir, { withFileTypes: true });
    const files: string[] = [];

    for (const entry of entries) {
        const absolutePath = join(dir, entry.name);
        if (entry.isDirectory()) {
            if (entry.name === '__tests__') continue;
            files.push(...collectTypeScriptFiles(absolutePath));
            continue;
        }

        if (entry.isFile() && absolutePath.endsWith('.ts')) {
            files.push(absolutePath);
        }
    }

    return files;
}

describe('provider decoupling', () => {
    it('runtime source does not import Convex generated files directly', () => {
        const runtimeRoot = join(process.cwd(), 'src', 'runtime');
        const runtimeFiles = collectTypeScriptFiles(runtimeRoot);
        const offenders = runtimeFiles.filter((file) =>
            readFileSync(file, 'utf8').includes('~~/convex/_generated')
        );

        expect(offenders).toEqual([]);
    });
});
