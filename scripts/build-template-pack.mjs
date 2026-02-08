#!/usr/bin/env node
import { existsSync, mkdirSync, readFileSync, readdirSync, writeFileSync } from 'node:fs';
import { dirname, relative, resolve, sep } from 'node:path';
import { fileURLToPath } from 'node:url';
import { gzipSync } from 'node:zlib';

const __dirname = dirname(fileURLToPath(import.meta.url));
const templateRoot = resolve(__dirname, '..', 'templates', 'convex');
const outputPath = resolve(__dirname, '..', 'templates', 'convex.pack.json.gz');

const IGNORED_FILE_NAMES = new Set(['.DS_Store']);

function toPosixPath(value) {
    return value.split(sep).join('/');
}

function collectTemplateFiles(rootDir) {
    if (!existsSync(rootDir)) {
        throw new Error(`Template directory not found: ${rootDir}`);
    }

    const pending = [rootDir];
    const files = [];

    while (pending.length > 0) {
        const current = pending.pop();
        if (!current) continue;
        const entries = readdirSync(current, { withFileTypes: true });
        for (const entry of entries) {
            if (IGNORED_FILE_NAMES.has(entry.name)) continue;
            const absolutePath = resolve(current, entry.name);
            if (entry.isDirectory()) {
                pending.push(absolutePath);
                continue;
            }
            if (!entry.isFile()) {
                throw new Error(`Unsupported template entry: ${absolutePath}`);
            }
            const relPath = toPosixPath(relative(rootDir, absolutePath));
            files.push(relPath);
        }
    }

    files.sort();
    if (files.length === 0) {
        throw new Error(`No template files found under ${rootDir}`);
    }
    return files;
}

const templateFiles = collectTemplateFiles(templateRoot);
const payload = {
    version: 1,
    files: Object.fromEntries(
        templateFiles.map((relativePath) => {
            const absolutePath = resolve(templateRoot, relativePath);
            if (!existsSync(absolutePath)) {
                throw new Error(
                    `Missing template file: ${relativePath} (${absolutePath})`
                );
            }
            return [relativePath, readFileSync(absolutePath, 'utf8')];
        })
    ),
};

const packed = gzipSync(Buffer.from(JSON.stringify(payload), 'utf8'), {
    level: 9,
});
mkdirSync(dirname(outputPath), { recursive: true });
writeFileSync(outputPath, packed);
console.log(
    `Template pack written: ${outputPath} (${packed.byteLength.toLocaleString()} bytes)`
);
