#!/usr/bin/env node
/**
 * or3-provider-convex init
 *
 * Copies Convex backend templates into the host project.
 *
 * Usage:
 *   npx or3-provider-convex init
 *   bunx or3-provider-convex init
 *   bunx or3-provider-convex init --update
 *   bunx or3-provider-convex init --force
 */
import { existsSync, mkdirSync, readdirSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { dirname, relative, resolve, sep } from 'node:path';
import { fileURLToPath } from 'node:url';
import { gunzipSync } from 'node:zlib';

const __dirname = dirname(fileURLToPath(import.meta.url));
const hostConvexDir = resolve(process.cwd(), 'convex');
const templateDir = resolve(__dirname, '..', 'templates', 'convex');
const templatePackPath = resolve(
    __dirname,
    '..',
    'templates',
    'convex.pack.json.gz'
);
const IGNORED_FILE_NAMES = new Set(['.DS_Store']);

function normalizeTemplateRelativePath(pathValue) {
    if (typeof pathValue !== 'string' || pathValue.trim().length === 0) {
        throw new Error('Invalid template entry path.');
    }
    if (pathValue.includes('\0')) {
        throw new Error(`Template path contains NUL byte: ${pathValue}`);
    }

    const normalized = pathValue.replace(/\\/g, '/');
    const segments = normalized.split('/').filter((segment) => segment.length > 0);
    if (
        segments.length === 0 ||
        segments.some((segment) => segment === '.' || segment === '..')
    ) {
        throw new Error(`Unsafe template path: ${pathValue}`);
    }

    return segments.join('/');
}

function resolveSafeDestination(rootDir, relativePath) {
    const localPath = relativePath.split('/').join(sep);
    const destination = resolve(rootDir, localPath);
    const rootPrefix = rootDir.endsWith(sep) ? rootDir : `${rootDir}${sep}`;
    if (!destination.startsWith(rootPrefix)) {
        throw new Error(`Template destination escapes root: ${relativePath}`);
    }
    return destination;
}

function loadTemplateFilesFromPack() {
    const packed = readFileSync(templatePackPath);
    const unpacked = gunzipSync(packed).toString('utf8');
    const payload = JSON.parse(unpacked);
    const files = payload?.files ?? {};
    const normalizedFiles = {};

    for (const [relativePath, content] of Object.entries(files)) {
        const safePath = normalizeTemplateRelativePath(relativePath);
        if (typeof content !== 'string') {
            throw new Error(`Template content is not a string: ${safePath}`);
        }
        normalizedFiles[safePath] = content;
    }

    return normalizedFiles;
}

function toPosixPath(pathValue) {
    return pathValue.split(sep).join('/');
}

function loadTemplateFilesFromDirectory() {
    if (!existsSync(templateDir)) {
        throw new Error(`Template directory not found: ${templateDir}`);
    }

    const pending = [templateDir];
    const files = {};

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

            const relativePath = normalizeTemplateRelativePath(
                toPosixPath(relative(templateDir, absolutePath))
            );
            files[relativePath] = readFileSync(absolutePath, 'utf8');
        }
    }

    return files;
}

function loadTemplateFiles() {
    if (existsSync(templatePackPath)) {
        return loadTemplateFilesFromPack();
    }
    if (existsSync(templateDir)) {
        return loadTemplateFilesFromDirectory();
    }
    throw new Error(
        `Template assets not found. Expected ${templatePackPath} or ${templateDir}.`
    );
}

function writeTemplateFiles(targetDir, files, options = {}) {
    const mode = options.mode ?? 'create';
    const result = {
        written: 0,
        unchanged: 0,
        skippedConflicts: 0,
    };

    for (const [relativePath, content] of Object.entries(files)) {
        const destination = resolveSafeDestination(targetDir, relativePath);
        const alreadyExists = existsSync(destination);

        if (alreadyExists && mode === 'update') {
            const current = readFileSync(destination, 'utf8');
            if (current === content) {
                result.unchanged += 1;
            } else {
                result.skippedConflicts += 1;
            }
            continue;
        }

        mkdirSync(dirname(destination), { recursive: true });
        writeFileSync(destination, content, 'utf8');
        result.written += 1;
    }

    return result;
}

function parseArgs(argv) {
    const args = argv.filter((arg) => arg !== 'init');
    const allowed = new Set(['--update', '--force']);
    for (const arg of args) {
        if (!allowed.has(arg)) {
            throw new Error(`Unknown argument: ${arg}`);
        }
    }
    const update = args.includes('--update');
    const force = args.includes('--force');
    if (update && force) {
        throw new Error('Use either --update or --force, not both.');
    }
    return { update, force };
}

let mode;
try {
    const parsed = parseArgs(process.argv.slice(2));
    mode = parsed.force ? 'force' : parsed.update ? 'update' : 'create';
} catch (error) {
    console.error(
        `Failed to parse arguments: ${error instanceof Error ? error.message : String(error)}`
    );
    process.exit(1);
}

const templateFiles = loadTemplateFiles();

if (mode === 'force' && existsSync(hostConvexDir)) {
    rmSync(hostConvexDir, { recursive: true, force: true });
}

if (!existsSync(hostConvexDir)) {
    mkdirSync(hostConvexDir, { recursive: true });
}

if (mode === 'create' && existsSync(hostConvexDir)) {
    const existingEntries = readdirSync(hostConvexDir);
    if (existingEntries.length > 0) {
        console.log('convex/ directory already exists. Skipping copy to avoid overwrite.');
        console.log('Run with --update to merge missing files or --force to replace all files.');
    } else {
        const result = writeTemplateFiles(hostConvexDir, templateFiles, { mode: 'create' });
        console.log(`Initialized Convex backend templates in ${hostConvexDir} (${result.written} files).`);
    }
} else if (mode === 'update') {
    const result = writeTemplateFiles(hostConvexDir, templateFiles, { mode: 'update' });
    console.log(`Updated Convex backend templates in ${hostConvexDir}.`);
    console.log(`- Added missing files: ${result.written}`);
    console.log(`- Unchanged files: ${result.unchanged}`);
    console.log(`- Existing files left untouched: ${result.skippedConflicts}`);
} else {
    const result = writeTemplateFiles(hostConvexDir, templateFiles, { mode: 'create' });
    console.log(`Initialized Convex backend templates in ${hostConvexDir} (${result.written} files).`);
}

console.log('\nNext steps:');
console.log('  1. Set VITE_CONVEX_URL in your .env');
console.log('  2. Run: bunx convex dev --once');
console.log('  3. Add convex/_generated/ to .gitignore');
