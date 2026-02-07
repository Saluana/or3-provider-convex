#!/usr/bin/env bun
/**
 * or3-provider-convex init
 *
 * Copies Convex backend templates into the host project and runs codegen.
 *
 * Usage:
 *   bunx or3-provider-convex init
 *   # or
 *   bun node_modules/or3-provider-convex/scripts/init.ts
 */
import { existsSync, cpSync, mkdirSync } from 'fs';
import { resolve, dirname } from 'path';
import { fileURLToPath } from 'url';

const __dirname = dirname(fileURLToPath(import.meta.url));
const hostConvexDir = resolve(process.cwd(), 'convex');
const templateDir = resolve(__dirname, '..', 'templates', 'convex');

if (!existsSync(templateDir)) {
    console.error('❌ Template directory not found:', templateDir);
    process.exit(1);
}

if (existsSync(hostConvexDir)) {
    console.log('⚠️  convex/ directory already exists — skipping copy to avoid overwriting.');
    console.log('   Delete it first if you want a fresh copy from templates.');
} else {
    mkdirSync(hostConvexDir, { recursive: true });
    cpSync(templateDir, hostConvexDir, { recursive: true });
    console.log('✅ Copied Convex backend templates to', hostConvexDir);
}

console.log('\nNext steps:');
console.log('  1. Set VITE_CONVEX_URL in your .env');
console.log('  2. Run: bunx convex dev --once');
console.log('  3. Add convex/_generated/ to .gitignore');
