import { defineConfig } from 'vitest/config';
import path from 'path';

const hostRoot = path.resolve(__dirname,
    process.env.OR3_CANONICAL_HOST_ROOT || '../or3-chat');

export default defineConfig({
    plugins: [
        {
            name: 'convex-template-generated-server-test-stub',
            enforce: 'pre',
            resolveId(source, importer) {
                if (
                    source === './_generated/server' &&
                    importer?.includes('/templates/convex/')
                ) {
                    return path.resolve(
                        __dirname,
                        'tests/fixtures/convex-generated-server.ts'
                    );
                }
                if (
                    source === '../shared/sync/table-metadata' &&
                    importer?.endsWith('/templates/convex/sync.ts')
                ) {
                    return path.resolve(
                        __dirname,
                        'tests/fixtures/convex-table-metadata.ts'
                    );
                }
                if (
                    source === '../shared/cloud/admin-identity' &&
                    importer?.includes('/templates/convex/')
                ) {
                    return path.resolve(
                        hostRoot,
                        'shared/cloud/admin-identity.ts'
                    );
                }
            },
        },
    ],
    resolve: {
        alias: {
            '~~/': hostRoot + '/',
            '~~': hostRoot,
            '#imports': path.join(hostRoot, 'tests/stubs/nuxt-imports.ts'),
        },
    },
    test: {
        globals: true,
        include: ['src/**/__tests__/**/*.test.ts'],
        exclude: ['node_modules', 'dist'],
        testTimeout: 10000,
    },
});
