import { defineConfig } from 'vitest/config';
import path from 'path';

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
            },
        },
    ],
    resolve: {
        alias: {
            '~~/': path.resolve(__dirname, '../or3-chat') + '/',
            '~~': path.resolve(__dirname, '../or3-chat'),
            '#imports': path.resolve(__dirname, '../or3-chat/tests/stubs/nuxt-imports.ts'),
        },
    },
    test: {
        globals: true,
        include: ['src/**/__tests__/**/*.test.ts'],
        exclude: ['node_modules', 'dist'],
        testTimeout: 10000,
    },
});
