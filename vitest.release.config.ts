import path from 'node:path';
import { defineConfig } from 'vitest/config';

const hostContractShim = path.resolve(
    __dirname,
    'src/shims/or3-chat-contract.ts'
);

export default defineConfig({
    resolve: {
        alias: [
            {
                find: '~~/server/utils/background-jobs/store',
                replacement: path.resolve(__dirname, 'src/shims/imports.ts'),
            },
            {
                find: '~~/shared/cloud/provider-ids',
                replacement: hostContractShim,
            },
            { find: /^~~\/.*$/, replacement: hostContractShim },
            {
                find: '#imports',
                replacement: path.resolve(__dirname, 'src/shims/imports.ts'),
            },
        ],
    },
    test: {
        globals: true,
        include: [
            'src/runtime/server/background-jobs/__tests__/convex-provider.test.ts',
        ],
    },
});
