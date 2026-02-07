import { defineNuxtModule, installModule, addPlugin, addServerPlugin, createResolver } from '@nuxt/kit';

export default defineNuxtModule({
    meta: { name: 'or3-provider-convex' },
    async setup() {
        const { resolve } = createResolver(import.meta.url);

        await installModule('convex-nuxt');

        // Append so convex-nuxt client context is initialized before provider plugins run.
        addPlugin(resolve('runtime/plugins/convex-auth.client'), { append: true });
        addPlugin(resolve('runtime/plugins/convex-sync.client'), { append: true });
        addPlugin(resolve('runtime/plugins/convex-storage.client'), { append: true });
        addServerPlugin(resolve('runtime/server/plugins/register'));
    },
});
