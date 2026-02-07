import { defineNuxtModule, installModule, addPlugin, addServerPlugin, createResolver } from '@nuxt/kit';

export default defineNuxtModule({
    meta: { name: 'or3-provider-convex' },
    async setup(_options, nuxt) {
        const { resolve } = createResolver(import.meta.url);
        const currentConvex = (nuxt.options as { convex?: { url?: string; manualInit?: boolean } }).convex ?? {};
        const url = currentConvex.url ?? process.env.VITE_CONVEX_URL ?? '';

        // Keep convex-nuxt config provider-local so host nuxt.config stays provider-agnostic.
        (nuxt.options as { convex?: { url: string; manualInit: boolean } }).convex = {
            ...currentConvex,
            url,
            manualInit: currentConvex.manualInit ?? !url,
        };

        await installModule('convex-nuxt');

        // Append so convex-nuxt client context is initialized before provider plugins run.
        addPlugin(resolve('runtime/plugins/convex-auth.client'), { append: true });
        addPlugin(resolve('runtime/plugins/convex-sync.client'), { append: true });
        addPlugin(resolve('runtime/plugins/convex-storage.client'), { append: true });
        addServerPlugin(resolve('runtime/server/plugins/register'));
    },
});
