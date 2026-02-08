import { useConvexClient } from 'convex-vue';
import {
    CONVEX_JWT_TEMPLATE,
    CONVEX_PROVIDER_ID,
} from '~~/shared/cloud/provider-ids';
import { registerSyncProvider, setActiveSyncProvider } from '~/core/sync/sync-provider-registry';
import { useAuthTokenBroker } from '~/composables/auth/useAuthTokenBroker.client';
import { createConvexSyncProvider } from '../app/sync/convex-sync-provider';

function patchConvexSetAuth(client: {
    setAuth: (
        fetchToken: () => Promise<string | null>,
        onChange?: (isAuthenticated: boolean) => void
    ) => void;
    __or3SetAuthPatched?: boolean;
}): void {
    if (client.__or3SetAuthPatched) return;
    const originalSetAuth = client.setAuth.bind(client);
    client.setAuth = (
        fetchToken: () => Promise<string | null>,
        onChange?: (isAuthenticated: boolean) => void
    ) => originalSetAuth(fetchToken, onChange ?? (() => {}));
    client.__or3SetAuthPatched = true;
}

export default defineNuxtPlugin(() => {
    if (import.meta.server) return;

    const config = useRuntimeConfig();
    if (!config.public.sync.enabled) return;
    if (config.public.sync.provider !== CONVEX_PROVIDER_ID) return;
    // In SSR auth mode, core sync engine uses gateway transport.
    // Skip registering direct Convex sync provider to prevent auth token loops.
    if (config.public.ssrAuthEnabled) return;

    try {
        const client = useConvexClient();
        const broker = useAuthTokenBroker();
        patchConvexSetAuth(client);

        // Bind auth directly on the same client instance used by sync operations.
        client.setAuth(
            async () =>
                broker.getProviderToken({
                    providerId: CONVEX_PROVIDER_ID,
                    template: CONVEX_JWT_TEMPLATE,
                }),
            () => {
                // Gateway mode reads auth state from SSR session; no-op for Convex client hook.
            }
        );

        registerSyncProvider(createConvexSyncProvider(client));
        setActiveSyncProvider(CONVEX_PROVIDER_ID);
    } catch (error) {
        console.warn('[or3-provider-convex] Convex context unavailable; skipping sync provider init', {
            error: error instanceof Error ? error.message : String(error),
        });
    }
});
