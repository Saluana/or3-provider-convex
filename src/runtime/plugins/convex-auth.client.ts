import { watch } from 'vue';
import { useConvexClient } from 'convex-vue';
import {
    CONVEX_JWT_TEMPLATE,
    CONVEX_PROVIDER_ID,
} from '~~/shared/cloud/provider-ids';
import { useAuthTokenBroker } from '~/composables/auth/useAuthTokenBroker.client';
import { useSessionContext } from '~/composables/auth/useSessionContext';

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
    // In SSR auth mode, sync/storage/auth run through gateway APIs.
    // Do not configure direct Convex client auth to avoid auth-manager conflicts.
    if (config.public.ssrAuthEnabled) return;

    let client: ReturnType<typeof useConvexClient>;
    try {
        client = useConvexClient();
    } catch (error) {
        console.warn(
            '[or3-provider-convex] Convex context unavailable; skipping auth bridge',
            {
                error: error instanceof Error ? error.message : String(error),
            }
        );
        return;
    }

    const broker = useAuthTokenBroker();
    const getToken = async (): Promise<string | null> =>
        broker.getProviderToken({
            providerId: CONVEX_PROVIDER_ID,
            template: CONVEX_JWT_TEMPLATE,
        });

    patchConvexSetAuth(client);

    // Register token fetcher so direct Convex queries/mutations include identity.
    client.setAuth(getToken, () => {
        // Gateway mode reads auth state from SSR session; no-op for Convex client hook.
    });

    const { data: sessionData } = useSessionContext();
    watch(
        () => sessionData.value?.session?.authenticated,
        () => {
            // Force Convex auth refresh when session state flips.
            client.setAuth(getToken, () => {
                // Gateway mode reads auth state from SSR session; no-op for Convex client hook.
            });
        }
    );
});
