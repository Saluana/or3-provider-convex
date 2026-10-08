import { defineNitroPlugin } from 'nitropack/runtime/plugin';
import { CONVEX_PROVIDER_ID, CONVEX_STORAGE_PROVIDER_ID } from '~~/shared/cloud/provider-ids';
import { registerAuthWorkspaceStore } from '~~/server/auth/store/registry';
import { registerSyncGatewayAdapter } from '~~/server/sync/gateway/registry';
import { registerStorageGatewayAdapter } from '~~/server/storage/gateway/registry';
import { registerProviderAdminAdapter } from '~~/server/admin/providers/registry';
import { registerAdminStoreProvider } from '~~/server/admin/stores/registry';
import { registerBackgroundJobProvider } from '~~/server/utils/background-jobs/registry';
import { registerRateLimitProvider } from '~~/server/utils/rate-limit/registry';
import { registerNotificationEmitter } from '~~/server/utils/notifications/registry';
import { registerWebhookStore } from '~~/server/utils/webhooks/store/registry';
import { registerDeploymentAdminChecker } from '~~/server/auth/deployment-admin';
import { registerConnectStore } from '~~/server/connect/store/registry';
import { createConvexAuthWorkspaceStore } from '../auth/convex-auth-workspace-store';
import { createConvexSyncGatewayAdapter } from '../sync/convex-sync-gateway-adapter';
import { createConvexStorageGatewayAdapter } from '../storage/convex-storage-gateway-adapter';
import { createConvexWebhookStore } from '../webhooks/convex-webhook-store';
import { convexSyncAdminAdapter } from '../admin/adapters/sync-convex';
import { convexStorageAdminAdapter } from '../admin/adapters/storage-convex';
import {
    createConvexWorkspaceAccessStore,
    createConvexWorkspaceSettingsStore,
    createConvexAdminUserStore,
} from '../admin/stores/convex-store';
import { ConvexDeploymentAdminChecker } from '../admin/deployment-admin-checker';
import { convexJobProvider } from '../background-jobs/convex-provider';
import { convexRateLimitProvider } from '../rate-limit/convex-provider';
import {
    emitBackgroundJobComplete,
    emitBackgroundJobError,
} from '../notifications/emit';
import { useRuntimeConfig } from '#imports';
import { createConvexConnectStore } from '../connect/convex-connect-store';
import { convexBackendContract } from '../../backend-contract';

const ALLOW_INSECURE_CONVEX_HTTP_ENV = 'OR3_CONVEX_ALLOW_INSECURE_HTTP';

type RuntimeConfigWithConvex = ReturnType<typeof useRuntimeConfig> & {
    sync?: {
        enabled?: boolean;
        provider?: string;
        convexUrl?: string;
    };
    storage?: {
        enabled?: boolean;
        provider?: string;
    };
    connect?: {
        enabled?: boolean;
        provider?: string;
    };
    public?: {
        sync?: {
            convexUrl?: string;
        };
    };
};

function isConvexSelected(config: RuntimeConfigWithConvex): boolean {
    const syncSelected =
        (config.sync?.enabled === true || config.auth.enabled === true) &&
        config.sync?.provider === CONVEX_PROVIDER_ID;
    const storageSelected =
        config.storage?.enabled === true &&
        config.storage?.provider === CONVEX_STORAGE_PROVIDER_ID;
    const connectSelected =
        config.connect?.enabled === true &&
        config.connect?.provider === CONVEX_PROVIDER_ID;
    return syncSelected || storageSelected || connectSelected;
}

function validateConvexStartupConfig(config: RuntimeConfigWithConvex): string[] {
    if (!isConvexSelected(config)) return [];

    const errors: string[] = [];
    const convexUrl =
        config.sync?.convexUrl?.trim() ??
        config.public?.sync?.convexUrl?.trim() ??
        '';

    if (!convexUrl) {
        errors.push('Missing Convex URL (runtimeConfig.sync.convexUrl).');
        return errors;
    }

    let parsed: URL | null = null;
    try {
        parsed = new URL(convexUrl);
    } catch {
        errors.push('Convex URL must be a valid URL.');
        return errors;
    }

    if (
        parsed.protocol === 'http:' &&
        process.env[ALLOW_INSECURE_CONVEX_HTTP_ENV] !== 'true'
    ) {
        errors.push(
            `Convex URL must use HTTPS unless ${ALLOW_INSECURE_CONVEX_HTTP_ENV}=true is explicitly set.`
        );
    }

    return errors;
}

// Nitro invokes plugins synchronously. Verify during module initialization so
// provider validation and the HTTP listener cannot race an unfinished probe.
const config = useRuntimeConfig() as RuntimeConfigWithConvex;
if (config.auth.enabled) {
    const errors = validateConvexStartupConfig(config);
    if (errors.length > 0) {
        throw new Error(
            `[or3-provider-convex] ${errors.join(' ')} Install/configure Convex provider env values and restart.`
        );
    }

    if (!import.meta.prerender && isConvexSelected(config)) {
        const url = config.sync?.convexUrl?.trim() || config.public?.sync?.convexUrl?.trim();
        try {
            const response = await fetch(`${url}/api/query`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ path: 'or3Backend:version', args: {}, format: 'json' }),
                signal: AbortSignal.timeout(10_000),
            });
            const result = await response.json();
            if (!response.ok || result.status !== 'success' || result.value?.digest !== convexBackendContract.digest) {
                throw new Error('Convex backend does not match this provider.');
            }
        } catch {
            throw new Error('[or3-provider-convex] Convex backend could not be verified or does not match this provider. Start the source checkout with bun run dev or bun run preview to apply its backend update; custom deployments must run the installed or3-provider-convex deploy command.');
        }
    }
}

export default defineNitroPlugin(() => {
    if (!config.auth.enabled) return;

    registerAuthWorkspaceStore({
        id: CONVEX_PROVIDER_ID,
        order: 100,
        create: createConvexAuthWorkspaceStore,
    });

    registerSyncGatewayAdapter({
        id: CONVEX_PROVIDER_ID,
        order: 100,
        create: createConvexSyncGatewayAdapter,
    });

    registerStorageGatewayAdapter({
        id: CONVEX_STORAGE_PROVIDER_ID,
        order: 100,
        create: createConvexStorageGatewayAdapter,
    });

    if (
        config.connect?.enabled === true &&
        config.connect.provider === CONVEX_PROVIDER_ID
    ) {
        registerConnectStore({
            id: CONVEX_PROVIDER_ID,
            order: 100,
            create: createConvexConnectStore,
        });
    }

    registerProviderAdminAdapter(convexSyncAdminAdapter);
    registerProviderAdminAdapter(convexStorageAdminAdapter);

    registerAdminStoreProvider({
        id: CONVEX_PROVIDER_ID,
        createWorkspaceAccessStore: createConvexWorkspaceAccessStore,
        createWorkspaceSettingsStore: createConvexWorkspaceSettingsStore,
        createAdminUserStore: createConvexAdminUserStore,
        getCapabilities: () => ({
            supportsServerSideAdmin: true,
            supportsUserSearch: true,
            supportsWorkspaceList: true,
            supportsWorkspaceManagement: true,
            supportsDeploymentAdminGrants: true,
        }),
    });

    registerBackgroundJobProvider(CONVEX_PROVIDER_ID, convexJobProvider);
    registerRateLimitProvider(CONVEX_PROVIDER_ID, convexRateLimitProvider);
    registerWebhookStore({
        id: CONVEX_PROVIDER_ID,
        order: 100,
        create: createConvexWebhookStore,
    });

    registerNotificationEmitter(CONVEX_PROVIDER_ID, {
        emitBackgroundJobComplete,
        emitBackgroundJobError,
    });

    registerDeploymentAdminChecker(CONVEX_PROVIDER_ID, () => new ConvexDeploymentAdminChecker());
});
