/**
 * @module server/admin/providers/adapters/sync-convex.ts
 *
 * Purpose:
 * Admin status adapter for the Convex sync provider.
 *
 * Constraints:
 * - History GC is unavailable until snapshot bootstrap is verified.
 */
import type { H3Event } from 'h3';
import { createError } from 'h3';
import {
    CLERK_PROVIDER_ID,
    CONVEX_PROVIDER_ID,
} from '~~/shared/cloud/provider-ids';
import { listProviderTokenBrokerIds } from '~~/server/auth/token-broker/registry';
import type {
    ProviderAdminAdapter,
    ProviderAdminStatusResult,
    ProviderStatusContext,
    ProviderActionContext,
} from '~~/server/admin/providers/types';
import { useRuntimeConfig } from '#imports';
import { SYNC_HISTORY_GC_POLICY } from '../../../utils/sync-history-gc-policy';

/**
 * Singleton implementation of the Convex Sync admin adapter.
 */
export const convexSyncAdminAdapter: ProviderAdminAdapter = {
    id: CONVEX_PROVIDER_ID,
    kind: 'sync',

    /**
     * Purpose:
     * Checks sync configuration and reports warnings.
     */
    async getStatus(_event: H3Event, ctx: ProviderStatusContext): Promise<ProviderAdminStatusResult> {
        const config = useRuntimeConfig();
        const warnings: ProviderAdminStatusResult['warnings'] = [];

        if (ctx.enabled && !config.sync.convexUrl) {
            warnings.push({
                level: 'error',
                message: 'Convex sync is enabled but no Convex URL is configured in the environment.',
            });
        }
        if (ctx.enabled) {
            const brokerIds = listProviderTokenBrokerIds();
            if (!brokerIds.includes(config.auth.provider)) {
                warnings.push({
                    level: 'warning',
                    message:
                        `No ProviderTokenBroker registered for auth provider "${config.auth.provider}". ` +
                        `Install the provider package that registers this broker (e.g. or3-provider-${config.auth.provider}).`,
                });
            } else if (config.auth.provider !== CLERK_PROVIDER_ID) {
                warnings.push({
                    level: 'warning',
                    message:
                        `Convex admin actions will use the "${config.auth.provider}" token broker for authentication.`,
                });
            }
        }
        warnings.push({
            level: 'warning',
            message: SYNC_HISTORY_GC_POLICY.reason,
        });

        return {
            details: {
                convexUrl: config.sync.convexUrl,
            },
            warnings,
            actions: [],
        };
    },

    /**
     * Purpose:
     * Rejects stale history-GC action IDs while snapshot bootstrap is absent.
     */
    async runAction(
        _event: H3Event,
        actionId: string,
        _payload: Record<string, unknown> | undefined,
        ctx: ProviderActionContext
    ): Promise<unknown> {
        if (!ctx.session.workspace?.id) {
            throw createError({
                statusCode: 400,
                statusMessage: 'Workspace not resolved',
            });
        }

        if (
            actionId === 'sync.gc-change-log' ||
            actionId === 'sync.gc-tombstones'
        ) {
            throw createError({
                statusCode: 503,
                statusMessage: SYNC_HISTORY_GC_POLICY.reason,
            });
        }

        throw createError({ statusCode: 400, statusMessage: 'Unknown action' });
    },
};
