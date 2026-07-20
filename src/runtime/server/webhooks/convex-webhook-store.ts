import { randomUUID } from 'node:crypto';
import { ConvexHttpClient } from 'convex/browser';
import { useRuntimeConfig } from '#imports';
import { convexInternalApi as internalApi } from '../../utils/convex-api';
import {
    throwAsConvexServiceUnavailable,
    withConvexTransportRetry,
} from '../utils/convex-transport';
import type {
    WebhookDeliveryLog,
    WebhookHealth,
    WebhookRegistration,
    WebhookStore,
} from '~~/server/utils/webhooks/store/types';

type RuntimeConfigWithConvex = ReturnType<typeof useRuntimeConfig> & {
    sync?: {
        convexUrl?: string;
        convexAdminKey?: string;
    };
};

type ConvexWebhookRow = {
    id: string;
    scope: 'user' | 'admin';
    user_id?: string;
    workspace_id?: string;
    url: string;
    label: string;
    events: string[];
    custom_hooks: string[];
    signing_secret_enc: string;
    enabled: boolean;
    health: WebhookHealth;
    created_at: number;
    updated_at: number;
};

type ConvexDeliveryLogRow = {
    id: string;
    webhook_id: string;
    event_id: string;
    event_type: string;
    attempt: number;
    status: WebhookDeliveryLog['status'];
    claimed_by?: string;
    claimed_at?: number;
    http_status?: number;
    error_message?: string;
    request_payload: string;
    response_body?: string;
    duration_ms?: number;
    next_retry_at?: number;
    created_at: number;
};

let adminClient: ConvexHttpClient | null = null;

function getConvexWebhookClient(): ConvexHttpClient {
    if (adminClient) {
        return adminClient;
    }

    const config = useRuntimeConfig() as RuntimeConfigWithConvex;
    const convexUrl = config.sync?.convexUrl?.trim();
    if (!convexUrl) {
        throw new Error('Convex URL not configured');
    }

    const client = new ConvexHttpClient(convexUrl);
    const adminKey = config.sync?.convexAdminKey?.trim();
    if (!adminKey) {
        throw new Error('Convex admin key not configured');
    }
    const issuer = 'https://or3.ai/internal';
    const subject = 'or3-webhooks-store';
    client.setAdminAuth(adminKey, {
        subject,
        issuer,
        tokenIdentifier: `${issuer}|${subject}`,
    });

    adminClient = client;
    return client;
}

async function runConvexOperation<T>(
    operation: string,
    run: () => Promise<T>
): Promise<T> {
    try {
        return await withConvexTransportRetry(operation, run);
    } catch (error) {
        throwAsConvexServiceUnavailable(error, 'Webhook store unavailable');
    }
}

function toWebhookRegistration(row: ConvexWebhookRow): WebhookRegistration {
    return {
        id: row.id,
        scope: row.scope,
        user_id: row.user_id ?? null,
        workspace_id: row.workspace_id ?? null,
        url: row.url,
        label: row.label,
        events: row.events,
        custom_hooks: row.custom_hooks,
        signing_secret_enc: row.signing_secret_enc,
        enabled: row.enabled,
        health: row.health,
        created_at: row.created_at,
        updated_at: row.updated_at,
    };
}

function toDeliveryLog(row: ConvexDeliveryLogRow): WebhookDeliveryLog {
    return {
        id: row.id,
        webhook_id: row.webhook_id,
        event_id: row.event_id,
        event_type: row.event_type,
        attempt: row.attempt,
        status: row.status,
        claimed_by: row.claimed_by ?? null,
        claimed_at: row.claimed_at ?? null,
        http_status: row.http_status ?? null,
        error_message: row.error_message ?? null,
        request_payload: row.request_payload,
        response_body: row.response_body ?? null,
        duration_ms: row.duration_ms ?? null,
        next_retry_at: row.next_retry_at ?? null,
        created_at: row.created_at,
    };
}

class ConvexWebhookStore implements WebhookStore {
    async createWebhook(
        webhook: Omit<
            WebhookRegistration,
            'id' | 'health' | 'created_at' | 'updated_at'
        >
    ): Promise<WebhookRegistration> {
        const now = Date.now();
        const row: WebhookRegistration = {
            ...webhook,
            id: randomUUID(),
            health: 'unknown',
            created_at: now,
            updated_at: now,
        };

        const client = getConvexWebhookClient();
        const created = await runConvexOperation('webhooks.createWebhook', () =>
            client.mutation(internalApi.webhooks.createWebhook, {
                id: row.id,
                scope: row.scope,
                user_id: row.user_id,
                workspace_id: row.workspace_id,
                url: row.url,
                label: row.label,
                events: row.events,
                custom_hooks: row.custom_hooks,
                signing_secret_enc: row.signing_secret_enc,
                enabled: row.enabled,
                health: row.health,
                created_at: row.created_at,
                updated_at: row.updated_at,
            })
        );

        return toWebhookRegistration(created as ConvexWebhookRow);
    }

    async updateWebhook(
        webhookId: string,
        patch: Partial<
            Pick<
                WebhookRegistration,
                'url' | 'label' | 'events' | 'custom_hooks' | 'enabled' | 'workspace_id'
            >
        >
    ): Promise<WebhookRegistration> {
        const current = await this.getWebhook(webhookId);
        if (!current) {
            throw new Error(`Webhook not found: ${webhookId}`);
        }

        const urlChanged =
            typeof patch.url === 'string' && patch.url !== current.url;

        const next: WebhookRegistration = {
            ...current,
            ...patch,
            events: patch.events ?? current.events,
            custom_hooks: patch.custom_hooks ?? current.custom_hooks,
            health: urlChanged ? 'unknown' : current.health,
            updated_at: Date.now(),
        };

        const client = getConvexWebhookClient();
        const updated = await runConvexOperation('webhooks.updateWebhook', () =>
            client.mutation(internalApi.webhooks.updateWebhook, {
                webhook_id: webhookId,
                url: next.url,
                label: next.label,
                events: next.events,
                custom_hooks: next.custom_hooks,
                enabled: next.enabled,
                workspace_id: next.workspace_id,
                health: next.health,
                updated_at: next.updated_at,
            })
        );

        if (!updated) {
            throw new Error(`Webhook not found: ${webhookId}`);
        }

        return toWebhookRegistration(updated as ConvexWebhookRow);
    }

    async deleteWebhook(webhookId: string): Promise<void> {
        const client = getConvexWebhookClient();
        await runConvexOperation('webhooks.deleteWebhook', () =>
            client.mutation(internalApi.webhooks.deleteWebhook, {
                webhook_id: webhookId,
            })
        );
    }

    async getWebhook(webhookId: string): Promise<WebhookRegistration | null> {
        const client = getConvexWebhookClient();
        const row = await runConvexOperation('webhooks.getWebhook', () =>
            client.query(internalApi.webhooks.getWebhook, {
                webhook_id: webhookId,
            })
        );

        return row ? toWebhookRegistration(row as ConvexWebhookRow) : null;
    }

    async listWebhooks(
        userId: string,
        workspaceId: string
    ): Promise<WebhookRegistration[]> {
        const client = getConvexWebhookClient();
        const rows = await runConvexOperation('webhooks.listWebhooks', () =>
            client.query(internalApi.webhooks.listWebhooks, {
                user_id: userId,
                workspace_id: workspaceId,
            })
        );

        return (rows as ConvexWebhookRow[]).map(toWebhookRegistration);
    }

    async listAdminWebhooks(): Promise<WebhookRegistration[]> {
        const client = getConvexWebhookClient();
        const rows = await runConvexOperation('webhooks.listAdminWebhooks', () =>
            client.query(internalApi.webhooks.listAdminWebhooks, {})
        );

        return (rows as ConvexWebhookRow[]).map(toWebhookRegistration);
    }

    async listWebhooksByEvent(
        eventType: string,
        scope: 'user' | 'admin',
        workspaceId?: string
    ): Promise<WebhookRegistration[]> {
        const client = getConvexWebhookClient();
        const rows = await runConvexOperation('webhooks.listWebhooksByEvent', () =>
            client.query(internalApi.webhooks.listWebhooksByEvent, {
                event_type: eventType,
                scope,
                workspace_id: workspaceId,
            })
        );

        return (rows as ConvexWebhookRow[]).map(toWebhookRegistration);
    }

    async listWebhooksByCustomHook(hookName: string): Promise<WebhookRegistration[]> {
        const client = getConvexWebhookClient();
        const rows = await runConvexOperation('webhooks.listWebhooksByCustomHook', () =>
            client.query(internalApi.webhooks.listWebhooksByCustomHook, {
                hook_name: hookName,
            })
        );

        return (rows as ConvexWebhookRow[]).map(toWebhookRegistration);
    }

    async listActiveCustomHookNames(): Promise<string[]> {
        const client = getConvexWebhookClient();
        const rows = await runConvexOperation('webhooks.listActiveCustomHookNames', () =>
            client.query(internalApi.webhooks.listActiveCustomHookNames, {})
        );

        return rows as string[];
    }

    async updateWebhookHealth(
        webhookId: string,
        health: WebhookHealth
    ): Promise<void> {
        const client = getConvexWebhookClient();
        await runConvexOperation('webhooks.updateWebhookHealth', () =>
            client.mutation(internalApi.webhooks.updateWebhookHealth, {
                webhook_id: webhookId,
                health,
                updated_at: Date.now(),
            })
        );
    }

    async disableAllWebhooks(userId: string, workspaceId: string): Promise<number> {
        const client = getConvexWebhookClient();
        const count = await runConvexOperation('webhooks.disableAllWebhooks', () =>
            client.mutation(internalApi.webhooks.disableAllWebhooks, {
                user_id: userId,
                workspace_id: workspaceId,
                updated_at: Date.now(),
            })
        );

        return count as number;
    }

    async createDeliveryLog(
        log: Omit<WebhookDeliveryLog, 'id'>
    ): Promise<WebhookDeliveryLog> {
        const row: WebhookDeliveryLog = {
            ...log,
            id: randomUUID(),
        };

        const client = getConvexWebhookClient();
        const created = await runConvexOperation('webhooks.createDeliveryLog', () =>
            client.mutation(internalApi.webhooks.createDeliveryLog, {
                id: row.id,
                webhook_id: row.webhook_id,
                event_id: row.event_id,
                event_type: row.event_type,
                attempt: row.attempt,
                status: row.status,
                claimed_by: row.claimed_by,
                claimed_at: row.claimed_at,
                http_status: row.http_status,
                error_message: row.error_message,
                request_payload: row.request_payload,
                response_body: row.response_body,
                duration_ms: row.duration_ms,
                next_retry_at: row.next_retry_at,
                created_at: row.created_at,
            })
        );

        return toDeliveryLog(created as ConvexDeliveryLogRow);
    }

    async updateDeliveryLog(
        logId: string,
        patch: Partial<
            Pick<
                WebhookDeliveryLog,
                | 'status'
                | 'http_status'
                | 'error_message'
                | 'response_body'
                | 'duration_ms'
                | 'next_retry_at'
                | 'attempt'
            >
        >
    ): Promise<void> {
        const client = getConvexWebhookClient();
        await runConvexOperation('webhooks.updateDeliveryLog', () =>
            client.mutation(internalApi.webhooks.updateDeliveryLog, {
                log_id: logId,
                status: patch.status,
                http_status: patch.http_status,
                error_message: patch.error_message,
                response_body: patch.response_body,
                duration_ms: patch.duration_ms,
                next_retry_at: patch.next_retry_at,
                attempt: patch.attempt,
            })
        );
    }

    async getDeliveryLogs(
        webhookId: string,
        since: number
    ): Promise<WebhookDeliveryLog[]> {
        const client = getConvexWebhookClient();
        const rows = await runConvexOperation('webhooks.getDeliveryLogs', () =>
            client.query(internalApi.webhooks.getDeliveryLogs, {
                webhook_id: webhookId,
                since,
            })
        );

        return (rows as ConvexDeliveryLogRow[]).map(toDeliveryLog);
    }

    async getRecentTerminalDeliveries(
        webhookId: string,
        limit: number
    ): Promise<WebhookDeliveryLog[]> {
        const client = getConvexWebhookClient();
        const rows = await runConvexOperation(
            'webhooks.getRecentTerminalDeliveries',
            () =>
                client.query(internalApi.webhooks.getRecentTerminalDeliveries, {
                    webhook_id: webhookId,
                    limit,
                })
        );

        return (rows as ConvexDeliveryLogRow[]).map(toDeliveryLog);
    }

    async claimPendingDeliveries(
        workerId: string,
        limit: number
    ): Promise<WebhookDeliveryLog[]> {
        const safeLimit = Math.max(0, Math.floor(limit));
        if (safeLimit === 0) {
            return [];
        }

        const client = getConvexWebhookClient();
        const rows = await runConvexOperation('webhooks.claimPendingDeliveries', () =>
            client.mutation(internalApi.webhooks.claimPendingDeliveries, {
                worker_id: workerId,
                limit: safeLimit,
                now: Date.now(),
            })
        );

        return (rows as ConvexDeliveryLogRow[]).map(toDeliveryLog);
    }

    async resetStaleInFlightDeliveries(olderThanMs: number): Promise<number> {
        const cutoff = Date.now() - Math.max(0, Math.floor(olderThanMs));
        const client = getConvexWebhookClient();
        const count = await runConvexOperation('webhooks.resetStaleInFlightDeliveries', () =>
            client.mutation(internalApi.webhooks.resetStaleInFlightDeliveries, {
                cutoff,
            })
        );

        return count as number;
    }

    async cancelDeliveriesByWebhook(webhookId: string): Promise<number> {
        const client = getConvexWebhookClient();
        const count = await runConvexOperation('webhooks.cancelDeliveriesByWebhook', () =>
            client.mutation(internalApi.webhooks.cancelDeliveriesByWebhook, {
                webhook_id: webhookId,
            })
        );

        return count as number;
    }

    async deleteDeliveryLogsByWebhook(webhookId: string): Promise<number> {
        const client = getConvexWebhookClient();
        const count = await runConvexOperation('webhooks.deleteDeliveryLogsByWebhook', () =>
            client.mutation(internalApi.webhooks.deleteDeliveryLogsByWebhook, {
                webhook_id: webhookId,
            })
        );

        return count as number;
    }

    async purgeExpiredLogs(beforeTimestamp: number): Promise<number> {
        const client = getConvexWebhookClient();
        const count = await runConvexOperation('webhooks.purgeExpiredLogs', () =>
            client.mutation(internalApi.webhooks.purgeExpiredLogs, {
                before_timestamp: beforeTimestamp,
            })
        );

        return count as number;
    }
}

export function createConvexWebhookStore(): WebhookStore {
    return new ConvexWebhookStore();
}
