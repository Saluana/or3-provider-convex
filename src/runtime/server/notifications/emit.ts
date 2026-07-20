/**
 * @module server/utils/notifications/emit
 *
 * Purpose:
 * Server-side helpers for emitting notification records to Convex.
 * These are used for background job completion and failure events.
 *
 * Responsibilities:
 * - Create notification records with consistent types and messages.
 * - Use the server Convex HTTP client and generated API.
 *
 * Non-Goals:
 * - Client delivery or realtime subscription logic.
 * - Deduplication or batching of notifications.
 *
 * Constraints:
 * - Server-only usage.
 */

import { convexInternalApi as internalApi } from '../../utils/convex-api';
import type { GenericId as Id } from 'convex/values';
import { getConvexClient } from '../utils/convex-client';
import { emitWebhookSystemHook } from '~~/server/utils/webhooks/runtime';

/**
 * Purpose:
 * Resolve the Convex client used for notification writes.
 */
const getNotificationsClient = () => getConvexClient();

async function emitNotificationWebhookEvent(input: {
    id: string;
    workspaceId: string;
    userId: string;
    threadId: string;
    type: string;
    title: string;
    body: string;
}): Promise<void> {
    const now = Date.now();
    await emitWebhookSystemHook('notify:action:push', {
        id: input.id,
        workspaceId: input.workspaceId,
        userId: input.userId,
        threadId: input.threadId,
        type: input.type,
        title: input.title,
        body: input.body,
        createdAt: now,
        updatedAt: now,
    });
}

/**
 * Purpose:
 * Emit a notification when a background job completes.
 *
 * Behavior:
 * - Writes to the Convex notifications table.
 * - Returns the created notification ID.
 *
 * Constraints:
 * - `jobId` is accepted for parity with callers but not stored in the payload.
 */
export async function emitBackgroundJobComplete(
    workspaceId: string,
    userId: string,
    threadId: string,
    jobId: string,
    messageId?: string
): Promise<string> {
    const client = getNotificationsClient();
    const type = 'ai.message.received';
    const title = 'AI response ready';
    const body = 'Your background response is ready.';
    const actions = [
        {
            id: crypto.randomUUID(),
            label: 'Open chat',
            kind: 'navigate',
            target: { threadId },
            data: messageId ? { messageId } : undefined,
        },
    ];

    const notificationId = await client.mutation(internalApi.notifications.create, {
        workspace_id: workspaceId as Id<'workspaces'>,
        user_id: userId,
        thread_id: threadId,
        type,
        title,
        body,
        actions,
    });

    await emitNotificationWebhookEvent({
        id: String(notificationId),
        workspaceId,
        userId,
        threadId,
        type,
        title,
        body,
    });

    return notificationId;
}

/**
 * Purpose:
 * Emit a notification when a background job fails.
 *
 * Behavior:
 * - Writes to the Convex notifications table.
 * - Returns the created notification ID.
 *
 * Constraints:
 * - `jobId` is accepted for parity with callers but not stored in the payload.
 */
export async function emitBackgroundJobError(
    workspaceId: string,
    userId: string,
    threadId: string,
    jobId: string,
    error: string
): Promise<string> {
    const client = getNotificationsClient();
    const type = 'ai.background.error';
    const title = 'Background response failed';
    const body = `Failed: ${error}`;

    const notificationId = await client.mutation(internalApi.notifications.create, {
        workspace_id: workspaceId as Id<'workspaces'>,
        user_id: userId,
        thread_id: threadId,
        type,
        title,
        body,
    });

    await emitNotificationWebhookEvent({
        id: String(notificationId),
        workspaceId,
        userId,
        threadId,
        type,
        title,
        body,
    });

    return notificationId;
}
