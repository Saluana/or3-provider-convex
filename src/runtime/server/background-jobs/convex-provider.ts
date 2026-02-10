/**
 * @module server/utils/background-jobs/providers/convex
 *
 * Purpose:
 * Convex-backed background job provider for multi-instance deployments.
 * Jobs persist across restarts and can be shared between servers.
 *
 * Responsibilities:
 * - Persist job lifecycle state in Convex.
 * - Enforce concurrency limits using Convex queries.
 * - Provide poll-based abort checks for streaming loops.
 *
 * Non-Goals:
 * - In-process AbortController support.
 * - Client-facing streaming delivery.
 */

import type {
    BackgroundJobProvider,
    BackgroundJob,
    CreateJobParams,
    JobUpdate,
} from '~~/server/utils/background-jobs/types';
import { getJobConfig } from '~~/server/utils/background-jobs/store';
import { convexApi as api } from '../../utils/convex-api';
import type { GenericId as Id } from 'convex/values';
import { getConvexClient } from '../utils/convex-client';
import { CONVEX_PROVIDER_ID } from '~~/shared/cloud/provider-ids';

/**
 * Purpose:
 * Resolve a Convex HTTP client for server-side calls.
 */
function getClient() {
    return getConvexClient();
}

function isExtraFieldValidationError(error: unknown): boolean {
    if (!(error instanceof Error)) return false;
    return (
        error.message.includes('ArgumentValidationError') &&
        error.message.includes('extra field')
    );
}

/**
 * Purpose:
 * Convex provider implementation for background jobs.
 *
 * Constraints:
 * - Abort is detected via polling, not AbortController.
 */
export const convexJobProvider: BackgroundJobProvider = {
    name: CONVEX_PROVIDER_ID,

    async createJob(params: CreateJobParams): Promise<string> {
        const client = getClient();
        const config = getJobConfig();

        // Check active count first
        const activeCount = await client.query(
            api.backgroundJobs.getActiveCount,
            {}
        );
        if (activeCount >= config.maxConcurrentJobs) {
            throw new Error(
                `Max concurrent background jobs reached (${config.maxConcurrentJobs})`
            );
        }

        const legacyCreatePayload = {
            user_id: params.userId,
            thread_id: params.threadId,
            message_id: params.messageId,
            model: params.model,
        };

        let jobId: unknown;
        try {
            jobId = await client.mutation(api.backgroundJobs.create, {
                ...legacyCreatePayload,
                kind: params.kind,
                tool_calls: params.tool_calls,
                workflow_state: params.workflow_state,
            });
        } catch (error) {
            // Backward-compat: deployed Convex validators may not include new fields yet.
            if (!isExtraFieldValidationError(error)) {
                throw error;
            }
            jobId = await client.mutation(
                api.backgroundJobs.create,
                legacyCreatePayload
            );
        }

        return jobId as string;
    },

    async getJob(jobId: string, userId: string): Promise<BackgroundJob | null> {
        const client = getClient();
        const job = await client.query(api.backgroundJobs.get, {
            job_id: jobId as Id<'background_jobs'>,
            user_id: userId,
        });

        if (!job) return null;

        return {
            id: job.id as string,
            userId: job.userId,
            threadId: job.threadId,
            messageId: job.messageId,
            model: job.model,
            kind: job.kind ?? undefined,
            status: job.status,
            content: job.content,
            chunksReceived: job.chunksReceived,
            startedAt: job.startedAt,
            completedAt: job.completedAt ?? undefined,
            error: job.error ?? undefined,
            tool_calls: job.tool_calls ?? undefined,
            workflow_state: job.workflow_state ?? undefined,
        };
    },

    async updateJob(jobId: string, update: JobUpdate): Promise<void> {
        const client = getClient();
        const legacyUpdatePayload: Record<string, unknown> = {
            job_id: jobId as Id<'background_jobs'>,
            ...(update.contentChunk !== undefined
                ? { content_chunk: update.contentChunk }
                : {}),
            ...(update.chunksReceived !== undefined
                ? { chunks_received: update.chunksReceived }
                : {}),
        };

        const extendedUpdatePayload: Record<string, unknown> = {
            ...legacyUpdatePayload,
            tool_calls: update.tool_calls,
            workflow_state: update.workflow_state,
        };

        try {
            await client.mutation(api.backgroundJobs.update, extendedUpdatePayload);
        } catch (error) {
            // Backward-compat: deployed Convex validators may not include new fields yet.
            if (
                !isExtraFieldValidationError(error) ||
                (update.tool_calls === undefined &&
                    update.workflow_state === undefined)
            ) {
                throw error;
            }
            await client.mutation(api.backgroundJobs.update, legacyUpdatePayload);
        }
    },

    async completeJob(jobId: string, finalContent: string): Promise<void> {
        const client = getClient();
        await client.mutation(api.backgroundJobs.complete, {
            job_id: jobId as Id<'background_jobs'>,
            content: finalContent,
        });
    },

    async failJob(jobId: string, error: string): Promise<void> {
        const client = getClient();
        await client.mutation(api.backgroundJobs.fail, {
            job_id: jobId as Id<'background_jobs'>,
            error,
        });
    },

    async abortJob(jobId: string, userId: string): Promise<boolean> {
        const client = getClient();
        return await client.mutation(api.backgroundJobs.abort, {
            job_id: jobId as Id<'background_jobs'>,
            user_id: userId,
        });
    },

    // Convex provider does not expose AbortControllers.
    getAbortController(_jobId: string): AbortController | undefined {
        return undefined;
    },

    async checkJobAborted(jobId: string): Promise<boolean> {
        const client = getClient();
        return await client.query(api.backgroundJobs.checkAborted, {
            job_id: jobId as Id<'background_jobs'>,
        });
    },

    async cleanupExpired(): Promise<number> {
        const client = getClient();
        const config = getJobConfig();
        return await client.mutation(api.backgroundJobs.cleanup, {
            timeout_ms: config.jobTimeoutMs,
            retention_ms: config.completedJobRetentionMs,
        });
    },

    async getActiveJobCount(): Promise<number> {
        const client = getClient();
        return await client.query(api.backgroundJobs.getActiveCount, {});
    },
};
