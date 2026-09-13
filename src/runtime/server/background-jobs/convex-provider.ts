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
    AdmissionCancellationResult,
    GenerationHistoryPhase,
    TerminalGenerationSnapshot,
} from '~~/server/utils/background-jobs/types';
import { getJobConfig } from '~~/server/utils/background-jobs/store';
import { convexInternalApi as internalApi } from '../../utils/convex-api';
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

function toBackgroundJob(job: any): BackgroundJob {
    return {
        id: job.id as string,
        userId: job.userId,
        threadId: job.threadId,
        messageId: job.messageId,
        model: job.model,
        kind: job.kind ?? undefined,
        status: job.status,
        content: job.content,
        reasoning: typeof job.reasoning === 'string' ? job.reasoning : '',
        generationId: job.generation_id ?? undefined,
        historyPhase: job.history_phase ?? undefined,
        syncProviderId: job.sync_provider_id ?? undefined,
        chunksReceived: job.chunksReceived,
        startedAt: job.startedAt,
        lastActivityAt: job.lastActivityAt ?? job.startedAt,
        completedAt: job.completedAt ?? undefined,
        error: job.error ?? undefined,
        tool_calls: job.tool_calls ?? undefined,
        workflow_state: job.workflow_state ?? undefined,
        execution: job.execution ?? undefined,
        leaseOwner: job.leaseOwner ?? undefined,
        leaseExpiresAt: job.leaseExpiresAt ?? undefined,
        attempts: job.attempts ?? 0,
    };
}

function assertLeaseWrite(result: unknown, leaseOwner?: string): void {
    if (leaseOwner && result === false) {
        const error = new Error('Background job lease was superseded');
        error.name = 'BackgroundJobLeaseLostError';
        throw error;
    }
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

        const result = await client.mutation(internalApi.backgroundJobs.create, {
            user_id: params.userId,
            thread_id: params.threadId,
            message_id: params.messageId,
            model: params.model,
            kind: params.kind,
            tool_calls: params.tool_calls,
            workflow_state: params.workflow_state,
            execution: params.execution,
            idempotency_key: params.idempotencyKey,
            generation_id: params.generationId,
            sync_provider_id: params.syncProviderId,
            history_phase: params.historyPhase,
            initial_content: params.initialContent,
            initial_reasoning: params.initialReasoning,
            max_concurrent_jobs: config.maxConcurrentJobs,
            max_concurrent_jobs_per_user: config.maxConcurrentJobsPerUser,
        });

        if (
            result &&
            typeof result === 'object' &&
            (result as { kind?: unknown }).kind === 'cancelled'
        ) {
            const error = new Error(
                `Background admission ${params.idempotencyKey ?? params.messageId} was cancelled`
            );
            error.name = 'AdmissionCancelledError';
            throw error;
        }

        return typeof result === 'string'
            ? result
            : String((result as { jobId: string }).jobId);
    },

    async getJob(jobId: string, userId: string): Promise<BackgroundJob | null> {
        const client = getClient();
        const job = await client.query(internalApi.backgroundJobs.get, {
            job_id: jobId as Id<'background_jobs'>,
            user_id: userId,
        });

        if (!job) return null;

        return toBackgroundJob(job);
    },

    async updateJob(jobId: string, update: JobUpdate): Promise<void> {
        const client = getClient();
        const updatePayload: Record<string, unknown> = {
            job_id: jobId as Id<'background_jobs'>,
            ...(update.contentChunk !== undefined
                ? { content_chunk: update.contentChunk }
                : {}),
            ...(update.reasoningChunk !== undefined
                ? { reasoning_chunk: update.reasoningChunk }
                : {}),
            ...(update.chunksReceived !== undefined
                ? { chunks_received: update.chunksReceived }
                : {}),
            lease_owner: update.leaseOwner,
        };

        const extendedUpdatePayload: Record<string, unknown> = {
            ...updatePayload,
            tool_calls: update.tool_calls,
            workflow_state: update.workflow_state,
        };

        const result = await client.mutation(
            internalApi.backgroundJobs.update,
            extendedUpdatePayload
        );
        assertLeaseWrite(result, update.leaseOwner);
    },

    async completeJob(
        jobId: string,
        finalContent: string,
        leaseOwner?: string
    ): Promise<void> {
        const client = getClient();
        const result = await client.mutation(internalApi.backgroundJobs.complete, {
            job_id: jobId as Id<'background_jobs'>,
            content: finalContent,
            lease_owner: leaseOwner,
        });
        assertLeaseWrite(result, leaseOwner);
    },

    async failJob(
        jobId: string,
        error: string,
        leaseOwner?: string
    ): Promise<void> {
        const client = getClient();
        const result = await client.mutation(internalApi.backgroundJobs.fail, {
            job_id: jobId as Id<'background_jobs'>,
            error,
            lease_owner: leaseOwner,
        });
        assertLeaseWrite(result, leaseOwner);
    },

    async saveTerminalSnapshot(
        jobId: string,
        snapshot: TerminalGenerationSnapshot,
        leaseOwner?: string
    ): Promise<boolean> {
        const client = getClient();
        return (
            (await client.mutation(
                internalApi.backgroundJobs.saveTerminalSnapshot,
                {
                    job_id: jobId as Id<'background_jobs'>,
                    status: snapshot.status,
                    content: snapshot.content,
                    reasoning: snapshot.reasoning,
                    tool_calls: snapshot.toolCalls,
                    error: snapshot.error,
                    completed_at: snapshot.completedAt,
                    lease_owner: leaseOwner,
                }
            )) === true
        );
    },

    async setHistoryPhase(
        jobId: string,
        phase: GenerationHistoryPhase,
        options?: { from?: GenerationHistoryPhase[] }
    ): Promise<boolean> {
        const client = getClient();
        return (
            (await client.mutation(internalApi.backgroundJobs.setHistoryPhase, {
                job_id: jobId as Id<'background_jobs'>,
                phase,
                from: options?.from,
            })) === true
        );
    },

    async abortJob(jobId: string, userId: string): Promise<boolean> {
        const client = getClient();
        return await client.mutation(internalApi.backgroundJobs.abort, {
            job_id: jobId as Id<'background_jobs'>,
            user_id: userId,
        });
    },

    async cancelAdmission(
        userId: string,
        admissionId: string
    ): Promise<AdmissionCancellationResult> {
        const client = getClient();
        const result = await client.mutation(
            internalApi.backgroundJobs.requestAdmissionCancel,
            {
                user_id: userId,
                admission_id: admissionId,
                ttl_ms: 10 * 60 * 1000,
            }
        );
        return {
            aborted: result?.aborted === true,
            pending: result?.pending === true,
            jobId:
                typeof result?.jobId === 'string' ? result.jobId : undefined,
        };
    },

    // Convex provider does not expose AbortControllers.
    getAbortController(_jobId: string): AbortController | undefined {
        return undefined;
    },

    async checkJobAborted(jobId: string): Promise<boolean> {
        const client = getClient();
        return await client.query(internalApi.backgroundJobs.checkAborted, {
            job_id: jobId as Id<'background_jobs'>,
        });
    },

    async claimJob(jobId, leaseOwner, now, leaseExpiresAt) {
        const client = getClient();
        const job = await client.mutation(internalApi.backgroundJobs.claim, {
            job_id: jobId as Id<'background_jobs'>,
            lease_owner: leaseOwner,
            lease_ms: Math.max(1, leaseExpiresAt - now),
        });
        return job ? toBackgroundJob(job) : null;
    },

    async claimNextJob(leaseOwner, now, leaseExpiresAt) {
        const client = getClient();
        const job = await client.mutation(internalApi.backgroundJobs.claimNext, {
            lease_owner: leaseOwner,
            lease_ms: Math.max(1, leaseExpiresAt - now),
        });
        return job ? toBackgroundJob(job) : null;
    },

    async renewJobLease(jobId, leaseOwner, now, leaseExpiresAt) {
        const client = getClient();
        return await client.mutation(internalApi.backgroundJobs.renewLease, {
            job_id: jobId as Id<'background_jobs'>,
            lease_owner: leaseOwner,
            lease_ms: Math.max(1, leaseExpiresAt - now),
        });
    },

    async updateJobExecution(jobId, execution, leaseOwner) {
        const client = getClient();
        return await client.mutation(
            internalApi.backgroundJobs.updateExecution,
            {
                job_id: jobId as Id<'background_jobs'>,
                execution,
                lease_owner: leaseOwner,
            }
        );
    },

    async getPendingHistoryJobs(limit) {
        const client = getClient();
        const jobs = await client.query(
            internalApi.backgroundJobs.listPendingHistory,
            { limit }
        );
        return Array.isArray(jobs) ? jobs.map(toBackgroundJob) : [];
    },

    async cleanupExpired(): Promise<number> {
        const client = getClient();
        const config = getJobConfig();
        return await client.mutation(internalApi.backgroundJobs.cleanup, {
            timeout_ms: config.jobTimeoutMs,
            retention_ms: config.completedJobRetentionMs,
        });
    },

    async getActiveJobCount(): Promise<number> {
        const client = getClient();
        return await client.query(internalApi.backgroundJobs.getActiveCount, {});
    },
};
