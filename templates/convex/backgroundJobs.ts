/**
 * @module convex/backgroundJobs
 *
 * Purpose:
 * Persists background streaming job state so jobs can survive reloads and
 * be observed from multiple clients or server processes.
 *
 * Behavior:
 * - Jobs are created in `streaming` state and updated incrementally
 * - Completion and failure are terminal states
 * - Aborts are explicit and only apply to streaming jobs
 * - Cleanup removes stale or timed-out jobs in batches
 *
 * Constraints:
 * - Every function is internal and may only be called by trusted server code.
 * - User-scoped reads and aborts still require an exact stored-owner match.
 * - Status transitions are not strictly enforced beyond simple guards.
 *
 * Non-Goals:
 * - Distributed job scheduling or retries
 * - Rich audit logging for job changes
 */

import { v } from 'convex/values';
import { internalMutation, internalQuery } from './_generated/server';

// ============================================================
// CONSTANTS
// ============================================================

/** Batch size for job cleanup operations */
const CLEANUP_BATCH_SIZE = 100;

// ============================================================
// MUTATIONS
// ============================================================

/**
 * `backgroundJobs.create` (internal mutation)
 *
 * Purpose:
 * Creates a new streaming job record for a user and thread.
 *
 * Behavior:
 * - Initializes `status` to `streaming`
 * - Initializes content and chunk counters
 */
export const create = internalMutation({
    args: {
        user_id: v.string(),
        thread_id: v.string(),
        message_id: v.string(),
        model: v.string(),
        kind: v.optional(v.union(v.literal('chat'), v.literal('workflow'))),
        tool_calls: v.optional(v.any()),
        workflow_state: v.optional(v.any()),
    },
    handler: async (ctx, args) => {
        const jobId = await ctx.db.insert('background_jobs', {
            user_id: args.user_id,
            thread_id: args.thread_id,
            message_id: args.message_id,
            model: args.model,
            kind: args.kind ?? 'chat',
            status: 'streaming',
            content: '',
            chunks_received: 0,
            ...(args.tool_calls !== undefined
                ? { tool_calls: args.tool_calls }
                : {}),
            ...(args.workflow_state !== undefined
                ? { workflow_state: args.workflow_state }
                : {}),
            started_at: Date.now(),
        });

        return jobId;
    },
});

/**
 * `backgroundJobs.get` (internal query)
 *
 * Purpose:
 * Retrieves a job by ID with an exact user ownership check.
 *
 * Authorization:
 * - The job must belong to the server-resolved `user_id`.
 */
export const get = internalQuery({
    args: {
        job_id: v.id('background_jobs'),
        user_id: v.string(),
    },
    handler: async (ctx, args) => {
        const job = await ctx.db.get(args.job_id);
        if (!job) return null;

        if (job.user_id !== args.user_id) {
            return null;
        }

        return {
            id: job._id,
            userId: job.user_id,
            threadId: job.thread_id,
            messageId: job.message_id,
            model: job.model,
            kind: job.kind,
            status: job.status,
            content: job.content,
            chunksReceived: job.chunks_received,
            startedAt: job.started_at,
            completedAt: job.completed_at,
            error: job.error,
            tool_calls: job.tool_calls,
            workflow_state: job.workflow_state,
        };
    },
});

/**
 * `backgroundJobs.update` (internal mutation)
 *
 * Purpose:
 * Appends streamed content and updates progress counters.
 *
 * Constraints:
 * - No-op if the job is not in `streaming` state.
 */
export const update = internalMutation({
    args: {
        job_id: v.id('background_jobs'),
        content_chunk: v.optional(v.string()),
        chunks_received: v.optional(v.number()),
        tool_calls: v.optional(v.any()),
        workflow_state: v.optional(v.any()),
    },
    handler: async (ctx, args) => {
        const job = await ctx.db.get(args.job_id);
        if (!job || job.status !== 'streaming') return;

        const patch: Record<string, unknown> = {};

        if (args.content_chunk !== undefined) {
            patch.content = job.content + args.content_chunk;
        }
        if (args.chunks_received !== undefined) {
            patch.chunks_received = args.chunks_received;
        }
        if (args.tool_calls !== undefined) {
            patch.tool_calls = args.tool_calls;
        }
        if (args.workflow_state !== undefined) {
            patch.workflow_state = args.workflow_state;
        }

        if (Object.keys(patch).length > 0) {
            await ctx.db.patch(args.job_id, patch);
        }
    },
});

/**
 * `backgroundJobs.complete` (internal mutation)
 *
 * Purpose:
 * Marks a job as completed and stores final content.
 */
export const complete = internalMutation({
    args: {
        job_id: v.id('background_jobs'),
        content: v.string(),
        tool_calls: v.optional(v.any()),
        workflow_state: v.optional(v.any()),
    },
    handler: async (ctx, args) => {
        const job = await ctx.db.get(args.job_id);
        if (!job) return;

        const patch: Record<string, unknown> = {
            status: 'complete',
            content: args.content,
            completed_at: Date.now(),
        };
        if (args.tool_calls !== undefined) {
            patch.tool_calls = args.tool_calls;
        }
        if (args.workflow_state !== undefined) {
            patch.workflow_state = args.workflow_state;
        }
        await ctx.db.patch(args.job_id, patch);
    },
});

/**
 * `backgroundJobs.fail` (internal mutation)
 *
 * Purpose:
 * Marks a job as failed and stores an error string.
 */
export const fail = internalMutation({
    args: {
        job_id: v.id('background_jobs'),
        error: v.string(),
    },
    handler: async (ctx, args) => {
        const job = await ctx.db.get(args.job_id);
        if (!job) return;

        await ctx.db.patch(args.job_id, {
            status: 'error',
            error: args.error,
            completed_at: Date.now(),
        });
    },
});

/**
 * `backgroundJobs.abort` (internal mutation)
 *
 * Purpose:
 * Requests cancellation of an active streaming job.
 *
 * Behavior:
 * - Returns `false` when the job is missing, not owned, or not streaming.
 */
export const abort = internalMutation({
    args: {
        job_id: v.id('background_jobs'),
        user_id: v.string(),
    },
    handler: async (ctx, args) => {
        const job = await ctx.db.get(args.job_id);
        if (!job) return false;

        if (job.user_id !== args.user_id) {
            return false;
        }

        // Can only abort streaming jobs
        if (job.status !== 'streaming') {
            return false;
        }

        await ctx.db.patch(args.job_id, {
            status: 'aborted',
            completed_at: Date.now(),
        });

        return true;
    },
});

/**
 * `backgroundJobs.checkAborted` (internal query)
 *
 * Purpose:
 * Lightweight polling endpoint to determine whether a job has been aborted.
 *
 * Behavior:
 * - Returns `true` when the job does not exist to allow callers to stop work.
 */
export const checkAborted = internalQuery({
    args: {
        job_id: v.id('background_jobs'),
    },
    handler: async (ctx, args) => {
        const job = await ctx.db.get(args.job_id);
        if (!job) return true; // Job doesn't exist, treat as aborted

        return job.status === 'aborted';
    },
});

/**
 * `backgroundJobs.cleanup` (internal mutation)
 *
 * Purpose:
 * Cleans up timed-out streaming jobs and removes stale completed jobs.
 *
 * Behavior:
 * - Times out streaming jobs older than `timeout_ms`
 * - Deletes completed, errored, or aborted jobs older than `retention_ms`
 */
export const cleanup = internalMutation({
    args: {
        timeout_ms: v.optional(v.number()),
        retention_ms: v.optional(v.number()),
    },
    handler: async (ctx, args) => {
        const timeoutMs = args.timeout_ms ?? 5 * 60 * 1000; // 5 minutes
        const retentionMs = args.retention_ms ?? 5 * 60 * 1000; // 5 minutes
        const now = Date.now();
        let cleaned = 0;

        // Get streaming jobs that have timed out (batched)
        const streamingJobs = await ctx.db
            .query('background_jobs')
            .withIndex('by_status', (q) => q.eq('status', 'streaming'))
            .take(CLEANUP_BATCH_SIZE);

        for (const job of streamingJobs) {
            const age = now - job.started_at;
            if (age > timeoutMs) {
                await ctx.db.patch(job._id, {
                    status: 'error',
                    error: 'Job timed out',
                    completed_at: now,
                });
                cleaned++;
            }
        }

        // Get completed jobs that are stale (batched)
        for (const status of ['complete', 'error', 'aborted'] as const) {
            const jobs = await ctx.db
                .query('background_jobs')
                .withIndex('by_status', (q) => q.eq('status', status))
                .take(CLEANUP_BATCH_SIZE);

            for (const job of jobs) {
                const completedAge = now - (job.completed_at ?? job.started_at);
                if (completedAge > retentionMs) {
                    await ctx.db.delete(job._id);
                    cleaned++;
                }
            }
        }

        return cleaned;
    },
});

/**
 * `backgroundJobs.getActiveCount` (internal query)
 *
 * Purpose:
 * Returns the number of currently streaming jobs.
 */
export const getActiveCount = internalQuery({
    args: {},
    handler: async (ctx) => {
        const jobs = await ctx.db
            .query('background_jobs')
            .withIndex('by_status', (q) => q.eq('status', 'streaming'))
            .collect();

        return jobs.length;
    },
});
