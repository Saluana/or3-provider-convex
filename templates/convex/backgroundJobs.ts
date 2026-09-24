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
 * - Admission and lease claims are atomic Convex mutations.
 *
 * Non-Goals:
 * - Rich audit logging for job changes
 */

import { v } from 'convex/values';
import { internalMutation, internalQuery } from './_generated/server';

// ============================================================
// CONSTANTS
// ============================================================

/** Batch size for job cleanup operations */
const CLEANUP_BATCH_SIZE = 100;

/**
 * Applies an atomic claim to a job row and returns the public projection.
 * Recovery resets text and reasoning to their durable checkpoints together.
 */
// eslint-disable-next-line @typescript-eslint/no-explicit-any
function claimJobRecord(
    job: any,
    leaseOwner: string,
    leaseExpiresAt: number,
    now: number
) {
    const execution = (job.execution ?? {}) as {
        contentBase?: unknown;
        reasoningBase?: unknown;
    };
    const contentBase =
        typeof execution.contentBase === 'string' ? execution.contentBase : '';
    const reasoningBase =
        typeof execution.reasoningBase === 'string'
            ? execution.reasoningBase
            : '';
    const attempts = ((job.attempts as number | undefined) ?? 0) + 1;
    const patch: Record<string, unknown> = {
        lease_owner: leaseOwner,
        lease_expires_at: leaseExpiresAt,
        last_activity_at: now,
        attempts,
        ...(attempts > 1
            ? {
                  content: contentBase,
                  reasoning: reasoningBase,
                  chunks_received: 0,
              }
            : {}),
    };
    const result = {
        id: job._id,
        userId: job.user_id,
        threadId: job.thread_id,
        messageId: job.message_id,
        model: job.model,
        kind: job.kind,
        status: job.status,
        content: attempts > 1 ? contentBase : job.content,
        reasoning:
            attempts > 1
                ? reasoningBase
                : typeof job.reasoning === 'string'
                  ? job.reasoning
                  : '',
        generationId: job.generation_id,
        historyPhase: job.history_phase,
        syncProviderId: job.sync_provider_id,
        chunksReceived: attempts > 1 ? 0 : job.chunks_received,
        startedAt: job.started_at,
        lastActivityAt: now,
        completedAt: job.completed_at,
        error: job.error,
        tool_calls: job.tool_calls,
        workflow_state: job.workflow_state,
        execution: job.execution,
        leaseOwner,
        leaseExpiresAt,
        attempts,
    };
    return { patch, result };
}

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
        execution: v.optional(v.any()),
        idempotency_key: v.optional(v.string()),
        generation_id: v.optional(v.string()),
        sync_provider_id: v.optional(v.string()),
        history_phase: v.optional(v.string()),
        initial_content: v.optional(v.string()),
        initial_reasoning: v.optional(v.string()),
        max_concurrent_jobs: v.number(),
        max_concurrent_jobs_per_user: v.number(),
    },
    handler: async (ctx, args) => {
        if (args.idempotency_key) {
            const matching = await ctx.db
                .query('background_jobs')
                .withIndex('by_user_idempotency', (q) =>
                    q
                        .eq('user_id', args.user_id)
                        .eq('idempotency_key', args.idempotency_key)
                )
                .collect();
            const existing = matching[0];
            if (existing) {
                if (
                    existing.status === 'streaming' &&
                    existing.kind !== 'workflow' &&
                    existing.execution === undefined
                ) {
                    await ctx.db.patch(existing._id, {
                        status: 'error',
                        error:
                            'Background job predates durable recovery. Retry the message.',
                        completed_at: Date.now(),
                    });
                }
                return existing._id;
            }
            const cancelMatches = await ctx.db
                .query('background_admission_cancels')
                .withIndex('by_user_admission', (q) =>
                    q
                        .eq('user_id', args.user_id)
                        .eq('admission_id', args.idempotency_key!)
                )
                .collect();
            const cancel = cancelMatches[0];
            if (cancel && cancel.expires_at > Date.now()) {
                // A Stop was recorded before this admission committed.
                return { kind: 'cancelled' as const };
            }
        }

        // Convex mutations are serialized transactions, so the count checks
        // and insert form one atomic admission decision across instances.
        const active = await ctx.db
            .query('background_jobs')
            .withIndex('by_status', (q) => q.eq('status', 'streaming'))
            .collect();
        // Jobs admitted before resumable execution payloads existed cannot be
        // safely claimed after a rolling upgrade. Terminalize them atomically
        // so they neither hang forever nor consume all admission capacity.
        const recoverable = active.filter(
            (job) => job.kind === 'workflow' || job.execution !== undefined
        );
        for (const job of active) {
            if (job.kind === 'workflow' || job.execution !== undefined)
                continue;
            await ctx.db.patch(job._id, {
                status: 'error',
                error:
                    'Background job predates durable recovery. Retry the message.',
                completed_at: Date.now(),
            });
        }
        const runnable = recoverable.filter(
            (job) => !job.execution?.clientToolCall
        );
        const parked = recoverable.filter(
            (job) => Boolean(job.execution?.clientToolCall)
        );
        if (runnable.length >= args.max_concurrent_jobs) {
            throw new Error(
                `Max concurrent background jobs reached (${args.max_concurrent_jobs})`
            );
        }
        const activeForUser = runnable.filter(
            (job) => job.user_id === args.user_id
        ).length;
        if (activeForUser >= args.max_concurrent_jobs_per_user) {
            throw new Error(
                `Max concurrent background jobs per user reached (${args.max_concurrent_jobs_per_user})`
            );
        }
        if (
            parked.length >= args.max_concurrent_jobs ||
            parked.filter((job) => job.user_id === args.user_id).length >=
                args.max_concurrent_jobs_per_user
        ) {
            throw new Error('Maximum pending browser tool handoffs reached');
        }

        const now = Date.now();
        const jobId = await ctx.db.insert('background_jobs', {
            user_id: args.user_id,
            thread_id: args.thread_id,
            message_id: args.message_id,
            model: args.model,
            kind: args.kind ?? 'chat',
            status: 'streaming',
            content: args.initial_content ?? '',
            reasoning: args.initial_reasoning ?? '',
            ...(args.generation_id !== undefined
                ? { generation_id: args.generation_id }
                : {}),
            ...(args.sync_provider_id !== undefined
                ? { sync_provider_id: args.sync_provider_id }
                : {}),
            history_phase: args.history_phase ?? 'ready',
            chunks_received: 0,
            ...(args.tool_calls !== undefined
                ? { tool_calls: args.tool_calls }
                : {}),
            ...(args.workflow_state !== undefined
                ? { workflow_state: args.workflow_state }
                : {}),
            ...(args.execution !== undefined
                ? { execution: args.execution }
                : {}),
            ...(args.idempotency_key !== undefined
                ? { idempotency_key: args.idempotency_key }
                : {}),
            attempts: 0,
            started_at: now,
            last_activity_at: now
        });

        return { kind: 'created' as const, jobId };
    },
});

/**
 * `backgroundJobs.requestAdmissionCancel` (internal mutation)
 *
 * Purpose:
 * Durably cancels an admission, aborting a committed streaming job when one
 * exists and otherwise leaving a marker that creation observes atomically.
 */
export const requestAdmissionCancel = internalMutation({
    args: {
        user_id: v.string(),
        admission_id: v.string(),
        ttl_ms: v.number(),
    },
    handler: async (ctx, args) => {
        const now = Date.now();
        const existingMarker = (
            await ctx.db
                .query('background_admission_cancels')
                .withIndex('by_user_admission', (q) =>
                    q
                        .eq('user_id', args.user_id)
                        .eq('admission_id', args.admission_id)
                )
                .collect()
        )[0];
        if (existingMarker) {
            await ctx.db.patch(existingMarker._id, {
                expires_at: now + args.ttl_ms,
            });
        } else {
            await ctx.db.insert('background_admission_cancels', {
                user_id: args.user_id,
                admission_id: args.admission_id,
                expires_at: now + args.ttl_ms,
            });
        }

        const matching = await ctx.db
            .query('background_jobs')
            .withIndex('by_user_idempotency', (q) =>
                q
                    .eq('user_id', args.user_id)
                    .eq('idempotency_key', args.admission_id)
            )
            .collect();
        const job = matching[0];
        if (!job) {
            return { aborted: false, pending: true };
        }
        if (job.status === 'streaming') {
            await ctx.db.patch(job._id, {
                status: 'aborted',
                error: 'Cancelled by user',
                completed_at: now,
                lease_owner: undefined,
                lease_expires_at: undefined,
                history_phase:
                    job.generation_id !== undefined &&
                    (job.history_phase ?? 'ready') === 'ready'
                        ? 'finalization_pending'
                        : job.history_phase,
            });
            return { aborted: true, jobId: job._id, pending: false };
        }
        return { aborted: false, jobId: job._id, pending: false };
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
            lastActivityAt: job.last_activity_at ?? job.started_at,
            completedAt: job.completed_at,
            error: job.error,
            tool_calls: job.tool_calls,
            workflow_state: job.workflow_state,
            execution: job.execution,
            leaseOwner: job.lease_owner,
            leaseExpiresAt: job.lease_expires_at,
            attempts: job.attempts ?? 0,
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
        reasoning_chunk: v.optional(v.string()),
        chunks_received: v.optional(v.number()),
        tool_calls: v.optional(v.any()),
        workflow_state: v.optional(v.any()),
        lease_owner: v.optional(v.string()),
    },
    handler: async (ctx, args) => {
        const job = await ctx.db.get(args.job_id);
        if (!job || job.status !== 'streaming') return false;
        if (
            job.lease_owner !== undefined &&
            (job.lease_owner !== args.lease_owner ||
                (job.lease_expires_at ?? 0) <= Date.now())
        ) {
            return false;
        }

        const patch: Record<string, unknown> = {
            last_activity_at: Date.now()
        };

        if (args.content_chunk !== undefined) {
            patch.content = job.content + args.content_chunk;
        }
        if (args.reasoning_chunk !== undefined) {
            patch.reasoning = (job.reasoning ?? '') + args.reasoning_chunk;
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
        return true;
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
        lease_owner: v.optional(v.string()),
    },
    handler: async (ctx, args) => {
        const job = await ctx.db.get(args.job_id);
        if (!job || job.status !== 'streaming') return false;
        if (
            job.lease_owner !== undefined &&
            (job.lease_owner !== args.lease_owner ||
                (job.lease_expires_at ?? 0) <= Date.now())
        ) {
            return false;
        }

        const patch: Record<string, unknown> = {
            status: 'complete',
            content: args.content,
            completed_at: Date.now(),
            last_activity_at: Date.now()
        };
        if (args.tool_calls !== undefined) {
            patch.tool_calls = args.tool_calls;
        }
        if (args.workflow_state !== undefined) {
            patch.workflow_state = args.workflow_state;
        }
        await ctx.db.patch(args.job_id, patch);
        return true;
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
        lease_owner: v.optional(v.string()),
    },
    handler: async (ctx, args) => {
        const job = await ctx.db.get(args.job_id);
        if (!job || job.status !== 'streaming') return false;
        if (
            job.lease_owner !== undefined &&
            (job.lease_owner !== args.lease_owner ||
                (job.lease_expires_at ?? 0) <= Date.now())
        ) {
            return false;
        }

        await ctx.db.patch(args.job_id, {
            status: 'error',
            error: args.error,
            completed_at: Date.now(),
            last_activity_at: Date.now()
        });
        return true;
    },
});

/** Atomically claim a specific recoverable job. */
/**
 * `backgroundJobs.saveTerminalSnapshot` (internal mutation)
 *
 * Purpose:
 * Atomically persists the terminal generation snapshot and marks history
 * finalization pending. The canonical history worker consumes this snapshot.
 */
export const saveTerminalSnapshot = internalMutation({
    args: {
        job_id: v.id('background_jobs'),
        status: v.union(
            v.literal('complete'),
            v.literal('error'),
            v.literal('aborted')
        ),
        content: v.string(),
        reasoning: v.string(),
        tool_calls: v.optional(v.any()),
        error: v.optional(v.string()),
        completed_at: v.number(),
        lease_owner: v.optional(v.string()),
    },
    handler: async (ctx, args) => {
        const job = await ctx.db.get(args.job_id);
        if (!job || job.status !== 'streaming') return false;
        if (
            job.lease_owner !== undefined &&
            (job.lease_owner !== args.lease_owner ||
                (job.lease_expires_at ?? 0) <= Date.now())
        ) {
            return false;
        }
        const patch: Record<string, unknown> = {
            status: args.status,
            content: args.content,
            reasoning: args.reasoning,
            error: args.error,
            completed_at: args.completed_at,
            history_phase: 'finalization_pending',
            lease_owner: undefined,
            lease_expires_at: undefined,
        };
        if (args.tool_calls !== undefined) {
            patch.tool_calls = args.tool_calls;
        }
        await ctx.db.patch(args.job_id, patch);
        return true;
    },
});

/**
 * `backgroundJobs.setHistoryPhase` (internal mutation)
 *
 * Purpose:
 * Conditionally updates the durable history phase. Supplying `from` prevents a
 * late failure from moving `committed` back to `finalization_pending`.
 */
export const setHistoryPhase = internalMutation({
    args: {
        job_id: v.id('background_jobs'),
        phase: v.string(),
        from: v.optional(v.array(v.string())),
    },
    handler: async (ctx, args) => {
        const job = await ctx.db.get(args.job_id);
        if (!job) return false;
        const current = job.history_phase ?? 'ready';
        if (args.from && !args.from.includes(current)) return false;
        await ctx.db.patch(args.job_id, { history_phase: args.phase });
        return true;
    },
});

export const claim = internalMutation({
    args: {
        job_id: v.id('background_jobs'),
        lease_owner: v.string(),
        lease_ms: v.number(),
    },
    handler: async (ctx, args) => {
        const now = Date.now();
        const leaseExpiresAt = now + Math.max(1, args.lease_ms);
        const job = await ctx.db.get(args.job_id);
        if (
            !job ||
            job.status !== 'streaming' ||
            job.execution === undefined ||
            job.execution.clientToolCall !== undefined ||
            (job.history_phase ?? 'ready') !== 'ready' ||
            (job.lease_owner !== undefined &&
                (job.lease_expires_at ?? 0) > now)
        ) {
            return null;
        }
        const { patch, result } = claimJobRecord(
            job,
            args.lease_owner,
            leaseExpiresAt,
            now
        );
        await ctx.db.patch(job._id, patch);
        return result;
    },
});

/** Atomically claim the next unowned or expired recoverable job. */
export const claimNext = internalMutation({
    args: {
        lease_owner: v.string(),
        lease_ms: v.number(),
    },
    handler: async (ctx, args) => {
        const now = Date.now();
        const leaseExpiresAt = now + Math.max(1, args.lease_ms);
        const candidates = await ctx.db
            .query('background_jobs')
            .withIndex('by_status', (q) => q.eq('status', 'streaming'))
            .collect();
        const job = candidates.find(
            (candidate) =>
                candidate.execution !== undefined &&
                candidate.execution.clientToolCall === undefined &&
                (candidate.history_phase ?? 'ready') === 'ready' &&
                (candidate.lease_owner === undefined ||
                    (candidate.lease_expires_at ?? 0) <= now)
        );
        if (!job) return null;

        const { patch, result } = claimJobRecord(
            job,
            args.lease_owner,
            leaseExpiresAt,
            now
        );
        await ctx.db.patch(job._id, patch);
        return result;
    },
});

/** Renew a claim only while the caller still owns a live lease. */
export const renewLease = internalMutation({
    args: {
        job_id: v.id('background_jobs'),
        lease_owner: v.string(),
        lease_ms: v.number(),
    },
    handler: async (ctx, args) => {
        const now = Date.now();
        const job = await ctx.db.get(args.job_id);
        if (
            !job ||
            job.status !== 'streaming' ||
            job.lease_owner !== args.lease_owner ||
            (job.lease_expires_at ?? 0) <= now
        ) {
            return false;
        }
        await ctx.db.patch(job._id, {
            lease_expires_at: now + Math.max(1, args.lease_ms),
            last_activity_at: now
        });
        return true;
    },
});

/** Persist a recovery checkpoint under the active lease. */
export const updateExecution = internalMutation({
    args: {
        job_id: v.id('background_jobs'),
        execution: v.any(),
        lease_owner: v.string(),
    },
    handler: async (ctx, args) => {
        const job = await ctx.db.get(args.job_id);
        if (
            !job ||
            job.status !== 'streaming' ||
            job.lease_owner !== args.lease_owner ||
            (job.lease_expires_at ?? 0) <= Date.now()
        ) {
            return false;
        }
        await ctx.db.patch(job._id, {
            execution: args.execution,
            last_activity_at: Date.now()
        });
        return true;
    },
});

/** Claim a browser-only tool call so competing tabs cannot execute it twice. */
export const claimClientTool = internalMutation({
    args: {
        job_id: v.id('background_jobs'),
        user_id: v.string(),
        call_id: v.string(),
        claim_token: v.string(),
        claim_expires_at: v.number(),
    },
    handler: async (ctx, args) => {
        const now = Date.now();
        const job = await ctx.db.get(args.job_id);
        const execution = job?.execution as any;
        const pending = execution?.clientToolCall;
        if (
            !job ||
            job.user_id !== args.user_id ||
            job.status !== 'streaming' ||
            !pending ||
            pending.callId !== args.call_id ||
            (pending.claimToken && (pending.claimExpiresAt ?? 0) > now)
        ) return null;
        const nextExecution = {
            ...execution,
            clientToolCall: {
                ...pending,
                claimToken: args.claim_token,
                claimExpiresAt: args.claim_expires_at,
            },
        };
        await ctx.db.patch(job._id, {
            execution: nextExecution,
            last_activity_at: now,
        });
        return {
            id: job._id,
            userId: job.user_id,
            threadId: job.thread_id,
            messageId: job.message_id,
            model: job.model,
            status: job.status,
            content: job.content,
            reasoning: job.reasoning ?? '',
            chunksReceived: job.chunks_received,
            startedAt: job.started_at,
            lastActivityAt: now,
            tool_calls: job.tool_calls,
            execution: nextExecution,
            attempts: job.attempts ?? 0,
        };
    },
});

/** Accept a claimed browser result and release the parked server lease. */
export const settleClientTool = internalMutation({
    args: {
        job_id: v.id('background_jobs'),
        user_id: v.string(),
        call_id: v.string(),
        claim_token: v.string(),
        execution: v.any(),
        tool_calls: v.any(),
    },
    handler: async (ctx, args) => {
        const now = Date.now();
        const job = await ctx.db.get(args.job_id);
        const pending = (job?.execution as any)?.clientToolCall;
        if (
            !job ||
            job.user_id !== args.user_id ||
            job.status !== 'streaming' ||
            !pending ||
            pending.callId !== args.call_id ||
            pending.claimToken !== args.claim_token ||
            (pending.claimExpiresAt ?? 0) <= now
        ) return false;
        await ctx.db.patch(job._id, {
            execution: args.execution,
            tool_calls: args.tool_calls,
            lease_owner: undefined,
            lease_expires_at: undefined,
            last_activity_at: now,
        });
        return true;
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
            last_activity_at: Date.now(),
            history_phase:
                job.generation_id !== undefined &&
                (job.history_phase ?? 'ready') === 'ready'
                    ? 'finalization_pending'
                    : job.history_phase,
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
            const idleAge = now - (job.last_activity_at ?? job.started_at);
            if (job.kind !== 'workflow' && job.execution === undefined) {
                await ctx.db.patch(job._id, {
                    status: 'error',
                    error:
                        'Background job predates durable recovery. Retry the message.',
                    completed_at: now,
                    history_phase:
                        job.generation_id !== undefined &&
                        (job.history_phase ?? 'ready') === 'ready'
                            ? 'finalization_pending'
                            : job.history_phase,
                });
                cleaned++;
            } else if (idleAge > timeoutMs) {
                await ctx.db.patch(job._id, {
                    status: 'error',
                    error: 'Job timed out',
                    completed_at: now,
                    history_phase:
                        job.generation_id !== undefined &&
                        (job.history_phase ?? 'ready') === 'ready'
                            ? 'finalization_pending'
                            : job.history_phase,
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
                const historyPhase = job.history_phase ?? 'ready';
                const historySettled =
                    historyPhase === 'ready' ||
                    historyPhase === 'committed' ||
                    historyPhase === 'superseded';
                if (completedAge > retentionMs && historySettled) {
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
 * Returns the number of worker-active jobs; browser handoffs have no worker lease.
 */
export const getActiveCount = internalQuery({
    args: {},
    handler: async (ctx) => {
        const jobs = await ctx.db
            .query('background_jobs')
            .withIndex('by_status', (q) => q.eq('status', 'streaming'))
            .collect();

        return jobs.filter((job) => !job.execution?.clientToolCall).length;
    },
});

/** Return a bounded page of canonical-history deliveries awaiting retry. */
export const listPendingHistory = internalQuery({
    args: { limit: v.number() },
    handler: async (ctx, args) => {
        const limit = Math.max(1, Math.min(100, Math.floor(args.limit)));
        const jobs = (
            await Promise.all([
                ctx.db.query('background_jobs')
                    .withIndex('by_history_phase', (q) =>
                        q.eq('history_phase', 'admission_pending')
                    ).take(limit),
                ctx.db.query('background_jobs')
                    .withIndex('by_history_phase', (q) =>
                        q.eq('history_phase', 'finalization_pending')
                    ).take(limit),
            ])
        ).flat();
        return jobs
            .sort((left, right) => left.started_at - right.started_at)
            .slice(0, limit)
            .map((job) => ({
                id: job._id,
                userId: job.user_id,
                threadId: job.thread_id,
                messageId: job.message_id,
                model: job.model,
                kind: job.kind,
                status: job.status,
                content: job.content,
                reasoning: job.reasoning ?? '',
                generation_id: job.generation_id,
                history_phase: job.history_phase,
                sync_provider_id: job.sync_provider_id,
                chunksReceived: job.chunks_received,
                startedAt: job.started_at,
                lastActivityAt: job.last_activity_at,
                completedAt: job.completed_at,
                error: job.error,
                tool_calls: job.tool_calls,
                workflow_state: job.workflow_state,
                execution: job.execution,
                leaseOwner: job.lease_owner,
                leaseExpiresAt: job.lease_expires_at,
                attempts: job.attempts,
            }));
    },
});
