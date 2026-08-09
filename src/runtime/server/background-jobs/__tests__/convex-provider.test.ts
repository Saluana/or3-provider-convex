import { beforeEach, describe, expect, it, vi } from 'vitest';
import { convexJobProvider } from '../convex-provider';

const query = vi.hoisted(() => vi.fn());
const mutation = vi.hoisted(() => vi.fn());

vi.mock('../../utils/convex-client', () => ({
    getConvexClient: () => ({ query, mutation }),
}));

vi.mock('../../../utils/convex-api', () => ({
    convexInternalApi: {
        backgroundJobs: {
            create: 'backgroundJobs.create',
            get: 'backgroundJobs.get',
            update: 'backgroundJobs.update',
            complete: 'backgroundJobs.complete',
            fail: 'backgroundJobs.fail',
            abort: 'backgroundJobs.abort',
            checkAborted: 'backgroundJobs.checkAborted',
            claim: 'backgroundJobs.claim',
            claimNext: 'backgroundJobs.claimNext',
            renewLease: 'backgroundJobs.renewLease',
            updateExecution: 'backgroundJobs.updateExecution',
            cleanup: 'backgroundJobs.cleanup',
            getActiveCount: 'backgroundJobs.getActiveCount',
        },
    },
}));

vi.mock('~~/server/utils/background-jobs/store', () => ({
    getJobConfig: () => ({
        maxConcurrentJobs: 20,
        maxConcurrentJobsPerUser: 5,
        jobTimeoutMs: 300_000,
        completedJobRetentionMs: 300_000,
    }),
}));

describe('convex background job provider', () => {
    beforeEach(() => {
        query.mockReset();
        mutation.mockReset();
    });

    it('performs admission through one atomic create mutation', async () => {
        mutation.mockResolvedValue('job-1');
        const execution = {
            version: 1 as const,
            body: { model: 'test-model' },
            workspaceId: 'workspace-1',
            referer: 'https://or3.example',
            apiKeyCiphertext: 'v1.encrypted',
        };

        await expect(
            convexJobProvider.createJob({
                userId: 'user-1',
                threadId: 'thread-1',
                messageId: 'message-1',
                model: 'test-model',
                kind: 'chat',
                idempotencyKey: 'message-1',
                execution,
            })
        ).resolves.toBe('job-1');

        expect(query).not.toHaveBeenCalled();
        expect(mutation).toHaveBeenCalledWith('backgroundJobs.create', {
            user_id: 'user-1',
            thread_id: 'thread-1',
            message_id: 'message-1',
            model: 'test-model',
            kind: 'chat',
            tool_calls: undefined,
            workflow_state: undefined,
            execution,
            idempotency_key: 'message-1',
            max_concurrent_jobs: 20,
            max_concurrent_jobs_per_user: 5,
        });
    });

    it('maps durable claim state and renews the exact lease owner', async () => {
        mutation
            .mockResolvedValueOnce({
                id: 'job-1',
                userId: 'user-1',
                threadId: 'thread-1',
                messageId: 'message-1',
                model: 'test-model',
                status: 'streaming',
                content: '',
                chunksReceived: 0,
                startedAt: 1,
                leaseOwner: 'worker-1',
                leaseExpiresAt: 40,
                attempts: 2,
                execution: { version: 1 },
            })
            .mockResolvedValueOnce(true);

        await expect(
            convexJobProvider.claimJob?.('job-1', 'worker-1', 10, 40)
        ).resolves.toMatchObject({
            id: 'job-1',
            leaseOwner: 'worker-1',
            attempts: 2,
        });
        await expect(
            convexJobProvider.renewJobLease?.(
                'job-1',
                'worker-1',
                20,
                50
            )
        ).resolves.toBe(true);
        expect(mutation).toHaveBeenNthCalledWith(2, 'backgroundJobs.renewLease', {
            job_id: 'job-1',
            lease_owner: 'worker-1',
            lease_ms: 30,
        });
    });

    it('rejects a fenced write after its lease is superseded', async () => {
        mutation.mockResolvedValue(false);

        await expect(
            convexJobProvider.updateJob('job-1', {
                contentChunk: 'stale',
                leaseOwner: 'worker-old',
            })
        ).rejects.toMatchObject({ name: 'BackgroundJobLeaseLostError' });
    });
});
