import { beforeEach, describe, expect, it, vi } from 'vitest';

const queryMock = vi.hoisted(() => vi.fn());
const getAdminConvexClientMock = vi.hoisted(() =>
    vi.fn(() => ({ query: queryMock }))
);

vi.mock('../../auth/convex-auth-workspace-store', () => ({
    getAdminConvexClient: getAdminConvexClientMock,
}));

vi.mock('../../../utils/convex-api', () => ({
    convexApi: {
        admin: { isAdmin: 'admin.isAdmin' },
    },
    convexInternalApi: {
        users: { getAuthAccountByProvider: 'users.getAuthAccountByProvider' },
    },
}));

import { ConvexDeploymentAdminChecker } from '../deployment-admin-checker';

describe('ConvexDeploymentAdminChecker', () => {
    beforeEach(() => {
        queryMock.mockReset();
        getAdminConvexClientMock.mockClear();
    });

    it('uses the subject-bound admin client for identity lookup', async () => {
        queryMock
            .mockResolvedValueOnce({ user_id: 'user-1' })
            .mockResolvedValueOnce(true);

        await expect(
            new ConvexDeploymentAdminChecker().checkDeploymentAdmin('subject-1', 'clerk')
        ).resolves.toBe(true);

        expect(getAdminConvexClientMock).toHaveBeenCalledWith('clerk', 'subject-1');
        expect(queryMock).toHaveBeenNthCalledWith(
            1,
            'users.getAuthAccountByProvider',
            { provider: 'clerk', provider_user_id: 'subject-1' }
        );
    });

    it('fails closed when the subject has no internal mapping', async () => {
        queryMock.mockResolvedValueOnce(null);

        await expect(
            new ConvexDeploymentAdminChecker().checkDeploymentAdmin('missing', 'clerk')
        ).resolves.toBe(false);
        expect(queryMock).toHaveBeenCalledTimes(1);
    });
});
