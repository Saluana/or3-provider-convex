/**
 * @module or3-provider-convex/runtime/server/admin/deployment-admin-checker
 *
 * Purpose:
 * Convex-backed deployment admin checker.
 */
import type { DeploymentAdminChecker } from '~~/server/auth/deployment-admin';
import {
    convexApi as api,
    convexInternalApi as internalApi,
} from '../../utils/convex-api';
import { getAdminConvexClient } from '../auth/convex-auth-workspace-store';

export class ConvexDeploymentAdminChecker implements DeploymentAdminChecker {
    async checkDeploymentAdmin(
        providerUserId: string,
        provider: string
    ): Promise<boolean> {
        try {
            const convex = getAdminConvexClient(provider, providerUserId);

            const authAccount = await convex.query(
                internalApi.users.getAuthAccountByProvider,
                {
                    provider,
                    provider_user_id: providerUserId,
                }
            );

            if (!authAccount) {
                return false;
            }

            const isAdmin = await convex.query(api.admin.isAdmin, {
                user_id: authAccount.user_id,
            });

            return isAdmin;
        } catch {
            return false;
        }
    }
}

export function createConvexDeploymentAdminChecker(): DeploymentAdminChecker {
    return new ConvexDeploymentAdminChecker();
}
