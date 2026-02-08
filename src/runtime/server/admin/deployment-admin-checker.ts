/**
 * @module or3-provider-convex/runtime/server/admin/deployment-admin-checker
 *
 * Purpose:
 * Convex-backed deployment admin checker.
 */
import type { DeploymentAdminChecker } from '~~/server/auth/deployment-admin';
import { convexApi as api } from '../../utils/convex-api';
import { getConvexClient } from '../utils/convex-client';

export class ConvexDeploymentAdminChecker implements DeploymentAdminChecker {
    async checkDeploymentAdmin(
        providerUserId: string,
        provider: string
    ): Promise<boolean> {
        try {
            const convex = getConvexClient();

            const authAccount = await convex.query(
                api.users.getAuthAccountByProvider,
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
