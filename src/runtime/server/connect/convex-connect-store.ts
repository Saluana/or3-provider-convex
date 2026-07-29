import type {
    ConnectAuthorizationRecord,
    ConnectEnvironmentRecord,
} from '~~/server/connect/types';
import {
    ConnectStoreError,
    type ApproveConnectAuthorizationInput,
    type ConnectStore,
    type CreateConnectAuthorizationInput,
} from '~~/server/connect/store/types';
import { getConvexClient } from '../utils/convex-client';
import { getConvexInternalApiReference } from '../../utils/convex-api';

export function createConvexConnectStore(): ConnectStore {
    return new ConvexConnectStore();
}

class ConvexConnectStore implements ConnectStore {
    async createAuthorization(
        input: CreateConnectAuthorizationInput
    ): Promise<void> {
        try {
            await getConvexClient().mutation(
                getConvexInternalApiReference(
                    'connect:createDeviceAuthorization'
                ),
                {
                    device_code_hash: input.deviceCodeHash,
                    user_code_hash: input.userCodeHash,
                    user_code_display: input.userCodeDisplay,
                    host: input.host,
                    expires_at: input.expiresAt,
                    now: input.now,
                }
            );
        } catch (error) {
            const message =
                error instanceof Error ? error.message : String(error);
            if (message.includes('OR3_CONNECT_CODE_CONFLICT')) {
                throw new ConnectStoreError(
                    'conflict',
                    'Pairing code collision. Generate another code.'
                );
            }
            throw error;
        }
    }

    async getAuthorizationByDeviceHash(
        deviceCodeHash: string,
        now: number
    ): Promise<ConnectAuthorizationRecord | null> {
        return (await getConvexClient().mutation(
            getConvexInternalApiReference(
                'connect:pollDeviceAuthorization'
            ),
            { device_code_hash: deviceCodeHash, now }
        )) as ConnectAuthorizationRecord | null;
    }

    async getAuthorizationByUserHash(
        userCodeHash: string,
        now: number
    ): Promise<ConnectAuthorizationRecord | null> {
        return (await getConvexClient().query(
            getConvexInternalApiReference(
                'connect:getDeviceAuthorizationByUserHash'
            ),
            { user_code_hash: userCodeHash, now }
        )) as ConnectAuthorizationRecord | null;
    }

    async approveAuthorization(
        input: ApproveConnectAuthorizationInput
    ): Promise<{ environment_id: string }> {
        try {
            return (await getConvexClient().mutation(
                getConvexInternalApiReference(
                    'connect:approveDeviceAuthorization'
                ),
                {
                    authorization_id: input.authorizationId,
                    user_id: input.userId,
                    workspace_id: input.workspaceId,
                    environment: input.environment,
                    credential_ciphertext: input.credentialCiphertext,
                    max_active_environments: input.maxActiveEnvironments,
                    now: input.now,
                }
            )) as { environment_id: string };
        } catch (error) {
            const message =
                error instanceof Error ? error.message : String(error);
            if (message.includes('connected computers')) {
                throw new ConnectStoreError(
                    'environment_limit_reached',
                    `This account already has ${input.maxActiveEnvironments} connected computers.`
                );
            }
            if (message.includes('no longer available')) {
                throw new ConnectStoreError(
                    'authorization_unavailable',
                    'This connection request is no longer available.'
                );
            }
            throw error;
        }
    }

    async denyAuthorization(
        authorizationId: string,
        now: number
    ): Promise<boolean> {
        return (await getConvexClient().mutation(
            getConvexInternalApiReference(
                'connect:denyDeviceAuthorization'
            ),
            { authorization_id: authorizationId, now }
        )) as boolean;
    }

    async getEnvironmentByControlTokenHash(
        controlTokenHash: string
    ): Promise<ConnectEnvironmentRecord | null> {
        return (await getConvexClient().query(
            getConvexInternalApiReference(
                'connect:getEnvironmentByControlTokenHash'
            ),
            { control_token_hash: controlTokenHash }
        )) as ConnectEnvironmentRecord | null;
    }

    async listEnvironmentsForUser(
        userId: string
    ): Promise<ConnectEnvironmentRecord[]> {
        return (await getConvexClient().query(
            getConvexInternalApiReference(
                'connect:listEnvironmentsForUser'
            ),
            { user_id: userId }
        )) as ConnectEnvironmentRecord[];
    }

    async revokeEnvironment(
        environmentId: string,
        now: number
    ): Promise<boolean> {
        return (await getConvexClient().mutation(
            getConvexInternalApiReference('connect:revokeEnvironment'),
            { environment_id: environmentId, now }
        )) as boolean;
    }
}
