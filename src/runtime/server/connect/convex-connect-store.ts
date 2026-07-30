import type {
    ConnectAuthorizationRecord,
    ConnectEnvironmentRecord,
} from '~~/server/connect/types';
import {
    ConnectStoreError,
    type ApproveConnectAuthorizationInput,
    type BeginConnectEnvironmentRevocationInput,
    type ConnectEnvironmentLifecycleClaim,
    type ConnectEnvironmentRelayProgress,
    type ConnectEnvironmentScope,
    type ConnectStore,
    type CreateConnectAuthorizationInput,
    type PurgeConnectRecordsInput,
    type PurgeConnectRecordsResult,
    type ReserveConnectAuthorizationInput,
} from '~~/server/connect/store/types';
import { ConvexError } from 'convex/values';
import { getConvexClient } from '../utils/convex-client';
import { getConvexInternalApiReference } from '../../utils/convex-api';

type ConnectConvexErrorCode =
    | 'authorization_unavailable'
    | 'code_conflict'
    | 'environment_limit_reached';

function getConnectConvexErrorCode(
    error: unknown
): ConnectConvexErrorCode | null {
    if (!(error instanceof ConvexError)) return null;
    const data = error.data;
    if (!data || typeof data !== 'object' || Array.isArray(data)) return null;
    const code = Reflect.get(data, 'code');
    return code === 'authorization_unavailable' ||
        code === 'code_conflict' ||
        code === 'environment_limit_reached'
        ? code
        : null;
}

function mapAuthorizationMutationError(
    error: unknown,
    limitPolicy: { scope: 'account' | 'workspace'; maxActiveEnvironments: number }
): never {
    switch (getConnectConvexErrorCode(error)) {
        case 'environment_limit_reached':
            throw new ConnectStoreError(
                'environment_limit_reached',
                `This ${limitPolicy.scope} already has ${limitPolicy.maxActiveEnvironments} connected computers.`
            );
        case 'authorization_unavailable':
            throw new ConnectStoreError(
                'authorization_unavailable',
                'This connection request is no longer available.'
            );
        default:
            throw error;
    }
}

export function createConvexConnectStore(): ConnectStore {
    return new ConvexConnectStore();
}

class ConvexConnectStore implements ConnectStore {
    #legacyPurge?: Promise<void>;

    async #ensureLegacyDisplaysPurged(): Promise<void> {
        if (!this.#legacyPurge) {
            this.#legacyPurge = this.#purgeLegacyDisplays().catch((error) => {
                this.#legacyPurge = undefined;
                throw error;
            });
        }
        return this.#legacyPurge;
    }

    async #purgeLegacyDisplays(): Promise<void> {
        let cursor: string | null = null;
        do {
            const result = (await getConvexClient().mutation(
                getConvexInternalApiReference(
                    'connect:purgeLegacyUserCodeDisplays'
                ),
                { cursor, batch_size: 100 }
            )) as {
                continue_cursor: string;
                is_done: boolean;
            };
            cursor = result.continue_cursor;
            if (result.is_done) return;
            if (!cursor) {
                throw new Error(
                    'Convex Connect legacy phrase purge returned an invalid cursor'
                );
            }
        } while (cursor);
    }

    async createAuthorization(
        input: CreateConnectAuthorizationInput
    ): Promise<void> {
        await this.#ensureLegacyDisplaysPurged();
        try {
            await getConvexClient().mutation(
                getConvexInternalApiReference(
                    'connect:createDeviceAuthorization'
                ),
                {
                    device_code_hash: input.deviceCodeHash,
                    user_code_hash: input.userCodeHash,
                    host: input.host,
                    expires_at: input.expiresAt,
                    now: input.now,
                }
            );
        } catch (error) {
            if (getConnectConvexErrorCode(error) === 'code_conflict') {
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
        now: number,
        redeliveryWindowMs: number
    ): Promise<ConnectAuthorizationRecord | null> {
        await this.#ensureLegacyDisplaysPurged();
        return (await getConvexClient().mutation(
            getConvexInternalApiReference(
                'connect:pollDeviceAuthorization'
            ),
            {
                device_code_hash: deviceCodeHash,
                now,
                redelivery_window_ms: redeliveryWindowMs,
            }
        )) as ConnectAuthorizationRecord | null;
    }

    async getAuthorizationByUserHash(
        userCodeHash: string,
        now: number
    ): Promise<ConnectAuthorizationRecord | null> {
        await this.#ensureLegacyDisplaysPurged();
        return (await getConvexClient().mutation(
            getConvexInternalApiReference(
                'connect:getDeviceAuthorizationByUserHash'
            ),
            { user_code_hash: userCodeHash, now }
        )) as ConnectAuthorizationRecord | null;
    }

    async approveAuthorization(
        input: ApproveConnectAuthorizationInput
    ): Promise<{ environment_id: string }> {
        await this.#ensureLegacyDisplaysPurged();
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
                    limit_scope: input.limitPolicy.scope,
                    max_active_environments:
                        input.limitPolicy.maxActiveEnvironments,
                    now: input.now,
                }
            )) as { environment_id: string };
        } catch (error) {
            mapAuthorizationMutationError(error, input.limitPolicy);
        }
    }

    async reserveAuthorization(
        input: ReserveConnectAuthorizationInput
    ): Promise<ConnectEnvironmentRecord> {
        await this.#ensureLegacyDisplaysPurged();
        try {
            return (await getConvexClient().mutation(
                getConvexInternalApiReference(
                    'connect:reserveDeviceAuthorization'
                ),
                {
                    authorization_id: input.authorizationId,
                    user_id: input.userId,
                    workspace_id: input.workspaceId,
                    environment: input.environment,
                    limit_scope: input.limitPolicy.scope,
                    max_active_environments:
                        input.limitPolicy.maxActiveEnvironments,
                    claim_token: input.claimToken,
                    claim_until: input.claimUntil,
                    provisioning_deadline_at:
                        input.provisioningDeadlineAt,
                    activation_deadline_at:
                        input.activationDeadlineAt,
                    authorization_expires_at:
                        input.authorizationExpiresAt,
                    now: input.now,
                }
            )) as ConnectEnvironmentRecord;
        } catch (error) {
            mapAuthorizationMutationError(error, input.limitPolicy);
        }
    }

    async claimNextEnvironmentLifecycle(
        claimToken: string,
        now: number,
        claimUntil: number
    ): Promise<ConnectEnvironmentRecord | null> {
        await this.#ensureLegacyDisplaysPurged();
        return (await getConvexClient().mutation(
            getConvexInternalApiReference(
                'connect:claimNextEnvironmentLifecycle'
            ),
            {
                claim_token: claimToken,
                now,
                claim_until: claimUntil,
            }
        )) as ConnectEnvironmentRecord | null;
    }

    async saveEnvironmentRelayProgress(
        environmentId: string,
        expectedStatus: 'provisioning' | 'revoking',
        claimToken: string,
        progress: ConnectEnvironmentRelayProgress,
        now: number
    ): Promise<boolean> {
        await this.#ensureLegacyDisplaysPurged();
        return (await getConvexClient().mutation(
            getConvexInternalApiReference(
                'connect:saveEnvironmentRelayProgress'
            ),
            {
                environment_id: environmentId,
                expected_status: expectedStatus,
                claim_token: claimToken,
                hostname: progress.hostname,
                tunnel_id: progress.tunnelId,
                dns_record_id: progress.dnsRecordId,
                relay_authenticator: progress.relayAuthenticator,
                now,
            }
        )) as boolean;
    }

    async completeEnvironmentProvisioning(
        environmentId: string,
        claimToken: string,
        credentialCiphertext: string,
        now: number
    ): Promise<boolean> {
        await this.#ensureLegacyDisplaysPurged();
        return (await getConvexClient().mutation(
            getConvexInternalApiReference(
                'connect:completeEnvironmentProvisioning'
            ),
            {
                environment_id: environmentId,
                claim_token: claimToken,
                credential_ciphertext: credentialCiphertext,
                now,
            }
        )) as boolean;
    }

    async beginEnvironmentRevocation(
        input: BeginConnectEnvironmentRevocationInput
    ): Promise<ConnectEnvironmentLifecycleClaim | null> {
        await this.#ensureLegacyDisplaysPurged();
        return (await getConvexClient().mutation(
            getConvexInternalApiReference(
                'connect:beginEnvironmentRevocation'
            ),
            {
                environment_id: input.environmentId,
                user_id: input.scope.userId,
                workspace_id: input.scope.workspaceId,
                claim_token: input.claimToken,
                claim_until: input.claimUntil,
                now: input.now,
            }
        )) as ConnectEnvironmentLifecycleClaim | null;
    }

    async abandonEnvironmentProvisioning(
        environmentId: string,
        claimToken: string,
        now: number
    ): Promise<ConnectEnvironmentRecord | null> {
        await this.#ensureLegacyDisplaysPurged();
        return (await getConvexClient().mutation(
            getConvexInternalApiReference(
                'connect:abandonEnvironmentProvisioning'
            ),
            {
                environment_id: environmentId,
                claim_token: claimToken,
                now,
            }
        )) as ConnectEnvironmentRecord | null;
    }

    async completeEnvironmentRevocation(
        environmentId: string,
        claimToken: string,
        now: number
    ): Promise<boolean> {
        await this.#ensureLegacyDisplaysPurged();
        return (await getConvexClient().mutation(
            getConvexInternalApiReference(
                'connect:completeEnvironmentRevocation'
            ),
            {
                environment_id: environmentId,
                claim_token: claimToken,
                now,
            }
        )) as boolean;
    }

    async recordEnvironmentLifecycleFailure(
        environmentId: string,
        expectedStatus: 'provisioning' | 'revoking',
        claimToken: string,
        errorMessage: string,
        nextAttemptAt: number,
        now: number
    ): Promise<boolean> {
        await this.#ensureLegacyDisplaysPurged();
        return (await getConvexClient().mutation(
            getConvexInternalApiReference(
                'connect:recordEnvironmentLifecycleFailure'
            ),
            {
                environment_id: environmentId,
                expected_status: expectedStatus,
                claim_token: claimToken,
                error_message: errorMessage,
                next_attempt_at: nextAttemptAt,
                now,
            }
        )) as boolean;
    }

    async denyAuthorization(
        authorizationId: string,
        now: number
    ): Promise<boolean> {
        await this.#ensureLegacyDisplaysPurged();
        return (await getConvexClient().mutation(
            getConvexInternalApiReference(
                'connect:denyDeviceAuthorization'
            ),
            { authorization_id: authorizationId, now }
        )) as boolean;
    }

    async getEnvironmentByControlTokenHash(
        controlTokenHash: string,
        scope: ConnectEnvironmentScope
    ): Promise<ConnectEnvironmentRecord | null> {
        await this.#ensureLegacyDisplaysPurged();
        return (await getConvexClient().query(
            getConvexInternalApiReference(
                'connect:getEnvironmentByControlTokenHash'
            ),
            {
                user_id: scope.userId,
                workspace_id: scope.workspaceId,
                control_token_hash: controlTokenHash,
            }
        )) as ConnectEnvironmentRecord | null;
    }

    async listEnvironments(
        scope: ConnectEnvironmentScope
    ): Promise<ConnectEnvironmentRecord[]> {
        await this.#ensureLegacyDisplaysPurged();
        return (await getConvexClient().query(
            getConvexInternalApiReference(
                'connect:listEnvironmentsForScope'
            ),
            {
                user_id: scope.userId,
                workspace_id: scope.workspaceId,
            }
        )) as ConnectEnvironmentRecord[];
    }

    async revokeEnvironment(
        environmentId: string,
        scope: ConnectEnvironmentScope,
        now: number
    ): Promise<boolean> {
        await this.#ensureLegacyDisplaysPurged();
        return (await getConvexClient().mutation(
            getConvexInternalApiReference('connect:revokeEnvironment'),
            {
                environment_id: environmentId,
                user_id: scope.userId,
                workspace_id: scope.workspaceId,
                now,
            }
        )) as boolean;
    }

    async purgeConnectRecords(
        input: PurgeConnectRecordsInput
    ): Promise<PurgeConnectRecordsResult> {
        await this.#ensureLegacyDisplaysPurged();
        return (await getConvexClient().mutation(
            getConvexInternalApiReference(
                'connect:purgeConnectRetention'
            ),
            {
                authorization_updated_before:
                    input.authorizationUpdatedBefore,
                revoked_environment_updated_before:
                    input.revokedEnvironmentUpdatedBefore,
                batch_size: input.batchSize,
            }
        )) as PurgeConnectRecordsResult;
    }

    async rotateAuthorizationCredential(
        authorizationId: string,
        expectedCiphertext: string,
        replacementCiphertext: string,
        now: number
    ): Promise<boolean> {
        await this.#ensureLegacyDisplaysPurged();
        return (await getConvexClient().mutation(
            getConvexInternalApiReference(
                'connect:rotateAuthorizationCredential'
            ),
            {
                authorization_id: authorizationId,
                expected_ciphertext: expectedCiphertext,
                replacement_ciphertext: replacementCiphertext,
                now,
            }
        )) as boolean;
    }

    async rotateEnvironmentCredential(
        environmentId: string,
        purpose: 'access' | 'tunnel',
        expectedCiphertext: string,
        replacementCiphertext: string,
        now: number
    ): Promise<boolean> {
        await this.#ensureLegacyDisplaysPurged();
        return (await getConvexClient().mutation(
            getConvexInternalApiReference(
                'connect:rotateEnvironmentCredential'
            ),
            {
                environment_id: environmentId,
                purpose,
                expected_ciphertext: expectedCiphertext,
                replacement_ciphertext: replacementCiphertext,
                now,
            }
        )) as boolean;
    }
}
