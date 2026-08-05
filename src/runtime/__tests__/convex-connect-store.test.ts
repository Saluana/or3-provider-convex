import { beforeEach, describe, expect, it, vi } from 'vitest';
import { ConvexError } from 'convex/values';
import { ConnectStoreError } from '~~/server/connect/store/types';

const mutationMock = vi.hoisted(() => vi.fn());
const queryMock = vi.hoisted(() => vi.fn());

vi.mock('../server/utils/convex-client', () => ({
    getConvexClient: () => ({
        mutation: mutationMock,
        query: queryMock,
    }),
}));

vi.mock('../utils/convex-api', () => ({
    getConvexInternalApiReference: (name: string) => name,
}));

import { createConvexConnectStore } from '../server/connect/convex-connect-store';

describe('Convex Connect store', () => {
    beforeEach(() => {
        mutationMock.mockReset();
        queryMock.mockReset();
        mutationMock.mockImplementation(async (operation: string) => {
            if (operation === 'connect:purgeLegacyUserCodeDisplays') {
                return {
                    continue_cursor: '',
                    is_done: true,
                    purged: 0,
                };
            }
            return null;
        });
    });

    it('purges legacy display values and never sends a readable phrase', async () => {
        const store = createConvexConnectStore();
        const legacyShapedInput = {
            deviceCodeHash: 'device-code-hash',
            userCodeHash: 'server-keyed-user-code-lookup',
            userCodeDisplay: 'BRIGHT-MOON-TREE-042',
            host: {
                name: 'Computer',
                platform: 'darwin',
                architecture: 'arm64',
                intern_version: '1.0.0',
            },
            expiresAt: 1_800_000_060_000,
            now: 1_800_000_000_000,
        };

        await store.createAuthorization(legacyShapedInput);

        expect(mutationMock).toHaveBeenNthCalledWith(
            1,
            'connect:purgeLegacyUserCodeDisplays',
            { cursor: null, batch_size: 100 }
        );
        expect(mutationMock).toHaveBeenNthCalledWith(
            2,
            'connect:createDeviceAuthorization',
            expect.not.objectContaining({
                user_code_display: expect.anything(),
            })
        );
        expect(JSON.stringify(mutationMock.mock.calls)).not.toContain(
            legacyShapedInput.userCodeDisplay
        );
    });

    it('uses a mutation when looking up a code so legacy values are scrubbed', async () => {
        const store = createConvexConnectStore();

        await store.getAuthorizationByUserHash('keyed-lookup', 123);

        expect(mutationMock).toHaveBeenCalledWith(
            'connect:getDeviceAuthorizationByUserHash',
            { user_code_hash: 'keyed-lookup', now: 123 }
        );
        expect(queryMock).not.toHaveBeenCalled();
    });

    it('passes the bounded credential-redelivery window to polling', async () => {
        const store = createConvexConnectStore();

        await store.getAuthorizationByDeviceHash(
            'device-code-hash',
            123,
            60_000
        );

        expect(mutationMock).toHaveBeenCalledWith(
            'connect:pollDeviceAuthorization',
            {
                device_code_hash: 'device-code-hash',
                now: 123,
                redelivery_window_ms: 60_000,
            }
        );
    });

    it('passes account and workspace scope to every environment operation', async () => {
        const store = createConvexConnectStore();
        const scope = {
            userId: 'user-one',
            workspaceId: 'workspace-one',
        };

        await store.listEnvironments(scope);
        await store.getEnvironmentByControlTokenHash('token-hash', scope);
        await store.revokeEnvironment('environment-one', scope, 123);

        expect(queryMock).toHaveBeenCalledWith(
            'connect:listEnvironmentsForScope',
            {
                user_id: 'user-one',
                workspace_id: 'workspace-one',
            }
        );
        expect(queryMock).toHaveBeenCalledWith(
            'connect:getEnvironmentByControlTokenHash',
            {
                user_id: 'user-one',
                workspace_id: 'workspace-one',
                control_token_hash: 'token-hash',
            }
        );
        expect(mutationMock).toHaveBeenCalledWith(
            'connect:revokeEnvironment',
            {
                environment_id: 'environment-one',
                user_id: 'user-one',
                workspace_id: 'workspace-one',
                now: 123,
            }
        );
    });

    it('passes the explicit environment limit policy to approval', async () => {
        const store = createConvexConnectStore();
        mutationMock.mockImplementation(async (operation: string) => {
            if (operation === 'connect:purgeLegacyUserCodeDisplays') {
                return {
                    continue_cursor: '',
                    is_done: true,
                    purged: 0,
                };
            }
            if (operation === 'connect:approveDeviceAuthorization') {
                return { environment_id: 'environment-one' };
            }
            return null;
        });

        await store.approveAuthorization({
            authorizationId: 'authorization-one',
            userId: 'user-one',
            workspaceId: 'workspace-one',
            environment: {
                id: 'environment-one',
                name: 'Computer',
                platform: 'darwin',
                architecture: 'arm64',
                hostname: 'computer.connect.example.test',
                tunnel_id: 'tunnel-one',
                dns_record_id: 'dns-one',
                control_token_hash: 'token-hash',
                access_credential_ciphertext: 'access-ciphertext',
            },
            credentialCiphertext: 'credential-ciphertext',
            limitPolicy: {
                scope: 'account',
                maxActiveEnvironments: 3,
            },
            now: 123,
        });

        expect(mutationMock).toHaveBeenCalledWith(
            'connect:approveDeviceAuthorization',
            expect.objectContaining({
                user_id: 'user-one',
                workspace_id: 'workspace-one',
                limit_scope: 'account',
                max_active_environments: 3,
            })
        );
    });

    it.each([
        ['approveDeviceAuthorization', 'approveAuthorization'],
        ['reserveDeviceAuthorization', 'reserveAuthorization'],
    ] as const)(
        'maps stable Convex codes for %s without depending on human prose',
        async (operation, method) => {
            const store = createConvexConnectStore();
            mutationMock.mockImplementation(async (calledOperation: string) => {
                if (
                    calledOperation ===
                    'connect:purgeLegacyUserCodeDisplays'
                ) {
                    return {
                        continue_cursor: '',
                        is_done: true,
                        purged: 0,
                    };
                }
                if (calledOperation === `connect:${operation}`) {
                    throw new ConvexError({
                        code: 'environment_limit_reached',
                        message: 'Completely different operator-facing copy.',
                    });
                }
                return null;
            });
            const shared = {
                authorizationId: 'authorization-one',
                userId: 'user-one',
                workspaceId: 'workspace-one',
                environment: {
                    id: 'environment-one',
                    name: 'Computer',
                    platform: 'darwin',
                    architecture: 'arm64',
                    control_token_hash: 'token-hash',
                    access_credential_ciphertext: 'access-ciphertext',
                    tunnel_secret_ciphertext: 'tunnel-secret-ciphertext',
                    hostname: 'computer.connect.example.test',
                    tunnel_id: 'tunnel-one',
                    dns_record_id: 'dns-one',
                },
                limitPolicy: {
                    scope: 'account' as const,
                    maxActiveEnvironments: 3,
                },
                now: 100,
            };
            const promise =
                method === 'approveAuthorization'
                    ? store.approveAuthorization({
                          ...shared,
                          credentialCiphertext: 'credential-ciphertext',
                      })
                    : store.reserveAuthorization({
                          ...shared,
                          claimToken: 'claim-one',
                          claimUntil: 200,
                          provisioningDeadlineAt: 1_000,
                          activationDeadlineAt: 2_000,
                          authorizationExpiresAt: 1_100,
                      });

            await expect(promise).rejects.toMatchObject({
                code: 'environment_limit_reached',
            } satisfies Partial<ConnectStoreError>);
        }
    );

    it('does not reinterpret matching human prose without a stable code', async () => {
        const store = createConvexConnectStore();
        const original = new Error(
            'This account already has connected computers and is no longer available.'
        );
        mutationMock.mockImplementation(async (operation: string) => {
            if (operation === 'connect:purgeLegacyUserCodeDisplays') {
                return {
                    continue_cursor: '',
                    is_done: true,
                    purged: 0,
                };
            }
            if (operation === 'connect:approveDeviceAuthorization') {
                throw original;
            }
            return null;
        });

        await expect(
            store.approveAuthorization({
                authorizationId: 'authorization-one',
                userId: 'user-one',
                workspaceId: 'workspace-one',
                environment: {
                    id: 'environment-one',
                    name: 'Computer',
                    platform: 'darwin',
                    architecture: 'arm64',
                    hostname: 'computer.connect.example.test',
                    tunnel_id: 'tunnel-one',
                    dns_record_id: 'dns-one',
                    control_token_hash: 'token-hash',
                    access_credential_ciphertext: 'access-ciphertext',
                },
                credentialCiphertext: 'credential-ciphertext',
                limitPolicy: {
                    scope: 'account',
                    maxActiveEnvironments: 3,
                },
                now: 123,
            })
        ).rejects.toBe(original);
    });

    it('maps a structured code collision while allowing its message to change', async () => {
        const store = createConvexConnectStore();
        mutationMock.mockImplementation(async (operation: string) => {
            if (operation === 'connect:purgeLegacyUserCodeDisplays') {
                return {
                    continue_cursor: '',
                    is_done: true,
                    purged: 0,
                };
            }
            if (operation === 'connect:createDeviceAuthorization') {
                throw new ConvexError({
                    code: 'code_conflict',
                    message: 'The display copy is intentionally unrelated.',
                });
            }
            return null;
        });

        await expect(
            store.createAuthorization({
                deviceCodeHash: 'device-code-hash',
                userCodeHash: 'user-code-hash',
                host: {
                    name: 'Computer',
                    platform: 'darwin',
                    architecture: 'arm64',
                    intern_version: '1.0.0',
                },
                expiresAt: 200,
                now: 100,
            })
        ).rejects.toMatchObject({
            code: 'conflict',
        } satisfies Partial<ConnectStoreError>);
    });

    it('maps authorization availability from its code, not its prose', async () => {
        const store = createConvexConnectStore();
        mutationMock.mockImplementation(async (operation: string) => {
            if (operation === 'connect:purgeLegacyUserCodeDisplays') {
                return {
                    continue_cursor: '',
                    is_done: true,
                    purged: 0,
                };
            }
            if (operation === 'connect:approveDeviceAuthorization') {
                throw new ConvexError({
                    code: 'authorization_unavailable',
                    message: 'This wording can change without breaking callers.',
                });
            }
            return null;
        });

        await expect(
            store.approveAuthorization({
                authorizationId: 'authorization-one',
                userId: 'user-one',
                workspaceId: 'workspace-one',
                environment: {
                    id: 'environment-one',
                    name: 'Computer',
                    platform: 'darwin',
                    architecture: 'arm64',
                    hostname: 'computer.connect.example.test',
                    tunnel_id: 'tunnel-one',
                    dns_record_id: 'dns-one',
                    control_token_hash: 'token-hash',
                    access_credential_ciphertext: 'access-ciphertext',
                },
                credentialCiphertext: 'credential-ciphertext',
                limitPolicy: {
                    scope: 'account',
                    maxActiveEnvironments: 3,
                },
                now: 123,
            })
        ).rejects.toMatchObject({
            code: 'authorization_unavailable',
        } satisfies Partial<ConnectStoreError>);
    });

    it('passes bounded retention cutoffs to the provider mutation', async () => {
        const store = createConvexConnectStore();
        await store.purgeConnectRecords({
            authorizationUpdatedBefore: 100,
            revokedEnvironmentUpdatedBefore: 200,
            batchSize: 50,
        });
        expect(mutationMock).toHaveBeenCalledWith(
            'connect:purgeConnectRetention',
            {
                authorization_updated_before: 100,
                revoked_environment_updated_before: 200,
                batch_size: 50,
            }
        );
    });

    it('passes durable lifecycle claims and per-resource progress to Convex', async () => {
        const store = createConvexConnectStore();
        const scope = {
            userId: 'user-one',
            workspaceId: 'workspace-one',
        };

        await store.reserveAuthorization({
            authorizationId: 'authorization-one',
            userId: scope.userId,
            workspaceId: scope.workspaceId,
            environment: {
                id: 'environment-one',
                name: 'Computer',
                platform: 'darwin',
                architecture: 'arm64',
                control_token_hash: 'token-hash',
                access_credential_ciphertext: 'access-ciphertext',
                tunnel_secret_ciphertext: 'tunnel-secret-ciphertext',
            },
            limitPolicy: {
                scope: 'account',
                maxActiveEnvironments: 3,
            },
            claimToken: 'claim-one',
            claimUntil: 200,
            provisioningDeadlineAt: 1_000,
            activationDeadlineAt: 2_000,
            authorizationExpiresAt: 1_100,
            now: 100,
        });
        await store.saveEnvironmentRelayProgress(
            'environment-one',
            'provisioning',
            'claim-one',
            { tunnelId: 'tunnel-one', dnsRecordId: 'dns-one' },
            101
        );
        await store.completeEnvironmentProvisioning(
            'environment-one',
            'claim-one',
            'device-credential',
            102
        );
        await store.beginEnvironmentRevocation({
            environmentId: 'environment-one',
            scope,
            claimToken: 'claim-two',
            claimUntil: 300,
            now: 200,
        });
        await store.recordEnvironmentLifecycleFailure(
            'environment-one',
            'revoking',
            'claim-two',
            'temporary failure',
            400,
            201
        );

        expect(mutationMock).toHaveBeenCalledWith(
            'connect:reserveDeviceAuthorization',
            expect.objectContaining({
                authorization_id: 'authorization-one',
                limit_scope: 'account',
                claim_token: 'claim-one',
                provisioning_deadline_at: 1_000,
            })
        );
        expect(mutationMock).toHaveBeenCalledWith(
            'connect:saveEnvironmentRelayProgress',
            expect.objectContaining({
                expected_status: 'provisioning',
                tunnel_id: 'tunnel-one',
                dns_record_id: 'dns-one',
            })
        );
        expect(mutationMock).toHaveBeenCalledWith(
            'connect:completeEnvironmentProvisioning',
            expect.objectContaining({
                credential_ciphertext: 'device-credential',
            })
        );
        expect(mutationMock).toHaveBeenCalledWith(
            'connect:beginEnvironmentRevocation',
            expect.objectContaining({
                user_id: scope.userId,
                workspace_id: scope.workspaceId,
                claim_token: 'claim-two',
            })
        );
        expect(mutationMock).toHaveBeenCalledWith(
            'connect:recordEnvironmentLifecycleFailure',
            expect.objectContaining({
                expected_status: 'revoking',
                next_attempt_at: 400,
            })
        );
    });
});
