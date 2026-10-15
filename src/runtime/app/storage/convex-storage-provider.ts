import { createGatewayStorageProvider } from '~~/app/core/storage/providers/gateway-storage-provider';
import { CONVEX_STORAGE_PROVIDER_ID } from '~~/shared/cloud/provider-ids';
import type { ObjectStorageProvider } from '~/core/storage/types';

/** Use the host's current authenticated storage protocol for every operation. */
export function createConvexStorageProvider(): ObjectStorageProvider {
    return createGatewayStorageProvider({
        id: CONVEX_STORAGE_PROVIDER_ID,
        displayName: 'Convex Storage',
    });
}
