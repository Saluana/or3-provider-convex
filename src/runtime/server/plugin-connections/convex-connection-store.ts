import type {
    ConnectionTestEvidence,
    PluginConnectionUpdate,
    StoredPluginConnection,
} from '~~/shared/plugins/connections/contracts';
import type { PluginConnectionStore } from '~~/server/utils/plugins/connections/store/registry';
import { convexInternalApi as internalApi } from '../../utils/convex-api';
import { getConvexClient } from '../utils/convex-client';

/** Credentials remain encrypted; only admin-authenticated host calls can use
 * these internal functions. The host owns authorization and encryption keys. */
export function createConvexPluginConnectionStore(): PluginConnectionStore {
    return {
        async list(input) {
            return await getConvexClient().query(internalApi.pluginConnections.list, input) as StoredPluginConnection[];
        },
        async get(id) {
            return await getConvexClient().query(internalApi.pluginConnections.get, { id }) as StoredPluginConnection | null;
        },
        async insert(connection: StoredPluginConnection) {
            return await getConvexClient().mutation(internalApi.pluginConnections.insert, {
                connection: { ...connection, scopes: [...connection.scopes] },
            }) as boolean;
        },
        async update(update: PluginConnectionUpdate) {
            return await getConvexClient().mutation(internalApi.pluginConnections.update, {
                update: { ...update, ...(update.scopes === undefined ? {} : { scopes: [...update.scopes] }) },
            }) as boolean;
        },
        async delete(id) {
            await getConvexClient().mutation(internalApi.pluginConnections.remove, { id });
        },
        async getTestEvidence(connectionId) {
            return await getConvexClient().query(internalApi.pluginConnections.getTestEvidence, { connectionId }) as ConnectionTestEvidence | null;
        },
        async setTestEvidence(evidence: ConnectionTestEvidence) {
            return await getConvexClient().mutation(internalApi.pluginConnections.setTestEvidence, { evidence }) as boolean;
        },
    };
}
