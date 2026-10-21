/**
 * @module server/utils/convex-client
 *
 * Purpose:
 * Provide a shared Convex HTTP client for server-side mutations and queries.
 *
 * Responsibilities:
 * - Lazily initialize the client from runtime config.
 * - Reuse the client across server calls in a single process.
 *
 * Non-Goals:
 * - Client-side Convex usage.
 * - Multi-tenant client pooling.
 *
 * Constraints:
 * - Requires `runtimeConfig.sync.convexUrl` and `convexAdminKey`.
 * - The admin credential is required because callers use internal Convex functions.
 * - Server-only usage.
 */

import { ConvexClient, ConvexHttpClient } from 'convex/browser';
import { convexApi as api, convexInternalApi } from '../../utils/convex-api';
import { useRuntimeConfig } from '#imports';

let client: ConvexHttpClient | null = null;

/**
 * Purpose:
 * Get or create the server-side Convex HTTP client.
 *
 * Behavior:
 * - Initializes once and caches the instance for reuse.
 * - Throws when Convex URL is missing.
 *
 * @throws Error when the Convex URL is not configured.
 */
export function getConvexClient() {
    if (client) return client;

    const runtimeConfig = useRuntimeConfig();
    const url = runtimeConfig.sync.convexUrl;
    const adminKey = runtimeConfig.sync.convexAdminKey?.trim();

    if (typeof url !== 'string' || url.length === 0) {
        throw new Error('CONVEX_URL is not defined in runtime config');
    }
    if (!adminKey) {
        throw new Error('Convex admin key not configured');
    }

    client = new ConvexHttpClient(url);
    const issuer = 'https://or3.ai/internal';
    const subject = 'or3-auxiliary-persistence';
    client.setAdminAuth(adminKey, {
        subject,
        issuer,
        tokenIdentifier: `${issuer}|${subject}`,
    });
    return client;
}

/**
 * Purpose:
 * Re-export the generated Convex API for convenience.
 */
export { api };

let realtime: { client: ConvexClient; key: string; watchers: number } | undefined;

/** One connection per host, SDK-deduplicated by workspace. It exists only while
 * authenticated host SSE listeners need it; no idle HTTP polling or sockets.
 */
export function watchConvexVersion(workspaceId: string, onVersion: (version: number) => void,
    onError: (error: unknown) => void): () => void {
    const config = useRuntimeConfig();
    const url = config.sync.convexUrl;
    const adminKey = config.sync.convexAdminKey?.trim();
    if (!url || !adminKey) throw new Error('Convex realtime notifications require server credentials');
    const key = `${url}\0${adminKey}`;
    if (realtime && realtime.key !== key) throw new Error('Convex realtime configuration changed');
    if (!realtime) {
        const client = new ConvexClient(url, { unsavedChangesWarning: false });
        // Admin impersonation is available at runtime but omitted from the SDK's public declarations.
        (client as unknown as { setAdminAuth(token: string, identity: { subject: string; issuer: string; tokenIdentifier: string }): void }).setAdminAuth(adminKey, { subject: 'or3-sync-notifications', issuer: 'https://or3.ai/internal',
            tokenIdentifier: 'https://or3.ai/internal|or3-sync-notifications' });
        realtime = { client, key, watchers: 0 };
    }
    const connection = realtime;
    connection.watchers++;
    const release = () => {
        if (--connection.watchers === 0) {
            if (realtime === connection) realtime = undefined;
            void connection.client.close().catch(() => {});
        }
    };
    let unsubscribe: () => void;
    try {
        unsubscribe = connection.client.onUpdate(convexInternalApi.sync.watchVersion, { workspace_id: workspaceId },
            version => { if (Number.isSafeInteger(version) && version >= 0) onVersion(version); else onError(new Error('Invalid workspace version')); }, onError);
    } catch (error) { release(); throw error; }
    let stopped = false;
    return () => { if (stopped) return; stopped = true; unsubscribe(); release(); };
}
