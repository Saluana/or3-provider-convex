import { anyApi } from 'convex/server';

type NamespaceApi = Record<string, any>;
type AnyApiShape = Record<string, NamespaceApi | undefined>;

function createNamespaceProxy(namespace: string): NamespaceApi {
    return new Proxy(
        {},
        {
            get(_target, prop) {
                if (typeof prop !== 'string') return undefined;
                const namespaceApi = (anyApi as AnyApiShape)[namespace];
                return namespaceApi?.[prop];
            },
        }
    );
}

export const convexApi = {
    admin: createNamespaceProxy('admin'),
    storage: createNamespaceProxy('storage'),
    sync: createNamespaceProxy('sync'),
    users: createNamespaceProxy('users'),
    workspaces: createNamespaceProxy('workspaces'),
} as const;

export const convexInternalApi = {
    backgroundJobs: createNamespaceProxy('backgroundJobs'),
    notifications: createNamespaceProxy('notifications'),
    rateLimits: createNamespaceProxy('rateLimits'),
    sync: createNamespaceProxy('sync'),
    users: createNamespaceProxy('users'),
    webhooks: createNamespaceProxy('webhooks'),
    workspaces: createNamespaceProxy('workspaces'),
} as const;

export const convexApiContractNames = [
    'admin:createWorkspace',
    'admin:ensureDeploymentAdmin',
    'admin:getWorkspace',
    'admin:getWorkspaceSetting',
    'admin:grantAdmin',
    'admin:isAdmin',
    'admin:listAdmins',
    'admin:listWorkspaceMembers',
    'admin:listWorkspaces',
    'admin:removeWorkspaceMember',
    'admin:restoreWorkspace',
    'admin:revokeAdmin',
    'admin:searchUsers',
    'admin:setWorkspaceMemberRole',
    'admin:setWorkspaceSetting',
    'admin:softDeleteWorkspace',
    'admin:upsertWorkspaceMember',
    'storage:commitUpload',
    'storage:gcDeletedFiles',
    'storage:generateUploadUrl',
    'storage:getFileUrl',
    'sync:pull',
    'sync:queryCanonicalStorage',
    'sync:snapshot',
    'sync:push',
    'sync:updateDeviceCursor',
    'sync:watchChanges',
    'workspaces:create',
    'workspaces:createInvite',
    'workspaces:consumeInvite',
    'workspaces:ensure',
    'workspaces:listInvites',
    'workspaces:listMyWorkspaces',
    'workspaces:remove',
    'workspaces:revokeInvite',
    'workspaces:setActive',
    'workspaces:update',
] as const;

export const convexInternalApiContractNames = [
    'backgroundJobs:abort',
    'backgroundJobs:checkAborted',
    'backgroundJobs:cleanup',
    'backgroundJobs:complete',
    'backgroundJobs:create',
    'backgroundJobs:fail',
    'backgroundJobs:get',
    'backgroundJobs:getActiveCount',
    'backgroundJobs:update',
    'notifications:create',
    'notifications:getByUser',
    'notifications:markRead',
    'rateLimits:checkAndRecord',
    'rateLimits:cleanup',
    'rateLimits:getStats',
    'sync:gcChangeLog',
    'sync:gcTombstones',
    'sync:runScheduledGc',
    'sync:runWorkspaceGc',
    'users:getAuthAccountByProvider',
    'users:getAuthAccountByUserId',
    'webhooks:cancelDeliveriesByWebhook',
    'webhooks:claimPendingDeliveries',
    'webhooks:createDeliveryLog',
    'webhooks:createWebhook',
    'webhooks:deleteDeliveryLogsByWebhook',
    'webhooks:deleteWebhook',
    'webhooks:disableAllWebhooks',
    'webhooks:getDeliveryLogs',
    'webhooks:getRecentTerminalDeliveries',
    'webhooks:getWebhook',
    'webhooks:listActiveCustomHookNames',
    'webhooks:listAdminWebhooks',
    'webhooks:listWebhooks',
    'webhooks:listWebhooksByCustomHook',
    'webhooks:listWebhooksByEvent',
    'webhooks:purgeExpiredLogs',
    'webhooks:resetStaleInFlightDeliveries',
    'webhooks:updateDeliveryLog',
    'webhooks:updateWebhook',
    'webhooks:updateWebhookHealth',
    'workspaces:acceptInviteAndProvisionUser',
    'workspaces:listInvitesInternal',
    'workspaces:resolveSession',
    'workspaces:validateInviteInternal',
] as const;

export function getConvexApiReference(functionName: string): any {
    if ((convexInternalApiContractNames as readonly string[]).includes(functionName)) {
        throw new Error(`Internal Convex function is not public: ${functionName}`);
    }
    const [namespace, handler] = functionName.split(':');
    if (!namespace || !handler) {
        throw new Error(`Invalid Convex function name: ${functionName}`);
    }
    const namespaceApi = (anyApi as AnyApiShape)[namespace];
    const reference = namespaceApi?.[handler];
    if (!reference) {
        throw new Error(`Missing Convex function reference: ${functionName}`);
    }
    return reference;
}

export function getConvexInternalApiReference(functionName: string): any {
    const [namespace, handler] = functionName.split(':');
    if (!namespace || !handler) {
        throw new Error(`Invalid Convex internal function name: ${functionName}`);
    }
    const namespaceApi = (anyApi as AnyApiShape)[namespace];
    const reference = namespaceApi?.[handler];
    if (!reference) {
        throw new Error(`Missing Convex internal function reference: ${functionName}`);
    }
    return reference;
}
