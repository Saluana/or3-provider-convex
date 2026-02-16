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
    backgroundJobs: createNamespaceProxy('backgroundJobs'),
    notifications: createNamespaceProxy('notifications'),
    rateLimits: createNamespaceProxy('rateLimits'),
    storage: createNamespaceProxy('storage'),
    sync: createNamespaceProxy('sync'),
    users: createNamespaceProxy('users'),
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
    'rateLimits:checkAndRecord',
    'rateLimits:getStats',
    'storage:commitUpload',
    'storage:gcDeletedFiles',
    'storage:generateUploadUrl',
    'storage:getFileUrl',
    'sync:gcChangeLog',
    'sync:gcTombstones',
    'sync:pull',
    'sync:push',
    'sync:updateDeviceCursor',
    'sync:watchChanges',
    'users:getAuthAccountByProvider',
    'workspaces:create',
    'workspaces:createInvite',
    'workspaces:consumeInvite',
    'workspaces:ensure',
    'workspaces:listInvites',
    'workspaces:listMyWorkspaces',
    'workspaces:remove',
    'workspaces:revokeInvite',
    'workspaces:resolveSession',
    'workspaces:setActive',
    'workspaces:update',
] as const;

export function getConvexApiReference(functionName: string): any {
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
