/** Function references derived from the bundled template handlers. */
import type { ApiFromModules, FilterApi, FunctionReference, FunctionType } from 'convex/server';
import type * as admin from '../admin';
import type * as backgroundJobs from '../backgroundJobs';
import type * as connect from '../connect';
import type * as hostSettings from '../hostSettings';
import type * as notifications from '../notifications';
import type * as rateLimits from '../rateLimits';
import type * as storage from '../storage';
import type * as sync from '../sync';
import type * as users from '../users';
import type * as webhooks from '../webhooks';
import type * as workspaces from '../workspaces';

declare const fullApi: ApiFromModules<{
    admin: typeof admin;
    backgroundJobs: typeof backgroundJobs;
    connect: typeof connect;
    hostSettings: typeof hostSettings;
    notifications: typeof notifications;
    rateLimits: typeof rateLimits;
    storage: typeof storage;
    sync: typeof sync;
    users: typeof users;
    webhooks: typeof webhooks;
    workspaces: typeof workspaces;
}>;
export declare const api: FilterApi<typeof fullApi, FunctionReference<FunctionType, 'public'>>;
export declare const internal: FilterApi<typeof fullApi, FunctionReference<FunctionType, 'internal'>>;
