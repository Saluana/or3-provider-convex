/** Standalone host contract fixture used by the provider release typecheck. */
export type RateLimitProvider = any;
export type RateLimitConfig = any;
export type RateLimitResult = any;
export type RateLimitStats = any;
export type BackgroundJobProvider = any;
export type BackgroundJob = any;
export type CreateJobParams = any;
export type JobUpdate = any;
export type ConnectAuthorizationRecord = any;
export type ConnectEnvironmentRecord = any;
export type WebhookDeliveryLog = any;
export type WebhookHealth = any;
export type WebhookRegistration = any;
export type WebhookStore = any;
export type CanonicalStorageQueryRequest = any;
export type CanonicalStorageQueryResponse = any;
export type SyncGatewayAdapter = any;
export type PendingOp = {
  stamp: { opId: string; [key: string]: any };
  tableName: string;
  operation: string;
  payload?: any;
  [key: string]: any;
};
export type PullRequest = any;
export type PullResponse = any;
export type SnapshotRequest = any;
export type SnapshotResponse = any;
export type PushBatch = { scope: any; ops: PendingOp[]; [key: string]: any };
export type PushResult = { results: Array<Record<string, any>>; [key: string]: any };
export type SyncProvider = any;
export type SyncScope = any;
export type SyncChange = any;
export type SyncSubscribeOptions = any;
export type AuthWorkspaceStore = any;
export type InviteProvisionResult = any;
export type InviteValidationResult = any;
export type WorkspaceRole = any;
export type ProviderAdminAdapter = any;
export type ProviderAdminStatusResult = any;
export type ProviderStatusContext = any;
export type ProviderActionContext = any;
export type DeploymentAdminChecker = any;
export type ConnectEnvironmentScope = any;
export type ConnectStore = any;
export type StorageGatewayAdapter = any;
export type ObjectStorageProvider = any;
export type PresignedUrlResult = any;
export type PresignUploadRequest = any;
export type PresignUploadResponse = any;
export type PresignDownloadRequest = any;
export type PresignDownloadResponse = any;
export type DeleteObjectRequest = any;
export type WorkspaceAccessStore = any;
export type WorkspaceSettingsStore = any;
export type AdminUserStore = any;
export type WorkspaceSummary = any;
export type AdminUserInfo = any;
export type CanonicalStorageQueryKind = any;
export type ApproveConnectAuthorizationInput = any;
export type BeginConnectEnvironmentRevocationInput = any;
export type ConnectEnvironmentLifecycleClaim = any;
export type ConnectEnvironmentRelayProgress = any;
export type CreateConnectAuthorizationInput = any;
export type PurgeConnectRecordsInput = any;
export type PurgeConnectRecordsResult = any;
export type ReserveConnectAuthorizationInput = any;

export class ConnectStoreError extends Error {
  constructor(...args: any[]) {
    super(typeof args[0] === 'string' ? args[0] : 'Connect store error');
  }
}

export const CLERK_PROVIDER_ID = 'clerk' as const;
export const CONVEX_PROVIDER_ID = 'convex' as const;
export const CONVEX_STORAGE_PROVIDER_ID = 'convex' as const;
export const CONVEX_JWT_TEMPLATE = 'convex' as const;
export const ADMIN_IDENTITY_ISSUER = 'or3-admin' as const;
export const memoryRateLimitProvider = {
  checkAndRecord: async (..._args: any[]): Promise<any> => ({ allowed: true, remaining: 0, retryAfterMs: 0 }),
  getStats: async (..._args: any[]): Promise<any> => null,
};
export const getJobConfig = (..._args: any[]): any => undefined;
export const emitWebhookSystemHook = async (..._args: any[]): Promise<void> => undefined;
export const listProviderTokenBrokerIds = (..._args: any[]): any[] => [];
export const resolveProviderToken = async (..._args: any[]): Promise<any> => null;
export const resolveSessionContext = async (..._args: any[]): Promise<any> => ({ authenticated: false });
export const registerAuthWorkspaceStore = (..._args: any[]): any => undefined;
export const registerSyncGatewayAdapter = (..._args: any[]): any => undefined;
export const registerStorageGatewayAdapter = (..._args: any[]): any => undefined;
export const registerProviderAdminAdapter = (..._args: any[]): any => undefined;
export const registerAdminStoreProvider = (..._args: any[]): any => undefined;
export const registerBackgroundJobProvider = (..._args: any[]): any => undefined;
export const registerRateLimitProvider = (..._args: any[]): any => undefined;
export const registerNotificationEmitter = (..._args: any[]): any => undefined;
export const registerWebhookStore = (..._args: any[]): any => undefined;
export const registerDeploymentAdminChecker = (..._args: any[]): any => undefined;
export const registerConnectStore = (..._args: any[]): any => undefined;
export const registerAuthProvider = (..._args: any[]): any => undefined;
export const registerProviderTokenBroker = (..._args: any[]): any => undefined;
export const registerStorageProvider = (..._args: any[]): any => undefined;
export const listStorageProviderIds = (..._args: any[]): any[] => [];
export const registerSyncProvider = (..._args: any[]): any => undefined;
export const setActiveSyncProvider = (..._args: any[]): any => undefined;
export const useAuthTokenBroker = (..._args: any[]): any => undefined;
export const useSessionContext = (..._args: any[]): any => undefined;
export const registerProvider = (..._args: any[]): any => undefined;
export const resolvePresignExpiresAt = (..._args: any[]): number => Date.now();
const schema: any = { safeParse: (..._args: any[]) => ({ success: true, data: _args[0], error: undefined }) };
export const PullResponseSchema = schema;
export const SnapshotResponseSchema = schema;
export const SyncChangesSchema = schema;
export const PushResultSchema = schema;
