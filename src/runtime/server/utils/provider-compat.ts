const LEGACY_CLERK_ONLY_BACKEND_KEY = '__or3_convex_legacy_clerk_only_backend__';

type LegacyFlagGlobal = typeof globalThis & {
    [LEGACY_CLERK_ONLY_BACKEND_KEY]?: boolean;
};

export function isLegacyClerkOnlyBackend(): boolean {
    const globalState = globalThis as LegacyFlagGlobal;
    return globalState[LEGACY_CLERK_ONLY_BACKEND_KEY] === true;
}

export function markLegacyClerkOnlyBackend(): void {
    const globalState = globalThis as LegacyFlagGlobal;
    globalState[LEGACY_CLERK_ONLY_BACKEND_KEY] = true;
}

export function resolveConvexAuthProvider(preferredProvider: string): string {
    return isLegacyClerkOnlyBackend() ? 'clerk' : preferredProvider;
}

export function isLegacyClerkOnlyError(error: unknown): boolean {
    const message = error instanceof Error ? error.message : String(error);
    return message.includes("Only 'clerk' is supported.");
}
