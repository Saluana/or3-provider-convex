/**
 * Ambient type declarations for Nuxt/Nitro auto-imports.
 *
 * These globals are provided by Nuxt's build pipeline when the module
 * is installed in a host app. This file makes standalone `tsc` work
 * without needing the full Nuxt type-generation chain.
 */

declare global {
    /** Define a Nuxt client/universal plugin. */
    const defineNuxtPlugin: typeof import('nuxt/app')['defineNuxtPlugin'];
    /** Define a Nitro server plugin. */
    const defineNitroPlugin: (handler: (nitro: any) => void | Promise<void>) => void;
    /** Define a Nitro event handler. */
    const defineEventHandler: typeof import('h3')['defineEventHandler'];
    /** Nuxt runtime config composable (auto-imported). */
    const useRuntimeConfig: typeof import('nuxt/app')['useRuntimeConfig'];
    /** Access the Nuxt app instance (auto-imported). */
    const useNuxtApp: typeof import('nuxt/app')['useNuxtApp'];

    interface ImportMeta {
        /** Vite HMR API — available in dev mode only. */
        readonly hot?: {
            readonly data: Record<string, any>;
            accept(): void;
            dispose(cb: (data: Record<string, any>) => void): void;
        };
    }
}

/** Augment Nuxt RuntimeConfig so `config.sync/storage/auth` are typed for standalone tsc. */
declare module 'nuxt/schema' {
    interface RuntimeConfig {
        auth: { enabled: boolean; provider: string };
        sync: { enabled: boolean; provider: string; convexUrl: string; convexAdminKey: string };
        storage: { enabled: boolean; provider: string };
        admin: { auth: { jwtSecret: string } };
    }
    interface PublicRuntimeConfig {
        ssrAuthEnabled?: boolean;
        sync: { enabled: boolean; provider: string; convexUrl: string };
        storage: { enabled: boolean; provider: string };
        limits: { enabled: boolean; maxConversations: number };
        [key: string]: unknown;
    }
}

export {};
