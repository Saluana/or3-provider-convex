/**
 * @module server/utils/rate-limit/providers/convex
 *
 * Purpose:
 * Convex-backed deployment-wide rate limit provider.
 *
 * Responsibilities:
 * - Use Convex mutations and queries for persistent rate limits.
 * - Fail closed on configuration or runtime failures.
 *
 * Non-Goals:
 * - Implement rate limit policy selection.
 * - Guarantee global ordering across multiple Convex clusters.
 */

import { ConvexHttpClient } from 'convex/browser';
import { convexInternalApi as internalApi } from '../../utils/convex-api';
import type {
    RateLimitProvider,
    RateLimitConfig,
    RateLimitResult,
    RateLimitStats,
} from '~~/server/utils/rate-limit/types';
import { CONVEX_PROVIDER_ID } from '~~/shared/cloud/provider-ids';
import { useRuntimeConfig } from '#imports';

/**
 * Purpose:
 * Convex-backed implementation of the rate limit provider.
 *
 * Behavior:
 * - Lazily initializes a Convex client from runtime config.
 * - Preserves durable enforcement when Convex is unavailable by rejecting calls.
 */
export class ConvexRateLimitProvider implements RateLimitProvider {
    readonly name = CONVEX_PROVIDER_ID;
    private client: ConvexHttpClient | null = null;

    private getClient(): ConvexHttpClient {
        if (this.client) {
            return this.client;
        }

        const config = useRuntimeConfig();
        const convexUrl =
            (config.sync as { convexUrl?: string } | undefined)?.convexUrl ??
            config.public.sync.convexUrl;
        const adminKey = (
            config.sync as { convexAdminKey?: string } | undefined
        )?.convexAdminKey?.trim();

        if (!convexUrl) throw new Error('Convex URL not configured');
        if (!adminKey) throw new Error('Convex admin key not configured');

        this.client = new ConvexHttpClient(convexUrl);
        const issuer = 'https://or3.ai/internal';
        const subject = 'or3-rate-limit-store';
        this.client.setAdminAuth(adminKey, {
            subject,
            issuer,
            tokenIdentifier: `${issuer}|${subject}`,
        });
        return this.client;
    }

    async checkAndRecord(key: string, config: RateLimitConfig): Promise<RateLimitResult> {
        requireConfig(config);
        const client = this.getClient();
        const result = await client.mutation(internalApi.rateLimits.checkAndRecord, {
            key,
            windowMs: config.windowMs,
            maxRequests: config.maxRequests,
        });

        return {
            allowed: result.allowed,
            remaining: result.remaining,
            retryAfterMs: result.retryAfterMs,
        };
    }

    async getStats(key: string, config: RateLimitConfig): Promise<RateLimitStats | null> {
        requireConfig(config);
        const client = this.getClient();
        return await client.query(internalApi.rateLimits.getStats, {
            key,
            windowMs: config.windowMs,
            maxRequests: config.maxRequests,
        });
    }
}

/**
 * Purpose:
 * Singleton Convex provider instance.
 */
export const convexRateLimitProvider = new ConvexRateLimitProvider();

function requireConfig(config: RateLimitConfig): void {
    if (!Number.isSafeInteger(config.windowMs) || config.windowMs <= 0 ||
        !Number.isSafeInteger(config.maxRequests) || config.maxRequests <= 0) {
        throw new Error('Invalid Convex rate-limit configuration');
    }
}
