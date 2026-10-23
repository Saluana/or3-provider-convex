import { v } from 'convex/values';

const usageExtras = {
    cost: v.optional(v.number()),
    cached_tokens: v.optional(v.number()),
    reasoning_tokens: v.optional(v.number()),
};

/** Persist only complete measurements; legacy jobs have no usage field. */
export const requestUsageValidator = v.object({
    prompt_tokens: v.number(),
    completion_tokens: v.number(),
    model: v.string(),
    request_id: v.string(),
    iteration: v.number(),
    measured_at: v.number(),
    prefix_message_count: v.number(),
    prefix_hash: v.string(),
    configuration_hash: v.string(),
    input_estimate_tokens: v.number(),
    ...usageExtras,
    totals: v.optional(v.object({
        requests: v.number(), prompt_tokens: v.number(), completion_tokens: v.number(),
        ...usageExtras,
    })),
});

type RequestUsage = typeof requestUsageValidator.type;
const counters = [
    'prompt_tokens', 'completion_tokens', 'iteration', 'measured_at',
    'prefix_message_count', 'input_estimate_tokens',
] as const;
const identifiers = ['model', 'request_id', 'prefix_hash', 'configuration_hash'] as const;

function copyExtras(input: Record<string, unknown>, result: Record<string, unknown>): void {
    for (const key of ['cost', 'cached_tokens', 'reasoning_tokens'] as const) {
        if (!(key in input)) continue;
        const value = input[key];
        result[key] = typeof value === 'number' && Number.isFinite(value) && value >= 0 &&
            (key === 'cost' || Number.isSafeInteger(value)) ? value : undefined;
    }
}

/**
 * Self-contained template equivalent of the host's readRequestUsage contract.
 * Convex number validators cannot enforce nonnegative safe integers. Normalize
 * permissive optional inputs before storage so bad metadata never fails text.
 * Copy only owned fields, just as the host schema strips unknown properties.
 */
export function readRequestUsage(value: unknown): RequestUsage | undefined {
    if (!value || typeof value !== 'object' || Array.isArray(value)) return undefined;
    const input = value as Record<string, unknown>;
    const result: Record<string, unknown> = {};
    for (const key of counters) {
        const count = input[key];
        if (typeof count !== 'number' || !Number.isSafeInteger(count) || count < 0) return undefined;
        result[key] = count;
    }
    for (const key of identifiers) {
        const id = input[key];
        if (typeof id !== 'string' || id.length === 0) return undefined;
        result[key] = id;
    }
    copyExtras(input, result);
    if ('totals' in input) {
        const totals = input.totals;
        result.totals = undefined;
        if (totals && typeof totals === 'object' && !Array.isArray(totals)) {
            const source = totals as Record<string, unknown>;
            const keys = ['requests', 'prompt_tokens', 'completion_tokens'] as const;
            if (keys.every(key => typeof source[key] === 'number' && Number.isSafeInteger(source[key]) && source[key] >= 0)) {
                const normalized: Record<string, unknown> = Object.fromEntries(keys.map(key => [key, source[key]]));
                copyExtras(source, normalized);
                result.totals = normalized;
            }
        }
    }
    return result as RequestUsage;
}
