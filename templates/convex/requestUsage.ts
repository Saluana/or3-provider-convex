import { v } from 'convex/values';

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
});

type RequestUsage = typeof requestUsageValidator.type;
const counters = [
    'prompt_tokens', 'completion_tokens', 'iteration', 'measured_at',
    'prefix_message_count', 'input_estimate_tokens',
] as const;
const identifiers = ['model', 'request_id', 'prefix_hash', 'configuration_hash'] as const;

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
    return result as RequestUsage;
}
