import { createError } from 'h3';

const RETRYABLE_CODES = new Set([
    'UND_ERR_CONNECT_TIMEOUT',
    'UND_ERR_HEADERS_TIMEOUT',
    'UND_ERR_SOCKET',
    'ECONNRESET',
    'ECONNREFUSED',
    'ENOTFOUND',
    'EAI_AGAIN',
    'ETIMEDOUT',
]);

const RETRYABLE_NAMES = new Set([
    'ConnectTimeoutError',
    'HeadersTimeoutError',
    'SocketError',
]);

const RETRYABLE_MESSAGE_SNIPPETS = [
    'fetch failed',
    'connect timeout',
    'headers timeout',
    'socket hang up',
    'connection refused',
    'network error',
    'timed out',
];

type ErrorLike = {
    code?: unknown;
    name?: unknown;
    message?: unknown;
    cause?: unknown;
};

const DEFAULT_MAX_ATTEMPTS = 3;
const DEFAULT_BASE_DELAY_MS = 120;

function isObjectRecord(value: unknown): value is Record<string, unknown> {
    return typeof value === 'object' && value !== null;
}

function asErrorLike(value: unknown): ErrorLike | null {
    if (value instanceof Error) {
        return value as ErrorLike;
    }
    if (!isObjectRecord(value)) {
        return null;
    }
    return value as ErrorLike;
}

function* walkErrorChain(error: unknown, maxDepth: number = 8): Generator<ErrorLike> {
    let current: unknown = error;
    for (let i = 0; i < maxDepth; i += 1) {
        const node = asErrorLike(current);
        if (!node) return;
        yield node;
        current = node.cause;
        if (current === undefined || current === null) return;
    }
}

function includesRetryableMessage(message: string): boolean {
    const normalized = message.toLowerCase();
    return RETRYABLE_MESSAGE_SNIPPETS.some((snippet) => normalized.includes(snippet));
}

function isLikelyH3Error(error: unknown): error is { statusCode: number } {
    return isObjectRecord(error) && typeof error.statusCode === 'number';
}

function getRetryDelayMs(baseDelayMs: number, attempt: number): number {
    const exponent = Math.max(0, attempt - 1);
    return baseDelayMs * (2 ** exponent);
}

function wait(delayMs: number): Promise<void> {
    if (delayMs <= 0) return Promise.resolve();
    return new Promise((resolve) => {
        setTimeout(resolve, delayMs);
    });
}

export function isRetryableConvexTransportError(error: unknown): boolean {
    for (const node of walkErrorChain(error)) {
        if (typeof node.code === 'string' && RETRYABLE_CODES.has(node.code)) {
            return true;
        }
        if (typeof node.name === 'string' && RETRYABLE_NAMES.has(node.name)) {
            return true;
        }
        if (typeof node.message === 'string' && includesRetryableMessage(node.message)) {
            return true;
        }
    }
    return false;
}

export async function withConvexTransportRetry<T>(
    operation: string,
    run: () => Promise<T>,
    options: {
        maxAttempts?: number;
        baseDelayMs?: number;
    } = {}
): Promise<T> {
    const maxAttempts = Math.max(1, Math.floor(options.maxAttempts ?? DEFAULT_MAX_ATTEMPTS));
    const baseDelayMs = Math.max(0, Math.floor(options.baseDelayMs ?? DEFAULT_BASE_DELAY_MS));

    let attempt = 0;
    while (attempt < maxAttempts) {
        attempt += 1;
        try {
            return await run();
        } catch (error) {
            if (!isRetryableConvexTransportError(error) || attempt >= maxAttempts) {
                throw error;
            }

            const delayMs = getRetryDelayMs(baseDelayMs, attempt);
            if (import.meta.dev) {
                console.warn('[convex] transient transport failure; retrying', {
                    operation,
                    attempt,
                    maxAttempts,
                    delayMs,
                    error: error instanceof Error ? error.message : String(error),
                });
            }
            await wait(delayMs);
        }
    }

    throw new Error(`[convex] ${operation} retry loop exited unexpectedly`);
}

export function throwAsConvexServiceUnavailable(
    error: unknown,
    statusMessage: string
): never {
    if (isLikelyH3Error(error)) {
        throw error;
    }
    if (isRetryableConvexTransportError(error)) {
        throw createError({
            statusCode: 503,
            statusMessage,
        });
    }
    throw error;
}
