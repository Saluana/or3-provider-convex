import { expect, it, vi } from 'vitest';
vi.mock('../../../templates/convex/_generated/server', () => ({
    internalMutation: (definition: unknown) => definition,
    internalQuery: (definition: unknown) => definition,
}));
import { createWebhook } from '../../../templates/convex/webhooks';

// Exercise the actual mutation against a full quota. Convex serializes the
// indexed read and insert inside this mutation; the host must not pre-count.
it('declines insertion when the durable registration scope is full', async () => {
    const insert = vi.fn();
    const ctx = { db: { insert, query: () => ({ withIndex: () => ({ take: async () => [{ id: 'existing' }] }) }) } };
    const input = { id: 'new', scope: 'user', user_id: 'member', workspace_id: 'workspace', url: 'https://example.test', label: '', events: [], custom_hooks: [], signing_secret_enc: '', enabled: true, health: 'unknown', created_at: 1, updated_at: 1, registration_limit: 1 };
    expect(await (createWebhook as unknown as { _handler: (ctx: unknown, args: unknown) => Promise<unknown> })._handler(ctx, input)).toBeNull();
    expect(insert).not.toHaveBeenCalled();
});
