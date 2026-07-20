import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { gunzipSync } from 'node:zlib';
import { describe, expect, it, vi } from 'vitest';

const templateRoot = new URL('../../../templates/convex/', import.meta.url);
const sync = await import(/* @vite-ignore */ new URL('sync.ts', templateRoot).href);

type Handler = (ctx: any, args: any) => Promise<any>;
const handler = (fn: unknown): Handler => (fn as { _handler: Handler })._handler;

function makeCursorContext(input: {
    userId?: string;
    role?: 'owner' | 'editor' | 'viewer';
    maximum?: number;
    existing?: { owner_user_id?: string; last_seen_version: number };
}) {
    const userId = input.userId ?? 'user-a';
    const inserted = vi.fn();
    const patched = vi.fn();
    const query = vi.fn((table: string) => ({
        withIndex: (_index: string, build: (q: any) => unknown) => {
            const q = { eq: vi.fn(() => q) };
            build(q);
            return {
                first: vi.fn(async () => {
                    if (table === 'auth_accounts') return { user_id: userId };
                    if (table === 'workspace_members' && input.role) {
                        return { user_id: userId, role: input.role };
                    }
                    if (table === 'server_version_counter') {
                        return { value: input.maximum ?? 0 };
                    }
                    if (table === 'device_cursors' && input.existing) {
                        return { _id: 'cursor-1', ...input.existing };
                    }
                    return null;
                }),
            };
        },
    }));
    return {
        ctx: {
            auth: { getUserIdentity: vi.fn(async () => ({ subject: 'subject-a', issuer: 'issuer-a' })) },
            db: { query, insert: inserted, patch: patched },
        },
        inserted,
        patched,
    };
}

describe('Convex cursor integrity and retention bounds', () => {
    it('records a bounded cursor owned by the authenticated user', async () => {
        const { ctx, inserted } = makeCursorContext({ role: 'viewer', maximum: 10 });
        await handler(sync.updateDeviceCursor)(ctx, {
            workspace_id: 'workspace-1', device_id: ' device-a ', last_seen_version: 7,
        });
        expect(inserted).toHaveBeenCalledWith('device_cursors', expect.objectContaining({
            workspace_id: 'workspace-1',
            device_id: 'device-a',
            owner_user_id: 'user-a',
            last_seen_version: 7,
        }));
    });

    it.each([-1, 1.5, Number.NaN, Number.POSITIVE_INFINITY])(
        'rejects malformed cursor version %s',
        async (lastSeenVersion) => {
            const { ctx } = makeCursorContext({ role: 'viewer', maximum: 10 });
            await expect(handler(sync.updateDeviceCursor)(ctx, {
                workspace_id: 'workspace-1', device_id: 'device-a', last_seen_version: lastSeenVersion,
            })).rejects.toThrow('Invalid last_seen_version');
        }
    );

    it('rejects future, regressing, cross-owner, and cross-workspace cursor claims', async () => {
        const future = makeCursorContext({ role: 'viewer', maximum: 4 });
        await expect(handler(sync.updateDeviceCursor)(future.ctx, {
            workspace_id: 'workspace-1', device_id: 'device-a', last_seen_version: 5,
        })).rejects.toThrow('exceeds workspace version');

        const regressing = makeCursorContext({
            role: 'viewer', maximum: 10,
            existing: { owner_user_id: 'user-a', last_seen_version: 8 },
        });
        await expect(handler(sync.updateDeviceCursor)(regressing.ctx, {
            workspace_id: 'workspace-1', device_id: 'device-a', last_seen_version: 7,
        })).rejects.toThrow('cannot regress');

        const wrongOwner = makeCursorContext({
            userId: 'user-b', role: 'viewer', maximum: 10,
            existing: { owner_user_id: 'user-a', last_seen_version: 7 },
        });
        await expect(handler(sync.updateDeviceCursor)(wrongOwner.ctx, {
            workspace_id: 'workspace-1', device_id: 'device-a', last_seen_version: 8,
        })).rejects.toThrow('belongs to another user');

        const crossWorkspace = makeCursorContext({ maximum: 10 });
        await expect(handler(sync.updateDeviceCursor)(crossWorkspace.ctx, {
            workspace_id: 'workspace-other', device_id: 'device-a', last_seen_version: 1,
        })).rejects.toThrow('Forbidden');
    });

    it.each([1, 3599, 3600.5, 31536001])(
        'rejects unsafe retention window %s while GC remains disabled',
        async (retentionSeconds) => {
            await expect(handler(sync.gcTombstones)({}, {
                workspace_id: 'workspace-1', retention_seconds: retentionSeconds,
            })).rejects.toThrow('Invalid retention_seconds');
            await expect(handler(sync.gcChangeLog)({}, {
                workspace_id: 'workspace-1', retention_seconds: retentionSeconds,
            })).rejects.toThrow('Invalid retention_seconds');
        }
    );

    it('rejects unsafe GC batch, cursor, and continuation values', async () => {
        await expect(handler(sync.gcTombstones)({}, {
            workspace_id: 'workspace-1', retention_seconds: 3600, batch_size: 1001,
        })).rejects.toThrow('Invalid batch_size');
        await expect(handler(sync.gcChangeLog)({}, {
            workspace_id: 'workspace-1', retention_seconds: 3600, cursor: -1,
        })).rejects.toThrow('Invalid cursor');
        await expect(handler(sync.runWorkspaceGc)({}, {
            workspace_id: 'workspace-1', retention_seconds: 3600, continuation_count: 1001,
        })).rejects.toThrow('Invalid continuation_count');
    });

    it('server-authors delete timestamps in the template and generated mirror', () => {
        const template = readFileSync(fileURLToPath(new URL('sync.ts', templateRoot)), 'utf8');
        const mirror = readFileSync(
            fileURLToPath(new URL('../../../../or3-chat/convex/sync.ts', import.meta.url)),
            'utf8'
        );
        for (const source of [template, mirror]) {
            expect(source).toContain('deleted_at: nowSec()');
            expect(source).not.toContain('deleted_at: payloadDeletedAt ?? nowSec()');
            expect(source).not.toContain('? payload.deleted_at');
            expect(source).toMatch(/ctx\.db\s*\.query\('change_log'\)/);
            expect(source).toMatch(/ctx\.db\s*\.query\('tombstones'\)/);
        }
    });

    it('ships cursor and retention guards in the generated template pack', () => {
        const packPath = fileURLToPath(new URL('../../../templates/convex.pack.json.gz', import.meta.url));
        const pack = JSON.parse(gunzipSync(readFileSync(packPath)).toString('utf8')) as {
            files: Record<string, string>;
        };
        const shipped = pack.files['sync.ts'] ?? '';
        const policy = pack.files['syncHistoryGcPolicy.ts'] ?? '';
        expect(shipped).toContain('owner_user_id: callerUserId');
        expect(shipped).toContain("assertSafeNonnegativeInteger(args.last_seen_version");
        expect(shipped).toContain('validateGcArguments(args)');
        expect(shipped).toContain('deleted_at: nowSec()');
        expect(shipped).toContain("ctx.db.query('change_log')");
        expect(policy).toContain('enabled: true');
        expect(policy).toContain('snapshotBootstrapVerified: true');
    });
});
