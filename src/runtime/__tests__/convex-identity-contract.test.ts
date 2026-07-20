import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { gunzipSync } from 'node:zlib';
import { describe, expect, it } from 'vitest';

function readIdentityTemplates(): { users: string; workspaces: string } {
    const templateRoot = fileURLToPath(new URL('../../../templates/convex/', import.meta.url));
    return {
        users: readFileSync(`${templateRoot}/users.ts`, 'utf8'),
        workspaces: readFileSync(`${templateRoot}/workspaces.ts`, 'utf8'),
    };
}

describe('Convex identity template contract', () => {
    it('returns canonical user ids and normalizes string ids before document lookup', () => {
        const { users, workspaces } = readIdentityTemplates();

        expect(users).toContain('user_id: v.string()');
        expect(users).toContain("ctx.db.normalizeId('users', args.user_id)");
        expect(workspaces.match(/user_id: userId/g)?.length).toBeGreaterThanOrEqual(2);
    });

    it('ships the identity contract in the generated template pack', () => {
        const packedPath = fileURLToPath(
            new URL('../../../templates/convex.pack.json.gz', import.meta.url)
        );
        const payload = JSON.parse(gunzipSync(readFileSync(packedPath)).toString('utf8')) as {
            files: Record<string, string>;
        };

        expect(payload.files['users.ts']).toContain('user_id: v.string()');
        expect(payload.files['users.ts']).toContain(
            "ctx.db.normalizeId('users', args.user_id)"
        );
        expect(
            payload.files['workspaces.ts'].match(/user_id: userId/g)?.length
        ).toBeGreaterThanOrEqual(2);
    });
});
