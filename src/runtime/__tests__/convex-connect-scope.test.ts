import { readFileSync } from 'node:fs';
import { resolve } from 'node:path';
import { describe, expect, it } from 'vitest';

const connectSource = readFileSync(
    resolve(process.cwd(), 'templates/convex/connect.ts'),
    'utf8'
);
const schemaSource = readFileSync(
    resolve(process.cwd(), 'templates/convex/schema.ts'),
    'utf8'
);

describe('Convex Connect workspace scope', () => {
    it('requires user and workspace IDs in list, token lookup, and revoke functions', () => {
        expect(connectSource).toMatch(
            /getEnvironmentByControlTokenHash[\s\S]+?user_id: v\.id\('users'\)[\s\S]+?workspace_id: v\.id\('workspaces'\)[\s\S]+?by_user_workspace_control_token_hash/
        );
        expect(connectSource).toMatch(
            /listEnvironmentsForScope[\s\S]+?user_id: v\.id\('users'\)[\s\S]+?workspace_id: v\.id\('workspaces'\)[\s\S]+?by_user_workspace_status/
        );
        expect(connectSource).toMatch(
            /revokeEnvironment[\s\S]+?environment_id: v\.string\(\)[\s\S]+?user_id: v\.id\('users'\)[\s\S]+?workspace_id: v\.id\('workspaces'\)[\s\S]+?by_user_workspace_environment_id/
        );
    });

    it('declares matching compound indexes and an explicit limit policy', () => {
        expect(schemaSource).toContain(
            ".index('by_user_workspace_status', ["
        );
        expect(schemaSource).toContain(
            ".index('by_user_workspace_control_token_hash', ["
        );
        expect(schemaSource).toContain(
            ".index('by_user_workspace_environment_id', ["
        );
        expect(connectSource).toContain(
            "limit_scope: v.union(v.literal('account'), v.literal('workspace'))"
        );
        expect(connectSource).toContain(
            "args.limit_scope === 'workspace'"
        );
    });

    it('declares atomic provisioning and revocation lifecycle transitions', () => {
        expect(schemaSource).toContain("v.literal('provisioning')");
        expect(schemaSource).toContain("v.literal('revoking')");
        expect(schemaSource).toContain(
            ".index('by_status_lifecycle_due', ["
        );
        expect(connectSource).toMatch(
            /reserveDeviceAuthorization[\s\S]+?status: 'provisioning'[\s\S]+?approved_user_id/
        );
        expect(connectSource).toMatch(
            /saveEnvironmentRelayProgress[\s\S]+?claim_token/
        );
        expect(connectSource).toMatch(
            /completeEnvironmentProvisioning[\s\S]+?status: 'active'/
        );
        expect(connectSource).toMatch(
            /beginEnvironmentRevocation[\s\S]+?status: 'revoking'/
        );
        expect(connectSource).toMatch(
            /completeEnvironmentRevocation[\s\S]+?status: 'revoked'[\s\S]+?access_credential_ciphertext: ''/
        );
    });

    it('indexes and bounds retention plus unclaimed activation cleanup', () => {
        expect(schemaSource).toContain(
            ".index('by_status_updated', ['status', 'updated_at'])"
        );
        expect(schemaSource).toContain(
            ".index('by_status_activation_due', ["
        );
        expect(connectSource).toMatch(
            /claimNextEnvironmentLifecycle[\s\S]+?activation_claimed_at[\s\S]+?activation_deadline_at[\s\S]+?status: 'revoking'/
        );
        expect(connectSource).toMatch(
            /purgeConnectRetention[\s\S]+?Math\.min\(500[\s\S]+?by_status_updated[\s\S]+?ctx\.db\.delete/
        );
    });
});
