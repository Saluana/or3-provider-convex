import { describe, expect, it } from 'vitest';
import * as hostSettings from '../../../templates/convex/hostSettings';
import * as adminFunctions from '../../../templates/convex/admin';
import * as syncFunctions from '../../../templates/convex/sync';
import * as workspaceFunctions from '../../../templates/convex/workspaces';

type Handler = (ctx: any, args: any) => Promise<any>;
const handler = (fn: unknown): Handler =>
    (fn as { _handler: Handler })._handler;

const SERVER_IDENTITY = {
    subject: 'or3-host-settings',
    issuer: 'https://or3.ai/auth/server',
    or3_server: true,
};

const EDITOR_IDENTITY = {
    subject: 'subject-1',
    issuer: 'https://clerk.test',
};

const ADMIN_IDENTITY = {
    subject: 'root',
    issuer: 'or3-admin',
};

function uuidOp(label: string): string {
    let hex = '';
    for (const ch of label) hex += ch.charCodeAt(0).toString(16).padStart(2, '0');
    hex = hex.padEnd(32, '0').slice(0, 32);
    return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-4${hex.slice(13, 16)}-a${hex.slice(17, 20)}-${hex.slice(20, 32)}`;
}

function fixture(options: {
    identity?: Record<string, unknown> | null;
    role?: 'owner' | 'editor' | 'viewer';
    memberships?: Array<{ workspace_id: string; user_id: string; role: string }>;
} = {}) {
    const tables: Record<string, any[]> = {
        auth_accounts: [
            {
                _id: 'account-1',
                provider: 'clerk',
                provider_user_id: 'subject-1',
                user_id: 'user-1',
            },
        ],
        admin_users: [],
        workspaces: [
            { _id: 'ws-1', name: 'Workspace', deleted: false },
            { _id: 'ws-2', name: 'Other', deleted: false },
        ],
        workspace_members: options.memberships ?? [],
        host_settings: [],
        kv: [],
        change_log: [],
        server_version_counter: [],
        tombstones: [],
    };
    if (options.role) {
        tables.workspace_members.push({
            _id: 'member-1',
            workspace_id: 'ws-1',
            user_id: 'user-1',
            role: options.role,
        });
    }
    let nextId = 1;

    const ctx = {
        auth: {
            getUserIdentity: async () =>
                options.identity === undefined ? SERVER_IDENTITY : options.identity,
        },
        db: {
            query(table: string) {
                let rows = [...(tables[table] ?? [])];
                const builder: any = {
                    withIndex: (_name: string, constrain: (query: any) => unknown) => {
                        const constraints: Array<[string, unknown]> = [];
                        const query = {
                            eq(field: string, value: unknown) {
                                constraints.push([field, value]);
                                return query;
                            },
                        };
                        constrain(query);
                        rows = rows.filter((row) =>
                            constraints.every(([field, value]) => row[field] === value)
                        );
                        return builder;
                    },
                    first: async () => rows[0] ?? null,
                    collect: async () => [...rows],
                    take: async (limit: number) => rows.slice(0, limit),
                };
                return builder;
            },
            get: async (id: string) =>
                Object.values(tables)
                    .flat()
                    .find((row: any) => row._id === id) ?? null,
            insert: async (table: string, value: any) => {
                const id = `${table}-${nextId++}`;
                if (!tables[table]) tables[table] = [];
                tables[table].push({ _id: id, _creationTime: nextId, ...value });
                return id;
            },
            patch: async (id: string, value: any) => {
                const row = Object.values(tables)
                    .flat()
                    .find((candidate: any) => candidate._id === id);
                if (!row) throw new Error(`missing row ${id}`);
                Object.assign(row, value);
            },
            delete: async (id: string) => {
                for (const rows of Object.values(tables)) {
                    const index = rows.findIndex((row: any) => row._id === id);
                    if (index >= 0) rows.splice(index, 1);
                }
            },
        },
    };
    return { ctx, tables };
}

describe('Convex private host settings', () => {
    it('lets the trusted host server read and write private settings', async () => {
        const f = fixture();
        await handler(hostSettings.setHostSetting)(f.ctx, {
            workspace_id: 'ws-1',
            key: 'plugins.enabled',
            value: '["demo"]',
        });
        await expect(
            handler(hostSettings.getHostSetting)(f.ctx, {
                workspace_id: 'ws-1',
                key: 'plugins.enabled',
            })
        ).resolves.toBe('["demo"]');
        expect(f.tables.kv).toEqual([]);
    });

    it('refuses workspace member tokens even with owner role', async () => {
        const f = fixture({ identity: EDITOR_IDENTITY, role: 'owner' });
        await expect(
            handler(hostSettings.getHostSetting)(f.ctx, {
                workspace_id: 'ws-1',
                key: 'plugins.enabled',
            })
        ).rejects.toThrow('host server identity required');
        await expect(
            handler(hostSettings.setHostSetting)(f.ctx, {
                workspace_id: 'ws-1',
                key: 'plugins.enabled',
                value: '["demo"]',
            })
        ).rejects.toThrow('host server identity required');
        await expect(
            handler(hostSettings.compareAndSetHostSetting)(f.ctx, {
                workspace_id: 'ws-1',
                key: 'plugins.enabled',
                expected_value: null,
                value: '["demo"]',
            })
        ).rejects.toThrow('host server identity required');
        expect(f.tables.host_settings).toEqual([]);
    });

    it('refuses viewer tokens for reads and writes', async () => {
        const f = fixture({ identity: EDITOR_IDENTITY, role: 'viewer' });
        await expect(
            handler(hostSettings.getHostSetting)(f.ctx, {
                workspace_id: 'ws-1',
                key: 'plugins.enabled',
            })
        ).rejects.toThrow('host server identity required');
    });

    it('scopes values to the workspace', async () => {
        const f = fixture();
        await handler(hostSettings.setHostSetting)(f.ctx, {
            workspace_id: 'ws-1',
            key: 'plugins.enabled',
            value: '["one"]',
        });
        await expect(
            handler(hostSettings.getHostSetting)(f.ctx, {
                workspace_id: 'ws-2',
                key: 'plugins.enabled',
            })
        ).resolves.toBeNull();
        await handler(hostSettings.setHostSetting)(f.ctx, {
            workspace_id: 'ws-2',
            key: 'plugins.enabled',
            value: '["two"]',
        });
        await expect(
            handler(hostSettings.getHostSetting)(f.ctx, {
                workspace_id: 'ws-1',
                key: 'plugins.enabled',
            })
        ).resolves.toBe('["one"]');
    });

    it('wins exactly one racing compare-and-set', async () => {
        const f = fixture();
        const compareAndSet = handler(hostSettings.compareAndSetHostSetting);
        const args = {
            workspace_id: 'ws-1',
            key: 'plugins.enabled',
            expected_value: null,
            value: '["winner"]',
        };
        await expect(compareAndSet(f.ctx, args)).resolves.toBe(true);
        await expect(compareAndSet(f.ctx, args)).resolves.toBe(false);
        await expect(
            compareAndSet(f.ctx, { ...args, expected_value: '["winner"]', value: '["next"]' })
        ).resolves.toBe(true);
        await expect(
            compareAndSet(f.ctx, args)
        ).resolves.toBe(false);
        expect(
            f.tables.host_settings.filter((row) => row.workspace_id === 'ws-1')
        ).toHaveLength(1);
    });

    it('exposes legacy kv values without promoting them', async () => {
        const f = fixture();
        f.tables.kv.push({
            _id: 'kv-1',
            workspace_id: 'ws-1',
            name: 'plugins.enabled',
            value: '["legacy"]',
            deleted: false,
        });
        await expect(
            handler(hostSettings.getLegacyWorkspaceSetting)(f.ctx, {
                workspace_id: 'ws-1',
                key: 'plugins.enabled',
            })
        ).resolves.toBe('["legacy"]');
        // The private table is still empty: the host decides what to migrate.
        await expect(
            handler(hostSettings.getHostSetting)(f.ctx, {
                workspace_id: 'ws-1',
                key: 'plugins.enabled',
            })
        ).resolves.toBeNull();
        expect(f.tables.host_settings).toEqual([]);
    });

    it('hides soft-deleted legacy kv values', async () => {
        const f = fixture();
        f.tables.kv.push({
            _id: 'kv-1',
            workspace_id: 'ws-1',
            name: 'plugins.enabled',
            value: '["legacy"]',
            deleted: true,
        });
        await expect(
            handler(hostSettings.getLegacyWorkspaceSetting)(f.ctx, {
                workspace_id: 'ws-1',
                key: 'plugins.enabled',
            })
        ).resolves.toBeNull();
    });

    it('deletes private host settings with the workspace', async () => {
        const f = fixture({ identity: EDITOR_IDENTITY, role: 'owner' });
        f.tables.host_settings.push({
            _id: 'host-setting-1',
            workspace_id: 'ws-1',
            key: 'plugins.enabled',
            value: '["demo"]',
            updated_at: 1,
        });
        f.tables.host_settings.push({
            _id: 'host-setting-2',
            workspace_id: 'ws-2',
            key: 'plugins.enabled',
            value: '["other"]',
            updated_at: 1,
        });
        f.tables.kv.push({
            _id: 'kv-1',
            workspace_id: 'ws-1',
            name: 'theme',
            value: 'dark',
            deleted: false,
        });

        await handler(workspaceFunctions.remove)(f.ctx, { workspace_id: 'ws-1' });

        expect(f.tables.host_settings.filter((row) => row.workspace_id === 'ws-1')).toEqual([]);
        expect(f.tables.host_settings.filter((row) => row.workspace_id === 'ws-2')).toHaveLength(1);
        expect(f.tables.kv.filter((row) => row.workspace_id === 'ws-1')).toEqual([]);
        expect(f.tables.workspaces.some((row) => row._id === 'ws-1')).toBe(false);
    });

    it('keeps the deployment-admin bridge functions on private storage', async () => {
        const f = fixture({ identity: ADMIN_IDENTITY });
        await handler(adminFunctions.setWorkspaceSetting)(f.ctx, {
            workspace_id: 'ws-1',
            key: 'admin.guest_access.enabled',
            value: 'true',
        });
        expect(
            f.tables.host_settings.filter(
                (row) => row.key === 'admin.guest_access.enabled'
            )
        ).toHaveLength(1);
        expect(f.tables.kv).toEqual([]);
        await expect(
            handler(adminFunctions.compareAndSetWorkspaceSetting)(f.ctx, {
                workspace_id: 'ws-1',
                key: 'admin.guest_access.enabled',
                expected_value: 'true',
                value: 'false',
            })
        ).resolves.toBe(true);
        await expect(
            handler(adminFunctions.getWorkspaceSetting)(f.ctx, {
                workspace_id: 'ws-1',
                key: 'admin.guest_access.enabled',
            })
        ).resolves.toBe('false');
    });
});

describe('Convex sync cannot reach host enforcement state', () => {
    function syncFixture() {
        return fixture({
            identity: EDITOR_IDENTITY,
            role: 'editor',
            memberships: undefined,
        });
    }

    async function push(f: ReturnType<typeof fixture>, op: Record<string, unknown>) {
        return await handler(syncFunctions.push)(f.ctx, {
            workspace_id: 'ws-1',
            ops: [op],
        }) as { results: Array<Record<string, unknown>> };
    }

    it('rejects editor sync writes to reserved host setting keys', async () => {
        const f = syncFixture();
        const result = await push(f, {
            op_id: uuidOp('reserved-put'),
            table_name: 'kv',
            operation: 'put',
            pk: 'ws-1:plugins.enabled',
            payload: { id: 'ws-1:plugins.enabled', name: 'plugins.enabled', value: '["evil"]', deleted: false },
            clock: 1,
            hlc: '1:device',
            device_id: 'device-1',
        });
        expect(result.results[0]).toMatchObject({
            success: false,
            error: 'Reserved host setting key: plugins.enabled',
        });

        const deleteResult = await push(f, {
            op_id: uuidOp('reserved-delete'),
            table_name: 'kv',
            operation: 'delete',
            pk: 'ws-1:admin.guest_access.enabled',
            clock: 2,
            hlc: '2:device',
            device_id: 'device-1',
        });
        expect(deleteResult.results[0]).toMatchObject({
            success: false,
            error: 'Reserved host setting key: admin.guest_access.enabled',
        });
        expect(f.tables.host_settings).toEqual([]);
        expect(f.tables.kv).toEqual([]);
    });

    it('rejects editor sync writes to the private setup-values namespace', async () => {
        const f = syncFixture();
        const names = [
            'plugin:alpha:setup-values',
            'plugin:alpha:setup-values.sha256-deadbeef',
            'plugin:alpha:setup-values.sha256-deadbeef.operation.op-1',
        ];
        for (let index = 0; index < names.length; index += 1) {
            const name = names[index]!;
            const result = await push(f, {
                op_id: uuidOp(`setup-${index}`),
                table_name: 'kv',
                operation: 'put',
                pk: `ws-1:${name}`,
                payload: {
                    id: `ws-1:${name}`,
                    name,
                    value: JSON.stringify({
                        schemaVersion: 1,
                        pluginId: 'alpha',
                        packageDigest: 'sha256-deadbeef',
                        operationId: null,
                        revision: 9999,
                        values: { injected: true },
                    }),
                    deleted: false,
                },
                clock: 1,
                hlc: '1:device',
                device_id: 'device-1',
            });
            expect(result.results[0]).toMatchObject({
                success: false,
                error: `Reserved host setting key: ${name}`,
            });
        }
        const deleteResult = await push(f, {
            op_id: uuidOp('setup-delete'),
            table_name: 'kv',
            operation: 'delete',
            pk: 'ws-1:plugin:alpha:setup-values.sha256-deadbeef',
            clock: 2,
            hlc: '2:device',
            device_id: 'device-1',
        });
        expect(deleteResult.results[0]).toMatchObject({
            success: false,
            error: 'Reserved host setting key: plugin:alpha:setup-values.sha256-deadbeef',
        });
        expect(f.tables.kv).toEqual([]);
    });

    it('still allows kv keys outside the reserved namespace', async () => {
        const f = syncFixture();
        const result = await push(f, {
            op_id: uuidOp('plugin-other'),
            table_name: 'kv',
            operation: 'put',
            pk: 'ws-1:plugin:alpha:permissions',
            payload: {
                id: 'ws-1:plugin:alpha:permissions',
                name: 'plugin:alpha:permissions',
                value: '{}',
                deleted: false,
            },
            clock: 1,
            hlc: '1:device',
            device_id: 'device-1',
        });
        expect(result.results[0]).toMatchObject({ success: true });
    });

    it('rejects pushes that name the private table directly', async () => {
        const f = syncFixture();
        const result = await push(f, {
            op_id: uuidOp('host-table'),
            table_name: 'host_settings',
            operation: 'put',
            pk: 'ws-1:plugins.enabled',
            payload: { value: '["evil"]' },
            clock: 1,
            hlc: '1:device',
            device_id: 'device-1',
        });
        expect(result.results[0]).toMatchObject({
            success: false,
            error: 'Invalid table: host_settings',
        });
        expect(f.tables.host_settings).toEqual([]);
    });

    it('still accepts ordinary kv preference writes', async () => {
        const f = syncFixture();
        const result = await push(f, {
            op_id: uuidOp('pref-put'),
            table_name: 'kv',
            operation: 'put',
            pk: 'ws-1:theme',
            payload: { id: 'ws-1:theme', name: 'theme', value: 'dark', deleted: false },
            clock: 1,
            hlc: '1:device',
            device_id: 'device-1',
        });
        expect(result.results[0]).toMatchObject({ success: true });
        expect(f.tables.kv).toHaveLength(1);
    });
});
