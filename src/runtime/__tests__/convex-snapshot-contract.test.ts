import { describe, expect, it } from "vitest";
import { readFileSync } from "node:fs";
import { gunzipSync } from "node:zlib";
import { verifySyncContract } from "~~/shared/testing/contracts/sync";
import { compareSyncRevision } from "~~/shared/sync/revision";

const syncFunctions = await import(
  /* @vite-ignore */ new URL(
    "../../../templates/convex/sync.ts",
    import.meta.url,
  ).href
);

type RegisteredFunction = {
  _handler: (ctx: any, args: any) => Promise<any>;
};

type Row = Record<string, any> & { _id: string };

const INDEX_FIELDS: Record<string, string[]> = {
  by_provider: ["provider", "provider_user_id"],
  by_workspace_user: ["workspace_id", "user_id"],
  by_workspace: ["workspace_id"],
  by_workspace_id: ["workspace_id", "id"],
  by_workspace_hash: ["workspace_id", "hash"],
  by_workspace_status: ["workspace_id", "status", "_creationTime"],
  by_workspace_table_pk: ["workspace_id", "table_name", "pk"],
  by_workspace_table_pk_version: [
    "workspace_id",
    "table_name",
    "pk",
    "server_version",
  ],
  by_workspace_version: ["workspace_id", "server_version"],
  by_op_id: ["op_id"],
};

class MemoryQuery {
  private conditions: Array<{
    op: "eq" | "gt" | "lte";
    field: string;
    value: unknown;
  }> = [];
  private indexName = "";
  private direction: "asc" | "desc" = "asc";

  constructor(
    private readonly rows: Row[],
    private readonly takeCounts: number[],
  ) {}

  withIndex(indexName: string, build: (q: any) => unknown): this {
    this.indexName = indexName;
    const chain = {
      eq: (field: string, value: unknown) => {
        this.conditions.push({ op: "eq", field, value });
        return chain;
      },
      gt: (field: string, value: unknown) => {
        this.conditions.push({ op: "gt", field, value });
        return chain;
      },
      lte: (field: string, value: unknown) => {
        this.conditions.push({ op: "lte", field, value });
        return chain;
      },
    };
    build(chain);
    return this;
  }

  order(direction: "asc" | "desc"): this {
    this.direction = direction;
    return this;
  }

  async first(): Promise<Row | null> {
    return this.materialize()[0] ?? null;
  }

  async take(count: number): Promise<Row[]> {
    this.takeCounts.push(count);
    return this.materialize().slice(0, count);
  }

  private materialize(): Row[] {
    const fields = INDEX_FIELDS[this.indexName] ?? ["_id"];
    const filtered = this.rows.filter((row) =>
      this.conditions.every(({ op, field, value }) => {
        if (op === "eq") return row[field] === value;
        if (op === "gt") return row[field] > (value as any);
        return row[field] <= (value as any);
      }),
    );
    filtered.sort((left, right) => {
      for (const field of fields) {
        if (left[field] < right[field])
          return this.direction === "asc" ? -1 : 1;
        if (left[field] > right[field])
          return this.direction === "asc" ? 1 : -1;
      }
      return 0;
    });
    return filtered;
  }
}

function createFixture(role: "viewer" | "editor" = "viewer") {
  const tables: Record<string, Row[]> = {
    auth_accounts: [
      {
        _id: "account-1",
        provider: "clerk",
        provider_user_id: "subject-1",
        user_id: "user-1",
      },
    ],
    workspace_members: [
      {
        _id: "member-1",
        workspace_id: "ws-1",
        user_id: "user-1",
        role,
      },
    ],
    server_version_counter: [
      { _id: "counter-1", workspace_id: "ws-1", value: 5 },
    ],
    device_cursors: [],
    messages: [
      {
        _id: "message-doc-1",
        workspace_id: "ws-1",
        id: "message-1",
        deleted: false,
        data: { text: "hello" },
        clock: 1,
        hlc: "1:0:dev",
        op_id: "op-1",
        server_version: 1,
      },
    ],
    projects: [
      {
        _id: "project-doc-1",
        workspace_id: "ws-1",
        id: "project-1",
        name: "deleted project",
        deleted: true,
        deleted_at: 1003,
        server_deleted_at: 1003,
        clock: 3,
        hlc: "3:0:dev",
        op_id: "op-3",
        server_version: 3,
      },
    ],
    threads: [
      {
        _id: "thread-doc-a",
        workspace_id: "ws-1",
        id: "thread-a",
        title: "before",
        deleted: false,
        clock: 4,
        hlc: "4:0:dev",
        op_id: "op-4",
        server_version: 4,
      },
      {
        _id: "thread-doc-b",
        workspace_id: "ws-1",
        id: "thread-b",
        title: "stable",
        deleted: false,
        clock: 5,
        hlc: "5:0:dev",
        op_id: "op-5",
        server_version: 5,
      },
    ],
    tombstones: [
      {
        _id: "tombstone-1",
        workspace_id: "ws-1",
        table_name: "projects",
        pk: "project-1",
        deleted_at: 1003,
        server_deleted_at: 1003,
        clock: 3,
        hlc: "3:0:dev",
        op_id: "op-3",
        server_version: 3,
      },
    ],
    upload_intents: [],
    sync_record_versions: [],
    sync_snapshot_sessions: [],
    change_log: [],
    file_meta: [],
    kv: [],
    notifications: [],
    posts: [],
  };
  const takeCounts: number[] = [];
  let inserted = 0;
  const db = {
    query: (table: string) => new MemoryQuery(tables[table] ?? [], takeCounts),
    insert: async (table: string, value: Record<string, unknown>) => {
      inserted += 1;
      const id = `${table}-${inserted}`;
      (tables[table] ??= []).push({ _id: id, ...value });
      return id;
    },
    get: async (id: string) =>
      Object.values(tables)
        .flat()
        .find((row) => row._id === id) ?? null,
    patch: async (id: string, value: Record<string, unknown>) => {
      const row = Object.values(tables)
        .flat()
        .find((candidate) => candidate._id === id);
      if (!row) throw new Error(`Missing row: ${id}`);
      Object.assign(row, value);
    },
    delete: async (id: string) => {
      for (const rows of Object.values(tables)) {
        const index = rows.findIndex((candidate) => candidate._id === id);
        if (index >= 0) {
          rows.splice(index, 1);
          return;
        }
      }
    },
  };
  const ctx = {
    auth: {
      getUserIdentity: async () => ({
        subject: "subject-1",
        issuer: "https://clerk.example.test",
      }),
    },
    db,
  };
  return { ctx, tables, takeCounts };
}

describe("Convex materialized snapshot contract", () => {
  it("executes the shared bootstrap and revision contract", async () => {
    const fixture = createFixture();
    const snapshot = (syncFunctions.snapshot as RegisteredFunction)._handler;
    await verifySyncContract({
      name: "convex",
      async reset() {
        for (const table of ["messages", "projects", "threads", "tombstones"]) {
          fixture.tables[table] = [];
        }
        fixture.tables.sync_snapshot_sessions = [];
      },
      async seedMaterialized(items, highWatermark) {
        fixture.tables.server_version_counter[0]!.value = highWatermark;
        for (const item of items) {
          if (item.kind === "row") {
            fixture.tables[item.tableName].push({
              _id: `${item.tableName}-${item.pk}`,
              workspace_id: "ws-1",
              ...(item.payload as Record<string, unknown>),
              deleted: false,
              clock: item.revision.clock,
              hlc: item.revision.hlc,
              op_id: item.revision.opId,
              server_version: highWatermark,
            });
          } else {
            fixture.tables.tombstones.push({
              _id: `tombstone-${item.pk}`,
              workspace_id: "ws-1",
              table_name: item.tableName,
              pk: item.pk,
              deleted_at: item.serverDeletedAt,
              server_deleted_at: item.serverDeletedAt,
              clock: item.revision.clock,
              hlc: item.revision.hlc,
              op_id: item.revision.opId,
              server_version: highWatermark,
            });
          }
        }
      },
      async bootstrap() {
        const items: any[] = [];
        let pageToken: string | undefined;
        let highWatermark = 0;
        do {
          const page = await snapshot(fixture.ctx, {
            workspace_id: "ws-1", page_size: 1, page_token: pageToken,
          });
          items.push(...page.items);
          highWatermark = page.highWatermark;
          pageToken = page.nextPageToken ?? undefined;
        } while (pageToken);
        return { items, highWatermark };
      },
      async resolveWinner(left, right) {
        return compareSyncRevision(left, right) >= 0 ? left : right;
      },
    });
  });
  it("fresh-device snapshot remains complete after verified retention deletes old history", async () => {
    const fixture = createFixture();
    fixture.tables.device_cursors.push({
      _id: "cursor-1", workspace_id: "ws-1", device_id: "device-1",
      last_seen_version: 5,
    });
    fixture.tables.change_log.push({
      _id: "log-1", workspace_id: "ws-1", server_version: 1,
      table_name: "messages", pk: "message-1", op: "put",
      clock: 1, hlc: "1:0:dev", device_id: "dev", op_id: "op-1",
      created_at: 1,
    });

    const gc = (syncFunctions.gcChangeLog as RegisteredFunction)._handler;
    await gc(fixture.ctx, {
      workspace_id: "ws-1", retention_seconds: 3600, batch_size: 10,
    });
    expect(fixture.tables.change_log).toEqual([]);

    const snapshot = (syncFunctions.snapshot as RegisteredFunction)._handler;
    const page = await snapshot(fixture.ctx, {
      workspace_id: "ws-1", page_size: 100,
    });
    expect(page.highWatermark).toBe(5);
    expect(page.items).toContainEqual(expect.objectContaining({
      kind: "row", tableName: "messages", pk: "message-1",
    }));
  });

  it("bootstraps unchanged materialized rows after their original change-log entries are pruned", async () => {
    const fixture = createFixture();
    expect(fixture.tables.change_log).toEqual([]);
    const snapshot = (syncFunctions.snapshot as RegisteredFunction)._handler;

    const page = await snapshot(fixture.ctx, {
      workspace_id: "ws-1",
      page_size: 100,
    });

    expect(page.highWatermark).toBe(5);
    expect(page.items).toContainEqual(
      expect.objectContaining({
        kind: "row",
        tableName: "messages",
        pk: "message-1",
        payload: expect.objectContaining({
          id: "message-1",
          data: { text: "hello" },
        }),
        revision: { clock: 1, hlc: "1:0:dev", opId: "op-1" },
      }),
    );
  });

  it("pages canonical live metadata and reference edges without consulting retained logs", async () => {
    const fixture = createFixture();
    const hashes = ["a", "b", "c"].map(
      (letter) => `sha256:${letter.repeat(64)}`,
    );
    fixture.tables.file_meta.push(
      {
        _id: "file-a",
        workspace_id: "ws-1",
        hash: hashes[0],
        deleted: false,
        size_bytes: 10,
        storage_id: "storage-a",
        updated_at: 100,
      },
      {
        _id: "file-b",
        workspace_id: "ws-1",
        hash: hashes[1],
        deleted: false,
        size_bytes: 20,
        updated_at: 101,
      },
      {
        _id: "file-deleted",
        workspace_id: "ws-1",
        hash: hashes[2],
        deleted: true,
        size_bytes: 30,
        updated_at: 102,
      },
    );
    fixture.tables.messages[0]!.file_hashes = JSON.stringify(hashes);
    fixture.tables.posts.push({
      _id: "post-edge",
      workspace_id: "ws-1",
      id: "post-1",
      deleted: false,
      file_hashes: JSON.stringify([hashes[0]]),
    });
    // A contradictory retained delete must have no influence on canonical reads.
    fixture.tables.change_log.push({
      _id: "losing-delete",
      workspace_id: "ws-1",
      server_version: 99,
      table_name: "file_meta",
      pk: hashes[0],
      op: "delete",
    });
    const queryCanonicalStorage = (
      syncFunctions.queryCanonicalStorage as RegisteredFunction
    )._handler;

    const metadata: any[] = [];
    let cursor: string | undefined;
    do {
      const page = await queryCanonicalStorage(fixture.ctx, {
        workspace_id: "ws-1",
        kind: "live_metadata",
        page_size: 1,
        cursor,
      });
      expect(page.items.length).toBeLessThanOrEqual(1);
      metadata.push(...page.items);
      cursor = page.nextCursor;
    } while (cursor);
    expect(metadata).toEqual([
      expect.objectContaining({
        kind: "metadata",
        hash: "a".repeat(64),
        sizeBytes: 10,
      }),
      expect.objectContaining({
        kind: "metadata",
        hash: "b".repeat(64),
        sizeBytes: 20,
      }),
    ]);

    const references: any[] = [];
    cursor = undefined;
    do {
      const page = await queryCanonicalStorage(fixture.ctx, {
        workspace_id: "ws-1",
        kind: "reference_edges",
        page_size: 1,
        cursor,
      });
      expect(page.items.length).toBeLessThanOrEqual(1);
      references.push(...page.items);
      cursor = page.nextCursor;
    } while (cursor);
    expect(
      references.map(
        (edge) => `${edge.sourceTable}:${edge.sourceId}:${edge.hash}`,
      ),
    ).toEqual([
      `messages:message-1:${"a".repeat(64)}`,
      `messages:message-1:${"b".repeat(64)}`,
      `messages:message-1:${"c".repeat(64)}`,
      `posts:post-1:${"a".repeat(64)}`,
    ]);
    expect(fixture.takeCounts.every((count) => count <= 2)).toBe(true);
  });

  it("binds canonical cursors to filters, caps pages, and exposes active reservations explicitly", async () => {
    const fixture = createFixture();
    const hash = `sha256:${"a".repeat(64)}`;
    fixture.tables.upload_intents.push(
      {
        _id: "intent-active", _creationTime: 1, workspace_id: "ws-1", status: "active",
        hash: "a".repeat(64), reserved_bytes: 12, expires_at: 200,
      },
      {
        _id: "intent-expired", _creationTime: 2, workspace_id: "ws-1", status: "active",
        hash: "b".repeat(64), reserved_bytes: 20, expires_at: 99,
      },
    );
    fixture.tables.file_meta.push(
      {
        _id: "file-a",
        workspace_id: "ws-1",
        hash,
        deleted: false,
        size_bytes: 10,
        updated_at: 100,
      },
      {
        _id: "file-b",
        workspace_id: "ws-1",
        hash: `sha256:${"b".repeat(64)}`,
        deleted: false,
        size_bytes: 20,
        updated_at: 101,
      },
    );
    const queryCanonicalStorage = (
      syncFunctions.queryCanonicalStorage as RegisteredFunction
    )._handler;
    const first = await queryCanonicalStorage(fixture.ctx, {
      workspace_id: "ws-1",
      kind: "live_metadata",
      page_size: 1,
      hash,
    });
    expect(first.items).toHaveLength(1);
    await expect(
      queryCanonicalStorage(fixture.ctx, {
        workspace_id: "ws-1",
        kind: "live_metadata",
        page_size: 501,
      }),
    ).rejects.toThrow("between 1 and 500");
    await expect(
      queryCanonicalStorage(fixture.ctx, {
        workspace_id: "ws-1",
        kind: "reference_edges",
        page_size: 1,
        cursor: first.nextCursor,
      }),
    ).rejects.toThrow("Invalid canonical storage cursor");
    await expect(
      queryCanonicalStorage(fixture.ctx, {
        workspace_id: "ws-1",
        kind: "active_reservations",
        page_size: 10,
        now: 100,
      }),
    ).resolves.toEqual({
      items: [{
        kind: "reservation",
        reservationId: "intent-active",
        hash: "a".repeat(64),
        sizeBytes: 12,
        expiresAt: 200,
      }],
      hasMore: false,
    });
  });

  it("prevents stale missing-row resurrection and accepts a newer full revision", async () => {
    const fixture = createFixture("editor");
    fixture.tables.tombstones.push({
      _id: "tombstone-dead",
      workspace_id: "ws-1",
      table_name: "messages",
      pk: "message-dead",
      deleted_at: 1005,
      server_deleted_at: 1005,
      clock: 5,
      hlc: "5:0:dev",
      op_id: "op-delete",
      server_version: 5,
      created_at: 1005,
    });
    const push = (syncFunctions.push as RegisteredFunction)._handler;

    const stale = await push(fixture.ctx, {
      workspace_id: "ws-1",
      ops: [
        {
          op_id: "op-before-delete",
          table_name: "messages",
          operation: "put",
          pk: "message-dead",
          payload: {
            thread_id: "thread-a",
            role: "user",
            index: 1,
            deleted: false,
          },
          clock: 5,
          hlc: "5:0:dev",
          device_id: "device-a",
        },
      ],
    });
    expect(stale.results[0]).toMatchObject({ success: true, applied: false });
    expect(
      fixture.tables.messages.some((row) => row.id === "message-dead"),
    ).toBe(false);

    const newer = await push(fixture.ctx, {
      workspace_id: "ws-1",
      ops: [
        {
          op_id: "op-z-newer",
          table_name: "messages",
          operation: "put",
          pk: "message-dead",
          payload: {
            thread_id: "thread-a",
            role: "user",
            index: 1,
            deleted: false,
          },
          clock: 5,
          hlc: "5:0:dev",
          device_id: "device-a",
        },
      ],
    });
    expect(newer.results[0]).toMatchObject({ success: true, applied: true });
    expect(
      fixture.tables.messages.find((row) => row.id === "message-dead"),
    ).toMatchObject({
      clock: 5,
      hlc: "5:0:dev",
      op_id: "op-z-newer",
    });
  });

  it("does not import client ref_count authority into Convex materialized state", async () => {
    const fixture = createFixture("editor");
    const push = (syncFunctions.push as RegisteredFunction)._handler;
    const hash = `sha256:${"a".repeat(64)}`;

    await push(fixture.ctx, {
      workspace_id: "ws-1",
      ops: [{
        op_id: "op-file-create",
        table_name: "file_meta",
        operation: "put",
        pk: hash,
        payload: {
          hash,
          name: "file.png",
          mime_type: "image/png",
          kind: "image",
          size_bytes: 10,
          ref_count: 999_999,
          deleted: false,
        },
        clock: 1,
        hlc: "1:0:device-a",
        device_id: "device-a",
      }],
    });
    expect(fixture.tables.file_meta.find((row) => row.hash === hash)).toMatchObject({
      ref_count: 0,
    });

    await push(fixture.ctx, {
      workspace_id: "ws-1",
      ops: [{
        op_id: "op-file-update",
        table_name: "file_meta",
        operation: "put",
        pk: hash,
        payload: { hash, name: "renamed.png", ref_count: -123 },
        clock: 2,
        hlc: "2:0:device-a",
        device_id: "device-a",
      }],
    });
    expect(fixture.tables.file_meta.find((row) => row.hash === hash)).toMatchObject({
      name: "renamed.png",
      ref_count: 0,
    });
  });

  it("rejects an older delete against newer materialized live state", async () => {
    const fixture = createFixture("editor");
    const push = (syncFunctions.push as RegisteredFunction)._handler;
    const before = { ...fixture.tables.threads.find((row) => row.id === "thread-b")! };

    const result = await push(fixture.ctx, {
      workspace_id: "ws-1",
      ops: [
        {
          op_id: "op-stale-delete",
          table_name: "threads",
          operation: "delete",
          pk: "thread-b",
          payload: { id: "thread-b", deleted: true, deleted_at: 900 },
          clock: 4,
          hlc: "4:9:device-z",
          device_id: "device-z",
        },
      ],
    });

    expect(result.results[0]).toMatchObject({
      success: true,
      applied: false,
      wasExisting: true,
    });
    expect(fixture.tables.threads.find((row) => row.id === "thread-b")).toEqual(before);
  });

  it("deduplicates identical same-batch operation IDs before allocating a version", async () => {
    const fixture = createFixture("editor");
    const push = (syncFunctions.push as RegisteredFunction)._handler;
    const op = {
      op_id: "op-identical",
      table_name: "threads",
      operation: "put",
      pk: "thread-identical",
      payload: { id: "thread-identical", title: "one write" },
      clock: 6,
      hlc: "6:0:device-a",
      device_id: "device-a",
    };

    const result = await push(fixture.ctx, {
      workspace_id: "ws-1",
      ops: [
        op,
        { ...op, payload: { title: "one write", id: "thread-identical" } },
      ],
    });

    expect(result.results).toHaveLength(2);
    expect(result.results[0]).toMatchObject({
      success: true,
      serverVersion: 6,
    });
    expect(result.results[1]).toMatchObject({
      success: true,
      serverVersion: 6,
    });
    expect(fixture.tables.server_version_counter[0]!.value).toBe(6);
    expect(
      fixture.tables.change_log.filter((row) => row.op_id === "op-identical"),
    ).toHaveLength(1);
  });

  it("rejects conflicting same-batch operation IDs without consuming a version", async () => {
    const fixture = createFixture("editor");
    const push = (syncFunctions.push as RegisteredFunction)._handler;

    const result = await push(fixture.ctx, {
      workspace_id: "ws-1",
      ops: [
        {
          op_id: "op-conflict",
          table_name: "threads",
          operation: "put",
          pk: "thread-a",
          payload: { id: "thread-a", title: "first" },
          clock: 6,
          hlc: "6:0:device-a",
          device_id: "device-a",
        },
        {
          op_id: "op-conflict",
          table_name: "threads",
          operation: "delete",
          pk: "thread-a",
          clock: 7,
          hlc: "7:0:device-a",
          device_id: "device-a",
        },
      ],
    });

    expect(result.results).toHaveLength(2);
    expect(
      result.results.every(
        (entry: any) => !entry.success && entry.errorCode === "CONFLICT",
      ),
    ).toBe(true);
    expect(fixture.tables.server_version_counter[0]!.value).toBe(5);
    expect(fixture.tables.change_log).toHaveLength(0);
  });

  it("isolates malformed operations and rejects logical key mutation while applying valid siblings", async () => {
    const fixture = createFixture("editor");
    const push = (syncFunctions.push as RegisteredFunction)._handler;

    const result = await push(fixture.ctx, {
      workspace_id: "ws-1",
      ops: [
        {
          op_id: "op-invalid-table",
          table_name: "not_a_table",
          operation: "put",
          pk: "bad",
          payload: { id: "bad" },
          clock: 6,
          hlc: "6:0:device-a",
          device_id: "device-a",
        },
        {
          op_id: "op-key-mutation",
          table_name: "threads",
          operation: "put",
          pk: "thread-key",
          payload: { id: "different-key", title: "bad" },
          clock: 6,
          hlc: "6:0:device-a",
          device_id: "device-a",
        },
        {
          op_id: "op-workspace-mutation",
          table_name: "threads",
          operation: "put",
          pk: "thread-workspace",
          payload: {
            id: "thread-workspace",
            workspace_id: "ws-other",
            title: "bad",
          },
          clock: 6,
          hlc: "6:0:device-a",
          device_id: "device-a",
        },
        {
          op_id: "op-valid-sibling",
          table_name: "threads",
          operation: "put",
          pk: "thread-valid",
          payload: { id: "thread-valid", title: "valid" },
          clock: 6,
          hlc: "6:0:device-a",
          device_id: "device-a",
        },
      ],
    });

    expect(result.results[0]).toMatchObject({
      success: false,
      errorCode: "VALIDATION_ERROR",
    });
    expect(result.results[1]).toMatchObject({
      success: false,
      errorCode: "VALIDATION_ERROR",
    });
    expect(result.results[1].error).toContain("'id' must match operation pk");
    expect(result.results[2]).toMatchObject({
      success: false,
      errorCode: "VALIDATION_ERROR",
    });
    expect(result.results[2].error).toContain("'workspace_id' is immutable");
    expect(result.results[3]).toMatchObject({
      success: true,
      serverVersion: 6,
      applied: true,
    });
    expect(fixture.tables.server_version_counter[0]!.value).toBe(6);
    expect(
      fixture.tables.threads.find((row) => row.id === "thread-valid"),
    ).toMatchObject({
      title: "valid",
    });
    expect(
      fixture.tables.threads.some((row) => row.id === "different-key"),
    ).toBe(false);
    expect(fixture.tables.change_log).toHaveLength(1);
  });

  it("repairs uniquely provable legacy tombstones idempotently and surfaces ambiguity", async () => {
    const fixture = createFixture();
    fixture.tables.tombstones.push(
      {
        _id: "legacy-unique",
        workspace_id: "ws-1",
        table_name: "messages",
        pk: "legacy-a",
        deleted_at: 1010,
        clock: 10,
        server_version: 10,
        created_at: 1010,
      },
      {
        _id: "legacy-ambiguous",
        workspace_id: "ws-1",
        table_name: "messages",
        pk: "legacy-b",
        deleted_at: 1011,
        clock: 11,
        server_version: 11,
        created_at: 1011,
      },
    );
    fixture.tables.change_log.push(
      {
        _id: "log-10",
        workspace_id: "ws-1",
        server_version: 10,
        table_name: "messages",
        pk: "legacy-a",
        op: "delete",
        clock: 10,
        hlc: "10:0:d",
        op_id: "delete-10",
        created_at: 2010,
      },
      {
        _id: "log-11a",
        workspace_id: "ws-1",
        server_version: 11,
        table_name: "messages",
        pk: "legacy-b",
        op: "delete",
        clock: 11,
        hlc: "11:0:a",
        op_id: "delete-11a",
        created_at: 2011,
      },
      {
        _id: "log-11b",
        workspace_id: "ws-1",
        server_version: 11,
        table_name: "messages",
        pk: "legacy-b",
        op: "delete",
        clock: 11,
        hlc: "11:0:b",
        op_id: "delete-11b",
        created_at: 2011,
      },
    );
    const repair = (syncFunctions.repairLegacyTombstones as RegisteredFunction)
      ._handler;

    const first = await repair(fixture.ctx, {
      workspace_id: "ws-1",
      limit: 100,
    });
    expect(first.repaired).toBe(1);
    expect(first.ambiguous).toContain("messages:legacy-b");
    expect(
      fixture.tables.tombstones.find((row) => row._id === "legacy-unique"),
    ).toMatchObject({
      hlc: "10:0:d",
      op_id: "delete-10",
      server_deleted_at: 2010,
    });
    expect(
      fixture.tables.tombstones.find((row) => row._id === "legacy-ambiguous")
        ?.hlc,
    ).toBeUndefined();

    const second = await repair(fixture.ctx, {
      workspace_id: "ws-1",
      limit: 100,
    });
    expect(second.repaired).toBe(0);
    expect(second.ambiguous).toContain("messages:legacy-b");
  });

  it("matches the shared SQLite logical fixture across frozen bounded pages", async () => {
    const fixture = createFixture();
    const snapshot = (syncFunctions.snapshot as RegisteredFunction)._handler;

    const first = await snapshot(fixture.ctx, {
      workspace_id: "ws-1",
      page_size: 2,
    });

    expect(first.highWatermark).toBe(5);
    expect(first.items).toHaveLength(2);
    expect(first.nextPageToken).toEqual(expect.any(String));

    // These writes occur after page one. The updated row receives a bounded
    // pre-image, while a newly-created key must not enter the frozen session.
    Object.assign(fixture.tables.threads[0]!, {
      title: "after",
      clock: 6,
      hlc: "6:0:dev",
      op_id: "op-6",
      server_version: 6,
    });
    fixture.tables.sync_record_versions.push({
      _id: "history-thread-a",
      workspace_id: "ws-1",
      table_name: "threads",
      pk: "thread-a",
      server_version: 4,
      kind: "row",
      payload: {
        id: "thread-a",
        title: "before",
        deleted: false,
        clock: 4,
        hlc: "4:0:dev",
      },
      clock: 4,
      hlc: "4:0:dev",
      op_id: "op-4",
    });
    fixture.tables.notifications.push({
      _id: "notification-doc-1",
      workspace_id: "ws-1",
      id: "notification-new",
      deleted: false,
      clock: 7,
      hlc: "7:0:dev",
      op_id: "op-7",
      server_version: 7,
    });
    fixture.tables.server_version_counter[0]!.value = 7;

    const second = await snapshot(fixture.ctx, {
      workspace_id: "ws-1",
      page_size: 2,
      page_token: first.nextPageToken,
    });

    const allItems = [...first.items, ...second.items];
    expect(second).toMatchObject({
      workspaceId: "ws-1",
      snapshotId: first.snapshotId,
      highWatermark: 5,
      nextPageToken: null,
    });
    expect(allItems).toEqual([
      {
        kind: "row",
        tableName: "messages",
        pk: "message-1",
        payload: expect.objectContaining({
          id: "message-1",
          data: { text: "hello" },
        }),
        revision: { clock: 1, hlc: "1:0:dev", opId: "op-1" },
      },
      {
        kind: "tombstone",
        tableName: "projects",
        pk: "project-1",
        revision: { clock: 3, hlc: "3:0:dev", opId: "op-3" },
        serverDeletedAt: 1003,
      },
      {
        kind: "row",
        tableName: "threads",
        pk: "thread-a",
        payload: {
          id: "thread-a",
          title: "before",
          deleted: false,
          clock: 4,
          hlc: "4:0:dev",
        },
        revision: { clock: 4, hlc: "4:0:dev", opId: "op-4" },
      },
      {
        kind: "row",
        tableName: "threads",
        pk: "thread-b",
        payload: expect.objectContaining({ id: "thread-b", title: "stable" }),
        revision: { clock: 5, hlc: "5:0:dev", opId: "op-5" },
      },
    ]);
    expect(
      new Set(allItems.map((item: any) => `${item.tableName}:${item.pk}`)).size,
    ).toBe(allItems.length);
    expect(fixture.takeCounts.every((count) => count <= 3)).toBe(true);
  });

  it("binds opaque continuation tokens to the original table filter", async () => {
    const fixture = createFixture();
    const snapshot = (syncFunctions.snapshot as RegisteredFunction)._handler;
    const first = await snapshot(fixture.ctx, {
      workspace_id: "ws-1",
      page_size: 1,
      tables: ["threads"],
    });

    await expect(
      snapshot(fixture.ctx, {
        workspace_id: "ws-1",
        page_size: 1,
        page_token: first.nextPageToken,
        tables: ["messages"],
      }),
    ).rejects.toThrow("does not match");
  });

  it("binds continuation tokens to the authenticated user", async () => {
    const fixture = createFixture();
    const snapshot = (syncFunctions.snapshot as RegisteredFunction)._handler;
    const first = await snapshot(fixture.ctx, {
      workspace_id: "ws-1",
      page_size: 1,
    });
    fixture.tables.auth_accounts.push({
      _id: "account-2",
      provider: "clerk",
      provider_user_id: "subject-2",
      user_id: "user-2",
    });
    fixture.tables.workspace_members.push({
      _id: "member-2",
      workspace_id: "ws-1",
      user_id: "user-2",
      role: "viewer",
    });
    fixture.ctx.auth.getUserIdentity = async () => ({
      subject: "subject-2",
      issuer: "https://clerk.example.test",
    });

    await expect(
      snapshot(fixture.ctx, {
        workspace_id: "ws-1",
        page_size: 1,
        page_token: first.nextPageToken,
      }),
    ).rejects.toThrow("unavailable");
  });

  it("ships the snapshot mutation, schema, helper, and ignored host mirror", () => {
    const packPath = new URL(
      "../../../templates/convex.pack.json.gz",
      import.meta.url,
    );
    const packed = JSON.parse(
      gunzipSync(readFileSync(packPath)).toString("utf8"),
    ) as { files: Record<string, string> };
    expect(packed.files["sync.ts"]).toContain(
      "export const snapshot = mutation",
    );
    expect(packed.files["sync.ts"]).toContain(
      "export const queryCanonicalStorage = query",
    );
    expect(packed.files["sync.ts"]).toContain(
      "Conflicting operations reuse op_id",
    );
    expect(packed.files["sync.ts"]).toContain(
      "must match operation pk",
    );
    expect(packed.files["sync.ts"]).toContain(
      "const shouldApplyDelete = incomingWinsStoredRevision",
    );
    expect(packed.files["schema.ts"]).toContain(
      "sync_snapshot_sessions: defineTable",
    );
    expect(packed.files["snapshot.ts"]).toContain("resolveSnapshotWinner");

    const mirrorSync = readFileSync(
      new URL("../../../../or3-chat/convex/sync.ts", import.meta.url),
      "utf8",
    );
    const mirrorSchema = readFileSync(
      new URL("../../../../or3-chat/convex/schema.ts", import.meta.url),
      "utf8",
    );
    expect(mirrorSync).toContain("export const snapshot = mutation");
    expect(mirrorSync).toContain("export const queryCanonicalStorage = query");
    expect(mirrorSync).toContain("Conflicting operations reuse op_id");
    expect(mirrorSync).toContain("must match operation pk");
    expect(mirrorSync).toContain(
      "const shouldApplyDelete = incomingWinsStoredRevision",
    );
    expect(mirrorSchema).toContain("sync_record_versions: defineTable");
  });
});
