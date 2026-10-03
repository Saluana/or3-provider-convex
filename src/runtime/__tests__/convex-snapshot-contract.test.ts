import { afterEach, describe, expect, it, vi } from "vitest";
import { getFunctionName } from "convex/server";
import { readFileSync } from "node:fs";
import { gunzipSync } from "node:zlib";
import { verifySyncContract } from "~~/shared/testing/contracts/sync";
import { compareSyncRevision } from "~~/shared/sync/revision";
import { ChangeStampSchema } from "~~/shared/sync/schemas";
import { readRequestUsage } from "~~/shared/chat/compaction";
import templateSchema from "../../../templates/convex/schema";
import { convexJobProvider } from "../server/background-jobs/convex-provider";
import { ConvexSyncGatewayAdapter } from "../server/sync/convex-sync-gateway-adapter";
import { reconcileBackgroundJobHistory } from "~~/server/utils/background-jobs/history";
import { registerSyncGatewayAdapter, getActiveSyncGatewayAdapter } from "~~/server/sync/gateway/registry";
import { registerAuthWorkspaceStore } from "~~/server/auth/store/registry";
import type { AuthWorkspaceStore } from "~~/server/auth/store/types";
import { canonicalHistoryContext } from "~~/server/utils/chat/canonical-history-context";
import { createHistoryRetrievalService } from "~~/shared/chat/history-retrieval";

const localTransport = vi.hoisted(() => ({
  query: vi.fn(), mutation: vi.fn(), adapter: undefined as any,
}));
vi.mock("../server/utils/convex-client", () => ({ getConvexClient: () => localTransport }));
vi.mock("~~/server/utils/background-jobs/store", () => ({
  getJobConfig: () => ({ maxConcurrentJobs: 20, maxConcurrentJobsPerUser: 5 }),
}));
vi.mock("~~/server/auth/token-broker/resolve", () => ({ resolveProviderToken: async () => "fixture-token" }));
vi.mock("~~/server/auth/session", () => ({ resolveSessionContext: async () => ({ authenticated: false }) }));
vi.mock("~~/server/utils/webhooks/runtime", () => ({ emitWebhookSystemHook: async () => undefined }));
vi.mock("../server/utils/convex-gateway", () => ({
  getConvexGatewayClient: () => localTransport, getConvexAdminGatewayClient: () => localTransport,
  buildGatewayAdminIdentity: () => ({}),
}));

const syncFunctions = await import(
  /* @vite-ignore */ new URL(
    "../../../templates/convex/sync.ts",
    import.meta.url,
  ).href
);
const notificationFunctions = await import(
  /* @vite-ignore */ new URL(
    "../../../templates/convex/notifications.ts",
    import.meta.url,
  ).href
);
const backgroundJobFunctions = await import(
  /* @vite-ignore */ new URL("../../../templates/convex/backgroundJobs.ts", import.meta.url).href
);

type RegisteredFunction = {
  _handler: (ctx: any, args: any) => Promise<any>;
  exportArgs: () => string;
  isInternal?: boolean;
};

type Row = Record<string, any> & { _id: string };
type FixtureTables = Record<string, Row[]> & Record<
  "auth_accounts" | "workspaces" | "workspace_members" | "server_version_counter" |
  "device_cursors" | "messages" | "projects" | "threads" | "tombstones" |
  "upload_intents" | "sync_record_versions" | "sync_snapshot_sessions" |
  "change_log" | "file_meta" | "kv" | "notifications" | "posts", Row[]>;

afterEach(() => vi.unstubAllGlobals());

function uuidOp(label: string): string {
  let hex = "";
  for (const ch of label) hex += ch.charCodeAt(0).toString(16).padStart(2, "0");
  hex = hex.padEnd(32, "0").slice(0, 32);
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-4${hex.slice(13, 16)}-a${hex.slice(17, 20)}-${hex.slice(20, 32)}`;
}

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
  by_history_order: ["workspace_id", "thread_id", "index", "order_key", "id"],
  by_workspace_thread: ["workspace_id", "thread_id"],
  by_op_id: ["op_id"],
};

class MemoryQuery {
  private conditions: Array<{
    op: "eq" | "gt" | "lt" | "lte";
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
      lt: (field: string, value: unknown) => {
        this.conditions.push({ op: "lt", field, value });
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

  async collect(): Promise<Row[]> {
    return this.materialize();
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
        if (op === "lt") return row[field] < (value as any);
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
  const tables: FixtureTables = {
    auth_accounts: [
      {
        _id: "account-1",
        provider: "clerk",
        provider_user_id: "subject-1",
        user_id: "user-1",
      },
    ],
    workspaces: [
      {
        _id: "ws-1",
        name: "Workspace",
        owner_user_id: "user-1",
        created_at: 1,
        deleted: false,
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

// Failure modes: transport allowlists, template storage/projections, and finalizer
// field picking can each lose usage. Recovery must discard an uncheckpointed
// measurement; invalid optional metadata cannot fail text or weaken ownership.
// This executes production handlers with the existing in-memory DB fixture; it
// does not emulate Convex's deployed transaction/validator execution engine.
function measuredUsage(prompt_tokens = 400, iteration = 2) {
  return {
    prompt_tokens, completion_tokens: 12, model: "test-model",
    request_id: `request-${iteration}`, iteration, measured_at: 123,
    prefix_message_count: 4, prefix_hash: `prefix-${iteration}`,
    configuration_hash: "configuration", input_estimate_tokens: prompt_tokens - 10,
  };
}

async function createUsageJob(fixture: ReturnType<typeof createFixture>) {
  const created = await (backgroundJobFunctions.create as RegisteredFunction)._handler(fixture.ctx, {
    user_id: "user-1", thread_id: "thread-a", message_id: "message-1", model: "test-model",
    generation_id: "generation-1", sync_provider_id: "convex", history_phase: "ready",
    execution: { version: 1, workspaceId: "ws-1", body: {}, contentBase: "base", reasoningBase: "base reason" },
    max_concurrent_jobs: 20, max_concurrent_jobs_per_user: 5,
  });
  return created.jobId as string;
}

function finalizationInput(usage: unknown = measuredUsage()) {
  return {
    workspace_id: "ws-1", actor_user_id: "user-1", generation_id: "generation-1",
    message_id: "message-1", admission_clock: 1, fingerprint: "terminal-fingerprint",
    device_id: "background:generation-1", op_id: uuidOp("finalize-usage"),
    snapshot: { status: "complete", content: "final text", reasoning: "final reasoning", usage,
      toolCalls: [{ name: "lookup", status: "complete" }], completedAt: 2000 },
  };
}

describe("Convex background request usage persistence", () => {
  it.each(["complete", "error", "aborted"] as const)("reconciles %s provider usage through the immutable host and actual gateway/finalizer/second-client pull", async (status) => {
    const fixture = createFixture("editor");
    fixture.tables.messages[0]!.data = { generation_id: "generation-1", custom: "keep", compaction: { marker: "keep" } };
    const route = async (reference: any, args: any) => {
      const [namespace, name] = getFunctionName(reference).split(":");
      const module = namespace === "backgroundJobs" ? backgroundJobFunctions : syncFunctions;
      // Model the JSON transport boundary, preserving no shared object identity.
      const result = await (module[name!] as RegisteredFunction)._handler(fixture.ctx, JSON.parse(JSON.stringify(args)));
      return result === undefined ? undefined : JSON.parse(JSON.stringify(result));
    };
    localTransport.query.mockImplementation(route);
    localTransport.mutation.mockImplementation(route);
    const adapter = new ConvexSyncGatewayAdapter();
    localTransport.adapter = adapter;
    registerSyncGatewayAdapter({ id: 'convex', create: () => adapter });
    const admission = {
      version: 1 as const, kind: "new-turn" as const, admissionId: "admission-1", generationId: "generation-1",
      workspaceId: "ws-1", threadId: "thread-a", messageId: "message-1",
      thread: { id: "thread-a", clock: 1 },
      assistantMessage: { id: "message-1", thread_id: "thread-a", role: "assistant", clock: 1,
        data: { generation_id: "generation-1" } },
      userMessage: { id: "user-message", thread_id: "thread-a", role: "user", clock: 1 },
    };
    const jobId = await convexJobProvider.createJob({
      userId: "user-1", threadId: "thread-a", messageId: "message-1", model: "test-model",
      generationId: "generation-1", syncProviderId: "convex", historyPhase: "ready",
      execution: { version: 1, body: {}, workspaceId: "ws-1", referer: "", apiKeyCiphertext: "fixture-only", history: admission },
    });
    for (const usage of [measuredUsage(150, 1), measuredUsage(), measuredUsage()]) {
      await convexJobProvider.updateJob(jobId, { usage });
    }
    expect((await convexJobProvider.getJob(jobId, "user-1"))?.usage).toEqual(measuredUsage());
    await convexJobProvider.saveTerminalSnapshot!(jobId, {
      status, content: "final text", reasoning: "reason", usage: measuredUsage(), completedAt: 2000,
    });
    const pending = (await convexJobProvider.getPendingHistoryJobs!(10))[0]!;
    expect(await reconcileBackgroundJobHistory(convexJobProvider, pending)).toBe("committed");
    const job = (await convexJobProvider.getJob(jobId, "user-1"))!;
    expect(job).toMatchObject({ usage: measuredUsage(), historyPhase: "committed" });
    expect(await reconcileBackgroundJobHistory(convexJobProvider, job)).toBe("unchanged");
    fixture.tables.auth_accounts.push({ _id: "account-2", provider: "clerk", provider_user_id: "subject-2", user_id: "user-2" });
    fixture.tables.workspace_members.push({ _id: "member-2", workspace_id: "ws-1", user_id: "user-2", role: "viewer" });
    fixture.ctx.auth.getUserIdentity = async () => ({ subject: "subject-2", issuer: "https://clerk.example.test" });
    const pulled = await adapter.pull({ context: {}, node: { req: { headers: {} } } } as any,
      { scope: { workspaceId: "ws-1" }, cursor: 5, limit: 10 });
    expect(pulled.changes[0]?.payload).toMatchObject({ data: {
      usage: measuredUsage(), content: "final text", custom: "keep", compaction: { marker: "keep" }, generation_id: "generation-1",
    } });
    expect(fixture.tables.change_log).toHaveLength(1);
    expect(await convexJobProvider.getPendingHistoryJobs!(10)).toEqual([]);
    fixture.tables.workspace_members = fixture.tables.workspace_members.filter((row) => row.user_id !== "user-2");
    await expect(adapter.pull({ context: {}, node: { req: { headers: {} } } } as any,
      { scope: { workspaceId: "ws-1" }, cursor: 5, limit: 10 })).rejects.toThrow("Forbidden");
  });

  it("registers optional measurement arguments and a legacy-compatible structured stored field", () => {
    for (const name of ["update", "saveTerminalSnapshot"]) {
      const fn = backgroundJobFunctions[name] as RegisteredFunction;
      expect(fn.isInternal).toBe(true);
      // The input is deliberately permissive so malformed usage can be ignored.
      expect(JSON.parse(fn.exportArgs()).value.usage).toEqual({ fieldType: { type: "any" }, optional: true });
    }
    const field = (templateSchema.tables.background_jobs as any).export().documentType.value.usage;
    expect(field?.optional).toBe(true);
    expect(field?.fieldType.type).toBe("object");
    expect(Object.keys(field?.fieldType.value ?? {}).sort()).toEqual(Object.keys(measuredUsage()).sort());
    for (const [key, value] of Object.entries(measuredUsage())) {
      expect(field.fieldType.value[key]).toEqual({ fieldType: { type: typeof value === "number" ? "number" : "string" }, optional: false });
    }
  });

  it("matches the host normalizer for valid, missing, malformed and extra-field measurements", async () => {
    const helper = await import(/* @vite-ignore */ new URL("../../../templates/convex/requestUsage.ts", import.meta.url).href);
    for (const value of [undefined, null, {}, [], measuredUsage(), { ...measuredUsage(), extra: "discard" },
      ...["prompt_tokens", "completion_tokens", "iteration", "measured_at", "prefix_message_count", "input_estimate_tokens"]
        .flatMap((key) => [-1, NaN, Infinity, 1.5, Number.MAX_SAFE_INTEGER + 1, "4", undefined]
          .map((invalid) => ({ ...measuredUsage(), [key]: invalid }))),
      ...["model", "request_id", "prefix_hash", "configuration_hash"]
        .flatMap((key) => ["", 4, undefined].map((invalid) => ({ ...measuredUsage(), [key]: invalid }))),
      { ...measuredUsage(), prompt_tokens: 0, completion_tokens: 0 },
    ]) expect(helper.readRequestUsage(value)).toEqual(readRequestUsage(value));
  });

  it.each(["complete", "error", "aborted"])("retains request 400 rather than 150 + 400 through %s and reconnect reads", async (status) => {
    const fixture = createFixture("editor");
    const jobId = await createUsageJob(fixture);
    for (const usage of [measuredUsage(150, 1), measuredUsage(), measuredUsage()]) {
      await (backgroundJobFunctions.update as RegisteredFunction)._handler(fixture.ctx, { job_id: jobId, usage });
    }
    expect(fixture.tables.background_jobs![0]!.usage).toEqual(measuredUsage());
    await (backgroundJobFunctions.saveTerminalSnapshot as RegisteredFunction)._handler(fixture.ctx, {
      job_id: jobId, status, content: "final", reasoning: "reason", usage: measuredUsage(), completed_at: 2000,
      error: status === "error" ? "upstream failure" : undefined,
    });
    // Serialize the fixture's durable rows before a fresh production read.
    fixture.tables.background_jobs = JSON.parse(JSON.stringify(fixture.tables.background_jobs));
    const job = await (backgroundJobFunctions.get as RegisteredFunction)._handler(fixture.ctx, { job_id: jobId, user_id: "user-1" });
    expect(job).toMatchObject({ status, usage: measuredUsage(), reasoning: "reason" });
    expect(job.generation_id ?? job.generationId).toBe("generation-1");
    const pending = await (backgroundJobFunctions.listPendingHistory as RegisteredFunction)._handler(fixture.ctx, { limit: 10 });
    expect(pending[0].usage).toEqual(measuredUsage());
    expect(await (backgroundJobFunctions.get as RegisteredFunction)._handler(fixture.ctx, { job_id: jobId, user_id: "other-user" })).toBeNull();
    expect(await (backgroundJobFunctions.update as RegisteredFunction)._handler(fixture.ctx, { job_id: jobId, usage: measuredUsage(150, 1) })).toBe(false);
    expect(fixture.tables.background_jobs![0]!.usage).toEqual(measuredUsage());
  });

  it("ignores missing/malformed usage, preserves existing measurements, and never manufactures zero", async () => {
    const fixture = createFixture("editor");
    const jobId = await createUsageJob(fixture);
    for (const usage of [undefined, { prompt_tokens: 0 }, { ...measuredUsage(), prompt_tokens: -1 }]) {
      expect(await (backgroundJobFunctions.update as RegisteredFunction)._handler(fixture.ctx, { job_id: jobId, usage, content_chunk: "ok" })).toBe(true);
    }
    expect(fixture.tables.background_jobs![0]!.usage).toBeUndefined();
    await (backgroundJobFunctions.update as RegisteredFunction)._handler(fixture.ctx, { job_id: jobId, usage: measuredUsage() });
    await (backgroundJobFunctions.saveTerminalSnapshot as RegisteredFunction)._handler(fixture.ctx, {
      job_id: jobId, status: "complete", content: "valid", reasoning: "", usage: { prompt_tokens: -1 }, completed_at: 2000,
    });
    expect(fixture.tables.background_jobs![0]!).toMatchObject({ content: "valid", usage: measuredUsage() });
  });

  it.each(["claim", "claimNext"])("%s restores checkpoint usage and fences stale workers", async (claimName) => {
    const fixture = createFixture("editor");
    const jobId = await createUsageJob(fixture);
    const claim = (backgroundJobFunctions[claimName] as RegisteredFunction)._handler;
    const first = await claim(fixture.ctx, { job_id: jobId, lease_owner: "old", lease_ms: 60_000 });
    expect(first.attempts).toBe(1);
    const checkpoint = { ...fixture.tables.background_jobs![0]!.execution, normalizedToolState: { requestUsage: measuredUsage(150, 1) } };
    expect(await (backgroundJobFunctions.updateExecution as RegisteredFunction)._handler(fixture.ctx,
      { job_id: jobId, lease_owner: "old", execution: checkpoint })).toBe(true);
    await (backgroundJobFunctions.update as RegisteredFunction)._handler(fixture.ctx,
      { job_id: jobId, lease_owner: "old", content_chunk: "discard", usage: measuredUsage() });
    fixture.tables.background_jobs![0]!.lease_expires_at = 0;
    const recovered = await claim(fixture.ctx, { job_id: jobId, lease_owner: "new", lease_ms: 60_000 });
    expect(recovered).toMatchObject({ attempts: 2, content: "base", reasoning: "base reason", usage: measuredUsage(150, 1) });
    expect(fixture.tables.background_jobs![0]!.usage).toEqual(measuredUsage(150, 1));
    expect(await (backgroundJobFunctions.update as RegisteredFunction)._handler(fixture.ctx,
      { job_id: jobId, lease_owner: "old", usage: measuredUsage() })).toBe(false);
    expect(await (backgroundJobFunctions.saveTerminalSnapshot as RegisteredFunction)._handler(fixture.ctx,
      { job_id: jobId, lease_owner: "old", status: "complete", content: "stale", reasoning: "", usage: measuredUsage(), completed_at: 2000 })).toBe(false);
    // A later attempt without a measured checkpoint must remove obsolete usage.
    fixture.tables.background_jobs![0]!.execution.normalizedToolState = {};
    fixture.tables.background_jobs![0]!.lease_expires_at = 0;
    expect((await claim(fixture.ctx, { job_id: jobId, lease_owner: "third", lease_ms: 60_000 })).usage).toBeUndefined();
    expect(fixture.tables.background_jobs![0]!.usage).toBeUndefined();
  });

  it.each(["abort", "fail"])("%s preserves an already measured iteration", async (name) => {
    const fixture = createFixture("editor");
    const jobId = await createUsageJob(fixture);
    await (backgroundJobFunctions.update as RegisteredFunction)._handler(fixture.ctx, { job_id: jobId, usage: measuredUsage() });
    await (backgroundJobFunctions[name] as RegisteredFunction)._handler(fixture.ctx, { job_id: jobId, user_id: "user-1", error: "upstream" });
    expect((await (backgroundJobFunctions.get as RegisteredFunction)._handler(fixture.ctx, { job_id: jobId, user_id: "user-1" })).usage).toEqual(measuredUsage());
  });

  it.each(["update", "saveTerminalSnapshot", "updateExecution"])("%s rejects unowned, superseded and expired lease writes", async (name) => {
    const fixture = createFixture("editor");
    const jobId = await createUsageJob(fixture);
    await (backgroundJobFunctions.claim as RegisteredFunction)._handler(fixture.ctx,
      { job_id: jobId, lease_owner: "owner", lease_ms: 60_000 });
    const write = (backgroundJobFunctions[name] as RegisteredFunction)._handler;
    const input = { job_id: jobId, usage: measuredUsage(), content_chunk: "stale", execution: { changed: true },
      status: "complete", content: "stale", reasoning: "", completed_at: 2000 };
    for (const lease_owner of [undefined, "other"]) {
      const before = JSON.stringify(fixture.tables.background_jobs);
      expect(await write(fixture.ctx, { ...input, lease_owner })).toBe(false);
      expect(JSON.stringify(fixture.tables.background_jobs)).toBe(before);
    }
    fixture.tables.background_jobs![0]!.lease_expires_at = 0;
    const before = JSON.stringify(fixture.tables.background_jobs);
    expect(await write(fixture.ctx, { ...input, lease_owner: "owner" })).toBe(false);
    expect(JSON.stringify(fixture.tables.background_jobs)).toBe(before);
  });

  it.each(["update", "saveTerminalSnapshot", "complete", "fail"])("%s rejects an old worker after a client-tool result releases its lease", async (name) => {
    const fixture = createFixture("editor");
    const jobId = await createUsageJob(fixture);
    await (backgroundJobFunctions.claim as RegisteredFunction)._handler(fixture.ctx,
      { job_id: jobId, lease_owner: "old", lease_ms: 60_000 });
    const execution = fixture.tables.background_jobs![0]!.execution;
    await (backgroundJobFunctions.updateExecution as RegisteredFunction)._handler(fixture.ctx, {
      job_id: jobId, lease_owner: "old", execution: { ...execution, clientToolCall: { callId: "call-1" } },
    });
    await (backgroundJobFunctions.claimClientTool as RegisteredFunction)._handler(fixture.ctx, {
      job_id: jobId, user_id: "user-1", call_id: "call-1", claim_token: "claim", claim_expires_at: Date.now() + 60_000,
    });
    expect(await (backgroundJobFunctions.settleClientTool as RegisteredFunction)._handler(fixture.ctx, {
      job_id: jobId, user_id: "user-1", call_id: "call-1", claim_token: "claim", execution, tool_calls: [],
    })).toBe(true);
    expect(fixture.tables.background_jobs![0]!.lease_owner).toBeUndefined();
    const before = JSON.stringify(fixture.tables.background_jobs);
    expect(await (backgroundJobFunctions[name] as RegisteredFunction)._handler(fixture.ctx, {
      job_id: jobId, lease_owner: "old", content_chunk: "stale", usage: measuredUsage(),
      status: "complete", content: "stale", reasoning: "", completed_at: 2000, error: "stale",
    })).toBe(false);
    expect(JSON.stringify(fixture.tables.background_jobs)).toBe(before);
  });

  it("still permits legitimate unleased workflow progress and completion", async () => {
    const fixture = createFixture("editor");
    const { jobId } = await (backgroundJobFunctions.create as RegisteredFunction)._handler(fixture.ctx, {
      user_id: "user-1", thread_id: "thread-a", message_id: "workflow-1", model: "workflow", kind: "workflow",
      max_concurrent_jobs: 20, max_concurrent_jobs_per_user: 5,
    });
    expect(await (backgroundJobFunctions.update as RegisteredFunction)._handler(fixture.ctx,
      { job_id: jobId, content_chunk: "workflow output" })).toBe(true);
    expect(await (backgroundJobFunctions.complete as RegisteredFunction)._handler(fixture.ctx,
      { job_id: jobId, content: "workflow output" })).toBe(true);
    expect(fixture.tables.background_jobs![0]!).toMatchObject({ status: "complete", content: "workflow output" });
  });

  it("preserves usage in authorized client-tool claims without exposing it to another user", async () => {
    const fixture = createFixture("editor");
    const jobId = await createUsageJob(fixture);
    await (backgroundJobFunctions.update as RegisteredFunction)._handler(fixture.ctx, { job_id: jobId, usage: measuredUsage() });
    fixture.tables.background_jobs![0]!.execution.clientToolCall = { callId: "call-1" };
    const claim = (backgroundJobFunctions.claimClientTool as RegisteredFunction)._handler;
    const input = { job_id: jobId, call_id: "call-1", claim_token: "claim-1", claim_expires_at: Date.now() + 60_000 };
    expect(await claim(fixture.ctx, { ...input, user_id: "other" })).toBeNull();
    expect(await claim(fixture.ctx, { ...input, user_id: "user-1" })).toMatchObject({ usage: measuredUsage() });
  });

  it("keeps a never-measured terminal job and canonical message unmeasured", async () => {
    const fixture = createFixture("editor");
    const jobId = await createUsageJob(fixture);
    await (backgroundJobFunctions.saveTerminalSnapshot as RegisteredFunction)._handler(fixture.ctx, {
      job_id: jobId, status: "complete", content: "valid", reasoning: "", usage: { prompt_tokens: -1 }, completed_at: 2000,
    });
    expect((await (backgroundJobFunctions.get as RegisteredFunction)._handler(fixture.ctx, { job_id: jobId, user_id: "user-1" })).usage).toBeUndefined();
    fixture.tables.messages[0]!.data = { generation_id: "generation-1", custom: "keep" };
    const input = finalizationInput({ prompt_tokens: -1 });
    expect(await (syncFunctions.finalizeChatGeneration as RegisteredFunction)._handler(fixture.ctx, input)).toMatchObject({ status: "committed" });
    expect(fixture.tables.messages[0]!.data.usage).toBeUndefined();
    expect(fixture.tables.messages[0]!.data.content).toBe("final text");
  });

  it.each(["complete", "error", "aborted"])("finalizes %s usage into canonical data and authorized pull without losing metadata", async (status) => {
    const fixture = createFixture("editor");
    fixture.tables.messages[0]!.data = { generation_id: "generation-1", compaction: { marker: "keep" }, custom: "keep", usage: measuredUsage(150, 1) };
    const input = finalizationInput();
    input.snapshot.status = status;
    const finalize = (syncFunctions.finalizeChatGeneration as RegisteredFunction)._handler;
    expect(await finalize(fixture.ctx, input)).toMatchObject({ status: "committed", replayed: false });
    expect(fixture.tables.messages[0]!.data).toMatchObject({ usage: measuredUsage(), compaction: { marker: "keep" }, custom: "keep", generation_id: "generation-1", content: "final text" });
    const pulled = await (syncFunctions.pull as RegisteredFunction)._handler(fixture.ctx, { workspace_id: "ws-1", cursor: 5, limit: 10 });
    expect(pulled.changes[0].payload.data.usage).toEqual(measuredUsage());
    expect(await finalize(fixture.ctx, input)).toMatchObject({ status: "committed", replayed: true });
    expect(fixture.tables.change_log).toHaveLength(1);
    await expect(finalize(fixture.ctx, { ...input, fingerprint: "conflicting" })).rejects.toThrow("Conflicting");
  });

  it.each([undefined, { prompt_tokens: -1 }])("does not let absent or malformed terminal usage erase canonical metadata", async (usage) => {
    const fixture = createFixture("editor");
    fixture.tables.messages[0]!.data = { generation_id: "generation-1", usage: measuredUsage(150, 1), custom: "keep" };
    const input = finalizationInput(); input.snapshot.usage = usage;
    expect(await (syncFunctions.finalizeChatGeneration as RegisteredFunction)._handler(fixture.ctx, input)).toMatchObject({ status: "committed" });
    expect(fixture.tables.messages[0]!.data).toMatchObject({ usage: measuredUsage(150, 1), custom: "keep", content: "final text" });
  });

  it.each(["generation", "clock", "deleted", "workspace", "actor", "viewer"])("preserves the %s finalization fence", async (fence) => {
    const fixture = createFixture(fence === "viewer" ? "viewer" : "editor");
    fixture.tables.messages[0]!.data = { generation_id: fence === "generation" ? "newer" : "generation-1", usage: measuredUsage(150, 1) };
    if (fence === "clock") fixture.tables.messages[0]!.clock = 2;
    if (fence === "deleted") fixture.tables.messages[0]!.deleted = true;
    const input = finalizationInput();
    if (fence === "workspace") input.workspace_id = "other-workspace";
    if (fence === "actor") input.actor_user_id = "other-user";
    const result = (syncFunctions.finalizeChatGeneration as RegisteredFunction)._handler(fixture.ctx, input);
    if (["workspace", "actor", "viewer"].includes(fence)) await expect(result).rejects.toThrow("Forbidden");
    else expect(await result).toMatchObject({ status: "superseded" });
    expect(fixture.tables.messages[0]!.data.usage).toEqual(measuredUsage(150, 1));
    expect(fixture.tables.change_log).toHaveLength(0);
  });
});

describe("Convex canonical history scaffold contract", () => {
  it("uses the selected built gateway and host scope service across partial delivery, missing capability and membership revocation", async () => {
    const fixture = createFixture('editor');
    const read = (syncFunctions.readChatHistory as RegisteredFunction)._handler;
    localTransport.query.mockImplementation(async (reference, args) => {
      expect(getFunctionName(reference)).toBe('sync:readChatHistory');
      const result = await read(fixture.ctx, JSON.parse(JSON.stringify(args)));
      return JSON.parse(JSON.stringify(result));
    });
    const parent = { ...fixture.tables.threads[0], _id: 'root-doc', id: 'root', clock: 1 };
    const child = { ...fixture.tables.threads[0]!, parent_thread_id: 'root', root_thread_id: 'root',
      branch_mode: 'compacted', anchor_message_id: 'root-message', summary_message_id: 'history-summary', fork_reason: 'compaction' };
    fixture.tables.threads = [parent, child];
    const original = { ...fixture.tables.messages[0]!, id: 'root-message', thread_id: 'root', role: 'user', index: 0,
      pending: false, data: { content: 'Authorized original decision' } };
    const summary = { ...original, _id: 'summary-doc', id: 'history-summary', thread_id: 'thread-a', role: 'system',
      data: { kind: 'compaction', content: 'Saved usable summary', compaction: { version: 1, compaction_id: 'operation',
        source_thread_id: 'root', anchor_message_id: 'root-message', anchor_index: 0, generated_at: 1, model: 'test-model',
        message_count: 1, prior_message_count: 0, summary_markdown: 'Saved usable summary', landmarks: [],
        history_scope: { version: 1, segments: [{ thread_id: 'root', messages: [{ message_id: 'root-message', clock: 1 }] }] } } } };
    fixture.tables.messages = [original];
    fixture.tables.chat_history_revisions = [];
    const adapter = new ConvexSyncGatewayAdapter(); registerSyncGatewayAdapter({ id: 'convex', create: () => adapter });
    const configuration = vi.fn(() => ({ public: { sync: { provider: 'not-installed' } } }));
    vi.stubGlobal('useRuntimeConfig', configuration);
    expect(getActiveSyncGatewayAdapter()).toBeNull();
    configuration.mockReturnValue({ public: { sync: { provider: 'convex' } } });
    expect(getActiveSyncGatewayAdapter()).toBe(adapter);
    registerAuthWorkspaceStore({ id: 'convex', create: () => ({ listUserWorkspaces: async (subject: string) =>
      fixture.tables.workspace_members.filter((row) => row.user_id === subject).map((row) => ({ id: row.workspace_id, name: 'Workspace', role: row.role })) }) as unknown as AuthWorkspaceStore });
    const context = () => canonicalHistoryContext({ subject: 'user-1', workspaceId: 'ws-1', threadId: 'thread-a',
      syncProviderId: 'convex', signal: new AbortController().signal });
    const service = createHistoryRetrievalService();
    expect(await service.inspect(context())).toMatchObject({ status: 'scope_incomplete' });
    fixture.tables.messages.push(summary);
    expect(await service.inspect(context())).toEqual({ status: 'ok' });
    expect(await service.getMessage(context(), { message_id: 'root-message' })).toMatchObject({ status: 'ok',
      message: { text: 'Authorized original decision', reference_only: true, index: 0 } });
    expect(await service.getMessage(context(), { message_id: 'sibling-private' })).toMatchObject({ status: 'out_of_scope' });
    const reads = localTransport.query.mock.calls.length;
    expect(() => canonicalHistoryContext({ ...context(), syncProviderId: 'not-installed' })).toThrow('unavailable');
    registerSyncGatewayAdapter({ id: 'legacy-host', create: () => ({ capabilities: {} }) as any });
    expect(() => canonicalHistoryContext({ ...context(), syncProviderId: 'legacy-host' })).toThrow('unavailable');
    expect(localTransport.query.mock.calls.length).toBe(reads);
    fixture.tables.workspace_members = [];
    expect(await service.getMessage(context(), { message_id: 'root-message' })).toMatchObject({ status: 'scope_incomplete' });
    expect(localTransport.query.mock.calls.length).toBe(reads);
    fixture.tables.workspace_members = [{ _id: 'member', workspace_id: 'ws-1', user_id: 'user-1', role: 'editor' }];
    fixture.tables.messages = [original];
    expect(await service.getMessage(context(), { message_id: 'root-message' })).toMatchObject({ status: 'scope_incomplete' });
  });
  // This owner executes generated handler policy with the existing storage
  // fixture. Deployed validators/transaction isolation remain separate gates.
  it("uses current membership and workspace-bound keysets without retained logs", async () => {
    const fixture = createFixture();
    const read = (syncFunctions.readChatHistory as RegisteredFunction)._handler;
    const { encodeChatSeek } = await import('../../../templates/convex/historySeek');
    fixture.tables.chat_history_revisions = [{ _id: 'revision', workspace_id: 'ws-1', thread_id: 'thread-1', value: 9 }];
    fixture.tables.messages = Array.from({ length: 140 }, (_, index) => ({ _id: `stored-${index}`, workspace_id: 'ws-1',
      id: `message-${String(index).padStart(3, '0')}`, thread_id: 'thread-1', index: Math.floor(index / 2),
      order_key: index % 2 ? 'ordered' : '', clock: 1, deleted: false, data: { content: `evidence ${index}` } }));
    fixture.tables.messages.push({ _id: 'foreign', workspace_id: 'ws-other', id: 'foreign-evidence', thread_id: 'thread-1', index: 0, order_key: '', data: { content: 'private' } });
    const args = { workspace_id: 'ws-1', actor_user_id: 'user-1' };
    const first = await read(fixture.ctx, { ...args, query: { kind: 'thread_page', thread_id: 'thread-1', limit: 100,
      cursor: encodeChatSeek({ thread_id: 'thread-1', backward: false }) } });
    const second = await read(fixture.ctx, { ...args, query: { kind: 'thread_page', thread_id: 'thread-1', limit: 100, cursor: first.next_cursor } });
    expect([...first.messages, ...second.messages].map((row: Row) => row.id)).toEqual(fixture.tables.messages.slice(0, 140).map((row) => row.id));
    const previous = await read(fixture.ctx, { ...args, query: { kind: 'thread_page', thread_id: 'thread-1', limit: 2,
      cursor: encodeChatSeek({ thread_id: 'thread-1', backward: true, key: [1, '', 'message-002'] }) } });
    expect(previous.messages.map((row: Row) => row.id)).toEqual(['message-001', 'message-000']);
    const foreign = await read(fixture.ctx, { ...args, query: { kind: 'messages', message_ids: ['foreign-evidence', 'unknown'] } });
    expect(foreign.messages).toEqual([]);
    const revision = await read(fixture.ctx, { ...args, query: { kind: 'thread', thread_id: 'thread-1' } });
    expect(revision.revision).toBe('9');
    await expect(read(fixture.ctx, { ...args, query: { kind: 'thread_page', thread_id: 'thread-1', limit: 101 } })).rejects.toThrow('bounded');
    await expect(read(fixture.ctx, { ...args, actor_user_id: 'forged', query: { kind: 'messages', message_ids: ['message-000'] } })).rejects.toThrow('Forbidden');
    fixture.tables.workspace_members = [];
    await expect(read(fixture.ctx, { ...args, query: { kind: 'messages', message_ids: ['message-000'] } })).rejects.toThrow('Forbidden');
    expect(fixture.takeCounts.every((count) => count <= 100)).toBe(true);
  });
});

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
            (fixture.tables[item.tableName] ??= []).push({
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
        name: "empty.bin",
        mime_type: "application/octet-stream",
        kind: "file",
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
        mimeType: "application/octet-stream",
        name: "empty.bin",
        fileKind: "file",
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
    const unfilteredFirst = await queryCanonicalStorage(fixture.ctx, {
      workspace_id: "ws-1",
      kind: "live_metadata",
      page_size: 1,
    });
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
        cursor: unfilteredFirst.nextCursor,
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
      op_id: uuidOp("op-delete"),
      server_version: 5,
      created_at: 1005,
    });
    const push = (syncFunctions.push as RegisteredFunction)._handler;

    const stale = await push(fixture.ctx, {
      workspace_id: "ws-1",
      ops: [
        {
          op_id: uuidOp("op-before-delete"),
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
          op_id: uuidOp("op-z-newer"),
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
      op_id: uuidOp("op-z-newer"),
    });
  });

  it("does not import client ref_count authority into Convex materialized state", async () => {
    const fixture = createFixture("editor");
    const push = (syncFunctions.push as RegisteredFunction)._handler;
    const hash = `sha256:${"a".repeat(64)}`;

    await push(fixture.ctx, {
      workspace_id: "ws-1",
      ops: [{
        op_id: uuidOp("op-file-create"),
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
        op_id: uuidOp("op-file-update"),
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
          op_id: uuidOp("op-stale-delete"),
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
      op_id: uuidOp("op-identical"),
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
      fixture.tables.change_log.filter((row) => row.op_id === uuidOp("op-identical")),
    ).toHaveLength(1);
  });

  it("rejects conflicting same-batch operation IDs without consuming a version", async () => {
    const fixture = createFixture("editor");
    const push = (syncFunctions.push as RegisteredFunction)._handler;

    const result = await push(fixture.ctx, {
      workspace_id: "ws-1",
      ops: [
        {
          op_id: uuidOp("op-conflict"),
          table_name: "threads",
          operation: "put",
          pk: "thread-a",
          payload: { id: "thread-a", title: "first" },
          clock: 6,
          hlc: "6:0:device-a",
          device_id: "device-a",
        },
        {
          op_id: uuidOp("op-conflict"),
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
          op_id: uuidOp("op-invalid-table"),
          table_name: "not_a_table",
          operation: "put",
          pk: "bad",
          payload: { id: "bad" },
          clock: 6,
          hlc: "6:0:device-a",
          device_id: "device-a",
        },
        {
          op_id: uuidOp("op-key-mutation"),
          table_name: "threads",
          operation: "put",
          pk: "thread-key",
          payload: { id: "different-key", title: "bad" },
          clock: 6,
          hlc: "6:0:device-a",
          device_id: "device-a",
        },
        {
          op_id: uuidOp("op-workspace-mutation"),
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
          op_id: uuidOp("op-valid-sibling"),
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

  it("mints UUID op_ids for notifications.create and skips legacy prefixed ids on pull", async () => {
    const fixture = createFixture("editor");
    const create = (notificationFunctions.create as RegisteredFunction)._handler;
    const pull = (syncFunctions.pull as RegisteredFunction)._handler;
    const id = await create(fixture.ctx, {
      workspace_id: "ws-1",
      user_id: "user-1",
      type: "info",
      title: "hello",
    });
    const created = fixture.tables.change_log.find(
      (row) => row.table_name === "notifications" && row.pk === id,
    );
    expect(created).toBeTruthy();
    expect(ChangeStampSchema.shape.opId.safeParse(created!.op_id).success).toBe(
      true,
    );
    expect(created!.op_id).not.toMatch(/^server:/);

    fixture.tables.change_log.push({
      _id: "legacy-notif",
      workspace_id: "ws-1",
      server_version: created!.server_version + 1,
      table_name: "notifications",
      pk: "legacy-note",
      op: "put",
      payload: { id: "legacy-note", user_id: "user-1" },
      clock: 9,
      hlc: "9:0:server",
      device_id: "server",
      op_id: `server:notif:${id}`,
      created_at: 9,
    });
    fixture.tables.server_version_counter[0]!.value = created!.server_version + 1;

    const page = await pull(fixture.ctx, {
      workspace_id: "ws-1",
      cursor: created!.server_version - 1,
      limit: 50,
    });
    expect(page.changes.every((change: { stamp: { opId: string } }) =>
      ChangeStampSchema.shape.opId.safeParse(change.stamp.opId).success
    )).toBe(true);
    expect(page.changes.map((change: { pk: string }) => change.pk)).toContain(id);
    expect(page.changes.map((change: { pk: string }) => change.pk)).not.toContain(
      "legacy-note",
    );
    expect(page.nextCursor).toBe(created!.server_version + 1);
    expect(page.oldestRetainedVersion).toBeGreaterThan(0);
    expect(page.requiresSnapshot).toBe(false);
  });

  it("rejects a non-UUID op_id before allocating a server version", async () => {
    const fixture = createFixture("editor");
    const before = fixture.tables.server_version_counter[0]!.value;
    const push = (syncFunctions.push as RegisteredFunction)._handler;
    const result = await push(fixture.ctx, {
      workspace_id: "ws-1",
      ops: [{
        op_id: "not-a-uuid",
        table_name: "threads",
        operation: "put",
        pk: "thread-uuid-check",
        payload: { id: "thread-uuid-check", title: "nope" },
        clock: 8,
        hlc: "8:0:dev",
        device_id: "dev",
      }],
    });
    expect(result.results[0]).toMatchObject({
      success: false,
      error: expect.stringContaining("UUID"),
    });
    expect(fixture.tables.server_version_counter[0]!.value).toBe(before);
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
    expect(packed.files["sync.ts"]).toContain(
      "from './tableMetadata'",
    );
    expect(packed.files["tableMetadata.ts"]).toContain(
      "export function getPkField",
    );
    expect(packed.files["schema.ts"]).toContain(
      "sync_snapshot_sessions: defineTable",
    );
    expect(packed.files["snapshot.ts"]).toContain("resolveSnapshotWinner");
    expect(packed.files["tsconfig.json"]).toContain('"strict": true');
    expect(packed.files["tsconfig.json"]).toContain('"noEmit": true');

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
