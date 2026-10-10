import { beforeAll, describe, expect, it } from "vitest";

type RegisteredFunction = {
  _handler: (ctx: any, args: any) => Promise<any>;
};

type Ctx = {
  db: {
    get: (id: string) => Promise<any>;
    patch: (id: string, patch: Record<string, any>) => Promise<void>;
  };
};

let claimClientTool!: RegisteredFunction;
let settleClientTool!: RegisteredFunction;

beforeAll(async () => {
  const mod = await import(
    /* @vite-ignore */ new URL(
      "../../../../../templates/convex/backgroundJobs.ts",
      import.meta.url,
    ).href
  );
  claimClientTool = mod.claimClientTool as RegisteredFunction;
  settleClientTool = mod.settleClientTool as RegisteredFunction;
});

function makeJob(pending: Record<string, any>) {
  return {
    _id: "job-1",
    id: "job-1",
    user_id: "user-1",
    status: "streaming",
    execution: { clientToolCall: pending },
  };
}

function makeCtx(job: any): { ctx: Ctx; rows: Map<string, any> } {
  const rows = new Map<string, any>([["job-1", job]]);
  const ctx: Ctx = {
    db: {
      get: async (id: string) => rows.get(id) ?? null,
      patch: async (id: string, patch: Record<string, any>) => {
        const row = rows.get(id);
        if (!row) throw new Error(`missing row ${id}`);
        rows.set(id, { ...row, ...patch });
      },
    },
  };
  return { ctx, rows };
}

const CLAIM_ARGS = {
  job_id: "job-1",
  user_id: "user-1",
  call_id: "call-1",
  claim_token: "token-1",
  claim_expires_at: Date.now() + 60_000,
};

const SETTLE_BASE = {
  job_id: "job-1",
  user_id: "user-1",
  call_id: "call-1",
  claim_token: "token-1",
  execution: {},
  tool_calls: [],
};

describe("convex claim fingerprint (or3-chat #233)", () => {
  it("snapshots the argument fingerprint when the claim is granted", async () => {
    const { ctx, rows } = makeCtx(
      makeJob({ callId: "call-1", argumentFingerprint: "fp-1" }),
    );
    const result = await claimClientTool._handler(ctx, CLAIM_ARGS);

    expect(result).not.toBeNull();
    const parked = rows.get("job-1").execution.clientToolCall;
    expect(parked.claimToken).toBe("token-1");
    expect(parked.claimFingerprint).toBe("fp-1");
    expect(result.execution.clientToolCall.claimFingerprint).toBe("fp-1");
  });

  it("settles when the parked arguments are unchanged since the claim", async () => {
    const { ctx } = makeCtx(
      makeJob({
        callId: "call-1",
        argumentFingerprint: "fp-1",
        claimToken: "token-1",
        claimExpiresAt: Date.now() + 60_000,
        claimFingerprint: "fp-1",
      }),
    );
    await expect(settleClientTool._handler(ctx, SETTLE_BASE)).resolves.toBe(
      true,
    );
  });

  it("rejects the settle when arguments were mutated after the claim", async () => {
    const { ctx } = makeCtx(
      makeJob({
        callId: "call-1",
        // Parked arguments changed in place after the claim was granted.
        argumentFingerprint: "fp-2",
        claimToken: "token-1",
        claimExpiresAt: Date.now() + 60_000,
        claimFingerprint: "fp-1",
      }),
    );
    await expect(settleClientTool._handler(ctx, SETTLE_BASE)).resolves.toBe(
      false,
    );
  });

  it("fails open for claims granted before the fingerprint existed", async () => {
    const { ctx } = makeCtx(
      makeJob({
        callId: "call-1",
        argumentFingerprint: "fp-1",
        claimToken: "token-1",
        claimExpiresAt: Date.now() + 60_000,
        // No claimFingerprint: legacy claim.
      }),
    );
    await expect(settleClientTool._handler(ctx, SETTLE_BASE)).resolves.toBe(
      true,
    );
  });

  it("rejects the settle when the parked call was replaced with a fresh object", async () => {
    const { ctx } = makeCtx(
      makeJob({
        callId: "call-1",
        argumentFingerprint: "fp-9",
        // Fresh re-park carries no token and no fingerprint.
      }),
    );
    await expect(settleClientTool._handler(ctx, SETTLE_BASE)).resolves.toBe(
      false,
    );
  });
});
