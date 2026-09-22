import assert from "node:assert/strict";
import { test } from "node:test";
import { handle as fixedHandle } from "../src/worker.ts";
import type { Job, WorkerConfig } from "../src/worker.ts";
import { handle as originalHandle } from "../src/original-worker.ts";
import { FakeClock, settle, waitFor, world } from "./fakes.ts";
import type { FakeConverter, FakeMessage, InMemoryJobStore } from "./fakes.ts";

// WORKER_IMPL=original runs the same tests against the unmodified starter handler.
type Handler = (
  message: FakeMessage,
  store: InMemoryJobStore,
  converter: FakeConverter,
  clock: FakeClock,
  config: WorkerConfig,
) => Promise<unknown>;

const handle: Handler =
  process.env.WORKER_IMPL === "original"
    ? (message, store, converter, clock) => originalHandle(message, store, converter, clock)
    : fixedHandle;

const config: WorkerConfig = {
  timeoutMs: 20 * 60_000,
  leaseGraceMs: 60_000,
  maxAttempts: 3,
  outputName: "result.json",
};

const INPUT = "uploads/example-city/inspections.mdb";
const opts = { timeout: 5_000 }; // the original handler can hang in some scenarios

function queuedJob(id: string, attempt = 0): Job {
  return { id, inputKey: INPUT, status: "queued", attempt };
}

test("a duplicate delivery does not start a second conversion", opts, async () => {
  const w = world();
  w.store.seed(queuedJob("j1"));
  const first = w.message("j1");
  const second = w.message("j1");

  // Two workers receive the same job 50 ms apart.
  const runs = [
    handle(first, w.store, w.converter, new FakeClock(0), config),
    handle(second, w.store, w.converter, new FakeClock(50), config),
  ];
  await waitFor(() => w.converter.started.length > 0, "a conversion to start");
  await settle();
  assert.equal(w.converter.started.length, 1, "only one worker may run the conversion");

  // A third delivery a minute later finds a live lease and stays out too.
  const third = w.message("j1", 2);
  await handle(third, w.store, w.converter, new FakeClock(60_000), config);
  assert.equal(w.converter.started.length, 1);
  assert.equal(third.acks + third.retries, 0, "a copy that stays out leaves its message alone");

  w.converter.started[0].succeed();
  await Promise.all(runs);
  const job = w.store.read("j1");
  assert.equal(job?.status, "succeeded");
  assert.equal(job?.attempt, 1);
  assert.equal(first.acks + second.acks, 1);
  assert.equal(first.retries + second.retries, 0);
});

test("a slow attempt that finishes late cannot replace the published result", opts, async () => {
  const w = world();
  w.store.seed(queuedJob("j1"));
  const slowRun = handle(w.message("j1"), w.store, w.converter, new FakeClock(0), config);
  await waitFor(() => w.converter.started.length === 1, "the first attempt to start");
  const slow = w.converter.started[0];

  // The first worker stalls: its timer never fires and it never reports back.
  // Another worker gets the redelivered message after the lease has expired.
  const afterLease = config.timeoutMs + config.leaseGraceMs + 1;
  const fastRun = handle(w.message("j1", 2), w.store, w.converter, new FakeClock(afterLease), config);
  await waitFor(() => w.converter.started.length === 2, "the second attempt to start");
  const fast = w.converter.started[1];
  assert.notEqual(fast.outputKey, slow.outputKey, "attempts must not share an output key");

  fast.succeed();
  await fastRun;
  slow.succeed(); // the stalled attempt finally completes
  await slowRun;

  const job = w.store.read("j1");
  assert.equal(job?.status, "succeeded");
  assert.equal(job?.attempt, 2);
  assert.equal(job?.outputKey, fast.outputKey, "the published result must stay the newer attempt's");
});

test("a timed-out conversion is killed before the job is released for retry", opts, async () => {
  const w = world();
  w.store.seed(queuedJob("j1"));
  const clock = new FakeClock(0);
  const message = w.message("j1");
  const run = handle(message, w.store, w.converter, clock, config);
  await waitFor(() => w.converter.started.length === 1, "the conversion to start");

  clock.fireTimeouts();
  await run;

  const conversion = w.converter.started[0];
  assert.ok(conversion.killed, "a timed-out conversion must be killed");
  const killedAt = w.log.indexOf(`kill ${conversion.outputKey}`);
  const releasedAt = w.log.findIndex((entry) => entry.startsWith("store running->queued"));
  assert.ok(releasedAt > killedAt, "the kill must finish before the job is released");
  assert.deepEqual(clock.timeoutsRequested, [config.timeoutMs], "the limit comes from config");
  assert.equal(message.retries, 1);
  assert.match(w.store.read("j1")?.error ?? "", /timed out/);
});

test("an invalid database fails on the first attempt without a retry", opts, async () => {
  const w = world();
  w.store.seed(queuedJob("j1"));
  const message = w.message("j1");
  const run = handle(message, w.store, w.converter, new FakeClock(0), config);
  await waitFor(() => w.converter.started.length === 1, "the conversion to start");

  w.converter.started[0].exit(2, "required table missing: Inspections");
  await run;

  const job = w.store.read("j1");
  assert.equal(job?.status, "failed");
  assert.equal(job?.attempt, 1);
  assert.match(job?.error ?? "", /required table missing/);
  assert.equal(message.acks, 1);
  assert.equal(message.retries, 0);
});

// Regression guard: passes on the original handler too. It checks that the new
// classification did not make transient failures permanent.
test("an exit 137 is retried and a later attempt can succeed", opts, async () => {
  const w = world();
  w.store.seed(queuedJob("j1"));
  const firstMessage = w.message("j1");
  const firstRun = handle(firstMessage, w.store, w.converter, new FakeClock(0), config);
  await waitFor(() => w.converter.started.length === 1, "the first attempt to start");
  w.converter.started[0].exit(137, "Killed");
  await firstRun;
  assert.equal(firstMessage.retries, 1);
  assert.notEqual(w.store.read("j1")?.status, "failed");

  const secondMessage = w.message("j1", 2);
  const secondRun = handle(secondMessage, w.store, w.converter, new FakeClock(30_000), config);
  await waitFor(() => w.converter.started.length === 2, "the second attempt to start");
  w.converter.started[1].succeed();
  await secondRun;

  const job = w.store.read("j1");
  assert.equal(job?.status, "succeeded");
  assert.equal(job?.outputKey, w.converter.started[1].outputKey);
  assert.equal(secondMessage.acks, 1);
});

test("the retry budget is counted in the job record, not SQS receiveCount", opts, async () => {
  const w = world();

  // Duplicates and visibility-timeout returns raise receiveCount without using attempts.
  w.store.seed(queuedJob("j1", 1));
  const bounced = w.message("j1", 5);
  const run = handle(bounced, w.store, w.converter, new FakeClock(0), config);
  await waitFor(() => w.converter.started.length === 1, "the second attempt to start");
  w.converter.started[0].exit(137, "Killed");
  await run;
  assert.equal(bounced.retries, 1, "attempt 2 of 3 must still be retried");
  assert.notEqual(w.store.read("j1")?.status, "failed");

  // Once the job's own budget is spent, it fails instead of retrying.
  w.store.seed(queuedJob("j2", 2));
  const last = w.message("j2", 1);
  const lastRun = handle(last, w.store, w.converter, new FakeClock(0), config);
  await waitFor(() => w.converter.started.length === 2, "the final attempt to start");
  w.converter.started[1].exit(137, "Killed");
  await lastRun;
  const job = w.store.read("j2");
  assert.equal(job?.status, "failed");
  assert.equal(job?.attempt, 3);
  assert.equal(last.acks, 1);
  assert.equal(last.retries, 0);
});

test("a job whose final attempt died is marked failed instead of running forever", opts, async () => {
  const w = world();
  // The task running attempt 3 was OOM-killed: no catch block ran, and the lease has expired.
  w.store.seed({ id: "j1", inputKey: INPUT, status: "running", attempt: 3, leaseExpiresAt: 1_000 });
  const message = w.message("j1", 4);
  const run = handle(message, w.store, w.converter, new FakeClock(5_000), config);
  await settle();
  assert.equal(w.converter.started.length, 0, "no fourth attempt");

  await run;
  const job = w.store.read("j1");
  assert.equal(job?.status, "failed");
  assert.match(job?.error ?? "", /3 attempts/);
  assert.equal(message.acks, 1);
});
