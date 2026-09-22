/**
 * Per-message handler for the conversion worker. Risk numbers refer to NOTES.md.
 *
 * 1. Exclusive claim [1]: a conditional write plus a lease, so a losing or
 *    late delivery never starts a second conversion.
 * 2. Fencing [2]: each attempt writes its own output key, and every write
 *    after the claim requires (status = running, attempt = ours), so a
 *    stale attempt can finish but never publish.
 * 3. Kill and reap on timeout, per-type instead of a hard-coded 30 s [3, 4].
 * 4. Exit 2 fails immediately; the retry budget lives in the job record,
 *    not SQS receiveCount [5, 6].
 * 5. A job whose final attempt died is marked failed, not left running [7].
 * 6. Only the conversion sits inside the try, so a store error is never
 *    mistaken for a conversion failure [8].
 *
 * Interface changes: JobStore.put -> conditional update (no write can be
 * unconditional); Job gains leaseExpiresAt (tells a live owner from a dead
 * one); Clock gains now() (leases need it); handle() takes a WorkerConfig
 * and returns an Outcome, emitted by the poll loop as AttemptOutcome.
 */

export type JobStatus = "queued" | "running" | "succeeded" | "failed";

export interface Job {
  id: string;
  inputKey: string;
  status: JobStatus;
  /** Attempts started so far. The current attempt's number is its fencing token. */
  attempt: number;
  /** Epoch ms after which a running attempt is presumed dead. Set only while running. */
  leaseExpiresAt?: number;
  outputKey?: string;
  error?: string;
}

/** The state a conditional write expects to find. */
export interface Expected {
  status: JobStatus;
  attempt: number;
}

/** Fields to change. A field set to undefined is removed. */
export type JobChanges = Partial<Omit<Job, "id" | "inputKey">>;

export interface JobStore {
  get(id: string): Promise<Job | undefined>;
  /**
   * Applies `changes` only if the stored job still matches `expected`.
   * Resolves true if applied, false if the condition failed.
   *
   * DynamoDB: UpdateItem with ConditionExpression "#status = :status AND attempt = :attempt".
   *
   * Replaces the starter's unconditional put(): every write the worker makes races
   * with other deliveries of the same job, so none of them can be unconditional.
   */
  update(id: string, expected: Expected, changes: JobChanges): Promise<boolean>;
}

export interface QueueMessage {
  jobId: string;
  receiveCount: number;
  /** Delete the message. */
  ack(): Promise<void>;
  /** Make the message visible again after a backoff delay. */
  retry(): Promise<void>;
  // Calling neither leaves the message in flight; it returns when its visibility timeout expires.
}

export interface RunningConversion {
  /**
   * Resolves once the output is fully written. On failure the adapter rejects with
   * ConversionFailed carrying the process exit code when there is one.
   */
  completion: Promise<void>;
  /** Terminates the conversion (its whole process group) and resolves only after it has exited. */
  kill(): Promise<void>;
}

export interface Converter {
  start(inputKey: string, outputKey: string): RunningConversion;
}

export interface Clock {
  /** Wall-clock epoch ms. Leases are compared across workers, so this must be real time. */
  now(): number;
  timeout(ms: number): Promise<never>;
}

/**
 * Per job type; the import and export services run the same code with different config.
 * The queue's visibility timeout must be at least timeoutMs + leaseGraceMs, and the
 * DLQ's maxReceiveCount must be above maxAttempts.
 */
export interface WorkerConfig {
  /** Hard limit for one attempt, e.g. imports 20 min, exports 90 min. */
  timeoutMs: number;
  /** Lease time beyond the timeout: covers kill, reap, the final write, and clock skew. */
  leaseGraceMs: number;
  /** Attempts per job, counted in the job record. */
  maxAttempts: number;
  /** Result file name inside the attempt prefix, e.g. "result.json" or "package.zip". */
  outputName: string;
}

export class ConversionFailed extends Error {
  readonly exitCode: number | undefined;

  constructor(message: string, exitCode?: number) {
    super(message);
    this.name = "ConversionFailed";
    this.exitCode = exitCode;
  }
}

export class AttemptTimedOut extends Error {
  constructor(ms: number) {
    super(`attempt timed out after ${ms} ms and was killed`);
    this.name = "AttemptTimedOut";
  }
}

/** Exit codes meaning the input itself is bad, so a retry cannot help. */
export const PERMANENT_EXIT_CODES: ReadonlySet<number> = new Set([2]);

/** What happened to this delivery. The poll loop emits one metric count per outcome. */
export type Outcome =
  | "not_found"
  | "already_terminal"
  | "owned_elsewhere" // another attempt holds a live lease; message left to return later
  | "lost_claim" // another delivery claimed the job between our read and our write
  | "succeeded"
  | "failed_permanent"
  | "failed_exhausted"
  | "retry_scheduled"
  | "superseded"; // our attempt finished but no longer owned the job; nothing published

const MAX_ERROR_LENGTH = 2_000;

export async function handle(
  message: QueueMessage,
  store: JobStore,
  converter: Converter,
  clock: Clock,
  config: WorkerConfig,
): Promise<Outcome> {
  const job = await store.get(message.jobId);
  if (!job) {
    await message.ack();
    return "not_found";
  }
  if (job.status === "succeeded" || job.status === "failed") {
    await message.ack();
    return "already_terminal";
  }

  const now = clock.now();
  if (job.status === "running" && (job.leaseExpiresAt ?? 0) > now) {
    // Another attempt holds a live lease. Leave the message alone: it comes back after
    // the visibility timeout, when the job is either terminal (ack) or its lease has
    // expired (take over). Not acking is what lets a crashed owner be recovered.
    return "owned_elsewhere";
  }

  const seen: Expected = { status: job.status, attempt: job.attempt };

  if (job.attempt >= config.maxAttempts) {
    // The final attempt died without recording an outcome (e.g. its task was OOM-killed).
    const failed = await store.update(job.id, seen, {
      status: "failed",
      error: job.error ?? `no outcome recorded after ${job.attempt} attempts`,
      leaseExpiresAt: undefined,
    });
    if (!failed) return "lost_claim";
    await message.ack();
    return "failed_exhausted";
  }

  const attempt = job.attempt + 1;
  const claimed = await store.update(job.id, seen, {
    status: "running",
    attempt,
    leaseExpiresAt: now + config.timeoutMs + config.leaseGraceMs,
  });
  if (!claimed) return "lost_claim";

  // From here on, every write is conditional on still owning this attempt.
  const ours: Expected = { status: "running", attempt };
  const outputKey = `jobs/${job.id}/attempts/${attempt}/${config.outputName}`;
  const result = await runWithTimeout(
    converter.start(job.inputKey, outputKey),
    clock,
    config.timeoutMs,
  );

  if (result.ok) {
    // Commit point: callers only ever see an outputKey from the attempt that owned the job.
    const published = await store.update(job.id, ours, {
      status: "succeeded",
      outputKey,
      error: undefined,
      leaseExpiresAt: undefined,
    });
    if (!published) return "superseded";
    await message.ack();
    return "succeeded";
  }

  const error = describe(result.error);
  const permanent = isPermanent(result.error);
  if (permanent || attempt >= config.maxAttempts) {
    const failed = await store.update(job.id, ours, {
      status: "failed",
      error,
      leaseExpiresAt: undefined,
    });
    if (!failed) return "superseded";
    await message.ack();
    return permanent ? "failed_permanent" : "failed_exhausted";
  }

  // Retryable: release the job so the next delivery can claim it without waiting out the lease.
  const released = await store.update(job.id, ours, {
    status: "queued",
    error,
    leaseExpiresAt: undefined,
  });
  if (!released) return "superseded";
  await message.retry();
  return "retry_scheduled";
}

type AttemptResult = { ok: true } | { ok: false; error: unknown };

const TIMED_OUT = Symbol("timed out");

async function runWithTimeout(
  conversion: RunningConversion,
  clock: Clock,
  timeoutMs: number,
): Promise<AttemptResult> {
  const timer = clock.timeout(timeoutMs).then(
    () => TIMED_OUT,
    () => TIMED_OUT,
  );
  try {
    const first = await Promise.race([conversion.completion.then(() => "done" as const), timer]);
    if (first === "done") return { ok: true };
  } catch (error) {
    return { ok: false, error };
  }
  // The process may still be running and writing. Kill and reap it before the job is
  // released, so it cannot overlap the next attempt or hold memory the next job needs.
  // If kill() throws, the error propagates: the job keeps this attempt's lease until it
  // expires, and the poll loop should treat it as fatal so ECS replaces the task.
  await conversion.kill();
  return { ok: false, error: new AttemptTimedOut(timeoutMs) };
}

function isPermanent(error: unknown): boolean {
  return (
    error instanceof ConversionFailed &&
    error.exitCode !== undefined &&
    PERMANENT_EXIT_CODES.has(error.exitCode)
  );
}

function describe(error: unknown): string {
  let text: string;
  if (error instanceof ConversionFailed && error.exitCode !== undefined) {
    text = `exit ${error.exitCode}: ${error.message}`;
  } else if (error instanceof Error) {
    text = error.message;
  } else {
    text = String(error);
  }
  return text.length > MAX_ERROR_LENGTH ? `${text.slice(0, MAX_ERROR_LENGTH)}…` : text;
}
