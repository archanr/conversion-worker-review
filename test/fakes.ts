import { ConversionFailed } from "../src/worker.ts";
import type {
  Clock,
  Converter,
  Expected,
  Job,
  JobChanges,
  JobStore,
  QueueMessage,
  RunningConversion,
} from "../src/worker.ts";

/** Yields to other pending work, the way a network round trip would. */
export const tick = (): Promise<void> => new Promise((resolve) => setImmediate(resolve));

/** Lets every in-flight handler run as far as it can without outside input. */
export async function settle(): Promise<void> {
  for (let i = 0; i < 25; i++) await tick();
}

export async function waitFor(condition: () => boolean, what: string): Promise<void> {
  for (let i = 0; i < 1_000; i++) {
    if (condition()) return;
    await tick();
  }
  throw new Error(`gave up waiting for ${what}`);
}

/**
 * Behaves like a DynamoDB item with conditional writes: the check and the write happen
 * together, and every call yields first so concurrent handlers interleave.
 */
export class InMemoryJobStore implements JobStore {
  readonly #jobs = new Map<string, Job>();
  readonly #log: string[];

  constructor(log: string[]) {
    this.#log = log;
  }

  seed(job: Job): void {
    this.#jobs.set(job.id, { ...job });
  }

  read(id: string): Job | undefined {
    const job = this.#jobs.get(id);
    return job && { ...job };
  }

  async get(id: string): Promise<Job | undefined> {
    await tick();
    return this.read(id);
  }

  async update(id: string, expected: Expected, changes: JobChanges): Promise<boolean> {
    await tick();
    const current = this.#jobs.get(id);
    if (!current || current.status !== expected.status || current.attempt !== expected.attempt) {
      return false;
    }
    const next: Record<string, unknown> = { ...current };
    for (const [key, value] of Object.entries(changes)) {
      if (value === undefined) delete next[key];
      else next[key] = value;
    }
    const job = next as unknown as Job;
    this.#jobs.set(id, job);
    this.#log.push(`store ${current.status}->${job.status} #${job.attempt}`);
    return true;
  }

  /** Unconditional write. Only the baseline (original) handler calls this. */
  async put(job: Job): Promise<void> {
    await tick();
    this.#jobs.set(job.id, { ...job });
    this.#log.push(`store put ${job.status} #${job.attempt}`);
  }
}

export class FakeMessage implements QueueMessage {
  readonly jobId: string;
  readonly receiveCount: number;
  acks = 0;
  retries = 0;
  readonly #log: string[];

  constructor(jobId: string, receiveCount: number, log: string[]) {
    this.jobId = jobId;
    this.receiveCount = receiveCount;
    this.#log = log;
  }

  async ack(): Promise<void> {
    this.acks++;
    this.#log.push(`ack ${this.jobId}`);
  }

  async retry(): Promise<void> {
    this.retries++;
    this.#log.push(`retry ${this.jobId}`);
  }
}

/** A conversion the test finishes by hand. */
export class FakeConversion implements RunningConversion {
  readonly inputKey: string;
  readonly outputKey: string;
  readonly completion: Promise<void>;
  killed = false;
  #resolve: () => void = () => {};
  #reject: (error: Error) => void = () => {};
  readonly #log: string[];

  constructor(inputKey: string, outputKey: string, log: string[]) {
    this.inputKey = inputKey;
    this.outputKey = outputKey;
    this.#log = log;
    this.completion = new Promise<void>((resolve, reject) => {
      this.#resolve = resolve;
      this.#reject = reject;
    });
    this.completion.catch(() => {}); // some tests never observe the outcome
  }

  succeed(): void {
    this.#resolve();
  }

  exit(code: number, message: string): void {
    this.#reject(new ConversionFailed(message, code));
  }

  async kill(): Promise<void> {
    this.killed = true;
    this.#log.push(`kill ${this.outputKey}`);
    this.#reject(new ConversionFailed("killed by owner", 137));
  }
}

export class FakeConverter implements Converter {
  readonly started: FakeConversion[] = [];
  readonly #log: string[];

  constructor(log: string[]) {
    this.#log = log;
  }

  start(inputKey: string, outputKey: string): FakeConversion {
    const conversion = new FakeConversion(inputKey, outputKey, this.#log);
    this.started.push(conversion);
    this.#log.push(`start ${outputKey}`);
    return conversion;
  }
}

/** One per simulated worker: each machine has its own wall clock and timers. */
export class FakeClock implements Clock {
  readonly timeoutsRequested: number[] = [];
  #now: number;
  #pending: Array<() => void> = [];

  constructor(now: number) {
    this.#now = now;
  }

  now(): number {
    return this.#now;
  }

  timeout(ms: number): Promise<never> {
    this.timeoutsRequested.push(ms);
    return new Promise<never>((_, reject) => {
      this.#pending.push(() => reject(new Error(`timer fired after ${ms} ms`)));
    });
  }

  /** Fires every pending timer, as if this worker's timeouts had all elapsed. */
  fireTimeouts(): void {
    const due = this.#pending;
    this.#pending = [];
    for (const fire of due) fire();
  }
}

export function world() {
  const log: string[] = [];
  return {
    log,
    store: new InMemoryJobStore(log),
    converter: new FakeConverter(log),
    message: (jobId: string, receiveCount = 1) => new FakeMessage(jobId, receiveCount, log),
  };
}
