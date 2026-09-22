// The starter handler from the brief, unmodified apart from formatting.
// Kept only so `npm run test:baseline` can run the same tests against it and show
// which ones fail before the change. Not used by the fixed worker.

export type JobStatus = "queued" | "running" | "succeeded" | "failed";

export interface Job {
  id: string;
  inputKey: string;
  status: JobStatus;
  attempt: number;
  outputKey?: string;
  error?: string;
}

export interface JobStore {
  get(id: string): Promise<Job | undefined>;
  put(job: Job): Promise<void>;
}

export interface QueueMessage {
  jobId: string;
  receiveCount: number;
  ack(): Promise<void>;
  retry(): Promise<void>;
}

export interface RunningConversion {
  completion: Promise<void>;
  kill(): Promise<void>;
}

export interface Converter {
  start(inputKey: string, outputKey: string): RunningConversion;
}

export interface Clock {
  timeout(ms: number): Promise<never>;
}

export async function handle(
  message: QueueMessage,
  store: JobStore,
  converter: Converter,
  clock: Clock,
): Promise<void> {
  const job = await store.get(message.jobId);
  if (!job) {
    await message.ack();
    return;
  }
  if (job.status === "succeeded" || job.status === "failed") {
    await message.ack();
    return;
  }
  const attempt = job.attempt + 1;
  await store.put({ ...job, status: "running", attempt });
  const outputKey = `jobs/${job.id}/result.json`;
  const conversion = converter.start(job.inputKey, outputKey);
  try {
    await Promise.race([conversion.completion, clock.timeout(30_000)]);
    await store.put({
      ...job,
      status: "succeeded",
      attempt,
      outputKey,
    });
    await message.ack();
  } catch (error) {
    if (message.receiveCount >= 3) {
      await store.put({
        ...job,
        status: "failed",
        attempt,
        error: String(error),
      });
      await message.ack();
      return;
    }
    await message.retry();
  }
}
