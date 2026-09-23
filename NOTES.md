# Notes

## Assumptions

- **Results are kept for 90 days, the same as job metadata.** This choice changes the cost estimate more than anything else
- **Durations and sizes.** Assuming average import takes 3 minutes and produces 100 MB of JSON ("seconds to several minutes", "up to 500 MB"), and an export takes 30 minutes and produces a 25 GB package ("tens of minutes", "10 GB to 40 GB")
- **Timeouts are 20 minutes for imports and 90 minutes for exports.** Taken from "several minutes" and "tens of minutes" in the brief, with a lenient grace period
- **A job waiting to "retry" still shows as** `queued`. 
- **Converters report exit codes.** The TypeScript import library runs in a child process so it can be killed and its memory freed
- **Queue behavior.** `retry()` means ChangeMessageVisibility with a backoff delay. Calling neither `ack()` nor `retry()` leaves the message to come back on its own when the visibility timeout runs out
- **Callers can read from S3 themselves**, so the job API hands back an S3 key rather than a presigned URL.
- **Callers pay for the S3 inputs they provide**, not this service

## Every risk I found in the code

Roughly in order of urgency.

1. Nothing claims a job exclusively. A `get` followed by an unconditional `put` lets two deliveries under 100 ms apart both run the conversion.
2. Every attempt writes to the same output key, and every state write is unconditional, so a slow or abandoned attempt can overwrite a newer result and roll the record back. The `{...job}` spreads write back a stale snapshot too.
3. A timeout never calls `kill()`, so the process keeps running, holds its 2 GB, and keeps writing.
4. The hard-coded 30 second timeout fails nearly every export and many imports.
5. Every failure is retried the same way, so an invalid database — exit 2, per the brief's observations — burns all three attempts for input that will never convert.
6. The retry budget comes from the SQS `receiveCount`, which also counts duplicate deliveries and visibility-timeout returns, so a job can run out of retries without ever having failed three times.
7. A worker that dies on its final attempt never runs its `catch`, so the job sits in `running` forever and the caller polls forever.
8. If the store errors while writing success, it lands in the `catch`, gets treated as a conversion failure, and either reruns the whole job or fails it.
9. The poll loop runs up to 10 conversions on one 4 GB task, which is what causes the out-of-memory kills.


## Where I stopped, and what I would do next

Risks 1–8 are in `src/worker.ts`. Risks 1–7 each have a test that fails on the original handler and passes on the fixed one. Risk 8 didn't need its own test since the fix was just moving code so only the conversion step runs inside the `try` block. Risk 9 isn't fixed yet.

1. **The poll loop (risk 9)** Limit runners to one job at a time. On SIGTERM, stop picking up new jobs but let the lease keep protecting the one already in progress. Hold ECS scale-in protection while a conversion runs.
2. **Lease renewal** While a conversion runs, keep renewing the lease and extending the SQS visibility timeout. Without this, a crashed worker's job just sits until the visibility timeout expires (up to 90 minutes for an export) before anything can pick it up again.
3. **Real adapters**, not placeholders: a child-process wrapper for the TypeScript import library, the JVM run in its own process group, and a multipart upload that reliably finishes within the attempt timeout.
4. **An integration test against DynamoDB Local**, to confirm the real conditional writes behave the same way the in-memory test store assumes.
5. **The DLQ-to-failed Lambda and the DynamoDB Stream webhook notifier**, so a job whose message gets dead-lettered still reaches a terminal state the caller can see.
6. **A load test on the onboarding traffic mix**, to replace the guessed durations and sizes with real numbers and confirm 4 GB is the right task size.


## How I used AI

I used Claude Sonnet in a chat session to complete this exercise and was used extensively for writing, formatting, and researching ideas for me. 

My process when using AI for tasks:

1. Made sure I completely read the problem statement and understand the overall purpose manually
2. Ask AI about any software system terms/ideas that were unclear to me.
3. Use the AI model in "Plan" mode where I type in plain english in my own words how the system should be designed and whether it has the right context.
4. Follow steps 2-3 until the plan created by the model makes logical sense to me. Then ask AI to implement the plan.
5. Review line-by-line all the code created/changed, any documentation written, and assumptions made and make sure you fully understand the output.