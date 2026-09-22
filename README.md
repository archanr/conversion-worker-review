# Conversion worker: design review and repair

- `DESIGN.md`: the design review
- `NOTES.md`: assumptions, what I did and didn't fix, where I stopped, and how I used AI.
- `src/worker.ts`: the revised per-message handler (part 2)
- `test/`: focused tests and in-memory fakes (part 2)
- `src/original-worker.ts`: the unmodified starter handler, kept so the tests can be run against it (part 2)

## Running the tests

This needs Node.js 22.6 or later. The tests use Node's built-in test runner and TypeScript type stripping, so they need **no install and no network**.

```sh
node --version          # v22.6.0 or later
npm test                # the fixed worker: 7 pass
npm run test:baseline   # the same tests against the original starter handler: 6 fail, 1 passes
```

To typecheck (optional; this one needs `npm install`):

```sh
npm install
npm run typecheck
```

On Windows, run the baseline as follows. The `npm run test:baseline` script uses POSIX `VAR=value` syntax.

```powershell
$env:WORKER_IMPL="original"; node --experimental-strip-types --no-warnings --test test/worker.test.ts
```

## What the tests cover

| Test | Risk it pins down | Original handler |
|---|---|---|
| A duplicate delivery does not start a second conversion | Two deliveries <100 ms apart both run | Fails: 2 conversions |
| A slow attempt that finishes late cannot replace the published result | Shared output key and unconditional writes | Fails: shared key |
| A timed-out conversion is killed before the job is released | Orphaned processes and the hard-coded 30 s timeout | Fails: never killed |
| An invalid database fails on the first attempt without a retry | Exit 2 retried like a transient failure (original bug) | Fails: retried |
| An exit 137 is retried and a later attempt can succeed | Regression guard: classification didn't over-reach | Passes (by design) |
| The retry budget is counted in the job record, not receiveCount | Duplicates waste the retry budget | Fails: failed early |
| A job whose final attempt died is marked failed | Jobs stuck in `running` forever | Fails: 4th attempt |

Each simulated worker gets its own `FakeClock`, because real workers on different machines have separate clocks and timers. The in-memory store does its check and its write together, the way a DynamoDB conditional update does, and it yields on every call so concurrent handlers interleave.
