# Declarative Resource Reconciler (in-process, local simulation)

A small Kubernetes-style **declarative reconciliation loop** that runs
entirely inside one Go process. The "managed resources" and "external
side effects" are simulated by an in-memory store — there is **no
cluster, no network and no credentials**. The point is to demonstrate
that desired state, observed state and the final result converge to the
same, *explainable* conclusion regardless of trigger ordering.

## Layout

| Path | Responsibility |
|---|---|
| `pkg/api/types.go` | Resource / Spec / Status / Condition model and stable condition reasons |
| `pkg/api/errors.go` | Failure classification: transient (retry) vs permanent (stop) |
| `pkg/api/codec.go` | JSON decode/encode that preserves unknown and migrates old fields |
| `pkg/state/store.go` | In-memory simulation: desired records, live objects, side-effect ledger, fault injection |
| `pkg/reconcile/queue.go` | Per-key serialized queue with at-least-once delivery and retry accounting |
| `pkg/reconcile/reconciler.go` | Convergence, idempotency, drift correction, compatibility, deletion cleanup |
| `cmd/demo/main.go` | Runnable end-to-end walkthrough |

## Reconciliation semantics

Each trigger only carries a resource *key*; the loop always re-reads both
desired and observed state, so processing is based on the latest truth,
not on the trigger payload.

1. **Finalizer first.** A cleanup finalizer is attached before any
   create/update, so a deletion request can never race a successful write.
2. **Compatibility pass.**
   - Missing required fields (`spec.replicas <= 0`, empty `spec.image`)
     are rebuilt from safe defaults → reason `MissingFieldRebuilt`.
   - An old-version field `replicas_v1` is migrated into `spec.replicas`
     → reason `LegacyFieldMigrated`.
   - Unknown top-level JSON fields are kept on the object and surfaced in
     `status.unknownFields` → reason `UnknownFieldsPreserved`. They never
     cause a crash and are never silently dropped.
3. **Convergence decision.**
   - No live object **and** never converged → create once (`Ready`).
   - Live object previously existed but is gone (external deletion) →
     recreate, reason `RecreatedAfterDrift`.
   - Live object differs (`replicas`/`image`) or carries an external
     mutation tag → overwrite with desired state, reason `DriftCorrected`.
   - Observed state already equals desired state → **no write**, reason
     `Ready` ("write skipped (idempotent)").
4. **External side effect.** `spec.sideEffectToken` fires the external
   action **at most once per token value**: reconciling again with the
   same token is a no-op; changing the token re-runs it exactly once.
5. **Status is written only when it changed.** Every result records a
   single stable `Ready` condition with a machine-readable reason, a
   human message and the observed retry count.

Conditions are intentionally *distinguishable*: `Ready`,
`TransientFailure`, `PermanentFailure`, `RecreatedAfterDrift`,
`DriftCorrected`, `MissingFieldRebuilt`, `UnknownFieldsPreserved`,
`LegacyFieldMigrated`, `DeleteCleanupWaiting`, `CleanupComplete`.

## Retry rules

Failures are explicitly classified (`api.Transient` / `api.Permanent`).

- **Transient** (e.g. flaky write/cleanup): the request stays in the
  queue, is retried with exponential backoff
  (`base, 2·base, 4·base, …` capped at `maxBackoff`) and is **never
  dropped**. A trigger arriving while the key is queued or in-flight is
  coalesced but guaranteed one more pass (dirty/working-set pattern).
- **Permanent**: a stable `PermanentFailure` condition is recorded and
  the loop stops scheduling the key — no wasted retries, no resource
  created.
- If transient failures never heal, retries stop at `MaxRetries` and the
  condition becomes `PermanentFailure` with the attempts observed.

## Deletion rules

- Deletion sets a `DeletionTimestamp`; the finalizer keeps the record.
- While cleanup fails transiently the resource and finalizer are
  retained, condition `DeleteCleanupWaiting` is reported with the retry
  count, and cleanup is retried.
- Only after cleanup succeeds is the finalizer removed **and** the
  record deleted (`CleanupComplete`). The object is never released early.
- The delete itself is counted in the side-effect ledger (`Deletes`)
  exactly once.

## Concurrency

`KeyedQueue` holds two sets per key — `dirty` (queued) and `work`
(in-flight). A key is handed to at most one worker at a time; different
keys proceed in parallel. 100 simultaneous triggers for one resource
therefore converge to exactly one create and one side effect.

## Local verification (no external dependencies)

```sh
go build ./...
go vet ./...
go test -race -count=1 ./...      # unit + concurrency/retry/delete tests
go run ./cmd/demo                 # end-to-end scenario walkthrough
```

The tests assert side-effect counts (`Creates/Updates/Deletes/SideCalls`),
final phases and the distinct condition reasons, and their `-v` logs print
the trigger inputs, retry attempts and the basis for each decision.
