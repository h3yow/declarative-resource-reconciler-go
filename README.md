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
| `pkg/reconcile/deps.go` | Dependency graph evaluation: readiness, cycle detection, dependents/blockers |
| `pkg/reconcile/reconciler.go` | Convergence, idempotency, drift correction, compatibility, dependency gating, deletion cleanup |
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
`LegacyFieldMigrated`, `DeleteCleanupWaiting`, `CleanupComplete`,
`WaitingForDependencies`, `DependencyFailed`, `DependencyMissing`,
`DependencyCycle`, `DeleteWaitingForDependents`.

## Dependency orchestration (multi-resource)

`spec.dependsOn: ["Kind/Name", ...]` declares that this resource may only
be landed after every referenced resource **in the same desired-state
batch** is truly `Ready`. The gate sits after the compatibility pass and
**before any live write or side effect**, so no trigger ordering or
concurrency can produce a "dependent landed first" intermediate state.

- **Waiting is not failing.** While a dependency has not converged, the
  dependent reports `Ready=False / WaitingForDependencies` (phase
  `Pending`) naming the exact dependencies it waits on. It does **not**
  burn backoff retries: it is woken by a fan-out trigger whenever a
  dependency's conclusion actually changes (phase/condition-reason
  transitions only — a no-op pass wakes nobody, so fan-out always
  terminates, and coalesced triggers are never lost).
- **Recovery is automatic.** When a dependency recovers from transient
  failure and reaches `Ready`, its dependents continue with no manual
  re-trigger.
- **Terminal dependency failure propagates.** If a dependency lands in
  `PhaseError` (permanent failure, exhausted retries, or its own
  dependency problem), dependents conclude `DependencyFailed` (phase
  `Error`) and stop — a stable, distinguishable verdict, no hot loop.
- **Missing references are explainable.** A `dependsOn` entry pointing
  at a resource that does not exist in the desired state concludes
  `DependencyMissing` (phase `Error`): stable, crash-free, and it never
  jams other keys.
- **Cycles are a config problem.** Any resource taking part in a
  dependency cycle (including self-references) concludes
  `DependencyCycle` (phase `Error`) listing the cycle members. Cyclic
  resources never land; unrelated resources reconcile normally.
- **Idempotency is preserved.** Repeating the same desired state with
  unchanged observations performs no writes and no side effects; each
  resource is created and each side effect fires exactly once.

## Cascade deletion

Deletion follows the dependency edges in reverse:

- While any downstream resource that `dependsOn` this one still has a
  desired record (even mid-cleanup), the upstream deletion is held at
  `Ready=False / DeleteWaitingForDependents` (phase `Deleting`) — the
  live object and finalizer are retained, and no cleanup is attempted.
- When a downstream resource finishes cleanup and its record is removed,
  it wakes its own dependencies; the upstream then re-evaluates and
  proceeds under the original finalizer/cleanup rules.
- Each cleanup is counted in the ledger (`Deletes`) exactly once; extra
  triggers of already-deleted keys are no-ops. Nothing is left behind.
- Edges inside a dependency cycle do not block deletion (the cycle is
  already reported as `DependencyCycle`); this keeps deleting a
  misconfigured cyclic graph terminable.

## Compatibility scope

- **Objects without `dependsOn`** behave exactly as before: the gate is
  skipped entirely and all single-resource semantics are unchanged.
- **Objects referencing a non-existent resource** get the stable
  `DependencyMissing` conclusion described above — no crash, no hang.
- **Objects in a dependency cycle** get the stable `DependencyCycle`
  conclusion; the rest of the batch is unaffected.
- `dependsOn` lives inside `spec`, so it round-trips through the JSON
  codec and the unknown-field preservation rules are untouched.

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
go test -race -count=1 ./...      # unit + concurrency/retry/delete/dependency/cascade tests
go test -race -count=1 -v -run 'TestDependency|TestCascade' ./pkg/reconcile/  # new orchestration scenarios
go run ./cmd/demo                 # end-to-end scenario walkthrough (incl. dependencies + cascade delete)
```

The tests assert side-effect counts (`Creates/Updates/Deletes/SideCalls`),
final phases and the distinct condition reasons, and their `-v` logs print
the trigger inputs, which dependency each resource is waiting on, retry
attempts and the basis for each decision.
