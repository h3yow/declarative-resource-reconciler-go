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
| `pkg/reconcile/deps.go` | Dependency evaluation, cycle detection and reverse (dependent) lookup |
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
`WaitingForDependency`, `DependencyFailed`, `DependencyMissing`,
`DependencyCycle`, `DeleteWaitingDependents`.

## Dependency orchestration

`spec.dependsOn` lists keys (`Kind/Name`) of other resources **in the
same desired set** that must truly reach their desired state (Ready
condition true at the current generation) before this resource is
allowed to land. Resources with no `dependsOn` behave exactly as
before — the gate is skipped entirely.

- **Ordering under any trigger pattern.** A dependent resource never
  creates or writes its managed object while a dependency is unready —
  regardless of trigger order, duplication or concurrency. Repeated
  triggers of an unchanged, already-converged resource remain
  observable no-ops (no duplicate creates, writes or side effects).
- **Waiting is parked, not retried.** While a dependency is still
  converging (or transiently failing), the dependent parks with
  `Pending` / `WaitingForDependency` — clearly distinct from its own
  `TransientFailure`. Parking consumes **no** retries and **no**
  backoff slots: the resource is woken automatically when a
  dependency's observable state changes, so a healed dependency
  resumes its dependents with no manual re-trigger, and triggers
  merged while parked are honored by the queue's dirty-set semantics.
- **Terminal dependency failure.** If a dependency reaches
  `PermanentFailure` (including retries exhausted), the dependent
  records `Error` / `DependencyFailed` — a stable conclusion,
  distinguishable from the dependency's own failure — and stops. No
  retry spin; if the dependency later recovers (e.g. its spec is
  fixed), the dependent is woken and re-evaluated.
- **Missing dependency.** Declaring a dependency on a key that does
  not exist in the desired set yields `Error` / `DependencyMissing`
  with the offending keys listed. Nothing crashes, nothing hangs, no
  object is created. If the dependency is declared later and
  converges, the dependent recovers automatically.
- **Cycles.** Dependency declarations that form a cycle (including
  self-references) are detected by graph walk and reported as `Error`
  / `DependencyCycle` with the cycle path (e.g. `A -> B -> A`) on
  every member. No resource in the cycle is created and the loop
  terminates: parked conditions are idempotent, so the wake-up
  cascade cannot spin.

## Cascading deletion

Deletion respects the dependency direction in reverse: **an upstream
resource is not released while any downstream resource that depends on
it still exists.**

- While dependents (even ones already deleting themselves) remain in
  the desired set, the upstream's deletion parks with `Deleting` /
  `DeleteWaitingDependents` — distinct from `DeleteCleanupWaiting`
  (its own cleanup) — and its live object stays put.
- When a dependent's record is removed, the upstream is woken
  automatically and resumes the ordinary finalizer cleanup path
  (`DeleteCleanupWaiting` → `CleanupComplete` → record removed).
- Each resource's delete is counted exactly once in the ledger and no
  live objects are left behind.

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
go test -race -count=1 ./...      # unit + concurrency/retry/delete/dependency tests
go run ./cmd/demo                 # end-to-end scenario walkthrough
```

The tests assert side-effect counts (`Creates/Updates/Deletes/SideCalls`),
final phases and the distinct condition reasons, and their `-v` logs print
the trigger inputs, retry attempts and the basis for each decision. The
orchestration tests (`pkg/reconcile/orchestration_test.go`) additionally
log which dependency a resource is parked on and which state change woke
it, e.g.:

```sh
go test -race -count=1 -v -run 'TestDependency|TestCascading' ./pkg/reconcile/
```

## Compatibility scope

- **Objects without `dependsOn`** take the exact same code path as
  before this feature: same reasons, same counters, same retry and
  deletion behavior (covered by the pre-existing test suite).
- **A dependency on a nonexistent key** is a stable configuration
  conclusion (`DependencyMissing`), never a crash or a deadlock; the
  dependent recovers automatically if the key appears and converges.
- **Cyclic declarations** are reported as `DependencyCycle` on every
  cycle member with the cycle path; no resource in the cycle converges
  and the reconciler keeps serving all other keys normally.
- **Deleting an upstream with live dependents** parks it in
  `DeleteWaitingDependents` until the dependents are gone; deleting a
  resource with no dependents is unchanged from the original rules.
