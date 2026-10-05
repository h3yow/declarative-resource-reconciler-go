// Package api defines the local resource model used by the reconciler.
//
// The model intentionally mimics the shape of a declarative Kubernetes
// style object (TypeMeta/ObjectMeta-ish fields, Spec/Status, finalizers)
// but it has no dependency on any cluster: everything is plain Go data
// that is persisted in an in-memory store.
package api

import "time"

// ConditionType identifies a status condition reported on a resource.
type ConditionType string

const (
	// ConditionReady reports whether the desired state has been fully
	// converged (the managed object exists and matches, side effects
	// applied, and no error remains).
	ConditionReady ConditionType = "Ready"
)

// ConditionReason is a stable, machine-readable reason. Distinct failure
// classes MUST use distinct reasons so callers can tell them apart.
type ConditionReason string

const (
	ReasonInitializing         ConditionReason = "Initializing"
	ReasonProgressing          ConditionReason = "Progressing"
	ReasonTransientFailure     ConditionReason = "TransientFailure"
	ReasonPermanentFailure     ConditionReason = "PermanentFailure"
	ReasonRecreateAfterDrift   ConditionReason = "RecreatedAfterExternalDrift"
	ReasonDriftCorrected       ConditionReason = "DriftCorrected"
	ReasonMissingFieldRebuilt  ConditionReason = "MissingFieldRebuilt"
	ReasonUnknownKept          ConditionReason = "UnknownFieldsPreserved"
	ReasonLegacyFieldMigrated  ConditionReason = "LegacyFieldMigrated"
	ReasonDeleteCleanupWaiting ConditionReason = "DeleteCleanupWaiting"
	ReasonCleanupComplete      ConditionReason = "CleanupComplete"
	ReasonReady                ConditionReason = "Ready"

	// Dependency orchestration reasons. Each is a stable, distinguishable
	// conclusion about the *relationship* layer, never confused with the
	// resource's own failure reasons above.
	//
	// ReasonDependencyWaiting: at least one dependency exists but has not
	// converged yet. The resource waits (no hot retry loop); it is
	// re-triggered automatically when a dependency changes.
	ReasonDependencyWaiting ConditionReason = "WaitingForDependencies"
	// ReasonDependencyFailed: a dependency reached a terminal failure, so
	// this resource can never become ready under the current desired
	// state. Stable conclusion; retries stop.
	ReasonDependencyFailed ConditionReason = "DependencyFailed"
	// ReasonDependencyMissing: a referenced dependency does not exist in
	// the desired state (or can never exist). Stable config explanation.
	ReasonDependencyMissing ConditionReason = "DependencyMissing"
	// ReasonDependencyCycle: the dependency declarations form a cycle
	// involving this resource. Distinguishable configuration problem.
	ReasonDependencyCycle ConditionReason = "DependencyCycle"
	// ReasonDeleteWaitingDependents: deletion is held because downstream
	// resources that depend on this one have not been cleaned up yet.
	ReasonDeleteWaitingDependents ConditionReason = "DeleteWaitingForDependents"
)

// Phase is a coarse lifecycle phase stored in status.
type Phase string

const (
	PhasePending     Phase = "Pending"
	PhaseProgressing Phase = "Progressing"
	PhaseReady       Phase = "Ready"
	PhaseError       Phase = "Error"
	PhaseDeleting    Phase = "Deleting"
)

// Condition is a single typed observation about a resource.
type Condition struct {
	Type    ConditionType   `json:"type"`
	Status  string          `json:"status"` // "True" | "False" | "Unknown"
	Reason  ConditionReason `json:"reason"`
	Message string          `json:"message,omitempty"`
	// Retries counts transient failures observed for the current attempt.
	Retries   int       `json:"retries,omitempty"`
	UpdatedAt time.Time `json:"updatedAt"`
}

// Spec is the desired state.
//
// Replicas is the key desired field. SideEffectToken models an
// external side effect that must run at most once per token value:
// changing the token re-runs it, repeating the same value is a no-op.
type Spec struct {
	Replicas        int    `json:"replicas"`
	Image           string `json:"image"`
	SideEffectToken string `json:"sideEffectToken,omitempty"`
	// DependsOn lists keys ("Kind/Name") of other resources in the same
	// desired-state batch that must be fully Ready before this resource
	// is allowed to converge. Empty means no orchestration: behavior is
	// exactly the single-resource semantics.
	DependsOn []string `json:"dependsOn,omitempty"`
}

// Status is the observed/result state written by the reconciler.
type Status struct {
	Phase         Phase             `json:"phase"`
	ObservedGen   int64             `json:"observedGeneration,omitempty"`
	LiveReplicas  int               `json:"liveReplicas,omitempty"`
	LiveImage     string            `json:"liveImage,omitempty"`
	AppliedToken  string            `json:"appliedSideEffectToken,omitempty"`
	Conditions    []Condition       `json:"conditions,omitempty"`
	UnknownFields map[string]string `json:"unknownFields,omitempty"`
	ReconciledAt  time.Time         `json:"reconciledAt,omitempty"`
}

// Resource is a single managed declarative object.
type Resource struct {
	Kind       string `json:"kind"`
	APIVersion string `json:"apiVersion"`
	Name       string `json:"name"`
	Generation int64  `json:"generation"`

	Spec   Spec   `json:"spec"`
	Status Status `json:"status"`

	// Finalizers block deletion until cleanup is complete.
	Finalizers []string `json:"finalizers,omitempty"`

	// DeletionTimestamp is set once an external caller asked for deletion.
	DeletionTimestamp *time.Time `json:"deletionTimestamp,omitempty"`

	// Unknown keeps raw top-level JSON fields that this build does not
	// understand, so they survive a round trip instead of being dropped.
	Unknown map[string]any `json:"-"`
}

// Key returns the per-resource identity used for serialization.
func (r *Resource) Key() string { return r.Kind + "/" + r.Name }

// GetCondition returns a pointer to the condition of t, or nil.
func (s *Status) GetCondition(t ConditionType) *Condition {
	for i := range s.Conditions {
		if s.Conditions[i].Type == t {
			return &s.Conditions[i]
		}
	}
	return nil
}

// SetCondition inserts or replaces the condition of c.Type.
func (s *Status) SetCondition(c Condition) {
	for i := range s.Conditions {
		if s.Conditions[i].Type == c.Type {
			s.Conditions[i] = c
			return
		}
	}
	s.Conditions = append(s.Conditions, c)
}
