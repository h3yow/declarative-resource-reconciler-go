// Package api defines the vocabulary of the in-process declarative
// resource reconciler: resources, desired states, conditions, decisions
// and error classifications.
//
// The model intentionally avoids any cluster or network dependency: every
// construct here is plain Go state that can be embedded in another process.
package api

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"time"
)

// WidgetSpec is the user-provided desired payload for a managed resource.
type WidgetSpec struct {
	Image    string `json:"image"`
	Replicas int    `json:"replicas"`
	Config   string `json:"config,omitempty"`
}

// KeyFieldMissing reports whether a required field is absent. Unknown or
// zero-valued optional fields never make a spec invalid; only the image
// reference is essential because without it the effect cannot be created.
func (s WidgetSpec) KeyFieldMissing() bool { return s.Image == "" }

// SpecHash is a stable hash of the desired payload used to detect drift
// between observed and desired state.
func SpecHash(s WidgetSpec) string {
	b, _ := json.Marshal(s)
	sum := sha256.Sum256(b)
	return "sha256:" + hex.EncodeToString(sum[:12])
}

// Condition reports one stable, explainable aspect of reconciliation.
//
// Type+Reason pairs form a stable vocabulary so callers can distinguish
// "retrying a transient failure" from "stopped on a permanent failure".
type Condition struct {
	Type    string    `json:"type"`
	Status  string    `json:"status"`
	Reason  string    `json:"reason"`
	Message string    `json:"message,omitempty"`
	Count   int       `json:"count"`
	Updated time.Time `json:"updated"`
}

// Equal reports whether two conditions carry the same Type/Status/Reason
// and Message. Count and Updated intentionally excluded so that condition
// retries do not produce write churn on every pass that merely re-affirms
// an already-recorded condition.
func (c Condition) Equal(o Condition) bool {
	return c.Type == o.Type &&
		c.Status == o.Status &&
		c.Reason == o.Reason &&
		c.Message == o.Message
}

// Resource is the observed managed state. Unknown fields decoded from JSON
// are preserved verbatim in Extra so old/new versions cannot silently drop
// data.
type Resource struct {
	ID                string               `json:"id"`
	Generation        int64                `json:"generation"`
	SpecHash          string               `json:"specHash"`
	Spec              WidgetSpec           `json:"spec"`
	Annotations       map[string]string    `json:"annotations,omitempty"`
	Finalizers        []string             `json:"finalizers,omitempty"`
	DeletionTimestamp *time.Time           `json:"deletionTimestamp,omitempty"`
	Conditions        map[string]Condition `json:"conditions,omitempty"`
	ResourceVersion   int64                `json:"resourceVersion"`
	// Extra holds fields unknown to this binary, preserved round-trip.
	Extra map[string]any `json:"extra,omitempty"`
}

type resourceAlias Resource

type resourceWire struct {
	resourceAlias
}

// MarshalJSON emits known fields plus any unknown fields preserved in Extra
// at the top level, so a newer or older peer still sees its own data.
func (r Resource) MarshalJSON() ([]byte, error) {
	out := make(map[string]json.RawMessage, len(r.Extra)+10)
	for k, v := range r.Extra {
		b, err := json.Marshal(v)
		if err != nil {
			return nil, err
		}
		out[k] = b
	}
	b, err := json.Marshal(resourceAlias(r))
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal(b, &out); err != nil {
		return nil, err
	}
	return json.Marshal(out)
}

// UnmarshalJSON decodes known fields and retains every unknown/old-version
// field in Extra; decoding never panics and never silently drops a field.
func (r *Resource) UnmarshalJSON(data []byte) error {
	var wire resourceWire
	if err := json.Unmarshal(data, &wire); err != nil {
		return err
	}
	known := knownResourceFields
	var all map[string]json.RawMessage
	if err := json.Unmarshal(data, &all); err != nil {
		return err
	}
	extra := map[string]any{}
	for k, v := range all {
		if _, ok := known[k]; ok {
			continue
		}
		var val any
		if err := json.Unmarshal(v, &val); err != nil {
			return err
		}
		extra[k] = val
	}
	*r = Resource(wire.resourceAlias)
	if len(extra) > 0 {
		if r.Extra == nil {
			r.Extra = map[string]any{}
		}
		for k, v := range extra {
			r.Extra[k] = v
		}
	}
	return nil
}

// knownResourceFields is the static schema of this binary. It is used
// instead of re-marshalling a value, because omitempty would let empty
// optional fields look "unknown" and duplicate them into Extra.
var knownResourceFields = map[string]struct{}{
	"id":                {},
	"generation":        {},
	"specHash":          {},
	"spec":              {},
	"annotations":       {},
	"finalizers":        {},
	"deletionTimestamp": {},
	"conditions":        {},
	"resourceVersion":   {},
	"extra":             {},
}

// Snapshot returns a value copy safe to hand out while reconciliation
// mutates stored state elsewhere.
func (r Resource) Snapshot() Resource {
	cp := r
	if r.Annotations != nil {
		cp.Annotations = make(map[string]string, len(r.Annotations))
		for k, v := range r.Annotations {
			cp.Annotations[k] = v
		}
	}
	if r.Conditions != nil {
		cp.Conditions = make(map[string]Condition, len(r.Conditions))
		for k, v := range r.Conditions {
			cp.Conditions[k] = v
		}
	}
	if r.Finalizers != nil {
		cp.Finalizers = append([]string(nil), r.Finalizers...)
	}
	if r.Extra != nil {
		cp.Extra = make(map[string]any, len(r.Extra))
		for k, v := range r.Extra {
			cp.Extra[k] = v
		}
	}
	if r.DeletionTimestamp != nil {
		t := *r.DeletionTimestamp
		cp.DeletionTimestamp = &t
	}
	return cp
}

// DesiredState is a trigger input: a resource id plus the requested payload
// and a monotonically increasing generation.
type DesiredState struct {
	ID         string
	Generation int64
	Spec       WidgetSpec
}

// DeleteRequest marks a resource for deletion. Actual removal only happens
// once cleanup (side-effect release) has completed.
type DeleteRequest struct {
	ID         string
	Generation int64
}

// Decision is the explainable outcome of one reconcile pass.
type Decision struct {
	ID       string
	Action   string
	Reason   string
	Attempt  int
	Detail   string
	Duration time.Duration
	At       time.Time
}

// Stats expose reconciler internals for tests and diagnostics.
type Stats struct {
	Passes       int64
	Triggers     int64
	Retries      int64
	Writes       int64
	Skips        int64
	EnsureCalls  int64
	CleanupCalls int64
}

// BackoffConfig bounds transient-failure retry pacing.
type BackoffConfig struct {
	Initial    time.Duration
	Max        time.Duration
	Multiplier float64
}

// DefaultBackoff is tuned to be fast enough for local tests while still
// demonstrating increasing delay.
func DefaultBackoff() BackoffConfig {
	return BackoffConfig{
		Initial:    10 * time.Millisecond,
		Max:        2 * time.Second,
		Multiplier: 2,
	}
}

// Condition vocabulary.
const (
	ConditionReady       = "Ready"
	ConditionReconciling = "Reconciling"
	ConditionDeleting    = "Deleting"

	StatusTrue    = "True"
	StatusFalse   = "False"
	StatusUnknown = "Unknown"
)

// Reason vocabulary: every value must be stable and mutually distinguishable.
const (
	ReasonCreated             = "Created"
	ReasonReconciled          = "Reconciled"
	ReasonIdle                = "ObservedMatchesDesired"
	ReasonRetryingTransient   = "RetryingTransientFailure"
	ReasonPermanentFailure    = "PermanentFailure"
	ReasonDriftRebuilt        = "ExternalDriftRebuilt"
	ReasonMissingFieldRebuilt = "MissingKeyFieldRebuilt"
	ReasonCleanupInProgress   = "CleanupInProgress"
	ReasonCleanupTransient    = "CleanupTransientFailure"
	ReasonDeleted             = "Deleted"
	ReasonDeletedBeforeReady  = "DeletedBeforeReady"
	ReasonCompatPreserved     = "LegacyFieldsPreserved"
)

// Action vocabulary for decisions.
const (
	ActionCreate   = "Create"
	ActionUpdate   = "Update"
	ActionSkip     = "Skip"
	ActionRetry    = "Retry"
	ActionFail     = "Fail"
	ActionDelete   = "Delete"
	ActionDeleting = "Deleting"
)

// Annotation keys.
const (
	AnnSpecHash   = "reconciler.example/spec-hash"
	AnnEffectRef  = "reconciler.example/effect-ref"
	AnnGeneration = "reconciler.example/generation"
)

// FinalizerExample marks that an external effect must be released before
// the resource may disappear.
const FinalizerExample = "reconciler.example/effect-cleanup"
