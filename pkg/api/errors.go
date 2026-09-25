package api

import (
	"errors"
	"fmt"
)

// TransientError marks a failure that may succeed on retry. Requests are
// never dropped: the reconcile pass is requeued with backoff.
type TransientError struct {
	Op      string
	Message string
}

func (e *TransientError) Error() string {
	return fmt.Sprintf("transient error during %s: %s", e.Op, e.Message)
}

// AsTransient extracts a *TransientError from err, if present.
func AsTransient(err error) (*TransientError, bool) {
	var t *TransientError
	if errors.As(err, &t) {
		return t, true
	}
	return nil, false
}

// PermanentError marks a failure that cannot be fixed by retrying the same
// request. The reconciler records a stable condition and stops requeuing
// until the desired state changes.
type PermanentError struct {
	Op      string
	Reason  string
	Message string
}

func (e *PermanentError) Error() string {
	return fmt.Sprintf("permanent error during %s (%s): %s", e.Op, e.Reason, e.Message)
}

// AsPermanent extracts a *PermanentError from err, if present.
func AsPermanent(err error) (*PermanentError, bool) {
	var p *PermanentError
	if errors.As(err, &p) {
		return p, true
	}
	return nil, false
}

// ConflictError indicates the stored resource changed between read and
// write; reconciliation should re-read (a transient condition).
type ConflictError struct {
	ID string
}

func (e *ConflictError) Error() string {
	return fmt.Sprintf("conflict: resource %s changed between read and write", e.ID)
}

// IsTransient reports whether err should be retried with backoff. Both
// explicit TransientError and optimistic-concurrency conflicts are
// retryable; permanent failures are not.
func IsTransient(err error) bool {
	if err == nil {
		return false
	}
	if _, ok := AsPermanent(err); ok {
		return false
	}
	var t *TransientError
	if errors.As(err, &t) {
		return true
	}
	var c *ConflictError
	return errors.As(err, &c)
}
