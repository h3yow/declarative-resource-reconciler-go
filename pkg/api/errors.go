package api

// ErrorClass categorizes a reconciliation failure so the queue can
// decide between backoff retry and permanent stop.
type ErrorClass int

const (
	ClassOK ErrorClass = iota
	ClassTransient
	ClassPermanent
)

// ReconcileError wraps a failure with a stable reason and a class.
type ReconcileError struct {
	Class   ErrorClass
	Reason  ConditionReason
	Message string
}

func (e *ReconcileError) Error() string {
	return string(e.Reason) + ": " + e.Message
}

// Transient builds a transient (retryable) error.
func Transient(reason ConditionReason, msg string) error {
	return &ReconcileError{Class: ClassTransient, Reason: reason, Message: msg}
}

// Permanent builds a permanent error: the condition is recorded and the
// resource is not requeued.
func Permanent(reason ConditionReason, msg string) error {
	return &ReconcileError{Class: ClassPermanent, Reason: reason, Message: msg}
}

// ClassOf extracts the class of err; nil means OK.
func ClassOf(err error) ErrorClass {
	if err == nil {
		return ClassOK
	}
	if re, ok := err.(*ReconcileError); ok {
		return re.Class
	}
	return ClassTransient
}
