package reconcile

import (
	"fmt"
	"strings"

	"github.com/h3yow/declarative-resource-reconciler-go/pkg/api"
	"github.com/h3yow/declarative-resource-reconciler-go/pkg/state"
)

// depVerdict is the outcome of evaluating one declared dependency.
type depVerdict int

const (
	depReady    depVerdict = iota // dependency converged to its desired state
	depWaiting                    // exists, still converging / transiently failing
	depFailed                     // exists, terminally failed
	depMissing                    // not present in the desired set
)

// depReport summarizes the dependency gate for one resource.
type depReport struct {
	// cycle is non-empty when the dependency declarations reachable
	// from this resource form a cycle; it holds the cycle path.
	cycle []string
	// blockers lists the dependencies that are not ready, with their
	// individual verdicts, in declaration order.
	blockers []depBlocker
}

type depBlocker struct {
	key     string
	verdict depVerdict
	detail  string
}

// ok reports whether every declared dependency is satisfied and no
// cycle was detected.
func (r depReport) ok() bool { return len(r.cycle) == 0 && len(r.blockers) == 0 }

// evaluateDeps inspects the declared dependencies of res against the
// current desired set. It never blocks and never errors: every
// situation (missing, failing, cycling) is reported as data so the
// caller can turn it into a stable, distinguishable condition.
func evaluateDeps(st *state.Store, res *api.Resource) depReport {
	rep := depReport{}
	if len(res.Spec.DependsOn) == 0 {
		return rep
	}

	// Cycle check first: a cyclic declaration can never be satisfied,
	// and evaluating it naively would be meaningless.
	if cyc := findCycle(st, res.Key()); len(cyc) > 0 {
		rep.cycle = cyc
		return rep
	}

	seen := map[string]struct{}{}
	for _, depKey := range res.Spec.DependsOn {
		if _, dup := seen[depKey]; dup {
			continue // duplicate declaration: evaluate once
		}
		seen[depKey] = struct{}{}

		dep, ok := st.GetDesired(depKey)
		if !ok {
			rep.blockers = append(rep.blockers, depBlocker{
				key: depKey, verdict: depMissing,
				detail: "declared dependency does not exist in the desired set",
			})
			continue
		}
		rep.blockers = appendIfNotReady(rep.blockers, depBlocker{
			key: depKey, verdict: verdictOf(dep), detail: detailOf(dep),
		})
	}
	return rep
}

func appendIfNotReady(blockers []depBlocker, b depBlocker) []depBlocker {
	if b.verdict == depReady {
		return blockers
	}
	return append(blockers, b)
}

// verdictOf classifies one dependency resource.
func verdictOf(dep *api.Resource) depVerdict {
	if dep.DeletionTimestamp != nil {
		// Being deleted: not ready now, and once gone it will be
		// reported as missing. Never mistake this for readiness.
		return depWaiting
	}
	cond := dep.Status.GetCondition(api.ConditionReady)
	switch {
	case dep.Status.Phase == api.PhaseError:
		return depFailed
	case cond != nil && cond.Status == "True" &&
		dep.Status.Phase == api.PhaseReady &&
		dep.Status.ObservedGen >= dep.Generation:
		// Truly converged: ready condition at the current generation.
		return depReady
	default:
		return depWaiting
	}
}

// detailOf renders a human explanation for a dependency's verdict.
func detailOf(dep *api.Resource) string {
	if dep.DeletionTimestamp != nil {
		return "dependency is being deleted"
	}
	cond := dep.Status.GetCondition(api.ConditionReady)
	if cond == nil {
		return "dependency has not been reconciled yet"
	}
	return fmt.Sprintf("dependency phase=%s ready=%s reason=%s",
		dep.Status.Phase, cond.Status, cond.Reason)
}

// findCycle detects a dependency cycle reachable from key by walking
// DependsOn edges over the desired set. Missing targets terminate the
// walk (they are reported separately as DependencyMissing). Returns the
// cycle path (e.g. [a, b, a]) or nil.
func findCycle(st *state.Store, start string) []string {
	const (
		unvisited = 0
		inStack   = 1
		done      = 2
	)
	color := map[string]int{}
	var stack []string

	var visit func(key string) []string
	visit = func(key string) []string {
		switch color[key] {
		case inStack:
			// Found a back edge: extract the cycle from the stack.
			for i, k := range stack {
				if k == key {
					return append(append([]string(nil), stack[i:]...), key)
				}
			}
			return []string{key, key}
		case done:
			return nil
		}
		color[key] = inStack
		stack = append(stack, key)
		res, ok := st.GetDesired(key)
		if ok && res.DeletionTimestamp == nil {
			for _, dep := range res.Spec.DependsOn {
				if cyc := visit(dep); cyc != nil {
					return cyc
				}
			}
		}
		stack = stack[:len(stack)-1]
		color[key] = done
		return nil
	}
	return visit(start)
}

// dependentsOf returns the keys of desired resources that declare a
// dependency on key. Used both to wake downstream resources when an
// upstream changes state and to gate deletion of an upstream.
func dependentsOf(st *state.Store, key string) []string {
	var out []string
	for _, res := range st.ListDesired() {
		for _, dep := range res.Spec.DependsOn {
			if dep == key {
				out = append(out, res.Key())
				break
			}
		}
	}
	return out
}

// formatCycle renders a cycle path for messages and logs.
func formatCycle(cyc []string) string { return strings.Join(cyc, " -> ") }
