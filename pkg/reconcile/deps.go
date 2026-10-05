package reconcile

import (
	"sort"

	"github.com/h3yow/declarative-resource-reconciler-go/pkg/api"
	"github.com/h3yow/declarative-resource-reconciler-go/pkg/state"
)

// depVerdict is the outcome of evaluating a resource's declared
// dependencies against the current desired/observed state.
type depVerdict int

const (
	// depOK: no dependencies declared, or every dependency is Ready.
	depOK depVerdict = iota
	// depWaiting: at least one dependency exists but has not converged
	// yet. Not a failure of this resource; it must wait and be
	// re-triggered when a dependency changes.
	depWaiting
	// depFailed: at least one dependency reached a terminal failure.
	depFailed
	// depMissing: a referenced dependency does not exist in the desired
	// state and therefore can never become Ready.
	depMissing
	// depCyclic: the dependency declarations form a cycle that includes
	// this resource.
	depCyclic
)

// depEvaluation is the explainable result of one dependency check.
type depEvaluation struct {
	verdict depVerdict
	// blockers are the dependency keys responsible for the verdict,
	// sorted for deterministic messages and logs.
	blockers []string
}

// evaluateDeps inspects res.Spec.DependsOn against the desired store.
// Resources without any DependsOn get depOK with zero cost, preserving
// the original single-resource behavior.
func evaluateDeps(store *state.Store, res *api.Resource) depEvaluation {
	deps := res.Spec.DependsOn
	if len(deps) == 0 {
		return depEvaluation{verdict: depOK}
	}

	// Cycle check first: a cyclic declaration is a configuration problem
	// and dominates every other verdict.
	if cycle := cycleThrough(store, res.Key()); len(cycle) > 0 {
		return depEvaluation{verdict: depCyclic, blockers: cycle}
	}

	eval := depEvaluation{verdict: depOK}
	for _, depKey := range deps {
		dep, ok := store.GetDesired(depKey)
		if !ok {
			eval.blockers = append(eval.blockers, depKey)
			if eval.verdict < depMissing {
				eval.verdict = depMissing
			}
			continue
		}
		switch {
		case dep.Status.Phase == api.PhaseError:
			// Terminal failure of the dependency (permanent failure,
			// its own dependency problem, ...): propagate as a stable
			// failure conclusion.
			eval.blockers = append(eval.blockers, depKey)
			if eval.verdict < depFailed {
				eval.verdict = depFailed
			}
		case dep.DeletionTimestamp != nil || !isReady(dep):
			eval.blockers = append(eval.blockers, depKey)
			if eval.verdict < depWaiting {
				eval.verdict = depWaiting
			}
		}
	}
	sort.Strings(eval.blockers)
	return eval
}

// isReady reports whether the dependency has fully converged.
func isReady(res *api.Resource) bool {
	if res.Status.Phase != api.PhaseReady {
		return false
	}
	c := res.Status.GetCondition(api.ConditionReady)
	return c != nil && c.Status == "True"
}

// cycleThrough returns the cycle path (sorted member keys) if key takes
// part in a dependency cycle, or nil. Only edges to resources that
// actually exist in the desired state are followed; a dangling reference
// is a "missing" problem, not a cycle.
func cycleThrough(store *state.Store, key string) []string {
	desired := map[string]*api.Resource{}
	for _, r := range store.ListDesired() {
		desired[r.Key()] = r
	}
	// Reachability walk from key; a cycle involving key exists iff key
	// is reachable from itself in >=1 steps.
	visited := map[string]bool{}
	stack := []string{}
	if r, ok := desired[key]; ok {
		stack = append(stack, r.Spec.DependsOn...)
	}
	for len(stack) > 0 {
		cur := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		if cur == key {
			return cycleMembers(desired, key)
		}
		if visited[cur] {
			continue
		}
		visited[cur] = true
		if r, ok := desired[cur]; ok {
			stack = append(stack, r.Spec.DependsOn...)
		}
	}
	return nil
}

// cycleMembers returns the sorted set of keys that sit on a cycle with
// key (key included): nodes reachable from key that can also reach key.
func cycleMembers(desired map[string]*api.Resource, key string) []string {
	reachableFrom := func(start string) map[string]bool {
		out := map[string]bool{}
		stack := []string{start}
		for len(stack) > 0 {
			cur := stack[len(stack)-1]
			stack = stack[:len(stack)-1]
			if out[cur] {
				continue
			}
			out[cur] = true
			if r, ok := desired[cur]; ok {
				stack = append(stack, r.Spec.DependsOn...)
			}
		}
		return out
	}
	forward := reachableFrom(key)
	members := []string{}
	for k := range forward {
		if reachableFrom(k)[key] {
			members = append(members, k)
		}
	}
	sort.Strings(members)
	return members
}

// dependentsOf returns the sorted keys of desired resources that declare
// a dependency on key. Used to wake waiters when key changes.
func dependentsOf(store *state.Store, key string) []string {
	var out []string
	for _, r := range store.ListDesired() {
		for _, d := range r.Spec.DependsOn {
			if d == key {
				out = append(out, r.Key())
				break
			}
		}
	}
	sort.Strings(out)
	return out
}

// blockingDependents returns the sorted keys of downstream resources
// whose existence still blocks the deletion of res. A dependent blocks
// as long as its desired record exists (its cleanup may still be in
// progress). Dependents caught in a dependency cycle with res do NOT
// block: the cycle is already reported as a configuration error and
// must not deadlock deletion.
func blockingDependents(store *state.Store, res *api.Resource) []string {
	desired := map[string]*api.Resource{}
	for _, r := range store.ListDesired() {
		desired[r.Key()] = r
	}
	var out []string
	for _, other := range desired {
		if other.Key() == res.Key() {
			continue
		}
		depends := false
		for _, d := range other.Spec.DependsOn {
			if d == res.Key() {
				depends = true
				break
			}
		}
		if !depends {
			continue
		}
		// Same-cycle dependents are skipped to keep deletion of a
		// (misconfigured) cyclic graph terminable.
		if reaches(desired, res.Key(), other.Key()) {
			continue
		}
		out = append(out, other.Key())
	}
	sort.Strings(out)
	return out
}

// reaches reports whether target is reachable from start by following
// DependsOn edges over existing desired resources.
func reaches(desired map[string]*api.Resource, start, target string) bool {
	visited := map[string]bool{}
	stack := []string{start}
	for len(stack) > 0 {
		cur := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		if cur == target {
			return true
		}
		if visited[cur] {
			continue
		}
		visited[cur] = true
		if r, ok := desired[cur]; ok {
			stack = append(stack, r.Spec.DependsOn...)
		}
	}
	return false
}
