// Package forward implements a lightweight, high-throughput TCP/UDP port
// forwarder. It accepts connections locally and relays them to a remote
// upstream so services on another machine (an API, a Redis instance, a
// database) appear as if they were running on localhost — without turning the
// host into a routing gateway.
//
// A Supervisor owns one ruleRunner per rule and can add, remove or replace
// runners on a live reload without dropping in-flight connections.
package forward

import (
	"context"
	"fmt"
	"log"
	"sync"
)

// Supervisor owns the set of running rule runners, keyed by Rule.Key(). It is
// the single entry point for starting, reloading and stopping the forwarder.
type Supervisor struct {
	verbose bool

	mu      sync.Mutex
	runners map[string]*ruleRunner
	ctx     context.Context // parent context for all runners
}

// NewSupervisor creates an empty supervisor.
func NewSupervisor(verbose bool) *Supervisor {
	return &Supervisor{verbose: verbose, runners: make(map[string]*ruleRunner)}
}

// Run starts a runner for every rule and blocks until ctx is cancelled, then
// stops all runners with a bounded drain. Bind failures during startup are
// fatal and returned (after stopping any runners already started).
func (s *Supervisor) Run(ctx context.Context, rules []Rule) error {
	s.mu.Lock()
	s.ctx = ctx
	for _, r := range rules {
		if err := s.startLocked(r); err != nil {
			s.mu.Unlock()
			s.Stop()
			return fmt.Errorf("start %s: %w", r.Key(), err)
		}
	}
	s.mu.Unlock()

	<-ctx.Done()
	s.Stop()
	return nil
}

// Reload diffs the running runners against newRules by Key and converges:
//   - added    → start a new runner.
//   - removed  → stop the old runner (live conns drain, no new accepts).
//   - changed  → stop the old runner and start a fresh one; old conns drain on
//     the old upstream while new conns use the new config.
//   - unchanged→ leave the runner running untouched (zero disruption).
//
// A bind error on an added/changed rule is logged and that rule is skipped; the
// rest of the reload still applies so one bad rule never takes down the proxy.
func (s *Supervisor) Reload(newRules []Rule) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	desired := make(map[string]Rule, len(newRules))
	for _, r := range newRules {
		desired[r.Key()] = r
	}

	// Removed or changed: stop runners no longer wanted as-is.
	for key, rr := range s.runners {
		want, ok := desired[key]
		if !ok {
			log.Printf("reload: removing %s", rr.rule)
			rr.stop(rr.rule.DrainTimeout)
			delete(s.runners, key)
		} else if !rr.rule.SameConfig(want) {
			log.Printf("reload: replacing %s -> %s", rr.rule, want)
			rr.stop(rr.rule.DrainTimeout)
			delete(s.runners, key)
		}
	}

	// Added or changed: start runners that are now missing.
	var firstErr error
	for key, r := range desired {
		if _, ok := s.runners[key]; ok {
			continue // unchanged, still running
		}
		log.Printf("reload: starting %s", r)
		if err := s.startLocked(r); err != nil {
			log.Printf("reload: start %s failed: %v", r.Key(), err)
			if firstErr == nil {
				firstErr = err
			}
		}
	}
	return firstErr
}

// Stop stops every runner concurrently, each bounded by its own DrainTimeout.
func (s *Supervisor) Stop() {
	s.mu.Lock()
	runners := make([]*ruleRunner, 0, len(s.runners))
	for key, rr := range s.runners {
		runners = append(runners, rr)
		delete(s.runners, key)
	}
	s.mu.Unlock()

	var wg sync.WaitGroup
	for _, rr := range runners {
		rr := rr
		wg.Add(1)
		go func() {
			defer wg.Done()
			rr.stop(rr.rule.DrainTimeout)
		}()
	}
	wg.Wait()
}

// startLocked starts a runner for r and records it. Caller holds s.mu.
func (s *Supervisor) startLocked(r Rule) error {
	rr := newRuleRunner(r, s.verbose)
	if err := rr.start(s.ctx); err != nil {
		return err
	}
	s.runners[r.Key()] = rr
	return nil
}
