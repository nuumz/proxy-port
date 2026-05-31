package forward

import (
	"context"
	"strings"
	"testing"
	"time"
)

// tagOf returns the "<tag>" prefix of a tagEcho reply like "A:hello\n".
func tagOf(reply string) string {
	return strings.SplitN(reply, ":", 2)[0]
}

// TestForwardTCPLoadBalance verifies a rule with multiple upstreams spreads new
// connections across all of them (weighted round-robin, equal weights).
func TestForwardTCPLoadBalance(t *testing.T) {
	addrA, stopA := tagEcho(t, "A")
	defer stopA()
	addrB, stopB := tagEcho(t, "B")
	defer stopB()
	addrC, stopC := tagEcho(t, "C")
	defer stopC()

	rule := baseRule(freePort(t), addrA)
	rule.Upstreams = ups(addrA, addrB, addrC)
	rule.Balance = BalanceWeighted

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	rr := newRuleRunner(rule, false)
	if err := rr.start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer rr.stop(time.Second)

	seen := map[string]int{}
	for i := 0; i < 30; i++ {
		seen[tagOf(ask(t, rule.Listen))]++
	}
	for _, tag := range []string{"A", "B", "C"} {
		if seen[tag] == 0 {
			t.Errorf("upstream %s never received a connection; distribution=%v", tag, seen)
		}
	}
}

// TestForwardTCPFailover verifies that when one upstream is down, the dial
// failure fails over to a healthy backend so clients keep being served.
func TestForwardTCPFailover(t *testing.T) {
	addrA, stopA := tagEcho(t, "A")
	addrB, stopB := tagEcho(t, "B")
	defer stopB()

	rule := baseRule(freePort(t), addrA)
	rule.Upstreams = ups(addrA, addrB)
	rule.Balance = BalanceWeighted
	rule.FailCooldown = time.Minute // keep A parked once it fails

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	rr := newRuleRunner(rule, false)
	if err := rr.start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer rr.stop(time.Second)

	// Take A down: every request must still succeed, served by B.
	stopA()
	for i := 0; i < 10; i++ {
		if got := tagOf(ask(t, rule.Listen)); got != "B" {
			t.Fatalf("request %d routed to %q, want B (A is down)", i, got)
		}
	}
}
