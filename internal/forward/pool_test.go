package forward

import (
	"sync"
	"testing"
	"time"
)

func mkPool(t *testing.T, balance string, cooldown time.Duration, up ...Upstream) *pool {
	t.Helper()
	p, err := newPool(up, balance, cooldown, "tcp")
	if err != nil {
		t.Fatalf("newPool: %v", err)
	}
	return p
}

// TestPoolWeighted checks weighted round-robin honours weights: with a:1 b:3 a
// run of picks lands on b three times as often as a, deterministically.
func TestPoolWeighted(t *testing.T) {
	p := mkPool(t, BalanceWeighted, time.Second, Upstream{"a", 1}, Upstream{"b", 3})
	counts := map[string]int{}
	for i := 0; i < 400; i++ {
		idx, ok := p.pick(0)
		if !ok {
			t.Fatal("pick returned not-ok with healthy upstreams")
		}
		counts[p.addr(idx)]++
		p.done(idx)
	}
	if counts["a"] != 100 || counts["b"] != 300 {
		t.Errorf("weighted distribution = %v, want a:100 b:300", counts)
	}
}

// TestPoolWeightedGCD checks equal-ratio weights expand to the smallest
// equivalent index slice (2:4 -> 1:2, three entries, not six).
func TestPoolWeightedGCD(t *testing.T) {
	p := mkPool(t, BalanceWeighted, time.Second, Upstream{"a", 2}, Upstream{"b", 4})
	if len(p.expanded) != 3 {
		t.Errorf("expanded length = %d, want 3 (GCD-reduced 1:2)", len(p.expanded))
	}
}

// TestPoolWeightedShardedDistribution checks weighted balancing still honours
// weights when picks are spread across cursor shards by varying client keys.
func TestPoolWeightedShardedDistribution(t *testing.T) {
	p := mkPool(t, BalanceWeighted, time.Second, Upstream{"a", 1}, Upstream{"b", 3})
	counts := map[string]int{}
	for i := 0; i < 4000; i++ {
		idx, ok := p.pick(uint64(i)) // distinct keys -> spread across shards
		if !ok {
			t.Fatal("pick failed")
		}
		counts[p.addr(idx)]++
		p.done(idx)
	}
	// b should get roughly 3x a; allow slack since shards round-robin independently.
	ratio := float64(counts["b"]) / float64(counts["a"])
	if ratio < 2.6 || ratio > 3.4 {
		t.Errorf("weighted-by-shard distribution %v (b/a=%.2f), want ~3.0", counts, ratio)
	}
}

// TestPoolLeastConn checks the second concurrent pick avoids the upstream the
// first is still holding.
func TestPoolLeastConn(t *testing.T) {
	p := mkPool(t, BalanceLeastConn, time.Second, Upstream{"a", 1}, Upstream{"b", 1})
	i1, ok1 := p.pick(0)
	i2, ok2 := p.pick(0)
	if !ok1 || !ok2 {
		t.Fatal("pick failed")
	}
	if i1 == i2 {
		t.Errorf("least_conn placed both picks on the same upstream (%d)", i1)
	}
	// Releasing one and picking again must reuse the now-idle upstream.
	p.done(i1)
	i3, _ := p.pick(0)
	if i3 != i1 {
		t.Errorf("least_conn picked %d, want the freed upstream %d", i3, i1)
	}
}

// TestPoolIPHashSticky checks a given client key maps to a stable upstream.
func TestPoolIPHashSticky(t *testing.T) {
	p := mkPool(t, BalanceIPHash, time.Second, Upstream{"a", 1}, Upstream{"b", 1}, Upstream{"c", 1})
	const key = 0xdeadbeef
	first, ok := p.pick(key)
	if !ok {
		t.Fatal("pick failed")
	}
	for i := 0; i < 20; i++ {
		idx, _ := p.pick(key)
		if idx != first {
			t.Fatalf("iphash not sticky: pick %d != first %d", idx, first)
		}
	}
}

// TestPoolPassiveFailover checks a failed dial parks a backend for the cooldown
// window (picks skip it), all-down yields ok=false, and it recovers afterwards.
func TestPoolPassiveFailover(t *testing.T) {
	p := mkPool(t, BalanceWeighted, 50*time.Millisecond, Upstream{"a", 1}, Upstream{"b", 1})

	p.fail(0) // park upstream 0
	for i := 0; i < 20; i++ {
		idx, ok := p.pick(0)
		if !ok {
			t.Fatal("pick returned not-ok while upstream 1 is healthy")
		}
		if idx == 0 {
			t.Fatal("picked a parked-down upstream before its cooldown elapsed")
		}
		p.done(idx)
	}

	p.fail(1) // now both are down
	if _, ok := p.pick(0); ok {
		t.Fatal("pick returned ok with every upstream parked down")
	}

	time.Sleep(70 * time.Millisecond) // past the cooldown
	if _, ok := p.pick(0); !ok {
		t.Fatal("pick did not recover after the cooldown window")
	}
}

func TestNewPoolErrors(t *testing.T) {
	if _, err := newPool(nil, BalanceWeighted, time.Second, "tcp"); err == nil {
		t.Error("expected error for empty upstreams")
	}
	if _, err := newPool([]Upstream{{"a", 1}}, "bogus", time.Second, "tcp"); err == nil {
		t.Error("expected error for unknown balance strategy")
	}
}

// BenchmarkPoolPick measures the hot path (pick + done) under GOMAXPROCS-way
// parallelism — the number that matters for "does balancing scale across
// cores". It is lock-free and allocation-free, so the only cross-core cost is
// cache-line traffic on the shared cursor / in-flight counters.
func BenchmarkPoolPick(b *testing.B) {
	four := []Upstream{{"a", 1}, {"b", 1}, {"c", 1}, {"d", 1}}
	for _, bc := range []struct{ name, bal string }{
		{"weighted", BalanceWeighted},
		{"least_conn", BalanceLeastConn},
		{"iphash", BalanceIPHash},
	} {
		b.Run(bc.name, func(b *testing.B) {
			p, err := newPool(four, bc.bal, time.Second, "tcp")
			if err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.RunParallel(func(pb *testing.PB) {
				var key uint64 = 1
				for pb.Next() {
					key++
					if idx, ok := p.pick(key); ok {
						p.done(idx)
					}
				}
			})
		})
	}
}

// TestPoolPickConcurrent hammers pick/done from many goroutines so the race
// detector verifies the lock-free path has no data races and least_conn's
// counters stay balanced (every pick is paired with exactly one done).
func TestPoolPickConcurrent(t *testing.T) {
	p := mkPool(t, BalanceLeastConn, time.Second,
		Upstream{"a", 1}, Upstream{"b", 1}, Upstream{"c", 1}, Upstream{"d", 1})
	const goroutines, iters = 32, 5000
	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(seed uint64) {
			defer wg.Done()
			for i := 0; i < iters; i++ {
				if idx, ok := p.pick(seed + uint64(i)); ok {
					p.done(idx)
				}
			}
		}(uint64(g))
	}
	wg.Wait()
	// With every pick paired to a done, all in-flight counters return to zero.
	for i := range p.ups {
		if n := p.ups[i].inflight.Load(); n != 0 {
			t.Errorf("upstream %d inflight = %d after balanced pick/done, want 0", i, n)
		}
	}
}
