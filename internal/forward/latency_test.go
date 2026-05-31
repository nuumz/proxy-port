package forward

import (
	"context"
	"io"
	"net"
	"os"
	"runtime"
	"sort"
	"sync"
	"testing"
	"time"
)

// TestTCPLatencyProfile is a closed-loop latency harness (not a Benchmark: we
// want fixed concurrency and full percentiles, not the b.N model). It fires a
// fixed number of round-trips per connection across many connections, records
// every round-trip latency, and reports p50/p90/p99/p99.9/max alongside GC
// stats — the numbers that tell us whether GC pauses (not throughput) are the
// thing a Rust rewrite would actually buy.
//
// Gated behind PROXY_LAT=1 so it never runs in the normal suite/CI:
//
//	PROXY_LAT=1 go test ./internal/forward/ -run TestTCPLatencyProfile -v
//	PROXY_LAT=1 GOGC=off GOMEMLIMIT=512MiB go test ... -run TestTCPLatencyProfile -v
func TestTCPLatencyProfile(t *testing.T) {
	if os.Getenv("PROXY_LAT") == "" {
		t.Skip("set PROXY_LAT=1 to run the latency profile")
	}

	const (
		conns       = 200  // concurrent client connections (in-flight load)
		perConn     = 5000 // round-trips each connection performs
		payloadSize = 64   // small payload: latency-bound, GC-pause-sensitive
	)

	// Echo upstream.
	upstream, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer upstream.Close()
	go func() {
		for {
			c, err := upstream.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				buf := make([]byte, payloadSize)
				for {
					n, err := c.Read(buf)
					if err != nil {
						return
					}
					if _, err := c.Write(buf[:n]); err != nil {
						return
					}
				}
			}(c)
		}
	}()

	rule := baseRule(freePort(t), upstream.Addr().String())
	rule.DrainTimeout = time.Second
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	rr := newRuleRunner(rule, false)
	if err := rr.start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer rr.stop(time.Second)

	// Per-connection latency slices, merged after — no shared-slice contention
	// on the hot measurement path so the recording itself adds no jitter.
	perConnLat := make([][]time.Duration, conns)

	var gcBefore runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&gcBefore)

	var wg sync.WaitGroup
	start := time.Now()
	for i := 0; i < conns; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			conn, err := net.Dial("tcp", rule.Listen)
			if err != nil {
				t.Errorf("dial: %v", err)
				return
			}
			defer conn.Close()
			payload := make([]byte, payloadSize)
			buf := make([]byte, payloadSize)
			lat := make([]time.Duration, 0, perConn)
			for r := 0; r < perConn; r++ {
				t0 := time.Now()
				if _, err := conn.Write(payload); err != nil {
					t.Errorf("write: %v", err)
					return
				}
				if _, err := io.ReadFull(conn, buf); err != nil {
					t.Errorf("read: %v", err)
					return
				}
				lat = append(lat, time.Since(t0))
			}
			perConnLat[id] = lat
		}(i)
	}
	wg.Wait()
	elapsed := time.Since(start)

	var gcAfter runtime.MemStats
	runtime.ReadMemStats(&gcAfter)

	// Merge and sort all latencies.
	all := make([]time.Duration, 0, conns*perConn)
	for _, s := range perConnLat {
		all = append(all, s...)
	}
	sort.Slice(all, func(i, j int) bool { return all[i] < all[j] })

	pct := func(p float64) time.Duration {
		if len(all) == 0 {
			return 0
		}
		idx := int(p / 100 * float64(len(all)))
		if idx >= len(all) {
			idx = len(all) - 1
		}
		return all[idx]
	}

	total := len(all)
	rps := float64(total) / elapsed.Seconds()

	// GC pause percentiles over the run, from the circular PauseNs buffer.
	numGC := gcAfter.NumGC - gcBefore.NumGC
	pauseDelta := time.Duration(gcAfter.PauseTotalNs - gcBefore.PauseTotalNs)
	var maxPause time.Duration
	n := gcAfter.NumGC
	if n > 256 {
		n = 256
	}
	for i := uint32(0); i < n; i++ {
		if pn := time.Duration(gcAfter.PauseNs[(gcAfter.NumGC-1-i)%256]); pn > maxPause {
			maxPause = pn
		}
	}

	t.Logf("=== TCP latency profile (GOGC=%s GOMEMLIMIT=%s) ===",
		envOr("GOGC", "100"), envOr("GOMEMLIMIT", "off"))
	t.Logf("load: %d conns x %d rt = %d round-trips in %s (%.0f rt/s)",
		conns, perConn, total, elapsed.Round(time.Millisecond), rps)
	t.Logf("latency: p50=%s p90=%s p99=%s p99.9=%s max=%s",
		pct(50).Round(time.Microsecond), pct(90).Round(time.Microsecond),
		pct(99).Round(time.Microsecond), pct(99.9).Round(time.Microsecond),
		all[len(all)-1].Round(time.Microsecond))
	t.Logf("GC: cycles=%d totalPause=%s maxPause(recent)=%s heapAlloc=%dKB",
		numGC, pauseDelta.Round(time.Microsecond), maxPause.Round(time.Microsecond),
		gcAfter.HeapAlloc/1024)
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
