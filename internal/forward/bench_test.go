package forward

import (
	"context"
	"io"
	"net"
	"testing"
	"time"
)

// BenchmarkForwardTCP measures request/response round-trip throughput through
// the forwarder under concurrent load. Run it via `make load` or:
//
//	go test -run '^$' -bench BenchmarkForwardTCP -benchmem ./internal/forward/
//
// b.RunParallel saturates the proxy with many in-flight clients so the result
// reflects accept + relay throughput, not single-stream latency.
func BenchmarkForwardTCP(b *testing.B) {
	// Upstream: echo each 64-byte request straight back.
	upstream, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.Fatal(err)
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
				buf := make([]byte, 64)
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

	rule := baseRule(freePort(b), upstream.Addr().String())
	rule.DrainTimeout = time.Second
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	rr := newRuleRunner(rule, false)
	if err := rr.start(ctx); err != nil {
		b.Fatalf("start: %v", err)
	}
	defer rr.stop(time.Second)

	payload := make([]byte, 64)
	b.ResetTimer()
	b.SetBytes(int64(len(payload)) * 2) // request + response
	b.RunParallel(func(pb *testing.PB) {
		conn, err := net.Dial("tcp", rule.Listen)
		if err != nil {
			b.Error(err)
			return
		}
		defer conn.Close()
		buf := make([]byte, len(payload))
		for pb.Next() {
			if _, err := conn.Write(payload); err != nil {
				b.Error(err)
				return
			}
			if _, err := io.ReadFull(conn, buf); err != nil {
				b.Error(err)
				return
			}
		}
	})
}
