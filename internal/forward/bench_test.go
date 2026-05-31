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

// BenchmarkForwardUDP measures datagram round-trip throughput through the UDP
// relay. With -benchmem it shows the per-packet allocation profile of the hot
// path: the netip.AddrPort key and ReadFrom/WriteToUDPAddrPort avoid the
// per-datagram *net.UDPAddr and String() allocations the demux would otherwise
// incur, so allocs/op stays low under load.
func BenchmarkForwardUDP(b *testing.B) {
	upAddr, _ := net.ResolveUDPAddr("udp", "127.0.0.1:0")
	up, err := net.ListenUDP("udp", upAddr)
	if err != nil {
		b.Fatal(err)
	}
	defer up.Close()
	go func() {
		buf := make([]byte, 2048)
		for {
			n, peer, err := up.ReadFromUDP(buf)
			if err != nil {
				return
			}
			if _, err := up.WriteToUDP(buf[:n], peer); err != nil {
				return
			}
		}
	}()

	rule := Rule{
		Proto: "udp", Listen: "127.0.0.1:0", Remote: up.LocalAddr().String(),
		DialTimeout: time.Second, ReusePort: 1, DrainTimeout: time.Second,
	}
	probe, _ := net.ListenUDP("udp", upAddr)
	rule.Listen = probe.LocalAddr().String()
	probe.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	rr := newRuleRunner(rule, false)
	if err := rr.start(ctx); err != nil {
		b.Fatalf("start: %v", err)
	}
	defer rr.stop(time.Second)

	payload := make([]byte, 64)
	b.ResetTimer()
	b.SetBytes(int64(len(payload)) * 2)
	b.RunParallel(func(pb *testing.PB) {
		conn, err := net.Dial("udp", rule.Listen)
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
			_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
			if _, err := io.ReadFull(conn, buf); err != nil {
				b.Error(err)
				return
			}
		}
	})
}
