package forward

import (
	"context"
	"net"
	"testing"
	"time"
)

// TestForwardUDP starts a real upstream UDP echo server, runs the forwarder in
// front of it, and verifies a client talking to the local port gets the
// upstream's reply routed back — exercising the per-client session demux,
// the netip.AddrPort key path and the pooled reply buffer end to end.
func TestForwardUDP(t *testing.T) {
	// Upstream: echo each datagram back to its sender.
	upAddr, err := net.ResolveUDPAddr("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	up, err := net.ListenUDP("udp", upAddr)
	if err != nil {
		t.Fatal(err)
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
	// Pick a free UDP port for the listener.
	probe, err := net.ListenUDP("udp", upAddr)
	if err != nil {
		t.Fatal(err)
	}
	rule.Listen = probe.LocalAddr().String()
	probe.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	rr := newRuleRunner(rule, false)
	if err := rr.start(ctx); err != nil {
		t.Fatalf("runner start: %v", err)
	}
	defer rr.stop(time.Second)

	client, err := net.Dial("udp", rule.Listen)
	if err != nil {
		t.Fatalf("dial forwarder: %v", err)
	}
	defer client.Close()

	for i := 0; i < 3; i++ {
		msg := []byte("ping")
		if _, err := client.Write(msg); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
		_ = client.SetReadDeadline(time.Now().Add(2 * time.Second))
		buf := make([]byte, 2048)
		n, err := client.Read(buf)
		if err != nil {
			t.Fatalf("read %d: %v", i, err)
		}
		if got := string(buf[:n]); got != "ping" {
			t.Fatalf("round %d: got %q, want ping", i, got)
		}
	}
}
