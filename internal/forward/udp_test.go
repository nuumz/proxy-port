package forward

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"
)

// udpEcho starts a UDP server that echoes each datagram back to its sender.
func udpEcho(t *testing.T) (string, func()) {
	t.Helper()
	addr, err := net.ResolveUDPAddr("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	up, err := net.ListenUDP("udp", addr)
	if err != nil {
		t.Fatal(err)
	}
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
	return up.LocalAddr().String(), func() { up.Close() }
}

// freeUDPAddr returns a currently-free 127.0.0.1 UDP address by binding then
// releasing it.
func freeUDPAddr(t *testing.T) string {
	t.Helper()
	addr, err := net.ResolveUDPAddr("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	c, err := net.ListenUDP("udp", addr)
	if err != nil {
		t.Fatal(err)
	}
	a := c.LocalAddr().String()
	c.Close()
	return a
}

// roundTrip dials the forwarder, sends msg and asserts it comes back echoed.
func roundTrip(t *testing.T, listen, msg string) {
	t.Helper()
	conn, err := net.Dial("udp", listen)
	if err != nil {
		t.Fatalf("dial forwarder: %v", err)
	}
	defer conn.Close()
	if _, err := conn.Write([]byte(msg)); err != nil {
		t.Fatalf("write: %v", err)
	}
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	buf := make([]byte, 2048)
	n, err := conn.Read(buf)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if got := string(buf[:n]); got != msg {
		t.Fatalf("got %q, want %q", got, msg)
	}
}

// TestForwardUDP runs the forwarder in front of a UDP echo server and verifies
// replies are routed back to the client over several round-trips — exercising
// the per-client session demux, the netip.AddrPort key path and the pooled
// reply buffer end to end.
func TestForwardUDP(t *testing.T) {
	remote, stop := udpEcho(t)
	defer stop()

	rule := Rule{
		Proto: "udp", Listen: freeUDPAddr(t), Remote: remote,
		DialTimeout: time.Second, ReusePort: 1, DrainTimeout: time.Second,
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	rr := newRuleRunner(rule, false)
	if err := rr.start(ctx); err != nil {
		t.Fatalf("runner start: %v", err)
	}
	defer rr.stop(time.Second)

	for i := 0; i < 3; i++ {
		roundTrip(t, rule.Listen, fmt.Sprintf("ping-%d", i))
	}
}

// TestForwardUDPReusePort verifies that with reuseport>1 the rule binds N
// sockets (each with its own serve loop and private session map) and still
// relays every client correctly — the kernel hashes each client to a fixed
// socket, so all clients must round-trip regardless of which loop owns them.
func TestForwardUDPReusePort(t *testing.T) {
	remote, stop := udpEcho(t)
	defer stop()

	rule := Rule{
		Proto: "udp", Listen: freeUDPAddr(t), Remote: remote,
		DialTimeout: time.Second, ReusePort: 4, DrainTimeout: time.Second,
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	rr := newRuleRunner(rule, false)
	if err := rr.start(ctx); err != nil {
		t.Fatalf("runner start: %v", err)
	}
	defer rr.stop(time.Second)

	// Many distinct client sockets so the kernel spreads them across the four
	// reuseport loops; every one must still get its echo back.
	for i := 0; i < 16; i++ {
		roundTrip(t, rule.Listen, fmt.Sprintf("client-%d", i))
	}
}
