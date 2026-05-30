package forward

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"testing"
	"time"
)

// TestForwardTCP starts a real upstream echo-ish server, runs the forwarder in
// front of it, and verifies a client talking to the local port gets the
// upstream's response — i.e. end-to-end relay works in both directions.
func TestForwardTCP(t *testing.T) {
	// Upstream: reads a line, replies "pong:<line>".
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
				line, _ := bufio.NewReader(c).ReadString('\n')
				fmt.Fprintf(c, "pong:%s", line)
			}(c)
		}
	}()

	// Forwarder listening on an ephemeral local port -> upstream.
	rule := Rule{Proto: "tcp", Listen: "127.0.0.1:0", Remote: upstream.Addr().String()}
	// We need the chosen local port, so bind it ourselves and hand serveTCP a
	// fixed address by reusing the resolved port.
	probe, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	localAddr := probe.Addr().String()
	probe.Close()
	rule.Listen = localAddr

	rule.TCPNoDelay = true
	rule.DialTimeout = time.Second
	rule.ReusePort = 1
	rule.DrainTimeout = time.Second

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	rr := newRuleRunner(rule, false)
	if err := rr.start(ctx); err != nil {
		t.Fatalf("runner start: %v", err)
	}
	defer rr.stop(time.Second)

	// Give the listener a moment to come up.
	deadline := time.Now().Add(2 * time.Second)
	var conn net.Conn
	for time.Now().Before(deadline) {
		conn, err = net.Dial("tcp", localAddr)
		if err == nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if conn == nil {
		t.Fatalf("could not connect to forwarder: %v", err)
	}
	defer conn.Close()

	fmt.Fprint(conn, "hello\n")
	got, err := bufio.NewReader(conn).ReadString('\n')
	if err != nil {
		t.Fatal(err)
	}
	if want := "pong:hello\n"; got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}
