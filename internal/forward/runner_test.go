package forward

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"testing"
	"time"
)

// tagEcho starts a TCP server that replies "<tag>:<line>" to each line. It
// returns the listen address and a cleanup func.
func tagEcho(t *testing.T, tag string) (string, func()) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				line, _ := bufio.NewReader(c).ReadString('\n')
				fmt.Fprintf(c, "%s:%s", tag, line)
			}(c)
		}
	}()
	return ln.Addr().String(), func() { ln.Close() }
}

// freePort returns a currently-free 127.0.0.1 address by binding then releasing.
func freePort(t testing.TB) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	ln.Close()
	return addr
}

func ups(addrs ...string) []Upstream {
	u := make([]Upstream, len(addrs))
	for i, a := range addrs {
		u[i] = Upstream{Addr: a, Weight: 1}
	}
	return u
}

func baseRule(listen, remote string) Rule {
	return Rule{
		Proto: "tcp", Listen: listen, Upstreams: ups(remote),
		TCPNoDelay: true, DialTimeout: time.Second, ReusePort: 1, DrainTimeout: time.Second,
	}
}

// ask dials addr, sends "hello\n" and returns the single-line reply.
func ask(t *testing.T, addr string) string {
	t.Helper()
	c, err := net.DialTimeout("tcp", addr, time.Second)
	if err != nil {
		t.Fatalf("dial %s: %v", addr, err)
	}
	defer c.Close()
	fmt.Fprint(c, "hello\n")
	_ = c.SetReadDeadline(time.Now().Add(time.Second))
	line, err := bufio.NewReader(c).ReadString('\n')
	if err != nil {
		t.Fatalf("read from %s: %v", addr, err)
	}
	return line
}

// TestSupervisorReloadReplacesUpstream verifies that reloading a rule whose
// remote changed routes new connections to the new upstream, and that an
// unchanged reload leaves the runner serving.
func TestSupervisorReloadReplacesUpstream(t *testing.T) {
	addrA, stopA := tagEcho(t, "A")
	defer stopA()
	addrB, stopB := tagEcho(t, "B")
	defer stopB()

	listen := freePort(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sup := NewSupervisor(false)
	sup.ctx = ctx
	if err := sup.startLocked(baseRule(listen, addrA)); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer sup.Stop()

	if got := ask(t, listen); got != "A:hello\n" {
		t.Fatalf("before reload: got %q, want A:hello", got)
	}

	// Unchanged reload: same config -> runner must keep serving A.
	if err := sup.Reload([]Rule{baseRule(listen, addrA)}); err != nil {
		t.Fatalf("unchanged reload: %v", err)
	}
	if got := ask(t, listen); got != "A:hello\n" {
		t.Fatalf("after unchanged reload: got %q, want A:hello", got)
	}

	// Changed reload: same Key, new remote -> new conns hit B.
	if err := sup.Reload([]Rule{baseRule(listen, addrB)}); err != nil {
		t.Fatalf("changed reload: %v", err)
	}
	if got := ask(t, listen); got != "B:hello\n" {
		t.Fatalf("after changed reload: got %q, want B:hello", got)
	}

	// Removed reload: empty rule set -> nothing should be listening.
	if err := sup.Reload(nil); err != nil {
		t.Fatalf("remove reload: %v", err)
	}
	if c, err := net.DialTimeout("tcp", listen, 200*time.Millisecond); err == nil {
		c.Close()
		t.Fatalf("expected %s to be closed after removal, but dial succeeded", listen)
	}
}

// TestMaxConnections asserts the per-rule semaphore sheds load: with a cap of 1
// against an upstream that holds connections open, the second client is
// accepted then immediately closed (rejected).
func TestMaxConnections(t *testing.T) {
	// Upstream that accepts and blocks forever (never replies, never closes).
	upstream, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer upstream.Close()
	go func() {
		var held []net.Conn
		for {
			c, err := upstream.Accept()
			if err != nil {
				for _, h := range held {
					h.Close()
				}
				return
			}
			held = append(held, c) // keep it open to occupy the runner slot
		}
	}()

	rule := baseRule(freePort(t), upstream.Addr().String())
	rule.MaxConns = 1
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	rr := newRuleRunner(rule, false)
	if err := rr.start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer rr.stop(time.Second)

	// conn1 occupies the only slot; its handler blocks on the held upstream.
	conn1, err := net.DialTimeout("tcp", rule.Listen, time.Second)
	if err != nil {
		t.Fatalf("dial conn1: %v", err)
	}
	defer conn1.Close()
	// Give the handler time to acquire the semaphore slot.
	time.Sleep(100 * time.Millisecond)

	// conn2 should be accepted then immediately closed (cap reached) -> Read EOF.
	conn2, err := net.DialTimeout("tcp", rule.Listen, time.Second)
	if err != nil {
		t.Fatalf("dial conn2: %v", err)
	}
	defer conn2.Close()
	_ = conn2.SetReadDeadline(time.Now().Add(time.Second))
	buf := make([]byte, 1)
	if _, err := conn2.Read(buf); err == nil {
		t.Fatal("conn2: expected rejection (EOF/closed), but read succeeded")
	}
}
