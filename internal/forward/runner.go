package forward

import (
	"context"
	"errors"
	"log"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

// ruleRunner owns the live machinery for a single rule: its listener socket(s),
// a per-rule concurrency semaphore, and a WaitGroup tracking in-flight
// connections. Its accept loop and its connection handlers have separate
// lifetimes — stopping closes the listeners (no new accepts) but lets existing
// connections drain. That separation is what makes SIGHUP reload non-disruptive.
type ruleRunner struct {
	rule    Rule
	verbose bool

	// sem caps concurrent connections per rule. nil means unlimited.
	sem chan struct{}

	listeners []net.Listener

	cancel context.CancelFunc // cancels the runner's own context
	connWG sync.WaitGroup     // in-flight connection handlers (TCP)
	loopWG sync.WaitGroup     // accept goroutines / UDP serve loop
	connID uint64
}

func newRuleRunner(r Rule, verbose bool) *ruleRunner {
	rr := &ruleRunner{rule: r, verbose: verbose}
	if r.MaxConns > 0 {
		rr.sem = make(chan struct{}, r.MaxConns)
	}
	return rr
}

// start brings the runner up: it opens the listener(s) and launches the accept
// loop(s). It returns once binding succeeds (or fails) so the supervisor can
// surface bind errors synchronously.
func (rr *ruleRunner) start(parent context.Context) error {
	ctx, cancel := context.WithCancel(parent)
	rr.cancel = cancel

	if rr.rule.Proto == "udp" {
		return rr.startUDP(ctx)
	}
	return rr.startTCP(ctx)
}

// listenerCount returns how many SO_REUSEPORT sockets to open for a rule. It
// clamps to at least one and, on platforms without SO_REUSEPORT, to exactly one
// so reuseport>1 degrades to a single socket instead of failing the second bind.
func listenerCount(r Rule) int {
	n := r.ReusePort
	if n < 1 {
		n = 1
	}
	if !reusePortAvailable && n > 1 {
		n = 1
	}
	return n
}

func (rr *ruleRunner) startTCP(ctx context.Context) error {
	n := listenerCount(rr.rule)
	lc := listenConfig(rr.rule)
	for i := 0; i < n; i++ {
		ln, err := lc.Listen(ctx, "tcp", rr.rule.Listen)
		if err != nil {
			// Roll back any sockets already opened so we don't leak FDs.
			rr.closeListeners()
			rr.cancel()
			return err
		}
		rr.listeners = append(rr.listeners, ln)
	}
	if n > 1 {
		log.Printf("listening %s (reuseport x%d)", rr.rule, n)
	} else {
		log.Printf("listening %s", rr.rule)
	}

	for _, ln := range rr.listeners {
		ln := ln
		rr.loopWG.Add(1)
		go func() {
			defer rr.loopWG.Done()
			rr.acceptLoop(ctx, ln)
		}()
	}
	return nil
}

func (rr *ruleRunner) acceptLoop(ctx context.Context, ln net.Listener) {
	for {
		client, err := ln.Accept()
		if err != nil {
			// stop() closes the listener to halt accepts; Accept then returns
			// net.ErrClosed. We must exit on that even though ctx is not yet
			// cancelled (stop defers cancel until after the drain window), or
			// the loop would spin forever and stop()'s loopWG.Wait would hang.
			if ctx.Err() != nil || errors.Is(err, net.ErrClosed) {
				return // shutdown: listener closed
			}
			// Transient accept errors (e.g. too many open files) should not
			// kill the listener; back off briefly and retry.
			var ne net.Error
			if errors.As(err, &ne) && ne.Timeout() {
				continue
			}
			log.Printf("%s: accept error: %v", rr.rule.Listen, err)
			time.Sleep(50 * time.Millisecond)
			continue
		}

		// Acquire a semaphore slot without blocking. If the rule is at its
		// connection cap, shed load immediately by closing the freshly accepted
		// conn — this protects against FD exhaustion and keeps latency bounded
		// for the connections we do serve.
		if rr.sem != nil {
			select {
			case rr.sem <- struct{}{}:
			default:
				log.Printf("%s: connection limit (%d) reached, rejecting %s",
					rr.rule.Listen, rr.rule.MaxConns, client.RemoteAddr())
				_ = client.Close()
				continue
			}
		}

		id := atomic.AddUint64(&rr.connID, 1)
		rr.connWG.Add(1)
		go func() {
			defer rr.connWG.Done()
			if rr.sem != nil {
				defer func() { <-rr.sem }()
			}
			handleTCP(ctx, id, client, rr.rule, rr.verbose)
		}()
	}
}

func (rr *ruleRunner) startUDP(ctx context.Context) error {
	remoteAddr, err := net.ResolveUDPAddr("udp", rr.rule.Remote)
	if err != nil {
		rr.cancel()
		return err
	}

	// Open N reuseport sockets and bind them all before launching any serve
	// loop, so a bind failure is reported synchronously (like startTCP) and we
	// can roll back cleanly without leaking FDs.
	n := listenerCount(rr.rule)
	lc := listenConfig(rr.rule)
	conns := make([]*net.UDPConn, 0, n)
	for i := 0; i < n; i++ {
		pc, err := lc.ListenPacket(ctx, "udp", rr.rule.Listen)
		if err != nil {
			for _, c := range conns {
				_ = c.Close()
			}
			rr.cancel()
			return err
		}
		conn := pc.(*net.UDPConn)
		if rr.rule.ReadBuffer > 0 {
			_ = conn.SetReadBuffer(rr.rule.ReadBuffer)
		}
		if rr.rule.WriteBuffer > 0 {
			_ = conn.SetWriteBuffer(rr.rule.WriteBuffer)
		}
		conns = append(conns, conn)
	}

	if n > 1 {
		log.Printf("listening %s (reuseport x%d)", rr.rule, n)
	} else {
		log.Printf("listening %s", rr.rule)
	}

	// One serve loop per socket. The kernel hashes each client's 4-tuple to a
	// fixed reuseport socket, so all of a client's datagrams land on the same
	// loop — letting every loop own a private session map with no cross-core
	// locking, and spreading the receive path across cores.
	for _, conn := range conns {
		conn := conn
		rr.loopWG.Add(1)
		go func() {
			defer rr.loopWG.Done()
			serveUDP(ctx, conn, remoteAddr, rr.rule, rr.verbose)
		}()
	}
	return nil
}

// stop closes the listener(s) so no new connections are accepted, then waits up
// to drainTimeout for in-flight connections to finish. Stragglers past the
// deadline are force-closed by cancelling the runner context.
func (rr *ruleRunner) stop(drainTimeout time.Duration) {
	// Stop accepting first. TCP accept loops unblock when their listener
	// closes; the UDP serve loop and stragglers unblock on context cancel,
	// which we defer until after the drain window for in-flight TCP conns.
	rr.closeListeners()
	if rr.rule.Proto == "udp" {
		rr.cancel()
	}

	// Wait for accept loops / the UDP serve loop to exit.
	rr.loopWG.Wait()

	done := make(chan struct{})
	go func() {
		rr.connWG.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(drainTimeout):
		log.Printf("%s: drain timeout (%s) exceeded, force-closing", rr.rule.Listen, drainTimeout)
	}
	// Cancel the context so any straggler handlers' dials/copies tear down.
	rr.cancel()
}

func (rr *ruleRunner) closeListeners() {
	for _, ln := range rr.listeners {
		_ = ln.Close()
	}
}
