package forward

import (
	"context"
	"errors"
	"io"
	"log"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

// dialTimeout bounds how long we wait to establish an upstream connection
// before giving up on a freshly accepted client.
const dialTimeout = 10 * time.Second

// serveTCP accepts connections on the rule's listener and relays each one to
// the upstream. It blocks until ctx is cancelled or a fatal accept error
// occurs.
func serveTCP(ctx context.Context, r Rule, verbose bool) error {
	var lc net.ListenConfig
	ln, err := lc.Listen(ctx, "tcp", r.Listen)
	if err != nil {
		return err
	}
	log.Printf("listening %s", r)

	// Close the listener when the context is cancelled so Accept unblocks.
	go func() {
		<-ctx.Done()
		_ = ln.Close()
	}()

	var wg sync.WaitGroup
	var connID uint64
	for {
		client, err := ln.Accept()
		if err != nil {
			if ctx.Err() != nil {
				// Shutdown in progress: drain in-flight connections.
				wg.Wait()
				return nil
			}
			// Transient accept errors (e.g. too many open files) should not
			// kill the listener; back off briefly and retry.
			var ne net.Error
			if errors.As(err, &ne) && ne.Timeout() {
				continue
			}
			log.Printf("%s: accept error: %v", r.Listen, err)
			time.Sleep(50 * time.Millisecond)
			continue
		}

		id := atomic.AddUint64(&connID, 1)
		wg.Add(1)
		go func() {
			defer wg.Done()
			handleTCP(ctx, id, client, r, verbose)
		}()
	}
}

func handleTCP(ctx context.Context, id uint64, client net.Conn, r Rule, verbose bool) {
	defer client.Close()
	tuneTCP(client)

	var d net.Dialer
	dialCtx, cancel := context.WithTimeout(ctx, dialTimeout)
	upstream, err := d.DialContext(dialCtx, "tcp", r.Remote)
	cancel()
	if err != nil {
		log.Printf("[%s#%d] dial %s failed: %v", r.Listen, id, r.Remote, err)
		return
	}
	defer upstream.Close()
	tuneTCP(upstream)

	if verbose {
		log.Printf("[%s#%d] open %s <-> %s", r.Listen, id, client.RemoteAddr(), upstream.RemoteAddr())
	}

	// Relay both directions concurrently. io.Copy on *net.TCPConn uses the
	// splice(2) syscall on Linux, moving bytes in-kernel with no userspace
	// buffer. When one side closes we half-close the other so EOF propagates
	// cleanly, then return once both copies finish.
	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); pipe(upstream, client) }()
	go func() { defer wg.Done(); pipe(client, upstream) }()
	wg.Wait()

	if verbose {
		log.Printf("[%s#%d] close", r.Listen, id)
	}
}

// pipe copies from src to dst, then signals end-of-data to dst by closing its
// write side (if supported). This lets request/response protocols see EOF
// without tearing down the whole connection prematurely.
func pipe(dst, src net.Conn) {
	_, _ = io.Copy(dst, src)
	if cw, ok := dst.(interface{ CloseWrite() error }); ok {
		_ = cw.CloseWrite()
	}
}

// tuneTCP disables Nagle's algorithm so small request/response payloads (the
// common case for Redis and HTTP APIs) are not delayed waiting for coalescing.
func tuneTCP(c net.Conn) {
	if tc, ok := c.(*net.TCPConn); ok {
		_ = tc.SetNoDelay(true)
	}
}
