package forward

import (
	"context"
	"io"
	"log"
	"net"
	"sync"
)

// handleTCP relays one accepted client connection to the rule's upstream,
// applying the rule's socket tunables to both ends. The runner owns the accept
// loop, semaphore and lifetime; this function just moves bytes.
func handleTCP(ctx context.Context, id uint64, client net.Conn, r Rule, verbose bool) {
	defer client.Close()
	tuneConn(client, r)

	var d net.Dialer
	dialCtx, cancel := context.WithTimeout(ctx, r.DialTimeout)
	upstream, err := d.DialContext(dialCtx, "tcp", r.Remote)
	cancel()
	if err != nil {
		log.Printf("[%s#%d] dial %s failed: %v", r.Listen, id, r.Remote, err)
		return
	}
	defer upstream.Close()
	tuneConn(upstream, r)

	// Force both ends closed if the runner context is cancelled — that only
	// happens once stop() has exceeded its drain window, and closing the conns
	// is what unblocks the io.Copy calls below so a straggler can't outlive the
	// drain timeout. context.AfterFunc keeps no goroutine alive while the
	// connection is live (unlike a per-conn watcher goroutine), so this costs
	// nothing per in-flight conn; the cleanup runs once, only on cancel. stop
	// deregisters it on normal completion (its return value is irrelevant: the
	// Close calls are idempotent).
	stop := context.AfterFunc(ctx, func() {
		_ = client.Close()
		_ = upstream.Close()
	})
	defer stop()

	if verbose {
		log.Printf("[%s#%d] open %s <-> %s", r.Listen, id, client.RemoteAddr(), upstream.RemoteAddr())
	}

	// Relay both directions. io.Copy on *net.TCPConn uses the splice(2) syscall
	// on Linux, moving bytes in-kernel with no userspace buffer. We run one
	// direction inline in this goroutine and spawn just one more for the other,
	// saving a goroutine per connection. When one side closes we half-close the
	// other so EOF propagates cleanly, then return once both copies finish.
	var wg sync.WaitGroup
	wg.Add(1)
	go func() { defer wg.Done(); pipe(upstream, client) }()
	pipe(client, upstream)
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
