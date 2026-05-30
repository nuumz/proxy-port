// Package forward implements a lightweight, high-throughput TCP/UDP port
// forwarder. It accepts connections locally and relays them to a remote
// upstream so services on another machine (an API, a Redis instance, a
// database) appear as if they were running on localhost — without turning the
// host into a routing gateway.
package forward

import (
	"context"
	"sync"
)

// Run starts a forwarder for every rule and blocks until ctx is cancelled or
// one of the listeners fails fatally. If any listener returns an error the
// shared context is cancelled so the others wind down too; the first error is
// returned.
func Run(ctx context.Context, rules []Rule, verbose bool) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var (
		wg      sync.WaitGroup
		once    sync.Once
		firstEr error
	)
	for _, r := range rules {
		r := r
		wg.Add(1)
		go func() {
			defer wg.Done()
			var err error
			if r.Proto == "udp" {
				err = serveUDP(ctx, r, verbose)
			} else {
				err = serveTCP(ctx, r, verbose)
			}
			if err != nil {
				once.Do(func() { firstEr = err })
				cancel()
			}
		}()
	}
	wg.Wait()
	return firstEr
}
