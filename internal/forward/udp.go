package forward

import (
	"context"
	"log"
	"net"
	"net/netip"
	"sync"
	"sync/atomic"
	"time"
)

const (
	// udpBufSize is the max datagram we relay; 64 KiB covers the largest
	// possible UDP payload.
	udpBufSize = 64 * 1024
	// udpIdleTimeout reclaims a per-client upstream socket after this long
	// with no traffic in either direction.
	udpIdleTimeout = 60 * time.Second
)

// udpBufPool recycles the 64 KiB relay buffers that each per-client reply
// goroutine holds for its lifetime. Pooling them means total buffer memory
// tracks peak *concurrent* sessions rather than the cumulative count over
// time, so a churn of short-lived clients (DNS, etc.) stops allocating a fresh
// 64 KiB per session and pressuring the GC. We pool *[]byte (not []byte) so
// Get/Put don't allocate a slice header each call.
var udpBufPool = sync.Pool{New: func() any { b := make([]byte, udpBufSize); return &b }}

// udpSession tracks one client's NAT mapping to a dedicated upstream socket.
// poolIdx records which balanced upstream it is pinned to (UDP affinity is
// sticky for the session's lifetime) so the pool slot is released on close.
// lastSeen is a unix-nanos atomic so touch() on the hot path is lock-free.
type udpSession struct {
	upstream *net.UDPConn
	poolIdx  int
	lastSeen atomic.Int64
}

// serveUDP implements a connectionless relay over an already-bound socket.
// Because UDP has no accept(), we demultiplex on the client's source address:
// each distinct client gets its own upstream socket so replies can be routed
// back correctly (classic symmetric-NAT behaviour). The client address is a
// netip.AddrPort — a comparable value used directly as the map key, so the hot
// read path neither allocates a *net.UDPAddr nor stringifies it per datagram.
// Each client is pinned to one balanced upstream for the session's lifetime.
func serveUDP(ctx context.Context, conn *net.UDPConn, r Rule, p *pool, verbose bool) {
	go func() {
		<-ctx.Done()
		_ = conn.Close()
	}()

	sessions := make(map[netip.AddrPort]*udpSession)
	var mu sync.Mutex

	// Periodically evict idle sessions.
	go reapUDP(ctx, sessions, &mu, p, verbose, r)

	// On shutdown (ctx cancelled, which closes conn and ends the loop below),
	// close every upstream socket so its reply goroutine unblocks from
	// upstream.Read and exits, and release its pool slot. Without this the
	// goroutines and FDs would leak across a reload that removes/replaces this
	// rule.
	defer closeSessions(sessions, &mu, p)

	buf := make([]byte, udpBufSize)
	for {
		n, client, err := conn.ReadFromUDPAddrPort(buf)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Printf("%s: read error: %v", r.Listen, err)
			return
		}

		mu.Lock()
		sess := sessions[client]
		if sess == nil {
			idx, ok := p.pick(clientKeyUDP(client.Addr()))
			if !ok {
				mu.Unlock()
				log.Printf("%s: no healthy upstream for %s", r.Listen, client)
				continue
			}
			up, derr := net.DialUDP("udp", nil, p.udpAddr(idx))
			if derr != nil {
				p.fail(idx)
				mu.Unlock()
				log.Printf("%s: dial %s failed: %v", r.Listen, p.addr(idx), derr)
				continue
			}
			p.markUp(idx)
			sess = &udpSession{upstream: up, poolIdx: idx}
			sess.touch()
			sessions[client] = sess
			if verbose {
				log.Printf("[%s udp] open %s <-> %s", r.Listen, client, p.addr(idx))
			}
			// Pump upstream replies back to this client.
			go relayUDPReplies(conn, up, client, sess)
		}
		mu.Unlock()

		sess.touch()
		if _, werr := sess.upstream.Write(buf[:n]); werr != nil && verbose {
			log.Printf("[%s udp] write upstream failed: %v", r.Listen, werr)
		}
	}
}

// relayUDPReplies forwards datagrams from the upstream socket back to the
// originating client until the upstream socket is closed (by the reaper).
func relayUDPReplies(client *net.UDPConn, upstream *net.UDPConn, dst netip.AddrPort, sess *udpSession) {
	bp := udpBufPool.Get().(*[]byte)
	defer udpBufPool.Put(bp)
	buf := *bp
	for {
		n, err := upstream.Read(buf)
		if err != nil {
			return
		}
		sess.touch()
		if _, err := client.WriteToUDPAddrPort(buf[:n], dst); err != nil {
			return
		}
	}
}

func reapUDP(ctx context.Context, sessions map[netip.AddrPort]*udpSession, mu *sync.Mutex, p *pool, verbose bool, r Rule) {
	ticker := time.NewTicker(udpIdleTimeout / 2)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			now := time.Now()
			mu.Lock()
			for key, sess := range sessions {
				if now.Sub(sess.seen()) > udpIdleTimeout {
					_ = sess.upstream.Close()
					p.done(sess.poolIdx)
					delete(sessions, key)
					if verbose {
						log.Printf("[%s udp] idle close %s", r.Listen, key)
					}
				}
			}
			mu.Unlock()
		}
	}
}

// closeSessions tears down every live session's upstream socket and releases
// its pool slot. Called when a serve loop exits so the per-client reply
// goroutines unblock and finish.
func closeSessions(sessions map[netip.AddrPort]*udpSession, mu *sync.Mutex, p *pool) {
	mu.Lock()
	defer mu.Unlock()
	for key, sess := range sessions {
		_ = sess.upstream.Close()
		p.done(sess.poolIdx)
		delete(sessions, key)
	}
}

func (s *udpSession) touch() {
	s.lastSeen.Store(time.Now().UnixNano())
}

func (s *udpSession) seen() time.Time {
	return time.Unix(0, s.lastSeen.Load())
}
