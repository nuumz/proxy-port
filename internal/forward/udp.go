package forward

import (
	"context"
	"log"
	"net"
	"sync"
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

// udpSession tracks one client's NAT mapping to a dedicated upstream socket.
type udpSession struct {
	upstream *net.UDPConn
	lastSeen time.Time
	mu       sync.Mutex
}

// serveUDPRunner is the runner entry point for UDP rules. It binds the listen
// socket, reports the bind outcome on errc exactly once (nil on success), and
// then serves until ctx is cancelled. Owning the bind here lets the supervisor
// surface bind errors synchronously while the serve loop runs in the background.
func serveUDPRunner(ctx context.Context, r Rule, verbose bool, errc chan<- error) {
	lc := listenConfig(r)
	pc, err := lc.ListenPacket(ctx, "udp", r.Listen)
	if err != nil {
		errc <- err
		return
	}
	conn := pc.(*net.UDPConn)
	if r.ReadBuffer > 0 {
		_ = conn.SetReadBuffer(r.ReadBuffer)
	}
	if r.WriteBuffer > 0 {
		_ = conn.SetWriteBuffer(r.WriteBuffer)
	}

	remoteAddr, err := net.ResolveUDPAddr("udp", r.Remote)
	if err != nil {
		_ = conn.Close()
		errc <- err
		return
	}
	log.Printf("listening %s", r)
	errc <- nil

	serveUDP(ctx, conn, remoteAddr, r, verbose)
}

// serveUDP implements a connectionless relay over an already-bound socket.
// Because UDP has no accept(), we demultiplex on the client's source address:
// each distinct client gets its own upstream socket so replies can be routed
// back correctly (classic symmetric-NAT behaviour).
func serveUDP(ctx context.Context, conn *net.UDPConn, remoteAddr *net.UDPAddr, r Rule, verbose bool) {
	go func() {
		<-ctx.Done()
		_ = conn.Close()
	}()

	sessions := make(map[string]*udpSession)
	var mu sync.Mutex

	// Periodically evict idle sessions.
	go reapUDP(ctx, sessions, &mu, verbose, r)

	buf := make([]byte, udpBufSize)
	for {
		n, client, err := conn.ReadFromUDP(buf)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Printf("%s: read error: %v", r.Listen, err)
			return
		}

		key := client.String()
		mu.Lock()
		sess := sessions[key]
		if sess == nil {
			up, derr := net.DialUDP("udp", nil, remoteAddr)
			if derr != nil {
				mu.Unlock()
				log.Printf("%s: dial %s failed: %v", r.Listen, r.Remote, derr)
				continue
			}
			sess = &udpSession{upstream: up, lastSeen: time.Now()}
			sessions[key] = sess
			if verbose {
				log.Printf("[%s udp] open %s <-> %s", r.Listen, client, r.Remote)
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
func relayUDPReplies(client *net.UDPConn, upstream *net.UDPConn, dst *net.UDPAddr, sess *udpSession) {
	buf := make([]byte, udpBufSize)
	for {
		n, err := upstream.Read(buf)
		if err != nil {
			return
		}
		sess.touch()
		if _, err := client.WriteToUDP(buf[:n], dst); err != nil {
			return
		}
	}
}

func reapUDP(ctx context.Context, sessions map[string]*udpSession, mu *sync.Mutex, verbose bool, r Rule) {
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

func (s *udpSession) touch() {
	s.mu.Lock()
	s.lastSeen = time.Now()
	s.mu.Unlock()
}

func (s *udpSession) seen() time.Time {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lastSeen
}
