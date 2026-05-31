package forward

import (
	"fmt"
	"net"
	"net/netip"
	"sync/atomic"
	"time"
)

// Balancing strategy names, as written in config (`balance:`).
const (
	BalanceWeighted  = "weighted"   // weighted round-robin; equal weights == plain round-robin
	BalanceLeastConn = "least_conn" // fewest in-flight conns/sessions wins
	BalanceIPHash    = "iphash"     // hash client IP -> stable upstream (session affinity)

	defaultBalance      = BalanceWeighted
	defaultFailCooldown = 10 * time.Second
)

// ValidBalance reports whether s is a known strategy (empty == default).
func ValidBalance(s string) bool {
	switch s {
	case "", BalanceWeighted, BalanceLeastConn, BalanceIPHash:
		return true
	}
	return false
}

type strategy int

const (
	stratWeighted strategy = iota
	stratLeastConn
	stratIPHash
)

func parseStrategy(s string) (strategy, error) {
	switch s {
	case "", BalanceWeighted:
		return stratWeighted, nil
	case BalanceLeastConn:
		return stratLeastConn, nil
	case BalanceIPHash:
		return stratIPHash, nil
	default:
		return 0, fmt.Errorf("unknown balance strategy %q", s)
	}
}

// upstreamState is one backend's live state. inflight and downUntil are atomics
// so the pick path is lock-free; downUntil holds a unix-nanos deadline (0 == up)
// for passive health: a failed dial parks the backend for the cooldown window.
type upstreamState struct {
	addr      string
	udpAddr   *net.UDPAddr // pre-resolved for udp rules so sessions don't re-resolve
	inflight  atomic.Int64
	downUntil atomic.Int64
}

// pool load-balances new connections/sessions across a rule's upstreams. It is
// shared by every accept loop / UDP serve loop of a rule, so least_conn and
// weighted state are global across cores; all hot-path state is atomic.
//
// expanded holds upstream indices repeated by weight (weights [3,1] -> indices
// [0,0,0,1]); weighted round-robin and iphash index into it, giving O(1)
// weighted selection with no per-pick allocation or locking.
type pool struct {
	ups      []*upstreamState
	expanded []int
	strat    strategy
	cursor   atomic.Uint64
	cooldown time.Duration
}

func newPool(ups []Upstream, balance string, cooldown time.Duration, proto string) (*pool, error) {
	if len(ups) == 0 {
		return nil, fmt.Errorf("rule has no upstreams")
	}
	strat, err := parseStrategy(balance)
	if err != nil {
		return nil, err
	}
	if cooldown <= 0 {
		cooldown = defaultFailCooldown
	}
	p := &pool{strat: strat, cooldown: cooldown}
	for _, u := range ups {
		w := u.Weight
		if w < 1 {
			w = 1
		}
		st := &upstreamState{addr: u.Addr}
		if proto == "udp" {
			ua, err := net.ResolveUDPAddr("udp", u.Addr)
			if err != nil {
				return nil, fmt.Errorf("resolve upstream %s: %w", u.Addr, err)
			}
			st.udpAddr = ua
		}
		idx := len(p.ups)
		p.ups = append(p.ups, st)
		for k := 0; k < w; k++ {
			p.expanded = append(p.expanded, idx)
		}
	}
	return p, nil
}

func (p *pool) len() int                   { return len(p.ups) }
func (p *pool) addr(i int) string          { return p.ups[i].addr }
func (p *pool) udpAddr(i int) *net.UDPAddr { return p.ups[i].udpAddr }

func (p *pool) healthy(i int, now int64) bool {
	d := p.ups[i].downUntil.Load()
	return d == 0 || now >= d // 0 == up; past the cooldown deadline allows a retry
}

// pick selects an upstream for a new conn/session. key is the client-IP hash,
// used only by iphash. For least_conn it increments the chosen upstream's
// inflight counter, which the caller must pair with exactly one done() (on
// close) or fail() (on dial error). ok is false only when every upstream is
// currently parked as down.
func (p *pool) pick(key uint64) (int, bool) {
	now := time.Now().UnixNano()

	switch p.strat {
	case stratLeastConn:
		best := -1
		var bestN int64
		for i := range p.ups {
			if !p.healthy(i, now) {
				continue
			}
			if c := p.ups[i].inflight.Load(); best < 0 || c < bestN {
				best, bestN = i, c
			}
		}
		if best < 0 {
			return 0, false
		}
		p.ups[best].inflight.Add(1)
		return best, true

	case stratIPHash:
		m := uint64(len(p.expanded))
		base := key % m
		for k := uint64(0); k < m; k++ {
			idx := p.expanded[(base+k)%m]
			if p.healthy(idx, now) {
				return idx, true
			}
		}
		return 0, false

	default: // weighted round-robin
		m := uint64(len(p.expanded))
		c := p.cursor.Add(1)
		for k := uint64(0); k < m; k++ {
			idx := p.expanded[(c+k)%m]
			if p.healthy(idx, now) {
				return idx, true
			}
		}
		return 0, false
	}
}

// done releases a successful pick (decrements the least_conn counter).
func (p *pool) done(i int) {
	if p.strat == stratLeastConn {
		p.ups[i].inflight.Add(-1)
	}
}

// fail parks an upstream for the cooldown window after a dial error and releases
// the pick, so subsequent picks skip it until the deadline passes.
func (p *pool) fail(i int) {
	p.ups[i].downUntil.Store(time.Now().Add(p.cooldown).UnixNano())
	p.done(i)
}

// markUp clears any parked-down state after a successful dial.
func (p *pool) markUp(i int) { p.ups[i].downUntil.Store(0) }

const (
	fnvOffset uint64 = 1469598103934665603
	fnvPrime  uint64 = 1099511628211
)

// hashBytes is FNV-1a; used to map a client IP to an upstream for iphash with
// no allocation.
func hashBytes(b []byte) uint64 {
	h := fnvOffset
	for _, c := range b {
		h ^= uint64(c)
		h *= fnvPrime
	}
	return h
}

// clientKeyTCP hashes the client's IP (not port) so all of a client's
// connections stick to the same upstream under iphash.
func clientKeyTCP(c net.Conn) uint64 {
	if ta, ok := c.RemoteAddr().(*net.TCPAddr); ok {
		return hashBytes(ta.IP)
	}
	return 0
}

// clientKeyUDP hashes a client's IP for iphash session affinity.
func clientKeyUDP(a netip.Addr) uint64 {
	b := a.As16()
	return hashBytes(b[:])
}
