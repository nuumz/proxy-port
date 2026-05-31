package forward

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// Default tunables used when a Rule is built straight from a "-L" spec (i.e.
// without a config file). They mirror config.Defaults so the flags-only path
// behaves identically to a minimal YAML config.
const (
	defaultKeepAlive    = 30 * time.Second
	defaultDialTimeout  = 10 * time.Second
	defaultDrainTimeout = 15 * time.Second
)

// Upstream is one balancing target: an address and its relative weight (>=1).
// Weight is only consulted by the weighted strategy; it is 1 for plain specs.
type Upstream struct {
	Addr   string
	Weight int
}

// Rule describes a single port-forwarding mapping: accept connections on
// Listen and relay them to one of Upstreams using the given Proto ("tcp" or
// "udp"), balanced per Balance, plus the resolved per-rule tunables that govern
// stability and latency.
type Rule struct {
	Name      string     // optional human label, used only in logs
	Proto     string     // "tcp" or "udp"
	Listen    string     // local bind address, e.g. ":6379" or "127.0.0.1:6379"
	Upstreams []Upstream // one or more balancing targets (>=1)
	Balance   string     // "weighted" (default) | "least_conn" | "iphash"

	// Tunables (resolved from defaults + overrides by config.Resolve, or from
	// the constants above for the "-L" path).
	TCPNoDelay   bool          // disable Nagle on both ends
	KeepAlive    time.Duration // 0 disables TCP keepalive
	DialTimeout  time.Duration // upstream dial timeout
	FailCooldown time.Duration // how long a backend stays parked-down after a dial failure
	MaxConns     int           // 0 = unlimited concurrent connections per rule
	ReadBuffer   int           // 0 = OS default; socket SO_RCVBUF in bytes
	WriteBuffer  int           // 0 = OS default; socket SO_SNDBUF in bytes
	ReusePort    int           // number of SO_REUSEPORT listener sockets (>=1)
	DrainTimeout time.Duration // max wait for in-flight conns on stop/reload
}

// ParseUpstreams parses a remote spec into one or more weighted upstreams. The
// spec is a comma-separated list of addresses, each with an optional "#weight"
// suffix (weight defaults to 1). IPv6 addresses keep their brackets/colons;
// only the last '#' is treated as the weight separator.
//
//	10.0.0.5:6379
//	10.0.0.1:80,10.0.0.2:80#3
//	[2001:db8::1]:443#2,[2001:db8::2]:443
func ParseUpstreams(spec string) ([]Upstream, error) {
	parts := strings.Split(spec, ",")
	ups := make([]Upstream, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		addr, weight := p, 1
		if i := strings.LastIndex(p, "#"); i >= 0 {
			addr = strings.TrimSpace(p[:i])
			w, err := strconv.Atoi(strings.TrimSpace(p[i+1:]))
			if err != nil || w < 1 {
				return nil, fmt.Errorf("invalid weight in %q (want a positive integer)", p)
			}
			if w > maxWeight {
				return nil, fmt.Errorf("weight %d in %q exceeds the maximum of %d", w, p, maxWeight)
			}
			weight = w
		}
		if addr == "" {
			return nil, fmt.Errorf("empty upstream address in %q", spec)
		}
		ups = append(ups, Upstream{Addr: addr, Weight: weight})
	}
	if len(ups) == 0 {
		return nil, fmt.Errorf("no upstream addresses in %q", spec)
	}
	return ups, nil
}

// ParseRule parses a "-L" spec into a Rule, filling tunables from the package
// defaults so a flags-only invocation needs no config file. The remote side may
// list several comma-separated upstreams (weighted round-robin by default).
//
// Accepted forms (proto defaults to tcp):
//
//	:6379=10.0.0.5:6379
//	127.0.0.1:8080=10.0.0.5:80
//	udp://:53=8.8.8.8:53
//	:80=10.0.0.1:80,10.0.0.2:80#3
func ParseRule(spec string) (Rule, error) {
	r := Rule{
		Proto:        "tcp",
		Balance:      defaultBalance,
		TCPNoDelay:   true,
		KeepAlive:    defaultKeepAlive,
		DialTimeout:  defaultDialTimeout,
		FailCooldown: defaultFailCooldown,
		ReusePort:    1,
		DrainTimeout: defaultDrainTimeout,
	}

	if i := strings.Index(spec, "://"); i >= 0 {
		r.Proto = strings.ToLower(spec[:i])
		spec = spec[i+len("://"):]
	}

	if r.Proto != "tcp" && r.Proto != "udp" {
		return Rule{}, fmt.Errorf("unsupported protocol %q (want tcp or udp)", r.Proto)
	}

	listen, remote, found := strings.Cut(spec, "=")
	if !found {
		return Rule{}, fmt.Errorf("missing '=' separator in %q (want LISTEN=REMOTE)", spec)
	}

	listen = strings.TrimSpace(listen)
	if listen == "" {
		return Rule{}, fmt.Errorf("empty listen address in %q", spec)
	}
	ups, err := ParseUpstreams(remote)
	if err != nil {
		return Rule{}, err
	}

	r.Listen = listen
	r.Upstreams = ups
	return r, nil
}

// Key identifies a rule by what it binds to. The supervisor diffs reloads by
// Key: two rules with the same Key occupy the same listen socket and so cannot
// run simultaneously.
func (r Rule) Key() string {
	return r.Proto + "/" + r.Listen
}

// configHash returns a value equal for two rules iff every field that affects
// runtime behaviour matches. It powers reload diffing: same Key but different
// hash ⇒ the runner must be replaced.
func (r Rule) configHash() string {
	var b strings.Builder
	fmt.Fprintf(&b, "%s|%s|%s|%s|%t|%d|%d|%d|%d|%d|%d|%d",
		r.Name, r.Proto, r.Listen, r.Balance,
		r.TCPNoDelay, r.KeepAlive, r.DialTimeout, r.FailCooldown,
		r.MaxConns, r.ReadBuffer, r.WriteBuffer, r.ReusePort)
	for _, u := range r.Upstreams {
		fmt.Fprintf(&b, "|%s#%d", u.Addr, u.Weight)
	}
	return b.String()
}

// SameConfig reports whether two rules are behaviourally identical (used by the
// supervisor to leave unchanged runners untouched on reload). DrainTimeout is
// intentionally excluded: it only affects shutdown, not steady-state forwarding.
func (r Rule) SameConfig(o Rule) bool {
	return r.configHash() == o.configHash()
}

// upstreamSummary renders the upstream set for logs: a bare address for a
// single target, or a bracketed list when balancing.
func (r Rule) upstreamSummary() string {
	if len(r.Upstreams) == 1 {
		return r.Upstreams[0].Addr
	}
	addrs := make([]string, len(r.Upstreams))
	for i, u := range r.Upstreams {
		addrs[i] = u.Addr
	}
	return "[" + strings.Join(addrs, " ") + "]"
}

func (r Rule) String() string {
	bal := ""
	if len(r.Upstreams) > 1 {
		bal = " (" + r.Balance + ")"
	}
	if r.Name != "" {
		return fmt.Sprintf("%s %s %s -> %s%s", r.Name, r.Proto, r.Listen, r.upstreamSummary(), bal)
	}
	return fmt.Sprintf("%s %s -> %s%s", r.Proto, r.Listen, r.upstreamSummary(), bal)
}
