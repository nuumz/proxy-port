package forward

import (
	"fmt"
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

// Rule describes a single port-forwarding mapping: accept connections on
// Listen and relay them to Remote using the given Proto ("tcp" or "udp"), plus
// the resolved per-rule tunables that govern stability and latency.
type Rule struct {
	Name   string // optional human label, used only in logs
	Proto  string // "tcp" or "udp"
	Listen string // local bind address, e.g. ":6379" or "127.0.0.1:6379"
	Remote string // upstream target, e.g. "192.168.1.10:6379"

	// Tunables (resolved from defaults + overrides by config.Resolve, or from
	// the constants above for the "-L" path).
	TCPNoDelay   bool          // disable Nagle on both ends
	KeepAlive    time.Duration // 0 disables TCP keepalive
	DialTimeout  time.Duration // upstream dial timeout
	MaxConns     int           // 0 = unlimited concurrent connections per rule
	ReadBuffer   int           // 0 = OS default; socket SO_RCVBUF in bytes
	WriteBuffer  int           // 0 = OS default; socket SO_SNDBUF in bytes
	ReusePort    int           // number of SO_REUSEPORT listener sockets (>=1)
	DrainTimeout time.Duration // max wait for in-flight conns on stop/reload
}

// ParseRule parses a "-L" spec into a Rule, filling tunables from the package
// defaults so a flags-only invocation needs no config file.
//
// Accepted forms (proto defaults to tcp):
//
//	:6379=10.0.0.5:6379
//	127.0.0.1:8080=10.0.0.5:80
//	udp://:53=8.8.8.8:53
//	tcp://0.0.0.0:5432=db.internal:5432
func ParseRule(spec string) (Rule, error) {
	r := Rule{
		Proto:        "tcp",
		TCPNoDelay:   true,
		KeepAlive:    defaultKeepAlive,
		DialTimeout:  defaultDialTimeout,
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
	remote = strings.TrimSpace(remote)
	if listen == "" {
		return Rule{}, fmt.Errorf("empty listen address in %q", spec)
	}
	if remote == "" {
		return Rule{}, fmt.Errorf("empty remote address in %q", spec)
	}

	r.Listen = listen
	r.Remote = remote
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
	return fmt.Sprintf("%s|%s|%s|%s|%t|%d|%d|%d|%d|%d|%d",
		r.Name, r.Proto, r.Listen, r.Remote,
		r.TCPNoDelay, r.KeepAlive, r.DialTimeout,
		r.MaxConns, r.ReadBuffer, r.WriteBuffer, r.ReusePort)
}

// SameConfig reports whether two rules are behaviourally identical (used by the
// supervisor to leave unchanged runners untouched on reload). DrainTimeout is
// intentionally excluded: it only affects shutdown, not steady-state forwarding.
func (r Rule) SameConfig(o Rule) bool {
	return r.configHash() == o.configHash()
}

func (r Rule) String() string {
	if r.Name != "" {
		return fmt.Sprintf("%s %s %s -> %s", r.Name, r.Proto, r.Listen, r.Remote)
	}
	return fmt.Sprintf("%s %s -> %s", r.Proto, r.Listen, r.Remote)
}
