package forward

import (
	"fmt"
	"strings"
)

// Rule describes a single port-forwarding mapping: accept connections on
// Listen and relay them to Remote using the given Proto ("tcp" or "udp").
type Rule struct {
	Proto  string // "tcp" or "udp"
	Listen string // local bind address, e.g. ":6379" or "127.0.0.1:6379"
	Remote string // upstream target, e.g. "192.168.1.10:6379"
}

// ParseRule parses a "-L" spec into a Rule.
//
// Accepted forms (proto defaults to tcp):
//
//	:6379=10.0.0.5:6379
//	127.0.0.1:8080=10.0.0.5:80
//	udp://:53=8.8.8.8:53
//	tcp://0.0.0.0:5432=db.internal:5432
func ParseRule(spec string) (Rule, error) {
	r := Rule{Proto: "tcp"}

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

func (r Rule) String() string {
	return fmt.Sprintf("%s %s -> %s", r.Proto, r.Listen, r.Remote)
}
