package config

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/nuumz/proxy-port/internal/forward"
)

// writeTmp writes content to a temp file and returns its path.
func writeTmp(t *testing.T, content string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(p, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	return p
}

// TestLoadResolveDefaults checks that a minimal config (one rule, no tunables)
// inherits the built-in defaults, while explicit per-rule overrides win.
func TestLoadResolveDefaults(t *testing.T) {
	path := writeTmp(t, `
defaults:
  dial_timeout: 5s
  reuseport: 4
rules:
  - name: redis
    listen: ":6379"
    remote: "10.0.0.5:6379"
  - name: api
    listen: ":8080"
    remote: "10.0.0.6:80"
    dial_timeout: 2s
    max_connections: 100
`)
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	rules := cfg.Resolve()
	if len(rules) != 2 {
		t.Fatalf("got %d rules, want 2", len(rules))
	}

	redis := rules[0]
	if redis.DialTimeout != 5*time.Second {
		t.Errorf("redis DialTimeout = %s, want inherited 5s", redis.DialTimeout)
	}
	if redis.ReusePort != 4 {
		t.Errorf("redis ReusePort = %d, want inherited 4", redis.ReusePort)
	}
	if !redis.TCPNoDelay {
		t.Errorf("redis TCPNoDelay = false, want built-in default true")
	}
	if redis.KeepAlive != 30*time.Second {
		t.Errorf("redis KeepAlive = %s, want built-in default 30s", redis.KeepAlive)
	}

	api := rules[1]
	if api.DialTimeout != 2*time.Second {
		t.Errorf("api DialTimeout = %s, want override 2s", api.DialTimeout)
	}
	if api.MaxConns != 100 {
		t.Errorf("api MaxConns = %d, want override 100", api.MaxConns)
	}
	if api.ReusePort != 4 {
		t.Errorf("api ReusePort = %d, want inherited 4", api.ReusePort)
	}
}

// TestExplicitZeroDisables verifies a pointer override of 0 means "disable",
// distinct from "unset" which inherits the default.
func TestExplicitZeroDisables(t *testing.T) {
	path := writeTmp(t, `
defaults:
  tcp_keepalive: 30s
rules:
  - listen: ":1"
    remote: "h:1"
    tcp_keepalive: 0
`)
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if got := cfg.Resolve()[0].KeepAlive; got != 0 {
		t.Errorf("KeepAlive = %s, want 0 (explicitly disabled)", got)
	}
}

// TestLoadBalanceConfig checks a rule with `remotes` + `balance` resolves to a
// weighted upstream set, while `remote` stays a single-upstream shorthand.
func TestLoadBalanceConfig(t *testing.T) {
	path := writeTmp(t, `
defaults:
  balance: iphash
rules:
  - name: api
    listen: ":8080"
    balance: least_conn
    remotes:
      - "10.0.0.1:80"
      - "10.0.0.2:80#3"
  - name: redis
    listen: ":6379"
    remote: "10.0.0.9:6379"
`)
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	rules := cfg.Resolve()

	api := rules[0]
	if api.Balance != "least_conn" {
		t.Errorf("api Balance = %q, want least_conn (per-rule override)", api.Balance)
	}
	want := []forward.Upstream{{Addr: "10.0.0.1:80", Weight: 1}, {Addr: "10.0.0.2:80", Weight: 3}}
	if !reflect.DeepEqual(api.Upstreams, want) {
		t.Errorf("api Upstreams = %v, want %v", api.Upstreams, want)
	}

	redis := rules[1]
	if redis.Balance != "iphash" {
		t.Errorf("redis Balance = %q, want inherited iphash", redis.Balance)
	}
	if len(redis.Upstreams) != 1 || redis.Upstreams[0].Addr != "10.0.0.9:6379" {
		t.Errorf("redis Upstreams = %v, want single 10.0.0.9:6379", redis.Upstreams)
	}
}

func TestValidateErrors(t *testing.T) {
	cases := map[string]string{
		"no rules":        "defaults: {}\n",
		"empty remote":    "rules:\n  - listen: \":1\"\n",
		"bad proto":       "rules:\n  - proto: sctp\n    listen: \":1\"\n    remote: \"h:1\"\n",
		"duplicate key":   "rules:\n  - listen: \":1\"\n    remote: \"a:1\"\n  - listen: \":1\"\n    remote: \"b:1\"\n",
		"reuseport zero":  "rules:\n  - listen: \":1\"\n    remote: \"h:1\"\n    reuseport: 0\n",
		"remote+remotes":  "rules:\n  - listen: \":1\"\n    remote: \"a:1\"\n    remotes: [\"b:1\"]\n",
		"bad balance":     "rules:\n  - listen: \":1\"\n    remote: \"h:1\"\n    balance: bogus\n",
		"bad weight":      "rules:\n  - listen: \":1\"\n    remotes: [\"h:1#0\"]\n",
		"bad default bal": "defaults:\n  balance: nope\nrules:\n  - listen: \":1\"\n    remote: \"h:1\"\n",
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := Load(writeTmp(t, body)); err == nil {
				t.Errorf("expected error for %q config, got nil", name)
			}
		})
	}
}

// TestWriteDefaultRoundtrip makes sure the starter config we ship actually
// parses, validates, and resolves — i.e. `init` produces a runnable config.
func TestWriteDefaultRoundtrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sub", "config.yaml")
	if err := WriteDefault(path); err != nil {
		t.Fatalf("WriteDefault: %v", err)
	}
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load starter: %v", err)
	}
	if len(cfg.Resolve()) == 0 {
		t.Fatal("starter config resolved to zero rules")
	}
	// Refuses to overwrite.
	if err := WriteDefault(path); err == nil {
		t.Error("WriteDefault overwrote an existing file, want error")
	}
}

func TestDurationUnmarshal(t *testing.T) {
	path := writeTmp(t, `
defaults:
  dial_timeout: 90      # bare number = seconds
rules:
  - listen: ":1"
    remote: "h:1"
`)
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if got := cfg.Resolve()[0].DialTimeout; got != 90*time.Second {
		t.Errorf("DialTimeout = %s, want 90s from bare number", got)
	}
}
