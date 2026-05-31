// Package config loads and validates proxy-port's YAML configuration and
// resolves it into fully-populated forward.Rule values. Per-rule fields are
// pointers so an unset override ("inherit the default") is distinguishable from
// an explicit zero ("disable").
package config

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/nuumz/proxy-port/internal/forward"
)

// Config is the top-level YAML document.
type Config struct {
	Defaults Defaults   `yaml:"defaults"`
	Rules    []RuleSpec `yaml:"rules"`
	Log      LogCfg     `yaml:"log"`
}

// Defaults are applied to every rule unless the rule overrides the field.
// Durations are YAML strings like "30s"; 0 has a field-specific meaning noted
// inline (disable / unlimited / OS default).
type Defaults struct {
	TCPNoDelay   bool     `yaml:"tcp_nodelay"`
	KeepAlive    Duration `yaml:"tcp_keepalive"`   // 0 disables keepalive
	DialTimeout  Duration `yaml:"dial_timeout"`    // upstream dial timeout
	Balance      string   `yaml:"balance"`         // weighted (default) | least_conn | iphash
	FailCooldown Duration `yaml:"fail_cooldown"`   // how long a backend stays down after a dial failure
	MaxConns     int      `yaml:"max_connections"` // 0 = unlimited
	ReadBuffer   int      `yaml:"read_buffer"`     // 0 = OS default (bytes)
	WriteBuffer  int      `yaml:"write_buffer"`    // 0 = OS default (bytes)
	ReusePort    int      `yaml:"reuseport"`       // listener sockets per rule
	DrainTimeout Duration `yaml:"drain_timeout"`   // in-flight conn drain wait
}

// RuleSpec mirrors Defaults with pointer overrides so "unset" is distinct from
// "explicit zero". Resolve fills unset fields from Defaults.
type RuleSpec struct {
	Name    string   `yaml:"name"`
	Proto   string   `yaml:"proto"` // tcp (default) or udp
	Listen  string   `yaml:"listen"`
	Remote  string   `yaml:"remote"`  // single upstream; shorthand for a one-element remotes
	Remotes []string `yaml:"remotes"` // load-balanced upstreams, each "addr" or "addr#weight"

	TCPNoDelay   *bool     `yaml:"tcp_nodelay"`
	KeepAlive    *Duration `yaml:"tcp_keepalive"`
	DialTimeout  *Duration `yaml:"dial_timeout"`
	Balance      *string   `yaml:"balance"`
	FailCooldown *Duration `yaml:"fail_cooldown"`
	MaxConns     *int      `yaml:"max_connections"`
	ReadBuffer   *int      `yaml:"read_buffer"`
	WriteBuffer  *int      `yaml:"write_buffer"`
	ReusePort    *int      `yaml:"reuseport"`
	DrainTimeout *Duration `yaml:"drain_timeout"`
}

// LogCfg controls logging behaviour.
type LogCfg struct {
	Verbose bool `yaml:"verbose"`
}

// Duration is a time.Duration that unmarshals from a YAML string ("30s") or a
// bare number (interpreted as seconds, the friendlier reading for a config).
type Duration time.Duration

// UnmarshalYAML accepts either a duration string ("30s") or a bare integer
// (interpreted as seconds). We branch on the node tag because YAML scalars are
// untyped: decoding a bare `90` into a string also succeeds, so we must check
// for !!int explicitly or the seconds form would reach time.ParseDuration and
// fail on the missing unit.
func (d *Duration) UnmarshalYAML(value *yaml.Node) error {
	if value.Tag == "!!int" {
		var n int64
		if err := value.Decode(&n); err != nil {
			return fmt.Errorf("invalid duration: %w", err)
		}
		*d = Duration(time.Duration(n) * time.Second)
		return nil
	}
	var s string
	if err := value.Decode(&s); err != nil {
		return fmt.Errorf("duration must be a string like \"30s\" or a number of seconds")
	}
	v, err := time.ParseDuration(s)
	if err != nil {
		return fmt.Errorf("invalid duration %q: %w", s, err)
	}
	*d = Duration(v)
	return nil
}

// D returns the underlying time.Duration.
func (d Duration) D() time.Duration { return time.Duration(d) }

// builtinDefaults seed any Defaults field the file leaves unset, so a minimal
// config (just rules) still gets sensible tunables.
func builtinDefaults() Defaults {
	return Defaults{
		TCPNoDelay:   true,
		KeepAlive:    Duration(30 * time.Second),
		DialTimeout:  Duration(10 * time.Second),
		Balance:      forward.BalanceWeighted,
		FailCooldown: Duration(10 * time.Second),
		MaxConns:     0,
		ReadBuffer:   0,
		WriteBuffer:  0,
		ReusePort:    1,
		DrainTimeout: Duration(15 * time.Second),
	}
}

// Load reads, parses, validates a config file and applies built-in defaults to
// any unset Defaults fields. It does not resolve rules — call Resolve for that.
func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var c Config
	c.Defaults = builtinDefaults()
	// Decode into a temp so present-but-zero fields override the seeds only
	// when actually specified. yaml.v3 leaves absent fields untouched, so
	// decoding directly onto the seeded Defaults gives the right merge.
	if err := yaml.Unmarshal(data, &c); err != nil {
		return nil, fmt.Errorf("parse %s: %w", path, err)
	}
	if c.Defaults.ReusePort == 0 {
		c.Defaults.ReusePort = 1
	}
	if c.Defaults.Balance == "" {
		c.Defaults.Balance = forward.BalanceWeighted
	}
	if c.Defaults.FailCooldown == 0 {
		c.Defaults.FailCooldown = Duration(10 * time.Second)
	}

	if err := c.Validate(); err != nil {
		return nil, err
	}
	return &c, nil
}

// Validate checks structural invariants: every rule has a listen and remote
// address, a supported protocol, reuseport >= 1, and a unique key.
func (c *Config) Validate() error {
	if len(c.Rules) == 0 {
		return fmt.Errorf("config has no rules")
	}
	seen := make(map[string]struct{}, len(c.Rules))
	for i, rs := range c.Rules {
		proto := rs.Proto
		if proto == "" {
			proto = "tcp"
		}
		if proto != "tcp" && proto != "udp" {
			return fmt.Errorf("rule %d (%s): unsupported proto %q (want tcp or udp)", i, rs.Name, rs.Proto)
		}
		if rs.Listen == "" {
			return fmt.Errorf("rule %d (%s): empty listen address", i, rs.Name)
		}
		if _, err := rs.upstreamSpecs(); err != nil {
			return fmt.Errorf("rule %d (%s): %v", i, rs.Name, err)
		}
		if rs.Balance != nil && !forward.ValidBalance(*rs.Balance) {
			return fmt.Errorf("rule %d (%s): unknown balance %q (want weighted, least_conn or iphash)", i, rs.Name, *rs.Balance)
		}
		if rs.ReusePort != nil && *rs.ReusePort < 1 {
			return fmt.Errorf("rule %d (%s): reuseport must be >= 1", i, rs.Name)
		}
		key := proto + "/" + rs.Listen
		if _, dup := seen[key]; dup {
			return fmt.Errorf("rule %d (%s): duplicate listen key %q", i, rs.Name, key)
		}
		seen[key] = struct{}{}
	}
	if c.Defaults.ReusePort < 1 {
		return fmt.Errorf("defaults.reuseport must be >= 1")
	}
	if !forward.ValidBalance(c.Defaults.Balance) {
		return fmt.Errorf("defaults.balance: unknown strategy %q (want weighted, least_conn or iphash)", c.Defaults.Balance)
	}
	return nil
}

// upstreamSpecs resolves a rule's upstreams from either `remote` (single) or
// `remotes` (load-balanced list). Exactly one of the two must be set; each
// remotes entry may carry an "#weight" suffix.
func (rs RuleSpec) upstreamSpecs() ([]forward.Upstream, error) {
	hasRemote := strings.TrimSpace(rs.Remote) != ""
	hasRemotes := len(rs.Remotes) > 0
	switch {
	case hasRemote && hasRemotes:
		return nil, fmt.Errorf("set either remote or remotes, not both")
	case hasRemotes:
		ups := make([]forward.Upstream, 0, len(rs.Remotes))
		for _, rem := range rs.Remotes {
			u, err := forward.ParseUpstreams(rem)
			if err != nil {
				return nil, err
			}
			ups = append(ups, u...)
		}
		if len(ups) == 0 {
			return nil, fmt.Errorf("remotes is empty")
		}
		return ups, nil
	case hasRemote:
		return forward.ParseUpstreams(rs.Remote)
	default:
		return nil, fmt.Errorf("empty remote address (set remote or remotes)")
	}
}

// Resolve turns every RuleSpec into a fully-populated forward.Rule by applying
// the (already defaulted) defaults to each unset override.
func (c *Config) Resolve() []forward.Rule {
	d := c.Defaults
	rules := make([]forward.Rule, 0, len(c.Rules))
	for _, rs := range c.Rules {
		proto := rs.Proto
		if proto == "" {
			proto = "tcp"
		}
		// upstreamSpecs already passed Validate, so the error is unreachable here.
		ups, _ := rs.upstreamSpecs()
		r := forward.Rule{
			Name:         rs.Name,
			Proto:        proto,
			Listen:       rs.Listen,
			Upstreams:    ups,
			Balance:      pickStr(rs.Balance, d.Balance),
			TCPNoDelay:   pickBool(rs.TCPNoDelay, d.TCPNoDelay),
			KeepAlive:    pickDur(rs.KeepAlive, d.KeepAlive),
			DialTimeout:  pickDur(rs.DialTimeout, d.DialTimeout),
			FailCooldown: pickDur(rs.FailCooldown, d.FailCooldown),
			MaxConns:     pickInt(rs.MaxConns, d.MaxConns),
			ReadBuffer:   pickInt(rs.ReadBuffer, d.ReadBuffer),
			WriteBuffer:  pickInt(rs.WriteBuffer, d.WriteBuffer),
			ReusePort:    pickInt(rs.ReusePort, d.ReusePort),
			DrainTimeout: pickDur(rs.DrainTimeout, d.DrainTimeout),
		}
		if r.ReusePort < 1 {
			r.ReusePort = 1
		}
		rules = append(rules, r)
	}
	return rules
}

func pickBool(o *bool, def bool) bool {
	if o != nil {
		return *o
	}
	return def
}

func pickInt(o *int, def int) int {
	if o != nil {
		return *o
	}
	return def
}

func pickStr(o *string, def string) string {
	if o != nil {
		return *o
	}
	return def
}

func pickDur(o *Duration, def Duration) time.Duration {
	if o != nil {
		return o.D()
	}
	return def.D()
}

// Search returns the first existing config path among, in order: the explicit
// -c flag, ./proxy-port.yaml, $XDG_CONFIG_HOME/proxy-port/config.yaml, and
// ~/.config/proxy-port/config.yaml. It returns "" (no error) when none exists.
func Search(flagPath string) string {
	if flagPath != "" {
		return flagPath
	}
	candidates := []string{"proxy-port.yaml"}
	if xdg := os.Getenv("XDG_CONFIG_HOME"); xdg != "" {
		candidates = append(candidates, filepath.Join(xdg, "proxy-port", "config.yaml"))
	}
	if home, err := os.UserHomeDir(); err == nil {
		candidates = append(candidates, filepath.Join(home, ".config", "proxy-port", "config.yaml"))
	}
	for _, p := range candidates {
		if _, err := os.Stat(p); err == nil {
			return p
		}
	}
	return ""
}

// DefaultUserPath is where `init` writes when no path is given: the XDG config
// dir (or ~/.config) — the canonical "remembered" config location.
func DefaultUserPath() string {
	base := os.Getenv("XDG_CONFIG_HOME")
	if base == "" {
		if home, err := os.UserHomeDir(); err == nil {
			base = filepath.Join(home, ".config")
		} else {
			base = "."
		}
	}
	return filepath.Join(base, "proxy-port", "config.yaml")
}

// WriteDefault writes a commented starter config to path, creating parent
// directories as needed. It refuses to overwrite an existing file.
func WriteDefault(path string) error {
	if _, err := os.Stat(path); err == nil {
		return fmt.Errorf("%s already exists (refusing to overwrite)", path)
	}
	if dir := filepath.Dir(path); dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return err
		}
	}
	return os.WriteFile(path, []byte(starterConfig), 0o644)
}

const starterConfig = `# proxy-port configuration.
# Durations accept Go syntax: "30s", "1m", "500ms". A value of 0 means:
#   tcp_keepalive  -> disabled
#   max_connections-> unlimited
#   read/write_buffer -> OS default
#
# defaults apply to every rule unless the rule overrides the field.
defaults:
  tcp_nodelay: true       # disable Nagle for low latency on small payloads
  tcp_keepalive: 30s      # detect dead peers; 0 disables
  dial_timeout: 10s       # give up establishing the upstream after this long
  balance: weighted       # load-balancing for multi-upstream rules: weighted | least_conn | iphash
  fail_cooldown: 10s      # park a backend this long after a dial failure before retrying it
  max_connections: 0      # per-rule concurrent connection cap; 0 = unlimited
  read_buffer: 0          # socket SO_RCVBUF in bytes; 0 = OS default
  write_buffer: 0         # socket SO_SNDBUF in bytes; 0 = OS default
  reuseport: 1            # SO_REUSEPORT sockets per rule (>1 spreads TCP accepts / UDP receive across cores)
  drain_timeout: 15s      # max wait for in-flight connections on stop/reload

rules:
  # Expose a remote Redis on local port 6379 (single upstream).
  - name: redis
    proto: tcp            # tcp (default) or udp
    listen: ":6379"
    remote: "192.168.1.10:6379"
    # max_connections: 5000   # per-rule override example

  # Load-balance an HTTP API across three backends (the third takes 2x traffic).
  # - name: api
  #   listen: ":8080"
  #   balance: least_conn   # weighted (default) | least_conn | iphash
  #   remotes:
  #     - "10.0.0.1:80"
  #     - "10.0.0.2:80"
  #     - "10.0.0.3:80#2"

  # Forward DNS over UDP.
  # - name: dns
  #   proto: udp
  #   listen: ":53"
  #   remote: "8.8.8.8:53"

log:
  verbose: false
`
