package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/nuumz/proxy-port/internal/config"
	"github.com/nuumz/proxy-port/internal/forward"
)

// ruleFlags collects repeated -L specifications from the command line.
type ruleFlags []string

func (f *ruleFlags) String() string { return strings.Join(*f, ", ") }
func (f *ruleFlags) Set(v string) error {
	*f = append(*f, v)
	return nil
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)

	// Subcommand: `proxy-port init [path]` writes a starter config and exits.
	if len(os.Args) > 1 && os.Args[1] == "init" {
		runInit(os.Args[2:])
		return
	}

	var specs ruleFlags
	flag.Var(&specs, "L", "forwarding rule LISTEN=REMOTE (repeatable). "+
		"Optional proto prefix tcp:// (default) or udp://")
	cfgPath := flag.String("c", "", "path to a YAML config file (overrides the search path)")
	verbose := flag.Bool("v", false, "log every connection open/close")
	flag.Usage = usage
	flag.Parse()

	cfg, rules, err := loadRules(*cfgPath, specs, *verbose)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		usage()
		os.Exit(2)
	}

	// SIGINT/SIGTERM cancel the context for a clean shutdown that drains
	// connections. SIGHUP is handled separately for hot reload.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	sup := forward.NewSupervisor(cfg.Log.Verbose)

	// Watch for SIGHUP and reload the config in place. Reload errors are logged
	// and the proxy keeps running on the previous config — never crash a live
	// forwarder over a bad edit.
	go watchReload(ctx, sup, *cfgPath, specs)

	if err := sup.Run(ctx, rules); err != nil {
		log.Fatalf("fatal: %v", err)
	}
	log.Println("shutdown complete")
}

// loadRules merges the config file (if any) with -L flags into the final rule
// set, returning the effective Config (for log settings) and resolved rules.
//
// Precedence: -L flags, when present, are appended to the config's rules (or
// used alone when no config exists). With neither a config nor -L it is a usage
// error. verboseFlag forces verbose when -v is passed even if the config is quiet.
func loadRules(cfgPath string, specs ruleFlags, verboseFlag bool) (*config.Config, []forward.Rule, error) {
	var cfg *config.Config
	if path := config.Search(cfgPath); path != "" {
		c, err := config.Load(path)
		if err != nil {
			return nil, nil, fmt.Errorf("config %s: %w", path, err)
		}
		cfg = c
		log.Printf("loaded config %s (%d rules)", path, len(c.Rules))
	} else if cfgPath != "" {
		return nil, nil, fmt.Errorf("config file %q not found", cfgPath)
	}

	rules, err := mergeRules(cfg, specs)
	if err != nil {
		return nil, nil, err
	}

	if cfg == nil {
		cfg = &config.Config{}
	}
	if verboseFlag {
		cfg.Log.Verbose = true
	}
	return cfg, rules, nil
}

// mergeRules builds the resolved rule slice from a config (may be nil) plus -L
// flag specs appended on top.
func mergeRules(cfg *config.Config, specs ruleFlags) ([]forward.Rule, error) {
	var rules []forward.Rule
	if cfg != nil {
		rules = cfg.Resolve()
	}
	for _, s := range specs {
		r, err := forward.ParseRule(s)
		if err != nil {
			return nil, fmt.Errorf("invalid -L %q: %w", s, err)
		}
		rules = append(rules, r)
	}
	if len(rules) == 0 {
		return nil, fmt.Errorf("no rules: pass -L LISTEN=REMOTE or provide a config file")
	}
	return rules, nil
}

// watchReload blocks on SIGHUP and reloads the merged rule set into the live
// supervisor until ctx is done.
func watchReload(ctx context.Context, sup *forward.Supervisor, cfgPath string, specs ruleFlags) {
	hup := make(chan os.Signal, 1)
	signal.Notify(hup, syscall.SIGHUP)
	defer signal.Stop(hup)

	for {
		select {
		case <-ctx.Done():
			return
		case <-hup:
			log.Println("SIGHUP received, reloading config")
			_, rules, err := loadRules(cfgPath, specs, false)
			if err != nil {
				log.Printf("reload failed, keeping current config: %v", err)
				continue
			}
			if err := sup.Reload(rules); err != nil {
				log.Printf("reload applied with errors: %v", err)
			} else {
				log.Printf("reload complete (%d rules)", len(rules))
			}
		}
	}
}

// runInit handles the `init [path]` subcommand: write a commented starter
// config to the given path (or the default user config path) and exit.
func runInit(args []string) {
	path := config.DefaultUserPath()
	if len(args) > 0 {
		path = args[0]
	}
	if err := config.WriteDefault(path); err != nil {
		fmt.Fprintf(os.Stderr, "init: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("wrote starter config to %s\n", path)
	fmt.Println("edit it, then run: proxy-port -c " + path)
}

func usage() {
	out := flag.CommandLine.Output()
	fmt.Fprintf(out, `proxy-port — simple, fast TCP/UDP port forwarder

Usage:
  proxy-port [-c config.yaml] [-L LISTEN=REMOTE ...] [-v]
  proxy-port init [path]        write a starter config (the "remembered" config)

Config is loaded from -c, else ./proxy-port.yaml, else
$XDG_CONFIG_HOME/proxy-port/config.yaml, else ~/.config/proxy-port/config.yaml.
-L rules are appended on top of any config file. Send SIGHUP to reload.

Examples:
  # Expose a remote Redis on the local port 6379
  proxy-port -L :6379=192.168.1.10:6379

  # Forward a local port to a remote HTTP API, and DNS over UDP
  proxy-port -L 127.0.0.1:8080=10.0.0.5:80 -L udp://:53=8.8.8.8:53

  # Run from a saved config, then hot-reload after editing it
  proxy-port init
  proxy-port -c ~/.config/proxy-port/config.yaml
  kill -HUP <pid>

Flags:
`)
	flag.PrintDefaults()
}
