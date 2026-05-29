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

	var specs ruleFlags
	flag.Var(&specs, "L", "forwarding rule LISTEN=REMOTE (repeatable). "+
		"Optional proto prefix tcp:// (default) or udp://")
	verbose := flag.Bool("v", false, "log every connection open/close")
	flag.Usage = usage
	flag.Parse()

	if len(specs) == 0 {
		usage()
		os.Exit(2)
	}

	rules := make([]forward.Rule, 0, len(specs))
	for _, s := range specs {
		r, err := forward.ParseRule(s)
		if err != nil {
			fmt.Fprintf(os.Stderr, "invalid -L %q: %v\n", s, err)
			os.Exit(2)
		}
		rules = append(rules, r)
	}

	// Cancel on SIGINT/SIGTERM for a clean shutdown that drains connections.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := forward.Run(ctx, rules, *verbose); err != nil {
		log.Fatalf("fatal: %v", err)
	}
	log.Println("shutdown complete")
}

func usage() {
	out := flag.CommandLine.Output()
	fmt.Fprintf(out, `proxy-port — simple, fast TCP/UDP port forwarder

Usage:
  proxy-port -L LISTEN=REMOTE [-L ...] [-v]

Examples:
  # Expose a remote Redis on the local port 6379
  proxy-port -L :6379=192.168.1.10:6379

  # Forward a local port to a remote HTTP API, and DNS over UDP
  proxy-port -L 127.0.0.1:8080=10.0.0.5:80 -L udp://:53=8.8.8.8:53

Flags:
`)
	flag.PrintDefaults()
}
