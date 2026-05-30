# CLAUDE.md

Guidance for working in this repository. proxy-port is a single-binary, high-throughput
TCP/UDP **port forwarder** in Go: it accepts connections locally and relays them to a
remote upstream so a service on another machine appears to run on `localhost`. The host
never becomes a routing gateway — it only shuttles bytes between two sockets.

## Commands

```bash
make build      # CGO_ENABLED=0 static binary with -ldflags "-s -w"
make test       # go test ./...
make vet        # go vet ./...
make fmt        # gofmt -w .
make load       # throughput benchmark through the forwarder (BenchmarkForwardTCP)
make clean

go test ./internal/forward/ -run TestName -v     # single test
go test ./internal/forward/ -race                # race detector (use when touching concurrency)
```

Go 1.25. Only two deps: `golang.org/x/sys` (socket options) and `gopkg.in/yaml.v3`.

## Architecture

Request path: `client → listener (per rule) → handleTCP/serveUDP → upstream`.

- **`main.go`** — CLI: `-L LISTEN=REMOTE` flags (repeatable), `-c config.yaml`, `-v`,
  and the `init` subcommand. Wires signals: SIGINT/SIGTERM → graceful drain via context
  cancel; SIGHUP → `watchReload`. `-L` rules are *appended on top of* the config file.
- **`internal/config`** — YAML load/validate/resolve. `Defaults` + per-rule pointer
  overrides (`*bool`/`*int`/`*Duration`) so "unset → inherit" is distinct from "explicit
  zero → disable". `Resolve()` flattens specs into `[]forward.Rule`. `Duration` unmarshals
  a Go duration string (`"30s"`) **or** a bare int (seconds).
- **`internal/forward`** — the engine:
  - `Supervisor` owns one `ruleRunner` per rule, keyed by `Rule.Key()` (`proto/listen`).
    `Reload()` diffs desired vs running by Key: added → start, removed → stop+drain,
    changed (same Key, different `configHash`) → replace, unchanged → leave untouched.
  - `ruleRunner` owns the listener socket(s), a per-rule concurrency semaphore, and two
    WaitGroups: `loopWG` (accept loops / UDP serve loop) and `connWG` (in-flight TCP
    handlers). Their separate lifetimes are what make reload/shutdown non-disruptive.
  - `tcp.go` — `handleTCP` relays both directions with `io.Copy` (uses `splice(2)`
    in-kernel on Linux) and **half-close**: when one side EOFs, the other's write half is
    closed so request/response protocols see EOF without a premature full teardown.
  - `udp.go` — connectionless: demux on client source addr, one upstream `*net.UDPConn`
    per client (symmetric-NAT style), idle sessions reaped after 60s.
  - `listener_unix.go` / `listener_other.go` — build-tagged socket tuning. Unix sets
    SO_REUSEADDR always and SO_REUSEPORT when `reuseport > 1`. Non-unix degrades reuseport
    to a single listener silently; keep both files in sync when changing `tuneConn`.

## Invariants — preserve these when editing

- **A bad config never crashes a live forwarder.** Reload errors are logged and the proxy
  keeps serving the previous config. Don't turn reload/parse errors into fatals.
- **In-flight connections are never dropped on reload of an unchanged rule.** Diffing is by
  `Rule.Key()`; `SameConfig` excludes `DrainTimeout` (shutdown-only, not steady-state).
- **Shutdown is bounded.** `stop()` closes listeners first (halts accepts), waits up to
  `DrainTimeout` for `connWG`, then cancels the context to force-close stragglers. The TCP
  accept loop must exit on `net.ErrClosed` even before ctx is cancelled, or `loopWG.Wait`
  hangs.
- **Load shedding is non-blocking.** The semaphore is acquired with a non-blocking `select`;
  at the cap, the freshly accepted conn is closed immediately (protects against FD
  exhaustion). Don't make this block.
- `reuseport` must be `>= 1` everywhere it's resolved/validated.

## Conventions

- Idiomatic Go, `gofmt`. Run `make fmt vet` before finishing; `-race` when touching
  goroutines/locks.
- Tests colocate as `_test.go` next to source under `internal/forward` and `internal/config`.
  Test real behaviour (forwarding, reload diffing, parsing, drain) — there's an in-process
  echo-server harness in `bench_test.go`.
- Comments explain *why* (the tricky concurrency/socket reasoning), not *what*. Match that
  density.
- Keep `README.md` and the `starterConfig` string in `config.go` in sync with any
  flag/config/default change.

## Git

Conventional Commits (`feat|fix|test|refactor|docs|chore(scope):`). Do not commit the built
`proxy-port` binary. Never commit/push without an explicit request in the current turn.
