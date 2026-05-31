# proxy-port

A simple, fast TCP/UDP **port forwarder** written in Go.

It accepts connections on a local port and relays them to a service running on
**another machine** — a remote API, a Redis instance, a database — so they
appear as if they were running on `localhost`. Your machine never becomes a
routing gateway; it just shuttles bytes between two sockets.

```
[ your app ] ──▶ localhost:6379 ──[ proxy-port ]──▶ 192.168.1.10:6379 [ remote redis ]
```

## Why

- **Single static binary**, no runtime. Run from CLI flags or a remembered
  YAML config.
- **Fast.** TCP relaying uses Go's `io.Copy` over `*net.TCPConn`, which on
  Linux transfers bytes in-kernel via the `splice(2)` syscall — zero userspace
  copies. `TCP_NODELAY` is enabled so small request/response payloads (Redis,
  HTTP) are not delayed by Nagle's algorithm.
- **Built for load.** `SO_REUSEPORT` opens N sockets per rule so the kernel
  spreads work across cores — TCP accepts for TCP rules, the datagram receive
  loop for UDP rules; an optional per-rule connection cap sheds load to protect
  against FD exhaustion; TCP keepalive reaps dead peers.
- **Concurrent.** One lightweight goroutine per connection; thousands of
  simultaneous connections are cheap.
- **UDP too**, with per-client NAT sessions and idle eviction (handy for DNS).
  With `reuseport > 1` each receive loop owns a private session map, so the hot
  path scales across cores with no shared locking.
- **Load balancing.** Point a rule at several upstreams and pick a strategy:
  `weighted` round-robin, `least_conn`, or `iphash` (client affinity). Selection
  is lock-free and allocation-free; a backend that fails to dial is parked for a
  cooldown and traffic fails over to the rest (passive health).
- **Hot reload.** Edit the config and `kill -HUP` to add, remove, or change
  rules without dropping in-flight connections.

## Install / Build

```bash
go build -o proxy-port .
# or
make build
```

## Usage

```
proxy-port [-c config.yaml] [-L LISTEN=REMOTE ...] [-v]
proxy-port init [path]      # write a starter config (the "remembered" config)
```

Each `-L` is a forwarding rule, `LISTEN=REMOTE`. Repeat `-L` for multiple
forwards. The protocol defaults to TCP; prefix with `tcp://` or `udp://` to be
explicit. `-L` rules are appended on top of any config file.

### Examples

Expose a remote Redis as a local port:

```bash
proxy-port -L :6379=192.168.1.10:6379
# now: redis-cli -p 6379   talks to the remote Redis
```

Forward a local port to a remote HTTP API, plus DNS over UDP, at once:

```bash
proxy-port -L 127.0.0.1:8080=10.0.0.5:80 -L udp://:53=8.8.8.8:53
```

Bind to all interfaces (so other hosts on your LAN can reach the remote too):

```bash
proxy-port -L 0.0.0.0:5432=db.internal:5432
```

Load-balance across several upstreams (comma-separated; `#N` sets a weight):

```bash
proxy-port -L :8080=10.0.0.1:80,10.0.0.2:80,10.0.0.3:80#2
```

Add `-v` to log every connection open/close.

## Config file (remembered config)

Generate a commented starter config, edit it, then run from it:

```bash
proxy-port init                                   # writes ~/.config/proxy-port/config.yaml
proxy-port -c ~/.config/proxy-port/config.yaml
```

When no `-c` is given, the config is searched for in order: `./proxy-port.yaml`,
`$XDG_CONFIG_HOME/proxy-port/config.yaml`, `~/.config/proxy-port/config.yaml`.

```yaml
defaults:                 # applied to every rule unless the rule overrides it
  tcp_nodelay: true       # disable Nagle for low latency on small payloads
  tcp_keepalive: 30s      # detect dead peers; 0 disables
  dial_timeout: 10s       # give up establishing the upstream after this long
  balance: weighted       # multi-upstream balancing: weighted | least_conn | iphash
  fail_cooldown: 10s      # park a backend this long after a dial failure before retrying it
  max_connections: 0      # per-rule concurrent connection cap; 0 = unlimited
  read_buffer: 0          # socket SO_RCVBUF in bytes; 0 = OS default
  write_buffer: 0         # socket SO_SNDBUF in bytes; 0 = OS default
  reuseport: 1            # SO_REUSEPORT sockets per rule (>1 spreads TCP accepts / UDP receive across cores)
  drain_timeout: 15s      # max wait for in-flight connections on stop/reload

rules:
  - name: redis
    listen: ":6379"
    remote: "192.168.1.10:6379"   # single upstream
    max_connections: 5000         # per-rule override
  - name: api                     # load-balanced across three backends
    listen: ":8080"
    balance: least_conn           # per-rule override of the default strategy
    remotes:
      - "10.0.0.1:80"
      - "10.0.0.2:80"
      - "10.0.0.3:80#2"           # weight 2: takes twice the share (weighted strategy)
  - name: dns
    proto: udp
    listen: ":53"
    remote: "8.8.8.8:53"

log:
  verbose: false
```

Durations accept Go syntax (`30s`, `1m`, `500ms`) or a bare number of seconds.

### Hot reload

Edit the config and send `SIGHUP`:

```bash
kill -HUP $(pgrep proxy-port)
```

Rules are diffed by listen address: unchanged rules keep serving (their live
connections are never touched), added rules start, removed/changed rules stop
accepting and drain. A bad edit is logged and the proxy keeps running on the
previous config.

## Flags

| Flag | Description |
|------|-------------|
| `-c PATH` | Path to a YAML config file (overrides the search path). |
| `-L LISTEN=REMOTE` | Forwarding rule. Repeatable. Optional `tcp://` (default) / `udp://` prefix. Appended on top of the config. |
| `-v` | Verbose: log each connection open and close. |

Sub-command: `proxy-port init [path]` writes a starter config.

## Behaviour notes

- **TCP** uses half-close: when one side finishes sending, its write half is
  closed so the peer sees EOF, while the other direction keeps flowing until it
  too completes. This is correct for request/response protocols.
- **UDP** is connectionless, so each distinct client source address gets its
  own upstream socket (symmetric-NAT style). Sessions idle for 60s are
  reclaimed. Under load balancing, a UDP client sticks to the upstream chosen
  when its session opened (per-client affinity for the session's lifetime).
- **Load balancing** distributes *new* connections (TCP) or *new* sessions
  (UDP) across a rule's upstreams. `weighted` (default) is round-robin honouring
  per-upstream `#weight`; `least_conn` sends to the backend with the fewest live
  connections; `iphash` pins each client IP to a stable upstream. A dial failure
  parks that backend for `fail_cooldown` and the request fails over to another;
  the backend is retried once the cooldown elapses.
- `SIGINT` / `SIGTERM` triggers a graceful shutdown that stops accepting new
  connections and drains in-flight TCP connections (bounded by `drain_timeout`;
  stragglers past the deadline are force-closed).
- `SIGHUP` reloads the config in place (see [Hot reload](#hot-reload)).

## Benchmark

`make load` runs a concurrent round-trip throughput benchmark through the
forwarder in front of an in-process echo server.

## License

MIT
