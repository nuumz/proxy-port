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

- **Single static binary**, no runtime, no config files.
- **Fast.** TCP relaying uses Go's `io.Copy` over `*net.TCPConn`, which on
  Linux transfers bytes in-kernel via the `splice(2)` syscall — zero userspace
  copies. `TCP_NODELAY` is enabled so small request/response payloads (Redis,
  HTTP) are not delayed by Nagle's algorithm.
- **Concurrent.** One lightweight goroutine per connection; thousands of
  simultaneous connections are cheap.
- **UDP too**, with per-client NAT sessions and idle eviction (handy for DNS).

## Install / Build

```bash
go build -o proxy-port .
# or
make build
```

## Usage

```
proxy-port -L LISTEN=REMOTE [-L ...] [-v]
```

Each `-L` is a forwarding rule, `LISTEN=REMOTE`. Repeat `-L` for multiple
forwards. The protocol defaults to TCP; prefix with `tcp://` or `udp://` to be
explicit.

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

Add `-v` to log every connection open/close.

## Flags

| Flag | Description |
|------|-------------|
| `-L LISTEN=REMOTE` | Forwarding rule. Repeatable. Optional `tcp://` (default) / `udp://` prefix. |
| `-v` | Verbose: log each connection open and close. |

## Behaviour notes

- **TCP** uses half-close: when one side finishes sending, its write half is
  closed so the peer sees EOF, while the other direction keeps flowing until it
  too completes. This is correct for request/response protocols.
- **UDP** is connectionless, so each distinct client source address gets its
  own upstream socket (symmetric-NAT style). Sessions idle for 60s are
  reclaimed.
- `SIGINT` / `SIGTERM` triggers a graceful shutdown that stops accepting new
  connections and drains in-flight TCP connections.

## License

MIT
