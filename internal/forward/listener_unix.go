//go:build unix

package forward

import (
	"net"
	"syscall"

	"golang.org/x/sys/unix"
)

// listenConfig builds a net.ListenConfig whose Control hook sets socket options
// before bind. SO_REUSEADDR is always set so a restart can rebind immediately;
// SO_REUSEPORT is set when the rule asks for more than one listener socket,
// letting the kernel load-balance incoming SYNs across N sockets on the same
// address — this spreads accept work across cores and cuts tail latency under
// high connection churn.
func listenConfig(r Rule) net.ListenConfig {
	reuse := r.ReusePort > 1
	return net.ListenConfig{
		Control: func(_, _ string, c syscall.RawConn) error {
			var serr error
			cerr := c.Control(func(fd uintptr) {
				if err := unix.SetsockoptInt(int(fd), unix.SOL_SOCKET, unix.SO_REUSEADDR, 1); err != nil {
					serr = err
					return
				}
				if reuse {
					serr = unix.SetsockoptInt(int(fd), unix.SOL_SOCKET, unix.SO_REUSEPORT, 1)
				}
			})
			if cerr != nil {
				return cerr
			}
			return serr
		},
	}
}

// tuneConn applies per-connection socket options from the rule: TCP_NODELAY,
// keepalive (with period), and optional SO_RCVBUF/SO_SNDBUF sizing for
// high bandwidth-delay-product paths. It is a no-op for non-TCP conns.
func tuneConn(c net.Conn, r Rule) {
	tc, ok := c.(*net.TCPConn)
	if !ok {
		return
	}
	if r.TCPNoDelay {
		_ = tc.SetNoDelay(true)
	}
	if r.KeepAlive > 0 {
		_ = tc.SetKeepAlive(true)
		_ = tc.SetKeepAlivePeriod(r.KeepAlive)
	} else {
		_ = tc.SetKeepAlive(false)
	}
	if r.ReadBuffer > 0 {
		_ = tc.SetReadBuffer(r.ReadBuffer)
	}
	if r.WriteBuffer > 0 {
		_ = tc.SetWriteBuffer(r.WriteBuffer)
	}
}
