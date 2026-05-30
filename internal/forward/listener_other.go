//go:build !unix

package forward

import "net"

// listenConfig is the portable fallback for platforms without unix socket
// options (e.g. Windows, Plan 9). SO_REUSEPORT is unavailable, so reuseport>1
// silently degrades to a single listener; behaviour is otherwise identical.
func listenConfig(_ Rule) net.ListenConfig {
	return net.ListenConfig{}
}

// tuneConn applies only the portable options available through net.TCPConn.
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
