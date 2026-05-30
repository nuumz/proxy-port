package forward

import "testing"

func TestParseRule(t *testing.T) {
	tests := []struct {
		in      string
		want    Rule
		wantErr bool
	}{
		{in: ":6379=10.0.0.5:6379", want: Rule{Proto: "tcp", Listen: ":6379", Remote: "10.0.0.5:6379"}},
		{in: "127.0.0.1:8080=10.0.0.5:80", want: Rule{Proto: "tcp", Listen: "127.0.0.1:8080", Remote: "10.0.0.5:80"}},
		{in: "tcp://0.0.0.0:5432=db:5432", want: Rule{Proto: "tcp", Listen: "0.0.0.0:5432", Remote: "db:5432"}},
		{in: "udp://:53=8.8.8.8:53", want: Rule{Proto: "udp", Listen: ":53", Remote: "8.8.8.8:53"}},
		{in: "UDP://:53=8.8.8.8:53", want: Rule{Proto: "udp", Listen: ":53", Remote: "8.8.8.8:53"}},
		{in: " :6379 = host:6379 ", want: Rule{Proto: "tcp", Listen: ":6379", Remote: "host:6379"}},

		{in: "no-separator", wantErr: true},
		{in: "=remote:1", wantErr: true},
		{in: ":1=", wantErr: true},
		{in: "sctp://:1=h:2", wantErr: true},
	}

	for _, tt := range tests {
		got, err := ParseRule(tt.in)
		if tt.wantErr {
			if err == nil {
				t.Errorf("ParseRule(%q): expected error, got %+v", tt.in, got)
			}
			continue
		}
		if err != nil {
			t.Errorf("ParseRule(%q): unexpected error: %v", tt.in, err)
			continue
		}
		// Compare only the parsed identity fields; ParseRule also fills tunable
		// defaults (TCPNoDelay, timeouts, ...) which these cases don't assert.
		if got.Proto != tt.want.Proto || got.Listen != tt.want.Listen || got.Remote != tt.want.Remote {
			t.Errorf("ParseRule(%q) = %s, want %s", tt.in, got, tt.want)
		}
	}
}
