package forward

import (
	"reflect"
	"testing"
)

func TestParseRule(t *testing.T) {
	tests := []struct {
		in      string
		proto   string
		listen  string
		want    []Upstream
		wantErr bool
	}{
		{in: ":6379=10.0.0.5:6379", proto: "tcp", listen: ":6379", want: ups("10.0.0.5:6379")},
		{in: "127.0.0.1:8080=10.0.0.5:80", proto: "tcp", listen: "127.0.0.1:8080", want: ups("10.0.0.5:80")},
		{in: "tcp://0.0.0.0:5432=db:5432", proto: "tcp", listen: "0.0.0.0:5432", want: ups("db:5432")},
		{in: "udp://:53=8.8.8.8:53", proto: "udp", listen: ":53", want: ups("8.8.8.8:53")},
		{in: "UDP://:53=8.8.8.8:53", proto: "udp", listen: ":53", want: ups("8.8.8.8:53")},
		{in: " :6379 = host:6379 ", proto: "tcp", listen: ":6379", want: ups("host:6379")},
		// Multiple comma-separated upstreams, with an optional weight suffix.
		{in: ":80=10.0.0.1:80,10.0.0.2:80#3", proto: "tcp", listen: ":80",
			want: []Upstream{{Addr: "10.0.0.1:80", Weight: 1}, {Addr: "10.0.0.2:80", Weight: 3}}},

		{in: "no-separator", wantErr: true},
		{in: "=remote:1", wantErr: true},
		{in: ":1=", wantErr: true},
		{in: "sctp://:1=h:2", wantErr: true},
		{in: ":1=h:2#0", wantErr: true},      // weight must be >= 1
		{in: ":1=h:2#abc", wantErr: true},    // weight must be an integer
		{in: ":1=h:2#100000", wantErr: true}, // weight must be <= maxWeight
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
		if got.Proto != tt.proto || got.Listen != tt.listen || !reflect.DeepEqual(got.Upstreams, tt.want) {
			t.Errorf("ParseRule(%q) = {proto:%s listen:%s upstreams:%v}, want {proto:%s listen:%s upstreams:%v}",
				tt.in, got.Proto, got.Listen, got.Upstreams, tt.proto, tt.listen, tt.want)
		}
	}
}
