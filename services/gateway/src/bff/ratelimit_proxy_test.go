package bff

import (
	"net/http/httptest"
	"testing"
)

func TestTrustedProxyClientIPResolver(t *testing.T) {
	resolve, err := NewTrustedProxyClientIPResolver([]string{"10.0.0.0/8", "2001:db8::/32"})
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name, remote, forwarded, xff, want string
	}{
		{name: "untrusted peer ignores headers", remote: "192.0.2.8:1234", xff: "198.51.100.7", want: "192.0.2.8"},
		{name: "trusted xff chain", remote: "10.0.0.4:1234", xff: "198.51.100.7, 10.0.0.3", want: "198.51.100.7"},
		{name: "trusted forwarded chain", remote: "10.0.0.4:1234", forwarded: `for=198.51.100.9;proto=https, for=10.0.0.3`, want: "198.51.100.9"},
		{name: "malformed falls back", remote: "10.0.0.4:1234", xff: "not-an-ip", want: "10.0.0.4"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest("GET", "/", nil)
			req.RemoteAddr = tc.remote
			req.Header.Set("Forwarded", tc.forwarded)
			req.Header.Set("X-Forwarded-For", tc.xff)
			if got := resolve(req); got != tc.want {
				t.Fatalf("client IP=%q want %q", got, tc.want)
			}
		})
	}
}

func TestTrustedProxyClientIPResolverRejectsInvalidCIDR(t *testing.T) {
	if _, err := NewTrustedProxyClientIPResolver([]string{"not-a-cidr"}); err == nil {
		t.Fatal("invalid trusted proxy CIDR must fail configuration")
	}
}
