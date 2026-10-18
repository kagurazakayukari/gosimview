package config

import "testing"

func TestPublicHTTPPort(t *testing.T) {
	cases := []struct {
		url  string
		want int
		bad  bool
	}{
		{"https://race-a.example.com", 443, false},
		{"http://race-b.example.com:18080", 18080, false},
		{"http://race-c.example.com", 80, false},
		{"http://0.0.0.0:8080", 8080, false},
		{"http://127.0.0.1:8080", 8080, false},
		{"http://localhost:8080", 8080, false},
		{"//race-a.example.com", 0, true},
		{"file:///tmp", 0, true},
		{"https://race-a.example.com:70000", 0, true},
	}
	for _, tc := range cases {
		t.Run(tc.url, func(t *testing.T) {
			got, err := PublicHTTPPort(tc.url)
			if tc.bad {
				if err == nil {
					t.Fatalf("PublicHTTPPort(%q) expected an error", tc.url)
				}
				return
			}
			if err != nil || got != tc.want {
				t.Fatalf("PublicHTTPPort(%q) = %d, %v; want %d", tc.url, got, err, tc.want)
			}
		})
	}
}

func TestValidatePublicHTTPURLRejectsLocalAddresses(t *testing.T) {
	for _, raw := range []string{
		"http://0.0.0.0:8080",
		"http://127.0.0.1:8080",
		"http://localhost:8080",
		"http://192.168.1.20:8080",
	} {
		t.Run(raw, func(t *testing.T) {
			if err := ValidatePublicHTTPURL(raw); err == nil {
				t.Fatalf("ValidatePublicHTTPURL(%q) expected local address to disable cross-node routing", raw)
			}
		})
	}
}
