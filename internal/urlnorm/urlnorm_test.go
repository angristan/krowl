package urlnorm

import (
	"testing"
)

func TestNormalize_Basic(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{"lowercase scheme", "HTTP://example.com/page", "http://example.com/page"},
		{"lowercase host", "https://EXAMPLE.COM/page", "https://example.com/page"},
		{"strip default http port", "http://example.com:80/page", "http://example.com/page"},
		{"strip default https port", "https://example.com:443/page", "https://example.com/page"},
		{"keep non-default port", "https://example.com:8080/page", "https://example.com:8080/page"},
		{"strip www", "https://www.example.com/page", "https://example.com/page"},
		{"strip fragment", "https://example.com/page#section", "https://example.com/page"},
		{"add root path", "https://example.com", "https://example.com/"},
		{"strip trailing slash", "https://example.com/page/", "https://example.com/page"},
		{"keep root trailing slash", "https://example.com/", "https://example.com/"},
		{"collapse double slashes", "https://example.com/a//b///c", "https://example.com/a/b/c"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := Normalize(tt.input)
			if got != tt.want {
				t.Errorf("Normalize(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

func TestNormalize_QuerySorting(t *testing.T) {
	// Same params in different order should normalize identically
	a := Normalize("https://example.com/search?b=2&a=1")
	b := Normalize("https://example.com/search?a=1&b=2")
	if a != b {
		t.Errorf("query sort failed: %q != %q", a, b)
	}
	if a != "https://example.com/search?a=1&b=2" {
		t.Errorf("unexpected result: %q", a)
	}
}

func TestNormalize_TrackingParams(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{"strip utm_source", "https://example.com/page?utm_source=google&id=123", "https://example.com/page?id=123"},
		{"strip fbclid", "https://example.com/page?fbclid=abc123", "https://example.com/page"},
		{"strip gclid", "https://example.com/page?gclid=xyz&q=test", "https://example.com/page?q=test"},
		{"strip multiple tracking", "https://example.com/?utm_source=a&utm_medium=b&utm_campaign=c&real=1", "https://example.com/?real=1"},
		{"strip msclkid", "https://example.com/page?msclkid=abc", "https://example.com/page"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := Normalize(tt.input)
			if got != tt.want {
				t.Errorf("Normalize(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

func TestNormalize_EmptyValueParams(t *testing.T) {
	// Params with empty values are likely tracking cruft
	got := Normalize("https://example.com/page?ref=&q=test")
	want := "https://example.com/page?q=test"
	if got != want {
		t.Errorf("Normalize empty param = %q, want %q", got, want)
	}
}

func TestNormalize_NonHTTP(t *testing.T) {
	tests := []string{
		"ftp://example.com/file",
		"mailto:user@example.com",
		"javascript:void(0)",
		"",
		"not-a-url",
	}
	for _, input := range tests {
		got := Normalize(input)
		if got != "" {
			t.Errorf("Normalize(%q) = %q, want empty", input, got)
		}
	}
}

func TestNormalize_Idempotent(t *testing.T) {
	urls := []string{
		"https://example.com/page?b=2&a=1#frag",
		"HTTP://WWW.Example.COM:443/path/?utm_source=test&real=1",
		"https://example.com/a//b/c/",
	}
	for _, u := range urls {
		first := Normalize(u)
		second := Normalize(first)
		if first != second {
			t.Errorf("not idempotent: Normalize(%q) = %q, Normalize(%q) = %q", u, first, first, second)
		}
	}
}

func TestNormalizeDomain(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"https://www.Example.COM/page", "example.com"},
		{"http://example.com:8080/page", "example.com"},
		{"https://sub.example.com/", "sub.example.com"},
		{"https://WWW.test.org", "test.org"},
	}
	for _, tt := range tests {
		got := NormalizeDomain(tt.input)
		if got != tt.want {
			t.Errorf("NormalizeDomain(%q) = %q, want %q", tt.input, got, tt.want)
		}
	}
}
