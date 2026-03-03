package parse

import (
	"net/url"
	"sort"
	"testing"
)

// ---------- extractLinks tests ----------

func TestExtractLinks_AbsoluteHTTPLinks(t *testing.T) {
	body := []byte(`<html><body>
		<a href="https://example.com/page1">Page 1</a>
		<a href="http://example.com/page2">Page 2</a>
	</body></html>`)

	links := extractLinks(body, "https://base.com/")

	want := []string{
		"https://example.com/page1",
		"http://example.com/page2",
	}
	if len(links) != len(want) {
		t.Fatalf("got %d links, want %d: %v", len(links), len(want), links)
	}
	for i, l := range links {
		if l.URL != want[i] {
			t.Errorf("links[%d].URL = %q, want %q", i, l.URL, want[i])
		}
		if l.Domain == "" {
			t.Errorf("links[%d].Domain is empty for URL %q", i, l.URL)
		}
	}
}

func TestExtractLinks_ResolvesRelativeLinks(t *testing.T) {
	body := []byte(`<html><body>
		<a href="/about">About</a>
		<a href="sub/page">Sub</a>
	</body></html>`)

	links := extractLinks(body, "https://example.com/dir/index.html")

	want := []string{
		"https://example.com/about",
		"https://example.com/dir/sub/page",
	}
	if len(links) != len(want) {
		t.Fatalf("got %d links, want %d: %v", len(links), len(want), links)
	}
	for i, l := range links {
		if l.URL != want[i] {
			t.Errorf("links[%d].URL = %q, want %q", i, l.URL, want[i])
		}
	}
}

func TestExtractLinks_IgnoresNonHTTPSchemes(t *testing.T) {
	body := []byte(`<html><body>
		<a href="mailto:user@example.com">Mail</a>
		<a href="tel:+1234567890">Call</a>
		<a href="javascript:void(0)">JS</a>
		<a href="ftp://files.example.com/file">FTP</a>
		<a href="https://example.com/valid">Valid</a>
	</body></html>`)

	links := extractLinks(body, "https://base.com/")

	if len(links) != 1 {
		t.Fatalf("got %d links, want 1: %v", len(links), links)
	}
	if links[0].URL != "https://example.com/valid" {
		t.Errorf("got %q, want %q", links[0].URL, "https://example.com/valid")
	}
}

func TestExtractLinks_IgnoresFragmentOnlyLinks(t *testing.T) {
	body := []byte(`<html><body>
		<a href="#section">Fragment</a>
		<a href="#top">Top</a>
		<a href="https://example.com/real">Real</a>
	</body></html>`)

	links := extractLinks(body, "https://base.com/")

	if len(links) != 1 {
		t.Fatalf("got %d links, want 1: %v", len(links), links)
	}
	if links[0].URL != "https://example.com/real" {
		t.Errorf("got %q, want %q", links[0].URL, "https://example.com/real")
	}
}

func TestExtractLinks_Deduplicates(t *testing.T) {
	body := []byte(`<html><body>
		<a href="https://example.com/page">First</a>
		<a href="https://example.com/page">Duplicate</a>
		<a href="https://example.com/page">Triple</a>
		<a href="https://example.com/other">Other</a>
	</body></html>`)

	links := extractLinks(body, "https://base.com/")

	if len(links) != 2 {
		t.Fatalf("got %d links, want 2: %v", len(links), links)
	}

	sorted := make([]string, len(links))
	for i, l := range links {
		sorted[i] = l.URL
	}
	sort.Strings(sorted)

	want := []string{
		"https://example.com/other",
		"https://example.com/page",
	}
	for i, l := range sorted {
		if l != want[i] {
			t.Errorf("sorted links[%d] = %q, want %q", i, l, want[i])
		}
	}
}

func TestExtractLinks_EmptyBody(t *testing.T) {
	links := extractLinks(nil, "https://example.com/")
	if len(links) != 0 {
		t.Fatalf("got %d links from nil body, want 0: %v", len(links), links)
	}

	links = extractLinks([]byte{}, "https://example.com/")
	if len(links) != 0 {
		t.Fatalf("got %d links from empty body, want 0: %v", len(links), links)
	}
}

func TestExtractLinks_MalformedHTML(t *testing.T) {
	bodies := [][]byte{
		[]byte(`<a href="https://example.com/ok"><div><a href=`),
		[]byte(`<<<>>>not html at all &&&`),
		[]byte(`<a href="https://example.com/x"><a href="https://example.com/y"`),
		[]byte(`<a href="https://example.com/z" broken`),
	}

	for i, body := range bodies {
		// Must not panic
		links := extractLinks(body, "https://base.com/")
		_ = links
		t.Logf("malformed case %d: got %d links", i, len(links))
	}
}

func TestExtractLinks_StripsFragments(t *testing.T) {
	body := []byte(`<html><body>
		<a href="https://example.com/page#section">Link</a>
		<a href="/about#top">About</a>
	</body></html>`)

	links := extractLinks(body, "https://base.com/")

	want := []string{
		"https://example.com/page",
		"https://base.com/about",
	}
	if len(links) != len(want) {
		t.Fatalf("got %d links, want %d: %v", len(links), len(want), links)
	}
	for i, l := range links {
		if l.URL != want[i] {
			t.Errorf("links[%d].URL = %q, want %q", i, l.URL, want[i])
		}
	}
}

func TestExtractLinks_NormalizesHostToLowercase(t *testing.T) {
	body := []byte(`<html><body>
		<a href="https://EXAMPLE.COM/Page">Upper</a>
		<a href="https://Example.Com/Other">Mixed</a>
	</body></html>`)

	links := extractLinks(body, "https://base.com/")

	for _, l := range links {
		u, err := url.Parse(l.URL)
		if err != nil {
			t.Fatalf("failed to parse link %q: %v", l.URL, err)
		}
		if u.Host != "example.com" {
			t.Errorf("host not normalized: got %q, want %q", u.Host, "example.com")
		}
	}
}

// ---------- resolveLink tests ----------

func TestResolveLink_ResolvesRelativePaths(t *testing.T) {
	base, _ := url.Parse("https://example.com/dir/page.html")

	tests := []struct {
		href string
		want string
	}{
		{"/absolute", "https://example.com/absolute"},
		{"sibling", "https://example.com/dir/sibling"},
		{"../parent", "https://example.com/parent"},
		{"./same", "https://example.com/dir/same"},
	}

	for _, tc := range tests {
		got, dom := resolveLink(base, tc.href)
		if got != tc.want {
			t.Errorf("resolveLink(%q) = %q, want %q", tc.href, got, tc.want)
		}
		if dom == "" {
			t.Errorf("resolveLink(%q) returned empty domain", tc.href)
		}
	}
}

func TestResolveLink_ReturnsEmptyForNonHTTPSchemes(t *testing.T) {
	base, _ := url.Parse("https://example.com/")

	hrefs := []string{
		"javascript:void(0)",
		"javascript:alert('hi')",
		"mailto:user@example.com",
		"tel:+1234567890",
	}

	for _, href := range hrefs {
		got, dom := resolveLink(base, href)
		if got != "" {
			t.Errorf("resolveLink(%q) = %q, want empty", href, got)
		}
		if dom != "" {
			t.Errorf("resolveLink(%q) domain = %q, want empty", href, dom)
		}
	}
}

func TestResolveLink_EmptyHref(t *testing.T) {
	base, _ := url.Parse("https://example.com/page")

	got, _ := resolveLink(base, "")
	if got != "" {
		t.Errorf("resolveLink(%q) = %q, want empty", "", got)
	}

	got, _ = resolveLink(base, "   ")
	if got != "" {
		t.Errorf("resolveLink(%q) = %q, want empty for whitespace-only", "   ", got)
	}
}

func TestResolveLink_NormalizesScheme(t *testing.T) {
	base, _ := url.Parse("https://example.com/")

	// Standard http and https should be preserved
	tests := []struct {
		href       string
		wantScheme string
	}{
		{"http://other.com/page", "http"},
		{"https://other.com/page", "https"},
	}

	for _, tc := range tests {
		got, _ := resolveLink(base, tc.href)
		u, err := url.Parse(got)
		if err != nil {
			t.Fatalf("failed to parse resolved link %q: %v", got, err)
		}
		if u.Scheme != tc.wantScheme {
			t.Errorf("resolveLink(%q) scheme = %q, want %q", tc.href, u.Scheme, tc.wantScheme)
		}
	}

	// ftp scheme should be rejected
	got, _ := resolveLink(base, "ftp://files.example.com/file")
	if got != "" {
		t.Errorf("resolveLink(ftp://...) = %q, want empty", got)
	}
}

func TestResolveLink_StripsFragment(t *testing.T) {
	base, _ := url.Parse("https://example.com/")

	got, _ := resolveLink(base, "https://example.com/page#section")
	if got != "https://example.com/page" {
		t.Errorf("resolveLink with fragment = %q, want %q", got, "https://example.com/page")
	}
}

func TestResolveLink_FragmentOnlyHref(t *testing.T) {
	base, _ := url.Parse("https://example.com/page")

	got, _ := resolveLink(base, "#section")
	if got != "" {
		t.Errorf("resolveLink(%q) = %q, want empty", "#section", got)
	}
}

func TestResolveLink_NormalizesHostToLowercase(t *testing.T) {
	base, _ := url.Parse("https://example.com/")

	got, _ := resolveLink(base, "https://EXAMPLE.COM/Page")
	want := "https://example.com/Page"
	if got != want {
		t.Errorf("resolveLink with uppercase host = %q, want %q", got, want)
	}
}
