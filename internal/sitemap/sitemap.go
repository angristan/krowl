// Package sitemap fetches and parses XML sitemaps to discover URLs.
// Handles both regular sitemaps and sitemap index files.
//
// Uses a lightweight byte-level parser instead of encoding/xml to avoid
// the massive allocation overhead of xml.Decoder (~30% of total alloc_objects
// in profiling). Sitemaps have a trivial structure: we only need <loc>
// text from <url> or <sitemap> elements.
package sitemap

import (
	"bytes"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"
)

const (
	MaxSitemapSize = 2 * 1024 * 1024 // 2MB (down from 10MB)
	FetchTimeout   = 15 * time.Second
	MaxURLsPerSite = 1000 // match MaxQueuePerDomain; extras would be rejected by Enqueue anyway
	MaxIndexDepth  = 2    // don't follow sitemap indexes more than 2 levels deep
)

// Fetcher fetches and parses sitemaps for domains.
type Fetcher struct {
	client    *http.Client
	userAgent string
}

// NewFetcher creates a sitemap fetcher.
func NewFetcher(userAgent string) *Fetcher {
	return &Fetcher{
		client: &http.Client{
			Timeout: FetchTimeout,
		},
		userAgent: userAgent,
	}
}

// FetchURLs fetches the sitemap for a domain and returns discovered URLs.
// Tries /sitemap.xml first, then any Sitemap: directives from robotsHints.
// Returns at most MaxURLsPerSite URLs.
func (f *Fetcher) FetchURLs(domain string, robotsHints []string) []string {
	// Collect sitemap URLs to try
	candidates := []string{fmt.Sprintf("https://%s/sitemap.xml", domain)}
	for _, hint := range robotsHints {
		hint = strings.TrimSpace(hint)
		if hint != "" && !contains(candidates, hint) {
			candidates = append(candidates, hint)
		}
	}

	var allURLs []string
	seen := make(map[string]struct{})

	for _, candidate := range candidates {
		urls := f.fetchSitemap(candidate, 0)
		for _, u := range urls {
			if _, ok := seen[u]; !ok {
				seen[u] = struct{}{}
				allURLs = append(allURLs, u)
				if len(allURLs) >= MaxURLsPerSite {
					slog.Info("sitemap URL cap reached", "domain", domain, "cap", MaxURLsPerSite)
					return allURLs
				}
			}
		}
	}

	if len(allURLs) > 0 {
		slog.Info("sitemap URLs discovered", "domain", domain, "count", len(allURLs))
	}
	return allURLs
}

func (f *Fetcher) fetchSitemap(url string, depth int) []string {
	if depth > MaxIndexDepth {
		return nil
	}

	body, err := f.fetch(url)
	if err != nil {
		return nil
	}
	defer func() { _ = body.Close() }()

	// Read entire body (capped at MaxSitemapSize) for byte-level parsing.
	data, err := io.ReadAll(body)
	if err != nil {
		return nil
	}

	return f.parseSitemap(data, depth)
}

// parseSitemap detects the sitemap type (urlset vs sitemapindex) and
// extracts <loc> values using byte-level scanning. This avoids the
// encoding/xml decoder which was 30% of total allocation objects.
func (f *Fetcher) parseSitemap(data []byte, depth int) []string {
	// Detect root element
	isSitemapIndex := bytes.Contains(data[:min(len(data), 512)], []byte("<sitemapindex"))

	if isSitemapIndex {
		return f.parseIndexBytes(data, depth)
	}
	return f.parseURLSetBytes(data)
}

// parseURLSetBytes extracts <loc> values from <url> elements using byte scanning.
func (f *Fetcher) parseURLSetBytes(data []byte) []string {
	var urls []string
	pos := 0
	for pos < len(data) {
		loc := extractLocFromTag(data, &pos, "url")
		if loc == "" {
			break
		}
		if strings.HasPrefix(loc, "http://") || strings.HasPrefix(loc, "https://") {
			urls = append(urls, loc)
			if len(urls) >= MaxURLsPerSite {
				return urls
			}
		}
	}
	return urls
}

// parseIndexBytes extracts <loc> values from <sitemap> elements in a
// sitemap index and recursively fetches child sitemaps.
func (f *Fetcher) parseIndexBytes(data []byte, depth int) []string {
	var urls []string
	pos := 0
	for pos < len(data) {
		loc := extractLocFromTag(data, &pos, "sitemap")
		if loc == "" {
			break
		}
		child := f.fetchSitemap(loc, depth+1)
		urls = append(urls, child...)
		if len(urls) >= MaxURLsPerSite {
			return urls[:MaxURLsPerSite]
		}
	}
	return urls
}

// extractLocFromTag finds the next <tag>...</tag> block starting at *pos
// and extracts the text content of its first <loc>...</loc> child.
// Advances *pos past the closing </tag>. Returns "" if no more tags found.
func extractLocFromTag(data []byte, pos *int, tag string) string {
	openTag := []byte("<" + tag)
	closeTag := []byte("</" + tag + ">")

	// Find opening tag
	idx := indexCaseInsensitive(data[*pos:], openTag)
	if idx < 0 {
		*pos = len(data)
		return ""
	}
	start := *pos + idx

	// Find closing tag
	end := indexCaseInsensitive(data[start:], closeTag)
	if end < 0 {
		*pos = len(data)
		return ""
	}
	block := data[start : start+end]
	*pos = start + end + len(closeTag)

	// Extract <loc>...</loc> within the block
	return extractLocText(block)
}

// extractLocText extracts the text content of the first <loc>...</loc>
// within a byte slice. Returns "" if not found.
func extractLocText(block []byte) string {
	locOpen := []byte("<loc>")
	locClose := []byte("</loc>")

	start := indexCaseInsensitive(block, locOpen)
	if start < 0 {
		return ""
	}
	start += len(locOpen)

	end := indexCaseInsensitive(block[start:], locClose)
	if end < 0 {
		return ""
	}

	loc := string(bytes.TrimSpace(block[start : start+end]))

	// Handle CDATA: <loc><![CDATA[...]]></loc>
	if strings.HasPrefix(loc, "<![CDATA[") && strings.HasSuffix(loc, "]]>") {
		loc = loc[9 : len(loc)-3]
		loc = strings.TrimSpace(loc)
	}

	return loc
}

// indexCaseInsensitive finds needle in haystack with case-insensitive
// matching for ASCII tag names. For our use case (XML tags), this is
// sufficient and avoids allocating lowercased copies.
func indexCaseInsensitive(haystack, needle []byte) int {
	if len(needle) > len(haystack) {
		return -1
	}
	// Fast path: try exact match first (most sitemaps use lowercase)
	if idx := bytes.Index(haystack, needle); idx >= 0 {
		return idx
	}
	// Slow path: case-insensitive scan
	for i := 0; i <= len(haystack)-len(needle); i++ {
		match := true
		for j := range needle {
			a, b := haystack[i+j], needle[j]
			if a != b && a|0x20 != b|0x20 {
				match = false
				break
			}
		}
		if match {
			return i
		}
	}
	return -1
}

// fetch downloads a sitemap URL, returning the response body as a reader.
// The caller MUST close the returned ReadCloser.
func (f *Fetcher) fetch(url string) (io.ReadCloser, error) {
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", f.userAgent)

	resp, err := f.client.Do(req)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != http.StatusOK {
		_ = resp.Body.Close()
		return nil, fmt.Errorf("status %d", resp.StatusCode)
	}

	ct := resp.Header.Get("Content-Type")
	if !strings.Contains(ct, "xml") && !strings.Contains(ct, "text/") {
		_ = resp.Body.Close()
		return nil, fmt.Errorf("non-xml content type: %s", ct)
	}

	// Wrap in LimitReader to cap memory, but return as ReadCloser
	// so the underlying connection gets released.
	return &limitedReadCloser{
		Reader: io.LimitReader(resp.Body, MaxSitemapSize),
		Closer: resp.Body,
	}, nil
}

// limitedReadCloser wraps an io.Reader with a separate io.Closer.
type limitedReadCloser struct {
	io.Reader
	io.Closer
}

func contains(ss []string, s string) bool {
	for _, v := range ss {
		if v == s {
			return true
		}
	}
	return false
}
