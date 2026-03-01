// Package sitemap fetches and parses XML sitemaps to discover URLs.
// Handles both regular sitemaps and sitemap index files.
package sitemap

import (
	"encoding/xml"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"
)

const (
	MaxSitemapSize = 10 * 1024 * 1024 // 10MB
	FetchTimeout   = 15 * time.Second
	MaxURLsPerSite = 10000 // cap to avoid memory issues on huge sitemaps
	MaxIndexDepth  = 2     // don't follow sitemap indexes more than 2 levels deep
)

// sitemapIndex represents a sitemap index file.
type sitemapIndex struct {
	XMLName  xml.Name         `xml:"sitemapindex"`
	Sitemaps []sitemapPointer `xml:"sitemap"`
}

type sitemapPointer struct {
	Loc string `xml:"loc"`
}

// urlSet represents a regular sitemap.
type urlSet struct {
	XMLName xml.Name     `xml:"urlset"`
	URLs    []sitemapURL `xml:"url"`
}

type sitemapURL struct {
	Loc string `xml:"loc"`
}

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

	// Try to parse as sitemap index first
	var idx sitemapIndex
	if err := xml.Unmarshal(body, &idx); err == nil && len(idx.Sitemaps) > 0 {
		var urls []string
		for _, s := range idx.Sitemaps {
			urls = append(urls, f.fetchSitemap(s.Loc, depth+1)...)
			if len(urls) >= MaxURLsPerSite {
				return urls[:MaxURLsPerSite]
			}
		}
		return urls
	}

	// Parse as regular sitemap
	var us urlSet
	if err := xml.Unmarshal(body, &us); err != nil {
		return nil
	}

	urls := make([]string, 0, len(us.URLs))
	for _, u := range us.URLs {
		loc := strings.TrimSpace(u.Loc)
		if loc != "" && (strings.HasPrefix(loc, "http://") || strings.HasPrefix(loc, "https://")) {
			urls = append(urls, loc)
		}
	}
	return urls
}

func (f *Fetcher) fetch(url string) ([]byte, error) {
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", f.userAgent)

	resp, err := f.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("status %d", resp.StatusCode)
	}

	ct := resp.Header.Get("Content-Type")
	if !strings.Contains(ct, "xml") && !strings.Contains(ct, "text/") {
		return nil, fmt.Errorf("non-xml content type: %s", ct)
	}

	return io.ReadAll(io.LimitReader(resp.Body, MaxSitemapSize))
}

func contains(ss []string, s string) bool {
	for _, v := range ss {
		if v == s {
			return true
		}
	}
	return false
}
