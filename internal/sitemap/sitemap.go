// Package sitemap fetches and parses XML sitemaps to discover URLs.
// Handles both regular sitemaps and sitemap index files.
// Uses streaming xml.Decoder to avoid loading full DOM into memory.
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

	return f.streamParse(body, depth)
}

// streamParse uses xml.Decoder to parse a sitemap without loading the full
// DOM into memory. It detects the root element to distinguish sitemap index
// files (<sitemapindex>) from regular sitemaps (<urlset>), then streams
// child elements extracting <loc> values.
func (f *Fetcher) streamParse(r io.Reader, depth int) []string {
	dec := xml.NewDecoder(r)
	// Disable strict mode to tolerate malformed sitemaps
	dec.Strict = false

	// Find the root element to determine sitemap type
	var rootName string
	for {
		tok, err := dec.Token()
		if err != nil {
			return nil
		}
		if se, ok := tok.(xml.StartElement); ok {
			rootName = se.Name.Local
			break
		}
	}

	switch rootName {
	case "sitemapindex":
		return f.parseIndex(dec, depth)
	case "urlset":
		return f.parseURLSet(dec)
	default:
		return nil
	}
}

// parseIndex streams a <sitemapindex>, extracting <loc> from each <sitemap>
// child and recursively fetching them.
func (f *Fetcher) parseIndex(dec *xml.Decoder, depth int) []string {
	var urls []string

	for {
		tok, err := dec.Token()
		if err != nil {
			break // EOF or error
		}

		se, ok := tok.(xml.StartElement)
		if !ok {
			continue
		}

		if se.Name.Local == "sitemap" {
			loc := extractLoc(dec, "sitemap")
			if loc != "" {
				child := f.fetchSitemap(loc, depth+1)
				urls = append(urls, child...)
				if len(urls) >= MaxURLsPerSite {
					return urls[:MaxURLsPerSite]
				}
			}
		}
	}
	return urls
}

// parseURLSet streams a <urlset>, extracting <loc> from each <url> child.
func (f *Fetcher) parseURLSet(dec *xml.Decoder) []string {
	var urls []string

	for {
		tok, err := dec.Token()
		if err != nil {
			break // EOF or error
		}

		se, ok := tok.(xml.StartElement)
		if !ok {
			continue
		}

		if se.Name.Local == "url" {
			loc := extractLoc(dec, "url")
			if loc != "" && (strings.HasPrefix(loc, "http://") || strings.HasPrefix(loc, "https://")) {
				urls = append(urls, loc)
				if len(urls) >= MaxURLsPerSite {
					return urls
				}
			}
		}
	}
	return urls
}

// extractLoc reads inside a parent element (e.g. <url> or <sitemap>) and
// returns the text content of the first <loc> child. Consumes tokens until
// the matching end element.
func extractLoc(dec *xml.Decoder, parent string) string {
	var loc string
	depth := 1

	for {
		tok, err := dec.Token()
		if err != nil {
			return loc
		}

		switch t := tok.(type) {
		case xml.StartElement:
			depth++
			if t.Name.Local == "loc" {
				// Read the text content of <loc>
				var content string
				for {
					inner, err := dec.Token()
					if err != nil {
						return ""
					}
					if cd, ok := inner.(xml.CharData); ok {
						content += string(cd)
					}
					if _, ok := inner.(xml.EndElement); ok {
						depth--
						break
					}
				}
				loc = strings.TrimSpace(content)
			}
		case xml.EndElement:
			depth--
			if depth == 0 && t.Name.Local == parent {
				return loc
			}
		}
	}
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
