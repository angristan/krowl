// Package parse implements the parser goroutine pool.
// Parsers extract links from fetched HTML pages, run them through
// dedup, and either enqueue locally or forward to the owning node.
package parse

import (
	"context"
	"hash/fnv"
	"log/slog"
	"net/url"
	"strings"
	"sync"

	"golang.org/x/net/html"

	"github.com/stanislas/krowl/internal/dedup"
	"github.com/stanislas/krowl/internal/domain"
	"github.com/stanislas/krowl/internal/fetch"
	"github.com/stanislas/krowl/internal/inbox"
	m "github.com/stanislas/krowl/internal/metrics"
	"github.com/stanislas/krowl/internal/ring"
	"github.com/stanislas/krowl/internal/urlnorm"
)

// soft404Threshold: if the same body hash appears this many times on a
// domain, we stop extracting links from it (likely a soft-404 template).
const soft404Threshold = 3

// Pool manages a pool of parser goroutines.
type Pool struct {
	results      <-chan fetch.Result
	dedup        *dedup.Dedup
	domains      *domain.Manager
	sender       *inbox.Sender
	ring         *ring.Ring
	myID         int
	workers      int
	contentDedup contentTracker
}

// contentTracker tracks per-domain body hashes for soft-404 detection.
type contentTracker struct {
	mu     sync.Mutex
	counts map[domainHash]int // (domain, body_hash) -> count
}

type domainHash struct {
	domain string
	hash   uint64
}

func (ct *contentTracker) isDuplicate(domain string, body []byte) bool {
	h := fnv.New64a()
	h.Write(body)
	key := domainHash{domain: domain, hash: h.Sum64()}

	ct.mu.Lock()
	defer ct.mu.Unlock()
	ct.counts[key]++
	return ct.counts[key] >= soft404Threshold
}

// NewPool creates a parser pool.
func NewPool(
	results <-chan fetch.Result,
	d *dedup.Dedup,
	dm *domain.Manager,
	sender *inbox.Sender,
	r *ring.Ring,
	myID int,
	workers int,
) *Pool {
	return &Pool{
		results: results,
		dedup:   d,
		domains: dm,
		sender:  sender,
		ring:    r,
		myID:    myID,
		workers: workers,
		contentDedup: contentTracker{
			counts: make(map[domainHash]int),
		},
	}
}

// Run starts all parser goroutines. Blocks until the results channel is closed.
func (p *Pool) Run(ctx context.Context) {
	var wg sync.WaitGroup
	for i := 0; i < p.workers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			p.parseLoop(ctx, id)
		}(i)
	}
	wg.Wait()
}

func (p *Pool) parseLoop(ctx context.Context, id int) {
	for {
		select {
		case <-ctx.Done():
			return
		case result, ok := <-p.results:
			if !ok {
				return
			}
			p.processResult(ctx, result)
		}
	}
}

func (p *Pool) processResult(ctx context.Context, result fetch.Result) {
	// Content hash dedup: skip link extraction for repeated soft-404 bodies
	if p.contentDedup.isDuplicate(result.Domain, result.Body) {
		m.ContentDedupSkipped.Inc()
		slog.Debug("skipping duplicate content body",
			"url", result.URL, "domain", result.Domain)
		return
	}

	links := extractLinks(result.Body, result.URL)

	deduped := 0
	forwarded := 0
	enqueued := 0

	childDepth := result.Depth + 1

	for _, link := range links {
		if !p.dedup.IsNew(link) {
			deduped++
			continue
		}

		d := domain.ExtractDomain(link)
		owner := p.ring.Owner(d)

		if owner == p.myID {
			p.domains.Enqueue(d, link, childDepth)
			enqueued++
		} else {
			if err := p.sender.Forward(ctx, link, childDepth); err != nil {
				// Peer unavailable, skip
				continue
			}
			forwarded++
		}
	}

	// Report metrics
	m.URLsDiscovered.Add(float64(len(links)))
	m.LinksPerPage.Observe(float64(len(links)))
	m.URLsDeduped.Add(float64(deduped))
	m.URLsForwarded.Add(float64(forwarded))
	m.URLsEnqueued.Add(float64(enqueued))

	slog.Debug("page parsed",
		"url", result.URL,
		"links", len(links),
		"deduped", deduped,
		"enqueued", enqueued,
		"forwarded", forwarded,
	)
}

// extractLinks parses HTML and returns all absolute http/https URLs
// found in <a href="..."> tags.
func extractLinks(body []byte, baseURL string) []string {
	base, err := url.Parse(baseURL)
	if err != nil {
		return nil
	}

	tokenizer := html.NewTokenizer(strings.NewReader(string(body)))
	seen := make(map[string]struct{})
	var links []string

	for {
		tt := tokenizer.Next()
		switch tt {
		case html.ErrorToken:
			return links
		case html.StartTagToken, html.SelfClosingTagToken:
			tn, hasAttr := tokenizer.TagName()
			if len(tn) != 1 || tn[0] != 'a' || !hasAttr {
				continue
			}

			for {
				key, val, more := tokenizer.TagAttr()
				if string(key) == "href" {
					link := resolveLink(base, string(val))
					if link != "" {
						if _, ok := seen[link]; !ok {
							seen[link] = struct{}{}
							links = append(links, link)
						}
					}
				}
				if !more {
					break
				}
			}
		}
	}
}

// resolveLink resolves a potentially relative href against the base URL,
// then normalizes aggressively (strip tracking params, sort query, strip
// www, etc). Returns empty string for non-http(s) URLs.
func resolveLink(base *url.URL, href string) string {
	href = strings.TrimSpace(href)
	if href == "" || strings.HasPrefix(href, "#") || strings.HasPrefix(href, "javascript:") ||
		strings.HasPrefix(href, "mailto:") || strings.HasPrefix(href, "tel:") {
		return ""
	}

	u, err := url.Parse(href)
	if err != nil {
		return ""
	}

	resolved := base.ResolveReference(u)
	if resolved.Scheme != "http" && resolved.Scheme != "https" {
		return ""
	}

	normalized := urlnorm.Normalize(resolved.String())
	if len(normalized) > domain.MaxURLLength {
		return ""
	}
	return normalized
}
