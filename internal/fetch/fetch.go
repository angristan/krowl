// Package fetch implements the fetcher goroutine pool.
// Fetchers pull domains from the frontier, respect rate limits,
// check robots.txt, and send results to parsers via a channel.
package fetch

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/stanislas/krowl/internal/domain"
)

const (
	FetchTimeout = 30 * time.Second
	MaxBodySize  = 1 * 1024 * 1024 // 1MB
	MaxRedirects = 5
)

// Result is the output of a successful fetch, sent to parsers.
type Result struct {
	URL     string
	Domain  string
	Body    []byte
	Status  int
	Headers http.Header
}

// Pool manages a pool of fetcher goroutines.
type Pool struct {
	domains   *domain.Manager
	results   chan<- Result
	client    *http.Client
	userAgent string
	workers   int
}

// NewPool creates a fetcher pool.
func NewPool(dm *domain.Manager, results chan<- Result, userAgent string, workers int) *Pool {
	client := &http.Client{
		Timeout: FetchTimeout,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			if len(via) >= MaxRedirects {
				return fmt.Errorf("too many redirects")
			}
			return nil
		},
	}

	return &Pool{
		domains:   dm,
		results:   results,
		client:    client,
		userAgent: userAgent,
		workers:   workers,
	}
}

// Run starts all fetcher goroutines. Blocks until context is cancelled.
func (p *Pool) Run(ctx context.Context) {
	var wg sync.WaitGroup
	for i := 0; i < p.workers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			p.fetchLoop(ctx, id)
		}(i)
	}
	wg.Wait()
}

func (p *Pool) fetchLoop(ctx context.Context, id int) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		// Find a domain with URLs that's ready to be fetched
		domains := p.domains.ActiveDomains()
		if len(domains) == 0 {
			time.Sleep(100 * time.Millisecond)
			continue
		}

		fetched := false
		for _, d := range domains {
			ok, wait := p.domains.CanFetch(d)
			if !ok {
				continue
			}
			if wait > 0 {
				continue // skip, try another domain
			}

			rawURL := p.domains.Dequeue(d)
			if rawURL == "" {
				continue
			}

			// Check robots.txt
			path := extractPath(rawURL)
			allowed, err := p.domains.IsAllowed(d, path)
			if err != nil || !allowed {
				continue
			}

			// Fetch
			p.domains.RecordFetch(d)
			result, err := p.fetch(ctx, rawURL, d)
			if err != nil {
				p.domains.RecordError(d)
				continue
			}

			select {
			case p.results <- *result:
			case <-ctx.Done():
				return
			}

			fetched = true
			break
		}

		if !fetched {
			// All domains are rate-limited or empty, wait a bit
			time.Sleep(50 * time.Millisecond)
		}
	}
}

func (p *Pool) fetch(ctx context.Context, rawURL, d string) (*Result, error) {
	ctx, cancel := context.WithTimeout(ctx, FetchTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, "GET", rawURL, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", p.userAgent)
	req.Header.Set("Accept", "text/html,application/xhtml+xml")
	req.Header.Set("Accept-Language", "en-US,en;q=0.9")

	resp, err := p.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	// Only process HTML responses
	ct := resp.Header.Get("Content-Type")
	if !strings.Contains(ct, "text/html") && !strings.Contains(ct, "application/xhtml") {
		return nil, fmt.Errorf("non-html content type: %s", ct)
	}

	body, err := io.ReadAll(io.LimitReader(resp.Body, MaxBodySize))
	if err != nil {
		return nil, err
	}

	return &Result{
		URL:     rawURL,
		Domain:  d,
		Body:    body,
		Status:  resp.StatusCode,
		Headers: resp.Header,
	}, nil
}

func extractPath(rawURL string) string {
	// Fast path extraction without full URL parse
	idx := strings.Index(rawURL, "://")
	if idx == -1 {
		return "/"
	}
	rest := rawURL[idx+3:]
	idx = strings.Index(rest, "/")
	if idx == -1 {
		return "/"
	}
	return rest[idx:]
}
