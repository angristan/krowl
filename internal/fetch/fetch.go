// Package fetch implements the fetcher goroutine pool.
// Fetchers pull domains from the frontier priority heap, respect rate
// limits, check robots.txt, and send results to parsers via a channel.
package fetch

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptrace"
	"strings"
	"sync"
	"time"

	"github.com/stanislas/krowl/internal/domain"
	"github.com/stanislas/krowl/internal/frontier"
	m "github.com/stanislas/krowl/internal/metrics"
)

// errNonRetryable wraps errors that should not be retried (non-HTML, bad request, etc.)
var errNonRetryable = errors.New("non-retryable")

const (
	FetchTimeout = 10 * time.Second
	MaxBodySize  = 1 * 1024 * 1024 // 1MB
	MaxRedirects = 5
	MaxRetries   = 1 // retry transient errors once

	// Connection pool tuning
	maxIdleConns        = 1000 // total idle connections across all hosts
	maxIdleConnsPerHost = 2    // keep-alive connections per domain
	idleConnTimeout     = 90 * time.Second
	tlsHandshakeTimeout = 10 * time.Second
	dialTimeout         = 10 * time.Second
	dialKeepAlive       = 30 * time.Second

	// DNS cache tuning
	dnsCacheTTL  = 5 * time.Minute
	dnsCacheSize = 50000
)

// Result is the output of a successful fetch, sent to parsers.
type Result struct {
	URL            string
	Domain         string
	Depth          int // crawl depth (hops from seed)
	Body           []byte
	Status         int
	Headers        http.Header
	RequestHeaders http.Header // captured request headers for WARC request record
	Method         string      // HTTP method (GET)
	RequestURI     string      // path + query as sent in the request line
}

// Pool manages a pool of fetcher goroutines.
type Pool struct {
	domains   *domain.Manager
	frontier  *frontier.Frontier
	results   chan<- Result
	client    *http.Client
	userAgent string
	workers   int
}

// NewPool creates a fetcher pool with connection pooling and DNS caching.
func NewPool(dm *domain.Manager, fr *frontier.Frontier, results chan<- Result, userAgent string, workers int) *Pool {
	dnsCache := newDNSCache(dnsCacheSize, dnsCacheTTL)

	dialer := &net.Dialer{
		Timeout:   dialTimeout,
		KeepAlive: dialKeepAlive,
	}

	transport := &http.Transport{
		DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			host, port, err := net.SplitHostPort(addr)
			if err != nil {
				return dialer.DialContext(ctx, network, addr)
			}
			// Check DNS cache
			if ip, ok := dnsCache.get(host); ok {
				return dialer.DialContext(ctx, network, net.JoinHostPort(ip, port))
			}
			// Resolve and cache
			ips, err := net.DefaultResolver.LookupHost(ctx, host)
			if err != nil {
				return nil, err
			}
			if len(ips) > 0 {
				dnsCache.put(host, ips[0])
				return dialer.DialContext(ctx, network, net.JoinHostPort(ips[0], port))
			}
			return dialer.DialContext(ctx, network, addr)
		},
		MaxIdleConns:        maxIdleConns,
		MaxIdleConnsPerHost: maxIdleConnsPerHost,
		IdleConnTimeout:     idleConnTimeout,
		TLSHandshakeTimeout: tlsHandshakeTimeout,
		DisableCompression:  true,  // we want raw content for WARC
		ForceAttemptHTTP2:   false, // HTTP/1.1 for simpler keep-alive
	}

	client := &http.Client{
		Timeout:   FetchTimeout,
		Transport: transport,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			if len(via) >= MaxRedirects {
				return fmt.Errorf("too many redirects")
			}
			return nil
		},
	}

	return &Pool{
		domains:   dm,
		frontier:  fr,
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

		// Pop the next ready domain from the min-heap (O(log n))
		d, wait := p.frontier.PopReady()
		if d == "" {
			if wait == 0 {
				// Frontier is empty, back off
				time.Sleep(100 * time.Millisecond)
			} else {
				// Next domain isn't ready yet; sleep the minimum of wait or 50ms
				sleep := wait
				if sleep > 50*time.Millisecond {
					sleep = 50 * time.Millisecond
				}
				time.Sleep(sleep)
			}
			continue
		}

		// Dequeue a URL from this domain's queue
		item, ok := p.domains.Dequeue(d)
		if !ok {
			// Domain was in frontier but queue drained (race). Don't re-push.
			continue
		}
		rawURL := item.URL

		// Skip dead domains (permanently abandoned after too many errors)
		if p.domains.IsDead(d) {
			continue
		}

		// Check robots.txt
		path := extractPath(rawURL)
		allowed, err := p.domains.IsAllowed(d, path)
		if err != nil || !allowed {
			if !allowed {
				m.RobotsBlocked.Inc()
			}
			p.repushIfNeeded(d)
			continue
		}

		// Fetch with retries on transient errors
		fetchStart := time.Now()
		result, err := p.fetchWithRetry(ctx, rawURL, d)
		latency := time.Since(fetchStart)
		if err != nil {
			p.domains.RecordFetch(d, latency)
			p.domains.RecordError(d)
			if p.domains.IsDead(d) {
				m.DomainsDead.Inc()
			}
			p.repushIfNeeded(d)
			continue
		}
		p.domains.RecordFetch(d, latency)

		result.Depth = item.Depth

		select {
		case p.results <- *result:
		case <-ctx.Done():
			return
		}

		// Re-push domain into frontier with next allowed fetch time
		p.repushIfNeeded(d)
	}
}

// repushIfNeeded re-inserts a domain into the frontier heap if it still has
// queued URLs. The next-fetch time comes from the domain manager.
func (p *Pool) repushIfNeeded(d string) {
	if p.domains.QueueLen(d) > 0 {
		p.frontier.Push(d, p.domains.NextFetchTime(d))
	}
}

// fetchWithRetry wraps fetch with retry logic for transient errors
// (network timeouts, DNS failures, connection resets).
// Non-retryable errors (non-HTML, request build failures) fail immediately.
func (p *Pool) fetchWithRetry(ctx context.Context, rawURL, d string) (*Result, error) {
	var lastErr error
	for attempt := 0; attempt <= MaxRetries; attempt++ {
		if attempt > 0 {
			backoff := time.Duration(attempt) * 500 * time.Millisecond
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(backoff):
			}
			m.FetchRetries.Inc()
		}
		result, err := p.fetch(ctx, rawURL, d)
		if err == nil {
			return result, nil
		}
		lastErr = err
		if errors.Is(err, errNonRetryable) {
			return nil, err
		}
	}
	return nil, lastErr
}

func (p *Pool) fetch(ctx context.Context, rawURL, d string) (*Result, error) {
	ctx, cancel := context.WithTimeout(ctx, FetchTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, "GET", rawURL, nil)
	if err != nil {
		m.FetchErrors.WithLabelValues("request_build").Inc()
		return nil, fmt.Errorf("%w: %v", errNonRetryable, err)
	}
	req.Header.Set("User-Agent", p.userAgent)
	req.Header.Set("Accept", "text/html,application/xhtml+xml")
	req.Header.Set("Accept-Language", "en-US,en;q=0.9")

	// Capture request headers before sending (for WARC request record)
	reqHeaders := req.Header.Clone()

	// httptrace: capture per-phase timings
	var (
		dnsStart, connectStart, tlsStart time.Time
		gotFirstByte                     time.Time
		fetchStart                       = time.Now()
		redirectCount                    int
	)

	trace := &httptrace.ClientTrace{
		DNSStart: func(_ httptrace.DNSStartInfo) {
			dnsStart = time.Now()
		},
		DNSDone: func(_ httptrace.DNSDoneInfo) {
			if !dnsStart.IsZero() {
				m.DNSDuration.Observe(time.Since(dnsStart).Seconds())
			}
		},
		ConnectStart: func(_, _ string) {
			connectStart = time.Now()
		},
		ConnectDone: func(_, _ string, err error) {
			if !connectStart.IsZero() && err == nil {
				m.ConnectDuration.Observe(time.Since(connectStart).Seconds())
			}
		},
		TLSHandshakeStart: func() {
			tlsStart = time.Now()
		},
		TLSHandshakeDone: func(_ tls.ConnectionState, _ error) {
			if !tlsStart.IsZero() {
				m.TLSDuration.Observe(time.Since(tlsStart).Seconds())
			}
		},
		GotFirstResponseByte: func() {
			gotFirstByte = time.Now()
			m.TTFBDuration.Observe(time.Since(fetchStart).Seconds())
		},
	}
	req = req.WithContext(httptrace.WithClientTrace(req.Context(), trace))

	resp, err := p.client.Do(req)
	if err != nil {
		m.FetchErrors.WithLabelValues("network").Inc()
		return nil, err
	}
	defer resp.Body.Close()

	// Count redirects by walking the response chain
	if resp.Request != nil {
		for r := resp.Request; r.Response != nil; r = r.Response.Request {
			redirectCount++
			if r.Response.Request == nil {
				break
			}
		}
	}
	_ = gotFirstByte // used in trace callback

	// Record status
	m.PagesFetched.WithLabelValues(m.StatusBucket(resp.StatusCode)).Inc()

	// Content type tracking
	ct := resp.Header.Get("Content-Type")
	ctShort := shortContentType(ct)
	m.ContentTypes.WithLabelValues(ctShort).Inc()

	// Only process HTML responses
	if !strings.Contains(ct, "text/html") && !strings.Contains(ct, "application/xhtml") {
		m.FetchErrors.WithLabelValues("non_html").Inc()
		return nil, fmt.Errorf("%w: non-html content type: %s", errNonRetryable, ct)
	}

	body, err := io.ReadAll(io.LimitReader(resp.Body, MaxBodySize))
	if err != nil {
		m.FetchErrors.WithLabelValues("body_read").Inc()
		return nil, err
	}

	// Record metrics
	totalDuration := time.Since(fetchStart).Seconds()
	m.FetchDuration.Observe(totalDuration)
	m.ResponseSize.Observe(float64(len(body)))
	m.RedirectsFollowed.Observe(float64(redirectCount))

	// Build request URI (path + query)
	requestURI := req.URL.RequestURI()

	return &Result{
		URL:            rawURL,
		Domain:         d,
		Body:           body,
		Status:         resp.StatusCode,
		Headers:        resp.Header,
		RequestHeaders: reqHeaders,
		Method:         req.Method,
		RequestURI:     requestURI,
	}, nil
}

// shortContentType extracts the base content type (e.g. "text/html" from "text/html; charset=utf-8").
func shortContentType(ct string) string {
	if ct == "" {
		return "unknown"
	}
	if idx := strings.IndexByte(ct, ';'); idx != -1 {
		ct = ct[:idx]
	}
	return strings.TrimSpace(strings.ToLower(ct))
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

// --- DNS cache ---

type dnsCacheEntry struct {
	ip      string
	expires time.Time
}

type dnsCache struct {
	mu      sync.RWMutex
	entries map[string]dnsCacheEntry
	maxSize int
	ttl     time.Duration
}

func newDNSCache(maxSize int, ttl time.Duration) *dnsCache {
	return &dnsCache{
		entries: make(map[string]dnsCacheEntry, maxSize),
		maxSize: maxSize,
		ttl:     ttl,
	}
}

func (c *dnsCache) get(host string) (string, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	e, ok := c.entries[host]
	if !ok || time.Now().After(e.expires) {
		m.DNSCacheMisses.Inc()
		return "", false
	}
	m.DNSCacheHits.Inc()
	return e.ip, true
}

func (c *dnsCache) put(host, ip string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	// Simple eviction: if at capacity, clear the whole cache.
	if len(c.entries) >= c.maxSize {
		c.entries = make(map[string]dnsCacheEntry, c.maxSize)
		m.DNSCacheEvictions.Inc()
	}
	c.entries[host] = dnsCacheEntry{
		ip:      ip,
		expires: time.Now().Add(c.ttl),
	}
	m.DNSCacheSize.Set(float64(len(c.entries)))
}
