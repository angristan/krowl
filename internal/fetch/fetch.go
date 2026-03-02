// Package fetch implements the fetcher goroutine pool.
// Fetchers pull domains from the frontier priority heap, respect rate
// limits, check robots.txt, and send results to parsers via a channel.
//
// WARC recording is handled transparently by the gowarc HTTP client:
// every request/response pair is captured at the transport layer via
// TeeReader/MultiWriter wrapping on the TCP connection.
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

	warc "github.com/internetarchive/gowarc"
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
)

// TLSInfo holds TLS connection state extracted by the patched gowarc dialer.
// A pointer to this struct is stored in the request context before client.Do();
// the DialTLSContext wrapper populates it via reflection on the utls connection.
type TLSInfo struct {
	Version     uint16
	CipherSuite uint16
	Set         bool // true if the dialer populated this
}

type tlsInfoKeyType struct{}

// TLSInfoKey is the context key for *TLSInfo.
var TLSInfoKey = tlsInfoKeyType{}

// Result is the output of a successful fetch, sent to parsers.
type Result struct {
	URL    string
	Domain string
	Depth  int // crawl depth (hops from seed)
	Body   []byte
	Status int
}

// Pool manages a pool of fetcher goroutines.
type Pool struct {
	domains   *domain.Manager
	frontier  *frontier.Frontier
	results   chan<- Result
	client    *warc.CustomHTTPClient
	userAgent string
	workers   int
}

// NewPool creates a fetcher pool using a gowarc WARC-writing HTTP client.
// The client handles WARC recording transparently at the transport layer.
func NewPool(dm *domain.Manager, fr *frontier.Frontier, results chan<- Result, client *warc.CustomHTTPClient, userAgent string, workers int) *Pool {
	// Override redirect policy to match our max redirects.
	client.CheckRedirect = func(req *http.Request, via []*http.Request) error {
		if len(via) >= MaxRedirects {
			return fmt.Errorf("too many redirects")
		}
		return nil
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
	// Note: Accept-Encoding is set by gowarc's transport (gzip).
	// Manual decompression is no longer needed — gowarc handles it.

	// httptrace: capture per-phase timings.
	// Note: gowarc does DNS and TLS in its custom dialer, so DNSStart/DNSDone
	// and TLSHandshakeStart/TLSHandshakeDone do NOT fire. ConnectStart/ConnectDone
	// and GotFirstResponseByte still work.
	var (
		connectStart  time.Time
		fetchStart    = time.Now()
		redirectCount int
	)

	trace := &httptrace.ClientTrace{
		ConnectStart: func(_, addr string) {
			connectStart = time.Now()
			// Track IPv4 vs IPv6 from the resolved address (ip:port).
			if host, _, err := net.SplitHostPort(addr); err == nil {
				if ip := net.ParseIP(host); ip != nil {
					if ip.To4() != nil {
						m.IPVersion.WithLabelValues("IPv4").Inc()
					} else {
						m.IPVersion.WithLabelValues("IPv6").Inc()
					}
				}
			}
		},
		ConnectDone: func(_, _ string, err error) {
			if !connectStart.IsZero() && err == nil {
				m.ConnectDuration.Observe(time.Since(connectStart).Seconds())
			}
		},
		GotFirstResponseByte: func() {
			m.TTFBDuration.Observe(time.Since(fetchStart).Seconds())
		},
	}
	req = req.WithContext(httptrace.WithClientTrace(req.Context(), trace))

	// Stash a TLSInfo pointer in context so the patched DialTLSContext can
	// write TLS state into it (gowarc hides TLS state from resp.TLS).
	tlsInfo := &TLSInfo{}
	req = req.WithContext(context.WithValue(req.Context(), TLSInfoKey, tlsInfo))

	resp, err := p.client.Do(req)
	if err != nil {
		m.FetchErrors.WithLabelValues("network").Inc()
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()

	// TLS version and cipher suite.
	// gowarc uses utls and wraps connections, so resp.TLS is always nil.
	// Instead, the patched DialTLSContext extracts TLS state via reflection
	// and writes it into the TLSInfo struct we stashed in the context.
	if tlsInfo.Set {
		m.TLSVersion.WithLabelValues(tlsVersionName(tlsInfo.Version)).Inc()
		m.TLSCipher.WithLabelValues(tls.CipherSuiteName(tlsInfo.CipherSuite)).Inc()
	} else if req.URL.Scheme == "https" {
		m.TLSVersion.WithLabelValues("unknown").Inc()
	}

	// Count redirects by walking the response chain
	if resp.Request != nil {
		for r := resp.Request; r.Response != nil; r = r.Response.Request {
			redirectCount++
			if r.Response.Request == nil {
				break
			}
		}
	}

	// Record status
	m.PagesFetched.WithLabelValues(m.StatusBucket(resp.StatusCode)).Inc()

	// HTTP protocol version (always HTTP/1.1 with gowarc — HTTP/2 disabled
	// because WARC recording requires per-connection TeeReader wrapping)
	m.HTTPVersion.WithLabelValues(resp.Proto).Inc()

	// URL scheme (http vs https)
	m.URLScheme.WithLabelValues(req.URL.Scheme).Inc()

	// Response content encoding (gowarc decompresses transparently,
	// but the Content-Encoding header is still present)
	encoding := strings.ToLower(resp.Header.Get("Content-Encoding"))
	if encoding == "" {
		encoding = "none"
	}
	m.ResponseEncoding.WithLabelValues(encoding).Inc()

	// Content type tracking
	ct := resp.Header.Get("Content-Type")
	ctShort := shortContentType(ct)
	m.ContentTypes.WithLabelValues(ctShort).Inc()

	// Only process HTML responses
	if !strings.Contains(ct, "text/html") && !strings.Contains(ct, "application/xhtml") {
		m.FetchErrors.WithLabelValues("non_html").Inc()
		return nil, fmt.Errorf("%w: non-html content type: %s", errNonRetryable, ct)
	}

	// Read body. gowarc has already decompressed gzip at the transport level.
	// Reading resp.Body also drives the TeeReader that feeds WARC recording.
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

	return &Result{
		URL:    rawURL,
		Domain: d,
		Body:   body,
		Status: resp.StatusCode,
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

// tlsVersionName maps TLS version constants to human-readable strings.
func tlsVersionName(v uint16) string {
	switch v {
	case tls.VersionTLS10:
		return "TLS 1.0"
	case tls.VersionTLS11:
		return "TLS 1.1"
	case tls.VersionTLS12:
		return "TLS 1.2"
	case tls.VersionTLS13:
		return "TLS 1.3"
	default:
		return "unknown"
	}
}
