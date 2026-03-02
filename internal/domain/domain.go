// Package domain manages per-domain state: robots.txt, rate limiting,
// crawl delay, error tracking, and URL frontiers.
package domain

import (
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/stanislas/krowl/internal/frontier"
	"github.com/stanislas/krowl/internal/sitemap"
	"github.com/temoto/robotstxt"
)

const (
	DefaultCrawlDelay  = 1 * time.Second
	MinCrawlDelay      = 250 * time.Millisecond // floor: never faster than this
	MaxCrawlDelay      = 30 * time.Second       // ceiling: give up sooner rather than crawling glacially
	RobotsExpiry       = 24 * time.Hour
	RobotsMaxSize      = 512 * 1024 // 512KB
	MaxConsecutiveErrs = 10         // start exponential backoff after this many
	MaxConsecutiveDead = 30         // permanently give up on a domain after this many

	MaxQueuePerDomain  = 1000      // cap URLs queued per domain (prevents crawler traps + forces diversity)
	DefaultMaxFrontier = 1_000_000 // default global cap on total queued URLs
	MaxURLLength       = 2048      // reject URLs longer than this
	MaxCrawlDepth      = 25        // maximum hops from a seed URL

	// Adaptive rate: delay = max(MinCrawlDelay, latency * multiplier)
	// A 200ms response -> 1s delay. A 2s response -> 10s delay.
	AdaptiveMultiplier = 5
)

// State holds all per-domain crawl state.
type State struct {
	// robots.txt (parsed via temoto/robotstxt)
	Robots       *robotstxt.RobotsData
	RobotsGroup  *robotstxt.Group // cached group for our user-agent
	RobotsExpiry time.Time

	// Rate limiting
	CrawlDelay       time.Duration
	RobotsCrawlDelay time.Duration // crawl-delay from robots.txt (0 if unset)
	LastFetch        time.Time
	AvgLatency       time.Duration // exponential moving average of response time

	// Health
	ConsecutiveErrors int
	BackoffUntil      time.Time

	// Sitemap
	SitemapChecked bool // true once we've attempted to fetch the sitemap

	// Permanent failure: domain exceeded MaxConsecutiveDead errors
	Dead bool

	// Frontier: URLs to crawl for this domain
	Queue []QueueItem
}

// QueueItem is a URL with its crawl depth (hops from a seed URL).
type QueueItem struct {
	URL   string
	Depth int
}

// Manager manages per-domain state. All methods are thread-safe.
type Manager struct {
	mu       sync.RWMutex
	domains  map[string]*State
	client   *http.Client
	frontier *frontier.Frontier // if non-nil, domains are pushed here on enqueue
	sitemap  *sitemap.Fetcher

	userAgent   string
	totalQueued atomic.Int64 // global count of URLs across all domain queues
	maxFrontier int64        // global cap; 0 = unlimited
}

// NewManager creates a domain manager with the given user agent string.
func NewManager(userAgent string, maxFrontier int) *Manager {
	return &Manager{
		domains: make(map[string]*State),
		client: &http.Client{
			Timeout: 5 * time.Second,
		},
		sitemap:     sitemap.NewFetcher(userAgent),
		userAgent:   userAgent,
		maxFrontier: int64(maxFrontier),
	}
}

// SetFrontier attaches a frontier heap so that Enqueue automatically
// pushes domains into the priority queue.
func (m *Manager) SetFrontier(f *frontier.Frontier) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.frontier = f
}

// GetOrCreate returns the state for a domain, creating it if needed.
func (m *Manager) GetOrCreate(domain string) *State {
	m.mu.Lock()
	defer m.mu.Unlock()
	s, ok := m.domains[domain]
	if !ok {
		s = &State{
			CrawlDelay: DefaultCrawlDelay,
		}
		m.domains[domain] = s
	}
	return s
}

// Enqueue adds a URL to a domain's frontier. If a frontier heap is
// attached, the domain is pushed into it so fetchers can find it.
// Silently drops URLs that are too long, too deep, from dead domains,
// or when the per-domain queue is at capacity.
func (m *Manager) Enqueue(domain, rawURL string, depth int) {
	if len(rawURL) > MaxURLLength || depth > MaxCrawlDepth {
		return
	}
	// Global backpressure: drop URL if frontier is full
	if m.maxFrontier > 0 && m.totalQueued.Load() >= m.maxFrontier {
		return
	}
	m.mu.Lock()
	s, ok := m.domains[domain]
	if !ok {
		s = &State{CrawlDelay: DefaultCrawlDelay}
		m.domains[domain] = s
	}
	if s.Dead || len(s.Queue) >= MaxQueuePerDomain {
		m.mu.Unlock()
		return
	}
	wasEmpty := len(s.Queue) == 0
	s.Queue = append(s.Queue, QueueItem{URL: rawURL, Depth: depth})
	m.totalQueued.Add(1)
	fr := m.frontier // capture under lock
	m.mu.Unlock()

	// Push domain into frontier if this is the first URL (domain wasn't
	// already in the heap). If it already has URLs, it's either in the
	// heap already or a fetcher has it checked out and will re-push.
	if wasEmpty && fr != nil {
		fr.Push(domain, m.NextFetchTime(domain))
	}
}

// Dequeue pops the next URL from a domain's frontier.
// Returns the item and true, or a zero item and false if empty.
func (m *Manager) Dequeue(domain string) (QueueItem, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	s, ok := m.domains[domain]
	if !ok || len(s.Queue) == 0 {
		return QueueItem{}, false
	}
	item := s.Queue[0]
	s.Queue = s.Queue[1:]
	m.totalQueued.Add(-1)
	return item, true
}

// QueueLen returns the number of URLs in a domain's frontier.
func (m *Manager) QueueLen(domain string) int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	s, ok := m.domains[domain]
	if !ok {
		return 0
	}
	return len(s.Queue)
}

// IsAllowed checks if a path is allowed by the domain's robots.txt.
// Fetches, parses, and caches robots.txt if not cached or expired.
func (m *Manager) IsAllowed(d, path string) (bool, error) {
	m.mu.RLock()
	s, ok := m.domains[d]
	robotsValid := ok && s.RobotsGroup != nil && time.Now().Before(s.RobotsExpiry)
	var group *robotstxt.Group
	if robotsValid {
		group = s.RobotsGroup
	}
	m.mu.RUnlock()

	if !robotsValid {
		body, err := m.fetchRobots(d)
		if err != nil {
			// Can't fetch robots.txt: allow by default
			return true, nil
		}

		robots, err := robotstxt.FromBytes([]byte(body))
		if err != nil {
			// Malformed robots.txt: allow by default
			return true, nil
		}

		group = robots.FindGroup(m.userAgent)

		m.mu.Lock()
		s = m.getOrCreateLocked(d)
		s.Robots = robots
		s.RobotsGroup = group
		s.RobotsExpiry = time.Now().Add(RobotsExpiry)

		// Store robots.txt crawl-delay as a floor for adaptive rate
		if group.CrawlDelay > 0 {
			s.RobotsCrawlDelay = time.Duration(group.CrawlDelay)
			if s.RobotsCrawlDelay > s.CrawlDelay {
				s.CrawlDelay = s.RobotsCrawlDelay
			}
		}
		m.mu.Unlock()

		// Trigger sitemap discovery in the background
		go m.DiscoverSitemap(d)
	}

	return group.Test(path), nil
}

// CanFetch checks if we can fetch from this domain now (rate limiting).
// Returns (allowed, waitDuration).
func (m *Manager) CanFetch(domain string) (bool, time.Duration) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	s, ok := m.domains[domain]
	if !ok {
		return true, 0
	}

	// Check backoff
	if time.Now().Before(s.BackoffUntil) {
		return false, time.Until(s.BackoffUntil)
	}

	// Check rate limit
	elapsed := time.Since(s.LastFetch)
	if elapsed < s.CrawlDelay {
		return true, s.CrawlDelay - elapsed
	}

	return true, 0
}

// RecordFetch marks that we just fetched from this domain and adapts
// the crawl delay based on observed response latency.
func (m *Manager) RecordFetch(domain string, latency time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	s := m.getOrCreateLocked(domain)
	s.LastFetch = time.Now()
	s.ConsecutiveErrors = 0

	// Update exponential moving average of latency (alpha=0.3)
	if s.AvgLatency == 0 {
		s.AvgLatency = latency
	} else {
		s.AvgLatency = time.Duration(float64(s.AvgLatency)*0.7 + float64(latency)*0.3)
	}

	// Adaptive delay: latency * multiplier, clamped to [min, max]
	// But never go below robots.txt crawl-delay if set
	adaptive := time.Duration(float64(s.AvgLatency) * AdaptiveMultiplier)
	if adaptive < MinCrawlDelay {
		adaptive = MinCrawlDelay
	}
	if adaptive > MaxCrawlDelay {
		adaptive = MaxCrawlDelay
	}
	// Respect robots.txt crawl-delay as a floor
	if s.RobotsCrawlDelay > 0 && adaptive < s.RobotsCrawlDelay {
		adaptive = s.RobotsCrawlDelay
	}
	s.CrawlDelay = adaptive
}

// RecordError records a fetch error for a domain.
// Applies exponential backoff after repeated failures.
// After MaxConsecutiveDead errors the domain is permanently abandoned.
func (m *Manager) RecordError(domain string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	s := m.getOrCreateLocked(domain)
	s.ConsecutiveErrors++
	if s.ConsecutiveErrors >= MaxConsecutiveDead {
		s.Dead = true
		s.Queue = nil // free memory
		return
	}
	if s.ConsecutiveErrors >= MaxConsecutiveErrs {
		// Back off exponentially, capped at 1 hour
		backoff := time.Duration(1<<min(s.ConsecutiveErrors-MaxConsecutiveErrs, 6)) * time.Minute
		s.BackoffUntil = time.Now().Add(backoff)
	}
}

// IsDead returns true if a domain has been permanently abandoned due
// to too many consecutive errors.
func (m *Manager) IsDead(domain string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if s, ok := m.domains[domain]; ok {
		return s.Dead
	}
	return false
}

// ActiveDomains returns all domains that have URLs in their queue.
func (m *Manager) ActiveDomains() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var out []string
	for domain, s := range m.domains {
		if len(s.Queue) > 0 {
			out = append(out, domain)
		}
	}
	return out
}

// DiscoverSitemap fetches the sitemap for a domain and enqueues any URLs found.
// Should be called after the first robots.txt fetch. Safe to call multiple times
// (only runs once per domain).
func (m *Manager) DiscoverSitemap(d string) {
	m.mu.Lock()
	s := m.getOrCreateLocked(d)
	if s.SitemapChecked {
		m.mu.Unlock()
		return
	}
	s.SitemapChecked = true

	// Collect Sitemap: directives from robots.txt
	var hints []string
	if s.Robots != nil {
		hints = s.Robots.Sitemaps
	}
	m.mu.Unlock()

	// Fetch sitemap (network I/O, don't hold lock)
	urls := m.sitemap.FetchURLs(d, hints)
	for _, u := range urls {
		m.Enqueue(d, u, 0) // sitemap URLs are seeds (depth 0)
	}
}

// NextFetchTime returns the earliest time this domain can be fetched.
// Takes into account crawl delay and error backoff.
func (m *Manager) NextFetchTime(domain string) time.Time {
	m.mu.RLock()
	defer m.mu.RUnlock()
	s, ok := m.domains[domain]
	if !ok {
		return time.Now() // new domain, fetch immediately
	}

	// If in backoff, that takes precedence
	if time.Now().Before(s.BackoffUntil) {
		return s.BackoffUntil
	}

	// Otherwise next fetch = last fetch + crawl delay
	next := s.LastFetch.Add(s.CrawlDelay)
	return next
}

// DomainCount returns the total number of tracked domains.
func (m *Manager) DomainCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.domains)
}

// TotalQueueLen returns the total number of URLs across all frontiers.
func (m *Manager) TotalQueueLen() int {
	return int(m.totalQueued.Load())
}

// Snapshot returns a copy of all non-empty domain queues.
// Used by checkpoint to persist the frontier.
func (m *Manager) Snapshot() map[string][]QueueItem {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make(map[string][]QueueItem)
	for domain, s := range m.domains {
		if len(s.Queue) > 0 {
			cp := make([]QueueItem, len(s.Queue))
			copy(cp, s.Queue)
			out[domain] = cp
		}
	}
	return out
}

// RestoreQueues bulk-enqueues items from a checkpoint.
// Should be called at startup before any fetchers begin.
func (m *Manager) RestoreQueues(queues map[string][]QueueItem) {
	for d, items := range queues {
		for _, item := range items {
			m.Enqueue(d, item.URL, item.Depth)
		}
	}
}

func (m *Manager) getOrCreateLocked(domain string) *State {
	s, ok := m.domains[domain]
	if !ok {
		s = &State{CrawlDelay: DefaultCrawlDelay}
		m.domains[domain] = s
	}
	return s
}

func (m *Manager) fetchRobots(domain string) (string, error) {
	req, err := http.NewRequest("GET", fmt.Sprintf("https://%s/robots.txt", domain), nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("User-Agent", m.userAgent)

	resp, err := m.client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", nil // no robots.txt = allow all
	}

	body, err := io.ReadAll(io.LimitReader(resp.Body, RobotsMaxSize))
	if err != nil {
		return "", err
	}
	return string(body), nil
}

// ExtractDomain extracts the hostname from a URL string.
// Strips the www. prefix so that www.example.com and example.com
// map to the same domain for consistent hash ring assignment.
func ExtractDomain(rawURL string) string {
	u, err := url.Parse(rawURL)
	if err != nil {
		return ""
	}
	host := strings.ToLower(u.Hostname())
	if strings.HasPrefix(host, "www.") {
		host = host[4:]
	}
	return host
}
