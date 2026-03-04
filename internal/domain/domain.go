// Package domain manages per-domain state: robots.txt, rate limiting,
// crawl delay, error tracking, and URL frontiers.
package domain

import (
	"encoding/binary"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/stanislas/krowl/internal/frontier"
	"github.com/stanislas/krowl/internal/metrics"
	"github.com/stanislas/krowl/internal/sitemap"
	"github.com/stanislas/krowl/internal/urlqueue"
)

const (
	DefaultCrawlDelay  = 1 * time.Second
	MinCrawlDelay      = 250 * time.Millisecond // floor: never faster than this
	MaxCrawlDelay      = 30 * time.Second       // ceiling: give up sooner rather than crawling glacially
	RobotsExpiry       = 24 * time.Hour
	RobotsMaxSize      = 512 * 1024 // 512KB
	MaxConsecutiveErrs = 5          // start exponential backoff after this many
	MaxConsecutiveDead = 10         // permanently give up on a domain after this many

	MaxQueuePerDomain  = 1000       // cap URLs queued per domain (prevents crawler traps + forces diversity)
	DefaultMaxFrontier = 50_000_000 // default global cap on total queued URLs (disk-backed, no memory concern)
	MaxURLLength       = 2048       // reject URLs longer than this
	MaxCrawlDepth      = 25         // maximum hops from a seed URL

	// Adaptive rate: delay = max(MinCrawlDelay, latency * multiplier)
	// A 200ms response -> 1s delay. A 2s response -> 10s delay.
	AdaptiveMultiplier = 5
)

// robotsRule is a compact representation of a single robots.txt rule.
// It stores only the path pattern and allow/disallow flag — no compiled
// regexes. Robots.txt wildcards (* and $) are matched inline.
type robotsRule struct {
	pattern string // path pattern (may contain * and $)
	allow   bool
}

// robotsRules is a compact replacement for *robotstxt.Group that does NOT
// hold references to the parsed robotstxt data or compiled regexes.
// Matching follows Google's spec: longest path match wins.
type robotsRules struct {
	rules []robotsRule
}

// test checks if a path is allowed by the rules. Returns true if allowed.
// Follows Google's robots.txt spec: most specific (longest) match wins,
// default is allow.
func (rr *robotsRules) test(path string) bool {
	if rr == nil || len(rr.rules) == 0 {
		return true
	}
	var bestLen int
	allowed := true
	for i := range rr.rules {
		r := &rr.rules[i]
		if matchRobotsPattern(path, r.pattern) {
			l := len(r.pattern)
			if l > bestLen {
				bestLen = l
				allowed = r.allow
			}
		}
	}
	return allowed
}

// matchRobotsPattern matches a path against a robots.txt pattern.
// Supports * (any sequence) and $ (end anchor). Without wildcards,
// it's a simple prefix match.
func matchRobotsPattern(path, pattern string) bool {
	// Fast path: no wildcards → prefix match
	if !strings.Contains(pattern, "*") && !strings.HasSuffix(pattern, "$") {
		return strings.HasPrefix(path, pattern)
	}

	// Strip end anchor
	anchored := strings.HasSuffix(pattern, "$")
	if anchored {
		pattern = pattern[:len(pattern)-1]
	}

	// Split on * and match each segment in order
	parts := strings.Split(pattern, "*")
	pos := 0
	for i, part := range parts {
		if part == "" {
			continue
		}
		if i == 0 {
			// First segment must match at start
			if !strings.HasPrefix(path[pos:], part) {
				return false
			}
			pos += len(part)
		} else {
			// Subsequent segments match anywhere after current pos
			idx := strings.Index(path[pos:], part)
			if idx < 0 {
				return false
			}
			pos += idx + len(part)
		}
	}
	if anchored {
		return pos == len(path)
	}
	return true
}

// robotsResult holds the output of our lightweight robots.txt parser.
type robotsResult struct {
	rules      *robotsRules
	crawlDelay time.Duration
	sitemaps   []string
}

// parseRobotsTxt parses a robots.txt body and returns rules for the
// given user-agent. This replaces temoto/robotstxt to avoid allocating
// compiled regexes and large parsed ASTs that pin memory.
//
// Implements Google's spec: most specific user-agent match wins,
// * is the fallback. Rules use longest-path-match precedence.
func parseRobotsTxt(body, agent string) robotsResult {
	agent = strings.ToLower(agent)

	type group struct {
		agents     []string
		rules      []robotsRule
		crawlDelay time.Duration
	}

	var (
		groups   []group
		current  *group
		sitemaps []string
	)

	for _, rawLine := range strings.Split(body, "\n") {
		line := strings.TrimSpace(rawLine)

		// Strip comments
		if idx := strings.IndexByte(line, '#'); idx >= 0 {
			line = strings.TrimSpace(line[:idx])
		}
		if line == "" {
			continue
		}

		// Split on first ':'
		idx := strings.IndexByte(line, ':')
		if idx < 0 {
			continue
		}
		key := strings.ToLower(strings.TrimSpace(line[:idx]))
		val := strings.TrimSpace(line[idx+1:])

		switch key {
		case "user-agent":
			ua := strings.ToLower(val)
			// If previous directive was also user-agent, extend the group
			if current != nil && len(current.rules) == 0 {
				current.agents = append(current.agents, ua)
			} else {
				groups = append(groups, group{agents: []string{ua}})
				current = &groups[len(groups)-1]
			}
		case "allow":
			if current != nil && val != "" {
				// strings.Clone detaches from the original robots.txt body.
				// Without this, each pattern substring pins the entire body
				// (up to 512KB) in memory. With 500K+ domains this was 44%
				// of total inuse_space.
				current.rules = append(current.rules, robotsRule{pattern: strings.Clone(val), allow: true})
			}
		case "disallow":
			if current != nil && val != "" {
				current.rules = append(current.rules, robotsRule{pattern: strings.Clone(val), allow: false})
			}
		case "crawl-delay":
			if current != nil {
				if secs, err := strconv.ParseFloat(val, 64); err == nil && secs > 0 {
					current.crawlDelay = time.Duration(secs * float64(time.Second))
				}
			}
		case "sitemap":
			if val != "" {
				sitemaps = append(sitemaps, strings.Clone(val))
			}
		}
	}

	// Find best matching group: longest agent prefix match wins, * is fallback.
	var best *group
	bestLen := 0
	for i := range groups {
		g := &groups[i]
		for _, a := range g.agents {
			if a == "*" && bestLen == 0 {
				best = g
				bestLen = 1
			} else if a != "*" && strings.HasPrefix(agent, a) && len(a) > bestLen {
				best = g
				bestLen = len(a)
			}
		}
	}

	result := robotsResult{sitemaps: sitemaps}
	if best != nil {
		result.rules = &robotsRules{rules: best.rules}
		result.crawlDelay = best.crawlDelay
	}
	return result
}

// State holds all per-domain crawl state.
type State struct {
	// robots.txt — stored as compact rules without compiled regexes.
	// The full *robotstxt.RobotsData and *robotstxt.Group are parsed
	// then discarded. Only path strings + allow/deny are retained.
	// This saves ~10KB+ per domain (at 1M+ domains = multi-GB heap).
	Robots         *robotsRules // compact allow/disallow rules
	RobotsSitemaps []string     // Sitemap: directives extracted from robots.txt
	RobotsExpiry   time.Time

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
}

// Manager manages per-domain state. All methods are thread-safe.
type Manager struct {
	mu       sync.RWMutex
	domains  map[string]*State
	client   *http.Client
	frontier *frontier.Frontier // if non-nil, domains are pushed here on enqueue
	sitemap  *sitemap.Fetcher
	queue    *urlqueue.Queue // bbolt-backed per-domain URL queues

	userAgent   string
	maxFrontier int64 // global cap; 0 = unlimited

	// sitemapSem limits concurrent sitemap discovery goroutines.
	// Without this, every first robots.txt fetch spawns a goroutine,
	// which can cause goroutine/FD leaks under heavy domain churn.
	sitemapSem chan struct{}
}

// maxConcurrentSitemaps limits the number of simultaneous sitemap fetch goroutines.
const maxConcurrentSitemaps = 50

// NewManager creates a domain manager with the given user agent string.
func NewManager(userAgent string, maxFrontier int, queue *urlqueue.Queue) *Manager {
	return &Manager{
		domains: make(map[string]*State),
		client: &http.Client{
			Timeout: 5 * time.Second,
		},
		sitemap:     sitemap.NewFetcher(userAgent),
		userAgent:   userAgent,
		maxFrontier: int64(maxFrontier),
		queue:       queue,
		sitemapSem:  make(chan struct{}, maxConcurrentSitemaps),
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
	// Check domain death under lock
	m.mu.RLock()
	if s, ok := m.domains[domain]; ok && s.Dead {
		m.mu.RUnlock()
		return
	}
	m.mu.RUnlock()

	wasEmpty := !m.queue.HasURLs(domain)

	if !m.queue.Enqueue(domain, rawURL, depth, MaxQueuePerDomain, m.maxFrontier) {
		return
	}

	// Ensure domain state exists
	m.mu.Lock()
	if _, ok := m.domains[domain]; !ok {
		m.domains[domain] = &State{CrawlDelay: DefaultCrawlDelay}
	}
	fr := m.frontier // capture under lock
	m.mu.Unlock()

	// Push domain into frontier if this is the first URL (domain wasn't
	// already in the heap). If it already has URLs, it's either in the
	// heap already or a fetcher has it checked out and will re-push.
	if wasEmpty && fr != nil {
		fr.Push(domain, m.NextFetchTime(domain))
	}
}

// EnqueueBatch adds multiple URLs in a single bbolt transaction. Used for
// seed loading where per-URL transactions would be wasteful. Pushes newly
// non-empty domains into the frontier after the batch commits.
func (m *Manager) EnqueueBatch(items []struct{ Domain, URL string }, depth int) {
	// Track which domains go from empty to non-empty.
	wasEmpty := make(map[string]bool)
	for _, it := range items {
		if _, seen := wasEmpty[it.Domain]; !seen {
			wasEmpty[it.Domain] = !m.queue.HasURLs(it.Domain)
		}
	}

	_ = m.queue.EnqueueBatch(MaxQueuePerDomain, m.maxFrontier, func(enqueue func(string, string, int) bool) {
		for _, it := range items {
			if len(it.URL) > MaxURLLength {
				continue
			}
			enqueue(it.Domain, it.URL, depth)
		}
	})

	// Ensure domain state exists and push to frontier.
	m.mu.Lock()
	fr := m.frontier
	for _, it := range items {
		if _, ok := m.domains[it.Domain]; !ok {
			m.domains[it.Domain] = &State{CrawlDelay: DefaultCrawlDelay}
		}
	}
	m.mu.Unlock()

	if fr != nil {
		for d, empty := range wasEmpty {
			if empty && m.queue.HasURLs(d) {
				fr.Push(d, m.NextFetchTime(d))
			}
		}
	}
}

// Dequeue pops the next URL from a domain's frontier.
// Returns the item and true, or a zero item and false if empty.
func (m *Manager) Dequeue(domain string) (urlqueue.Item, bool) {
	return m.queue.Dequeue(domain)
}

// QueueLen returns the number of URLs in a domain's frontier.
func (m *Manager) QueueLen(domain string) int {
	return m.queue.QueueLen(domain)
}

// IsAllowed checks if a path is allowed by the domain's robots.txt.
// Fetches, parses, and caches robots.txt if not cached or expired.
func (m *Manager) IsAllowed(d, path string) (bool, error) {
	m.mu.RLock()
	s, ok := m.domains[d]
	robotsValid := ok && s.Robots != nil && time.Now().Before(s.RobotsExpiry)
	var rules *robotsRules
	if robotsValid {
		rules = s.Robots
	}
	m.mu.RUnlock()

	if !robotsValid {
		body, err := m.fetchRobots(d)
		if err != nil {
			// Can't fetch robots.txt: allow by default
			return true, nil
		}

		parsed := parseRobotsTxt(body, m.userAgent)
		rules = parsed.rules

		m.mu.Lock()
		s = m.getOrCreateLocked(d)
		s.Robots = rules
		s.RobotsSitemaps = parsed.sitemaps
		s.RobotsExpiry = time.Now().Add(RobotsExpiry)

		// Store robots.txt crawl-delay as a floor for adaptive rate
		if parsed.crawlDelay > 0 {
			s.RobotsCrawlDelay = parsed.crawlDelay
			if s.RobotsCrawlDelay > s.CrawlDelay {
				s.CrawlDelay = s.RobotsCrawlDelay
			}
		}
		m.mu.Unlock()

		// Trigger sitemap discovery in the background (bounded by sitemapSem)
		select {
		case m.sitemapSem <- struct{}{}:
			go func() {
				defer func() { <-m.sitemapSem }()
				m.DiscoverSitemap(d)
			}()
		default:
			// Semaphore full: skip sitemap for this domain.
			// It will be attempted again on the next robots.txt refresh (24h).
		}
	}

	return rules.test(path), nil
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

// RecordRateLimit handles a 429 Too Many Requests response.
// It doubles the domain's crawl delay (capped at MaxCrawlDelay) and sets a
// backoff period. If retryAfter > 0 (from Retry-After header), that value is
// used as the backoff instead.
func (m *Manager) RecordRateLimit(domain string, retryAfter time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	s := m.getOrCreateLocked(domain)

	// Double the crawl delay, capped at MaxCrawlDelay
	newDelay := s.CrawlDelay * 2
	if newDelay > MaxCrawlDelay {
		newDelay = MaxCrawlDelay
	}
	if newDelay < MinCrawlDelay {
		newDelay = MinCrawlDelay
	}
	s.CrawlDelay = newDelay

	// Set backoff: use Retry-After if provided, otherwise use the new delay
	backoff := newDelay
	if retryAfter > 0 {
		backoff = retryAfter
	}
	s.BackoffUntil = time.Now().Add(backoff)
}

// RecordError records a fetch error for a domain.
// Applies exponential backoff after repeated failures.
// After MaxConsecutiveDead errors the domain is permanently abandoned.
func (m *Manager) RecordError(domain string) {
	var dead bool
	m.mu.Lock()
	s := m.getOrCreateLocked(domain)
	s.ConsecutiveErrors++
	if s.ConsecutiveErrors >= MaxConsecutiveDead {
		s.Dead = true
		dead = true
		// Release memory held by robots rules and sitemaps — dead domains
		// are never fetched again so these are pure waste.
		s.Robots = nil
		s.RobotsSitemaps = nil
	} else if s.ConsecutiveErrors >= MaxConsecutiveErrs {
		// Back off exponentially, capped at 1 hour
		backoff := time.Duration(1<<min(s.ConsecutiveErrors-MaxConsecutiveErrs, 6)) * time.Minute
		s.BackoffUntil = time.Now().Add(backoff)
	}
	m.mu.Unlock()
	if dead {
		m.queue.DropDomain(domain) // free disk
	}
}

// RecordDNSError marks a domain as permanently dead immediately.
// DNS NXDOMAIN / no records means the domain doesn't exist — no point
// retrying it 10 times with exponential backoff.
func (m *Manager) RecordDNSError(domain string) {
	m.mu.Lock()
	s := m.getOrCreateLocked(domain)
	if s.Dead {
		m.mu.Unlock()
		return
	}
	s.Dead = true
	s.Robots = nil
	s.RobotsSitemaps = nil
	m.mu.Unlock()
	m.queue.DropDomain(domain)
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
	return m.queue.ActiveDomains()
}

// DiscoverSitemap fetches the sitemap for a domain and enqueues any URLs found.
// Should be called after the first robots.txt fetch. Safe to call multiple times
// (only runs once per domain). Concurrency is limited by sitemapSem.
func (m *Manager) DiscoverSitemap(d string) {
	m.mu.Lock()
	s := m.getOrCreateLocked(d)
	if s.SitemapChecked {
		m.mu.Unlock()
		return
	}
	s.SitemapChecked = true

	// Collect Sitemap: directives from robots.txt
	hints := s.RobotsSitemaps
	m.mu.Unlock()

	// Fetch sitemap (network I/O, don't hold lock)
	urls := m.sitemap.FetchURLs(d, hints)
	for _, u := range urls {
		m.Enqueue(d, u, 0) // sitemap URLs are seeds (depth 0)
	}
	if len(urls) > 0 {
		metrics.SitemapURLsDiscovered.Add(float64(len(urls)))
	}
}

// RebuildFrontier pushes all domains with pending URLs in the urlqueue
// into the frontier heap. Must be called after SetFrontier and
// RestoreAllState so that dead domains are skipped and NextFetchTime
// uses restored crawl delays. Without this, a restart leaves the
// frontier empty and fetch workers idle despite a full urlqueue.
func (m *Manager) RebuildFrontier() int {
	m.mu.RLock()
	fr := m.frontier
	m.mu.RUnlock()
	if fr == nil {
		return 0
	}

	active := m.queue.ActiveDomains()
	rebuilt := 0
	for _, d := range active {
		m.mu.RLock()
		dead := false
		if s, ok := m.domains[d]; ok && s.Dead {
			dead = true
		}
		m.mu.RUnlock()
		if dead {
			continue
		}
		fr.Push(d, m.NextFetchTime(d))
		rebuilt++
	}
	slog.Info("frontier rebuilt from urlqueue", "domains", rebuilt, "skipped", len(active)-rebuilt)
	return rebuilt
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
	return int(m.queue.TotalLen())
}

// URLQueue returns the underlying bbolt-backed URL queue.
func (m *Manager) URLQueue() *urlqueue.Queue {
	return m.queue
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
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return "", nil // no robots.txt = allow all
	}

	body, err := io.ReadAll(io.LimitReader(resp.Body, RobotsMaxSize))
	if err != nil {
		return "", err
	}
	return string(body), nil
}

// SaveAllState persists metadata for all tracked domains to the URL queue's
// bbolt store. Called periodically and on shutdown. All domains are written
// in a single bbolt transaction (one fsync).
func (m *Manager) SaveAllState() int {
	m.mu.RLock()
	valBuf := make([]byte, stateEncodingSize)
	count := len(m.domains)
	_ = m.queue.SaveMetaBatch(func(put func(string, []byte)) {
		for d, s := range m.domains {
			encodeStateBuf(s, valBuf)
			put(d, valBuf)
		}
	})
	m.mu.RUnlock()
	return count
}

// RestoreAllState loads persisted domain metadata from bbolt and
// populates the in-memory domain map. Should be called at startup
// before any fetchers begin.
func (m *Manager) RestoreAllState() int {
	count := 0
	m.queue.IterMeta(func(d string, data []byte) {
		s := decodeState(data)
		if s == nil {
			return
		}
		m.mu.Lock()
		m.domains[d] = s
		m.mu.Unlock()
		count++
	})
	slog.Info("domain state restored", "domains", count)
	return count
}

// Domain state binary encoding (version 1):
//
//	byte 0:       version (1)
//	byte 1:       flags (bit 0: Dead, bit 1: SitemapChecked)
//	bytes 2-9:    CrawlDelay (int64 nanoseconds)
//	bytes 10-17:  RobotsCrawlDelay (int64 nanoseconds)
//	bytes 18-25:  AvgLatency (int64 nanoseconds)
//	bytes 26-33:  LastFetch (int64 unix nano)
//	bytes 34-41:  BackoffUntil (int64 unix nano)
//	bytes 42-45:  ConsecutiveErrors (int32)
//
// Total: 46 bytes per domain.

const stateEncodingSize = 46

// encodeStateBuf encodes domain state into an existing buffer (must be >= stateEncodingSize).
// Used by SaveAllState to reuse a single buffer across 1M+ domains.
func encodeStateBuf(s *State, buf []byte) {
	buf[0] = 1 // version

	var flags byte
	if s.Dead {
		flags |= 0x01
	}
	if s.SitemapChecked {
		flags |= 0x02
	}
	buf[1] = flags

	binary.LittleEndian.PutUint64(buf[2:], uint64(s.CrawlDelay))
	binary.LittleEndian.PutUint64(buf[10:], uint64(s.RobotsCrawlDelay))
	binary.LittleEndian.PutUint64(buf[18:], uint64(s.AvgLatency))
	binary.LittleEndian.PutUint64(buf[26:], uint64(s.LastFetch.UnixNano()))
	binary.LittleEndian.PutUint64(buf[34:], uint64(s.BackoffUntil.UnixNano()))
	binary.LittleEndian.PutUint32(buf[42:], uint32(s.ConsecutiveErrors))
}

func decodeState(data []byte) *State {
	if len(data) < stateEncodingSize || data[0] != 1 {
		return nil
	}

	flags := data[1]
	s := &State{
		Dead:              flags&0x01 != 0,
		SitemapChecked:    flags&0x02 != 0,
		CrawlDelay:        time.Duration(binary.LittleEndian.Uint64(data[2:])),
		RobotsCrawlDelay:  time.Duration(binary.LittleEndian.Uint64(data[10:])),
		AvgLatency:        time.Duration(binary.LittleEndian.Uint64(data[18:])),
		LastFetch:         time.Unix(0, int64(binary.LittleEndian.Uint64(data[26:]))),
		BackoffUntil:      time.Unix(0, int64(binary.LittleEndian.Uint64(data[34:]))),
		ConsecutiveErrors: int(binary.LittleEndian.Uint32(data[42:])),
	}

	// Sanity: if CrawlDelay was 0 (shouldn't happen), reset to default
	if s.CrawlDelay <= 0 {
		s.CrawlDelay = DefaultCrawlDelay
	}

	return s
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
	host = strings.TrimPrefix(host, "www.")
	return host
}
