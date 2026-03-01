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
	"time"
)

const (
	DefaultCrawlDelay  = 1 * time.Second
	RobotsExpiry       = 24 * time.Hour
	RobotsMaxSize      = 512 * 1024 // 512KB
	MaxConsecutiveErrs = 10
)

// State holds all per-domain crawl state.
type State struct {
	// robots.txt
	RobotsBody   string
	RobotsExpiry time.Time

	// Rate limiting
	CrawlDelay time.Duration
	LastFetch  time.Time

	// Health
	ConsecutiveErrors int
	BackoffUntil      time.Time

	// Frontier: URLs to crawl for this domain
	Queue []string
}

// Manager manages per-domain state. All methods are thread-safe.
type Manager struct {
	mu      sync.RWMutex
	domains map[string]*State
	client  *http.Client

	userAgent string
}

// NewManager creates a domain manager with the given user agent string.
func NewManager(userAgent string) *Manager {
	return &Manager{
		domains: make(map[string]*State),
		client: &http.Client{
			Timeout: 10 * time.Second,
		},
		userAgent: userAgent,
	}
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

// Enqueue adds a URL to a domain's frontier.
func (m *Manager) Enqueue(domain, rawURL string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	s, ok := m.domains[domain]
	if !ok {
		s = &State{CrawlDelay: DefaultCrawlDelay}
		m.domains[domain] = s
	}
	s.Queue = append(s.Queue, rawURL)
}

// Dequeue pops the next URL from a domain's frontier.
// Returns empty string if the queue is empty.
func (m *Manager) Dequeue(domain string) string {
	m.mu.Lock()
	defer m.mu.Unlock()
	s, ok := m.domains[domain]
	if !ok || len(s.Queue) == 0 {
		return ""
	}
	url := s.Queue[0]
	s.Queue = s.Queue[1:]
	return url
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
// Fetches and caches robots.txt if not cached or expired.
func (m *Manager) IsAllowed(domain, path string) (bool, error) {
	m.mu.RLock()
	s, ok := m.domains[domain]
	robotsValid := ok && time.Now().Before(s.RobotsExpiry)
	var body string
	if robotsValid {
		body = s.RobotsBody
	}
	m.mu.RUnlock()

	if !robotsValid {
		var err error
		body, err = m.fetchRobots(domain)
		if err != nil {
			// Can't fetch robots.txt: allow by default
			return true, nil
		}
		m.mu.Lock()
		s = m.getOrCreateLocked(domain)
		s.RobotsBody = body
		s.RobotsExpiry = time.Now().Add(RobotsExpiry)
		m.mu.Unlock()
	}

	return isPathAllowed(body, m.userAgent, path), nil
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

// RecordFetch marks that we just fetched from this domain.
func (m *Manager) RecordFetch(domain string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	s := m.getOrCreateLocked(domain)
	s.LastFetch = time.Now()
	s.ConsecutiveErrors = 0
}

// RecordError records a fetch error for a domain.
// Applies exponential backoff after repeated failures.
func (m *Manager) RecordError(domain string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	s := m.getOrCreateLocked(domain)
	s.ConsecutiveErrors++
	if s.ConsecutiveErrors >= MaxConsecutiveErrs {
		// Back off exponentially, capped at 1 hour
		backoff := time.Duration(1<<min(s.ConsecutiveErrors-MaxConsecutiveErrs, 6)) * time.Minute
		s.BackoffUntil = time.Now().Add(backoff)
	}
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

// DomainCount returns the total number of tracked domains.
func (m *Manager) DomainCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.domains)
}

// TotalQueueLen returns the total number of URLs across all frontiers.
func (m *Manager) TotalQueueLen() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	total := 0
	for _, s := range m.domains {
		total += len(s.Queue)
	}
	return total
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

// isPathAllowed is a simple robots.txt parser.
// Checks if the given user agent is allowed to access the path.
func isPathAllowed(robotsTxt, userAgent, path string) bool {
	if robotsTxt == "" {
		return true
	}

	lines := strings.Split(robotsTxt, "\n")
	inBlock := false
	defaultAllow := true

	for _, line := range lines {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "#") || line == "" {
			continue
		}

		parts := strings.SplitN(line, ":", 2)
		if len(parts) != 2 {
			continue
		}
		field := strings.TrimSpace(strings.ToLower(parts[0]))
		value := strings.TrimSpace(parts[1])

		switch field {
		case "user-agent":
			ua := strings.ToLower(value)
			inBlock = ua == "*" || strings.Contains(strings.ToLower(userAgent), ua)
		case "disallow":
			if inBlock && value != "" {
				if strings.HasPrefix(path, value) {
					return false
				}
			}
		case "allow":
			if inBlock && value != "" {
				if strings.HasPrefix(path, value) {
					return true
				}
			}
		case "crawl-delay":
			// Parsed elsewhere when caching robots
		}
	}

	return defaultAllow
}

// ExtractDomain extracts the hostname from a URL string.
func ExtractDomain(rawURL string) string {
	u, err := url.Parse(rawURL)
	if err != nil {
		return ""
	}
	return strings.ToLower(u.Hostname())
}
