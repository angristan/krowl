package domain

import (
	"sort"
	"testing"
	"time"

	"github.com/stanislas/krowl/internal/frontier"
)

func TestNewManager(t *testing.T) {
	m := NewManager("testbot/1.0", 0)
	if m == nil {
		t.Fatal("NewManager returned nil")
	}
	if m.userAgent != "testbot/1.0" {
		t.Fatalf("expected userAgent %q, got %q", "testbot/1.0", m.userAgent)
	}
	if m.domains == nil {
		t.Fatal("domains map is nil")
	}
	if m.client == nil {
		t.Fatal("http client is nil")
	}
	if m.DomainCount() != 0 {
		t.Fatalf("expected 0 domains, got %d", m.DomainCount())
	}
}

func TestEnqueueDequeue_FIFO(t *testing.T) {
	m := NewManager("bot", 0)
	m.Enqueue("example.com", "https://example.com/a", 0)
	m.Enqueue("example.com", "https://example.com/b", 1)
	m.Enqueue("example.com", "https://example.com/c", 2)

	got, ok := m.Dequeue("example.com")
	if !ok || got.URL != "https://example.com/a" || got.Depth != 0 {
		t.Fatalf("expected first URL at depth 0, got %+v, ok=%v", got, ok)
	}
	got, ok = m.Dequeue("example.com")
	if !ok || got.URL != "https://example.com/b" || got.Depth != 1 {
		t.Fatalf("expected second URL at depth 1, got %+v, ok=%v", got, ok)
	}
	got, ok = m.Dequeue("example.com")
	if !ok || got.URL != "https://example.com/c" || got.Depth != 2 {
		t.Fatalf("expected third URL at depth 2, got %+v, ok=%v", got, ok)
	}
}

func TestDequeue_EmptyQueue(t *testing.T) {
	m := NewManager("bot", 0)

	// Dequeue from non-existent domain
	_, ok := m.Dequeue("noexist.com")
	if ok {
		t.Fatal("expected ok=false from non-existent domain")
	}

	// Enqueue one, dequeue two
	m.Enqueue("example.com", "https://example.com/a", 0)
	m.Dequeue("example.com")
	_, ok = m.Dequeue("example.com")
	if ok {
		t.Fatal("expected ok=false from exhausted queue")
	}
}

func TestQueueLen(t *testing.T) {
	m := NewManager("bot", 0)

	if m.QueueLen("example.com") != 0 {
		t.Fatal("expected 0 for unknown domain")
	}

	m.Enqueue("example.com", "https://example.com/1", 0)
	m.Enqueue("example.com", "https://example.com/2", 0)
	m.Enqueue("example.com", "https://example.com/3", 0)

	if n := m.QueueLen("example.com"); n != 3 {
		t.Fatalf("expected QueueLen 3, got %d", n)
	}

	m.Dequeue("example.com")
	if n := m.QueueLen("example.com"); n != 2 {
		t.Fatalf("expected QueueLen 2 after dequeue, got %d", n)
	}
}

func TestGetOrCreate_Defaults(t *testing.T) {
	m := NewManager("bot", 0)

	s := m.GetOrCreate("example.com")
	if s == nil {
		t.Fatal("GetOrCreate returned nil")
	}
	if s.CrawlDelay != DefaultCrawlDelay {
		t.Fatalf("expected default CrawlDelay %v, got %v", DefaultCrawlDelay, s.CrawlDelay)
	}
	if s.ConsecutiveErrors != 0 {
		t.Fatalf("expected 0 ConsecutiveErrors, got %d", s.ConsecutiveErrors)
	}
	if s.AvgLatency != 0 {
		t.Fatalf("expected 0 AvgLatency, got %v", s.AvgLatency)
	}
	if !s.LastFetch.IsZero() {
		t.Fatalf("expected zero LastFetch, got %v", s.LastFetch)
	}

	// Second call returns same state
	s2 := m.GetOrCreate("example.com")
	if s != s2 {
		t.Fatal("GetOrCreate returned different pointer for same domain")
	}

	if m.DomainCount() != 1 {
		t.Fatalf("expected 1 domain, got %d", m.DomainCount())
	}
}

func TestCanFetch_NewDomain(t *testing.T) {
	m := NewManager("bot", 0)

	allowed, wait := m.CanFetch("newdomain.com")
	if !allowed {
		t.Fatal("expected CanFetch to return true for unknown domain")
	}
	if wait != 0 {
		t.Fatalf("expected 0 wait for unknown domain, got %v", wait)
	}
}

func TestRecordFetch_UpdatesLatencyAndDelay(t *testing.T) {
	m := NewManager("bot", 0)

	// First fetch: AvgLatency should equal the latency
	m.RecordFetch("example.com", 200*time.Millisecond)
	s := m.GetOrCreate("example.com")

	if s.AvgLatency != 200*time.Millisecond {
		t.Fatalf("expected AvgLatency 200ms after first fetch, got %v", s.AvgLatency)
	}
	if s.ConsecutiveErrors != 0 {
		t.Fatalf("expected 0 consecutive errors after fetch, got %d", s.ConsecutiveErrors)
	}
	if s.LastFetch.IsZero() {
		t.Fatal("expected LastFetch to be set")
	}

	// Adaptive delay = 200ms * 5 = 1s, clamped to [250ms, 30s]
	expectedDelay := time.Duration(float64(200*time.Millisecond) * AdaptiveMultiplier)
	if s.CrawlDelay != expectedDelay {
		t.Fatalf("expected CrawlDelay %v, got %v", expectedDelay, s.CrawlDelay)
	}

	// Second fetch with different latency: EMA = 0.7*200 + 0.3*600 = 320ms
	m.RecordFetch("example.com", 600*time.Millisecond)
	expectedAvg := time.Duration(float64(200*time.Millisecond)*0.7 + float64(600*time.Millisecond)*0.3)
	if s.AvgLatency != expectedAvg {
		t.Fatalf("expected AvgLatency %v after second fetch, got %v", expectedAvg, s.AvgLatency)
	}

	// CrawlDelay should be adaptive = 320ms * 5 = 1.6s
	expectedDelay2 := time.Duration(float64(expectedAvg) * AdaptiveMultiplier)
	if s.CrawlDelay != expectedDelay2 {
		t.Fatalf("expected CrawlDelay %v, got %v", expectedDelay2, s.CrawlDelay)
	}
}

func TestRecordFetch_ClampsToMinDelay(t *testing.T) {
	m := NewManager("bot", 0)

	// Very fast response: 10ms * 5 = 50ms, should be clamped to MinCrawlDelay
	m.RecordFetch("fast.com", 10*time.Millisecond)
	s := m.GetOrCreate("fast.com")
	if s.CrawlDelay != MinCrawlDelay {
		t.Fatalf("expected CrawlDelay clamped to %v, got %v", MinCrawlDelay, s.CrawlDelay)
	}
}

func TestRecordFetch_ClampsToMaxDelay(t *testing.T) {
	m := NewManager("bot", 0)

	// Very slow response: 10s * 5 = 50s, should be clamped to MaxCrawlDelay
	m.RecordFetch("slow.com", 10*time.Second)
	s := m.GetOrCreate("slow.com")
	if s.CrawlDelay != MaxCrawlDelay {
		t.Fatalf("expected CrawlDelay clamped to %v, got %v", MaxCrawlDelay, s.CrawlDelay)
	}
}

func TestRecordError_ExponentialBackoff(t *testing.T) {
	m := NewManager("bot", 0)

	// Errors below threshold: no backoff
	for i := 0; i < MaxConsecutiveErrs-1; i++ {
		m.RecordError("fail.com")
	}
	s := m.GetOrCreate("fail.com")
	if !s.BackoffUntil.IsZero() {
		t.Fatal("expected no backoff below MaxConsecutiveErrs")
	}

	// One more error hits the threshold
	before := time.Now()
	m.RecordError("fail.com")
	if s.BackoffUntil.IsZero() {
		t.Fatal("expected backoff to be set at MaxConsecutiveErrs")
	}
	// First backoff: 2^0 * 1min = 1 minute
	expectedBackoff := 1 * time.Minute
	if s.BackoffUntil.Before(before.Add(expectedBackoff - time.Second)) {
		t.Fatalf("backoff too short: %v", s.BackoffUntil.Sub(before))
	}
	if s.BackoffUntil.After(before.Add(expectedBackoff + time.Second)) {
		t.Fatalf("backoff too long: %v", s.BackoffUntil.Sub(before))
	}

	// Additional error: backoff should grow (2^1 = 2 min)
	before = time.Now()
	m.RecordError("fail.com")
	expectedBackoff = 2 * time.Minute
	if s.BackoffUntil.Before(before.Add(expectedBackoff - time.Second)) {
		t.Fatalf("second backoff too short: %v", s.BackoffUntil.Sub(before))
	}
}

func TestRecordError_ResetsOnFetch(t *testing.T) {
	m := NewManager("bot", 0)

	// Accumulate errors
	for i := 0; i < MaxConsecutiveErrs+2; i++ {
		m.RecordError("recover.com")
	}
	s := m.GetOrCreate("recover.com")
	if s.ConsecutiveErrors == 0 {
		t.Fatal("expected errors to be tracked")
	}

	// Successful fetch resets consecutive errors
	m.RecordFetch("recover.com", 100*time.Millisecond)
	if s.ConsecutiveErrors != 0 {
		t.Fatalf("expected 0 consecutive errors after fetch, got %d", s.ConsecutiveErrors)
	}
}

func TestCanFetch_RespectsBackoff(t *testing.T) {
	m := NewManager("bot", 0)

	// Put domain into backoff
	for i := 0; i < MaxConsecutiveErrs+1; i++ {
		m.RecordError("backoff.com")
	}

	allowed, wait := m.CanFetch("backoff.com")
	if allowed {
		t.Fatal("expected CanFetch to return false during backoff")
	}
	if wait <= 0 {
		t.Fatalf("expected positive wait during backoff, got %v", wait)
	}
}

func TestCanFetch_RespectsRateLimit(t *testing.T) {
	m := NewManager("bot", 0)

	// Record a fetch (sets LastFetch to now and CrawlDelay adaptively)
	m.RecordFetch("rate.com", 200*time.Millisecond)

	// Immediately after, CanFetch should return true but with a wait > 0
	allowed, wait := m.CanFetch("rate.com")
	if !allowed {
		t.Fatal("expected CanFetch to return true (rate limited but allowed)")
	}
	if wait <= 0 {
		t.Fatalf("expected positive wait for rate limiting, got %v", wait)
	}
}

func TestActiveDomains(t *testing.T) {
	m := NewManager("bot", 0)

	m.Enqueue("a.com", "https://a.com/1", 0)
	m.Enqueue("b.com", "https://b.com/1", 0)
	m.GetOrCreate("c.com") // create domain but no URLs

	active := m.ActiveDomains()
	sort.Strings(active)

	if len(active) != 2 {
		t.Fatalf("expected 2 active domains, got %d: %v", len(active), active)
	}
	if active[0] != "a.com" || active[1] != "b.com" {
		t.Fatalf("expected [a.com b.com], got %v", active)
	}

	// Dequeue all from a.com
	m.Dequeue("a.com")
	active = m.ActiveDomains()
	if len(active) != 1 || active[0] != "b.com" {
		t.Fatalf("expected [b.com], got %v", active)
	}
}

func TestTotalQueueLen(t *testing.T) {
	m := NewManager("bot", 0)

	if m.TotalQueueLen() != 0 {
		t.Fatal("expected 0 total queue len initially")
	}

	m.Enqueue("a.com", "https://a.com/1", 0)
	m.Enqueue("a.com", "https://a.com/2", 0)
	m.Enqueue("b.com", "https://b.com/1", 0)

	if n := m.TotalQueueLen(); n != 3 {
		t.Fatalf("expected TotalQueueLen 3, got %d", n)
	}

	m.Dequeue("a.com")
	if n := m.TotalQueueLen(); n != 2 {
		t.Fatalf("expected TotalQueueLen 2 after dequeue, got %d", n)
	}
}

func TestDomainCount(t *testing.T) {
	m := NewManager("bot", 0)

	if m.DomainCount() != 0 {
		t.Fatal("expected 0 domains initially")
	}

	m.GetOrCreate("a.com")
	m.GetOrCreate("b.com")
	m.GetOrCreate("c.com")
	if m.DomainCount() != 3 {
		t.Fatalf("expected 3 domains, got %d", m.DomainCount())
	}

	// Duplicate should not increase count
	m.GetOrCreate("a.com")
	if m.DomainCount() != 3 {
		t.Fatalf("expected 3 domains after duplicate, got %d", m.DomainCount())
	}

	// Enqueue to new domain creates it
	m.Enqueue("d.com", "https://d.com/1", 0)
	if m.DomainCount() != 4 {
		t.Fatalf("expected 4 domains after enqueue, got %d", m.DomainCount())
	}
}

func TestExtractDomain(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"https://example.com/path", "example.com"},
		{"https://www.example.com/path", "example.com"},
		{"http://WWW.EXAMPLE.COM/PATH", "example.com"},
		{"https://sub.example.com/path", "sub.example.com"},
		{"https://www.Sub.Example.COM/path", "sub.example.com"},
		{"https://example.com:8080/path", "example.com"},
		{"https://www.example.com:443/path", "example.com"},
		{"://bad-url", ""},
		{"", ""},
		{"https://example.com", "example.com"},
		{"http://www.example.com", "example.com"},
	}

	for _, tt := range tests {
		got := ExtractDomain(tt.input)
		if got != tt.expected {
			t.Errorf("ExtractDomain(%q) = %q, want %q", tt.input, got, tt.expected)
		}
	}
}

func TestSetFrontier_EnqueuePushesToFrontier(t *testing.T) {
	m := NewManager("bot", 0)
	f := frontier.New()
	m.SetFrontier(f)

	// Enqueue a URL to a new domain - should push to frontier
	m.Enqueue("example.com", "https://example.com/page1", 0)

	if !f.Contains("example.com") {
		t.Fatal("expected domain to be pushed to frontier on first enqueue")
	}

	// Frontier should have exactly 1 entry
	if f.Len() != 1 {
		t.Fatalf("expected frontier len 1, got %d", f.Len())
	}
}

func TestSetFrontier_SecondEnqueueDoesNotDuplicate(t *testing.T) {
	m := NewManager("bot", 0)
	f := frontier.New()
	m.SetFrontier(f)

	m.Enqueue("example.com", "https://example.com/page1", 0)
	m.Enqueue("example.com", "https://example.com/page2", 0)

	// Should still be 1 entry in frontier (second enqueue was not empty->non-empty)
	if f.Len() != 1 {
		t.Fatalf("expected frontier len 1 after second enqueue, got %d", f.Len())
	}
}

func TestSetFrontier_MultipleDomains(t *testing.T) {
	m := NewManager("bot", 0)
	f := frontier.New()
	m.SetFrontier(f)

	m.Enqueue("a.com", "https://a.com/1", 0)
	m.Enqueue("b.com", "https://b.com/1", 0)

	if f.Len() != 2 {
		t.Fatalf("expected frontier len 2, got %d", f.Len())
	}
	if !f.Contains("a.com") || !f.Contains("b.com") {
		t.Fatal("expected both domains in frontier")
	}
}

func TestNextFetchTime_NewDomain(t *testing.T) {
	m := NewManager("bot", 0)

	before := time.Now()
	next := m.NextFetchTime("new.com")
	after := time.Now()

	if next.Before(before) || next.After(after) {
		t.Fatalf("NextFetchTime for new domain should be ~now, got %v (expected between %v and %v)", next, before, after)
	}
}

func TestNextFetchTime_AfterFetch(t *testing.T) {
	m := NewManager("bot", 0)

	m.RecordFetch("example.com", 200*time.Millisecond)
	s := m.GetOrCreate("example.com")

	next := m.NextFetchTime("example.com")
	expected := s.LastFetch.Add(s.CrawlDelay)

	// Allow 1ms tolerance
	diff := next.Sub(expected)
	if diff < -time.Millisecond || diff > time.Millisecond {
		t.Fatalf("NextFetchTime = %v, expected %v (diff %v)", next, expected, diff)
	}
}

func TestNextFetchTime_DuringBackoff(t *testing.T) {
	m := NewManager("bot", 0)

	// Put domain in backoff
	for i := 0; i < MaxConsecutiveErrs+1; i++ {
		m.RecordError("backoff.com")
	}
	s := m.GetOrCreate("backoff.com")

	next := m.NextFetchTime("backoff.com")
	if !next.Equal(s.BackoffUntil) {
		t.Fatalf("NextFetchTime during backoff = %v, expected BackoffUntil = %v", next, s.BackoffUntil)
	}
}
