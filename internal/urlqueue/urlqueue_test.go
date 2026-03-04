package urlqueue

import (
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
)

func openTestQueue(t *testing.T) *Queue {
	t.Helper()
	dir := t.TempDir()
	q, err := Open(filepath.Join(dir, "queue.db"))
	if err != nil {
		t.Fatalf("Open() error: %v", err)
	}
	t.Cleanup(func() { _ = q.Close() })
	return q
}

func TestOpen(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "queue.db")

	q, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error: %v", err)
	}
	if err := q.Close(); err != nil {
		t.Fatalf("Close() error: %v", err)
	}

	// Reopen
	q2, err := Open(path)
	if err != nil {
		t.Fatalf("reopen error: %v", err)
	}
	_ = q2.Close()
}

func TestEnqueueDequeue_FIFO(t *testing.T) {
	q := openTestQueue(t)

	q.Enqueue("example.com", "https://example.com/a", 0, 0, 0)
	q.Enqueue("example.com", "https://example.com/b", 1, 0, 0)
	q.Enqueue("example.com", "https://example.com/c", 2, 0, 0)

	got, ok := q.Dequeue("example.com")
	if !ok || got.URL != "https://example.com/a" || got.Depth != 0 {
		t.Fatalf("first dequeue: got %+v, ok=%v", got, ok)
	}
	got, ok = q.Dequeue("example.com")
	if !ok || got.URL != "https://example.com/b" || got.Depth != 1 {
		t.Fatalf("second dequeue: got %+v, ok=%v", got, ok)
	}
	got, ok = q.Dequeue("example.com")
	if !ok || got.URL != "https://example.com/c" || got.Depth != 2 {
		t.Fatalf("third dequeue: got %+v, ok=%v", got, ok)
	}
}

func TestDequeue_Empty(t *testing.T) {
	q := openTestQueue(t)

	_, ok := q.Dequeue("noexist.com")
	if ok {
		t.Fatal("expected ok=false from non-existent domain")
	}

	q.Enqueue("example.com", "https://example.com/a", 0, 0, 0)
	q.Dequeue("example.com")
	_, ok = q.Dequeue("example.com")
	if ok {
		t.Fatal("expected ok=false from exhausted queue")
	}
}

func TestEnqueueDequeue_MultiDomain(t *testing.T) {
	q := openTestQueue(t)

	q.Enqueue("a.com", "https://a.com/1", 0, 0, 0)
	q.Enqueue("b.com", "https://b.com/1", 0, 0, 0)

	gotA, ok := q.Dequeue("a.com")
	if !ok || gotA.URL != "https://a.com/1" {
		t.Fatalf("a.com dequeue: got %+v, ok=%v", gotA, ok)
	}
	gotB, ok := q.Dequeue("b.com")
	if !ok || gotB.URL != "https://b.com/1" {
		t.Fatalf("b.com dequeue: got %+v, ok=%v", gotB, ok)
	}

	// Domains should not interfere
	_, ok = q.Dequeue("a.com")
	if ok {
		t.Fatal("a.com should be empty")
	}
}

func TestPerDomainCap(t *testing.T) {
	q := openTestQueue(t)

	for i := range 5 {
		ok := q.Enqueue("capped.com", fmt.Sprintf("https://capped.com/%d", i), 0, 5, 0)
		if !ok {
			t.Fatalf("enqueue %d should succeed", i)
		}
	}
	ok := q.Enqueue("capped.com", "https://capped.com/over", 0, 5, 0)
	if ok {
		t.Fatal("enqueue over per-domain cap should return false")
	}
	if q.QueueLen("capped.com") != 5 {
		t.Fatalf("expected QueueLen 5, got %d", q.QueueLen("capped.com"))
	}
}

func TestGlobalCap(t *testing.T) {
	q := openTestQueue(t)

	for i := range 3 {
		q.Enqueue("a.com", fmt.Sprintf("https://a.com/%d", i), 0, 0, 3)
	}
	ok := q.Enqueue("b.com", "https://b.com/over", 0, 0, 3)
	if ok {
		t.Fatal("enqueue over global cap should return false")
	}
	if q.TotalLen() != 3 {
		t.Fatalf("expected TotalLen 3, got %d", q.TotalLen())
	}
}

func TestDropDomain(t *testing.T) {
	q := openTestQueue(t)

	q.Enqueue("drop.com", "https://drop.com/1", 0, 0, 0)
	q.Enqueue("drop.com", "https://drop.com/2", 0, 0, 0)
	q.Enqueue("keep.com", "https://keep.com/1", 0, 0, 0)

	q.DropDomain("drop.com")

	if q.QueueLen("drop.com") != 0 {
		t.Fatalf("expected 0 after drop, got %d", q.QueueLen("drop.com"))
	}
	if q.TotalLen() != 1 {
		t.Fatalf("expected TotalLen 1 after drop, got %d", q.TotalLen())
	}
	_, ok := q.Dequeue("drop.com")
	if ok {
		t.Fatal("dequeue after drop should return false")
	}
	// keep.com should be unaffected
	got, ok := q.Dequeue("keep.com")
	if !ok || got.URL != "https://keep.com/1" {
		t.Fatalf("keep.com should be unaffected, got %+v, ok=%v", got, ok)
	}
}

func TestQueueLen_TotalLen(t *testing.T) {
	q := openTestQueue(t)

	if q.TotalLen() != 0 {
		t.Fatal("expected 0 total initially")
	}

	q.Enqueue("a.com", "https://a.com/1", 0, 0, 0)
	q.Enqueue("a.com", "https://a.com/2", 0, 0, 0)
	q.Enqueue("b.com", "https://b.com/1", 0, 0, 0)

	if q.QueueLen("a.com") != 2 {
		t.Fatalf("expected a.com QueueLen 2, got %d", q.QueueLen("a.com"))
	}
	if q.TotalLen() != 3 {
		t.Fatalf("expected TotalLen 3, got %d", q.TotalLen())
	}

	q.Dequeue("a.com")
	if q.QueueLen("a.com") != 1 {
		t.Fatalf("expected a.com QueueLen 1 after dequeue, got %d", q.QueueLen("a.com"))
	}
	if q.TotalLen() != 2 {
		t.Fatalf("expected TotalLen 2 after dequeue, got %d", q.TotalLen())
	}
}

func TestActiveDomains(t *testing.T) {
	q := openTestQueue(t)

	q.Enqueue("a.com", "https://a.com/1", 0, 0, 0)
	q.Enqueue("b.com", "https://b.com/1", 0, 0, 0)

	active := q.ActiveDomains()
	if len(active) != 2 {
		t.Fatalf("expected 2 active domains, got %d", len(active))
	}

	q.Dequeue("a.com")
	active = q.ActiveDomains()
	if len(active) != 1 {
		t.Fatalf("expected 1 active domain after dequeue, got %d", len(active))
	}
}

func TestDomainCount(t *testing.T) {
	q := openTestQueue(t)

	if q.DomainCount() != 0 {
		t.Fatal("expected 0 domains initially")
	}

	q.Enqueue("a.com", "https://a.com/1", 0, 0, 0)
	q.Enqueue("b.com", "https://b.com/1", 0, 0, 0)
	if q.DomainCount() != 2 {
		t.Fatalf("expected 2 domains, got %d", q.DomainCount())
	}

	q.Dequeue("a.com")
	if q.DomainCount() != 1 {
		t.Fatalf("expected 1 domain after dequeue, got %d", q.DomainCount())
	}
}

func TestHasURLs(t *testing.T) {
	q := openTestQueue(t)

	if q.HasURLs("a.com") {
		t.Fatal("expected false for empty domain")
	}

	q.Enqueue("a.com", "https://a.com/1", 0, 0, 0)
	if !q.HasURLs("a.com") {
		t.Fatal("expected true after enqueue")
	}

	q.Dequeue("a.com")
	if q.HasURLs("a.com") {
		t.Fatal("expected false after dequeue")
	}
}

func TestPersistence(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "queue.db")

	// First session: enqueue URLs.
	q1, err := Open(path)
	if err != nil {
		t.Fatalf("first Open() error: %v", err)
	}
	q1.Enqueue("example.com", "https://example.com/a", 0, 0, 0)
	q1.Enqueue("example.com", "https://example.com/b", 1, 0, 0)
	q1.Enqueue("other.org", "https://other.org/x", 2, 0, 0)
	if err := q1.Close(); err != nil {
		t.Fatalf("first Close() error: %v", err)
	}

	// Second session: reopen and verify.
	q2, err := Open(path)
	if err != nil {
		t.Fatalf("second Open() error: %v", err)
	}
	defer func() { _ = q2.Close() }()

	got, ok := q2.Dequeue("example.com")
	if !ok || got.URL != "https://example.com/a" || got.Depth != 0 {
		t.Fatalf("persistence: first dequeue got %+v, ok=%v", got, ok)
	}
	got, ok = q2.Dequeue("example.com")
	if !ok || got.URL != "https://example.com/b" || got.Depth != 1 {
		t.Fatalf("persistence: second dequeue got %+v, ok=%v", got, ok)
	}
	got, ok = q2.Dequeue("other.org")
	if !ok || got.URL != "https://other.org/x" || got.Depth != 2 {
		t.Fatalf("persistence: other.org dequeue got %+v, ok=%v", got, ok)
	}
}

func TestPersistenceCounters(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "queue.db")

	q1, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	q1.Enqueue("a.com", "https://a.com/1", 0, 0, 0)
	q1.Enqueue("a.com", "https://a.com/2", 0, 0, 0)
	q1.Enqueue("b.com", "https://b.com/1", 0, 0, 0)
	_ = q1.Close()

	q2, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = q2.Close() }()

	if q2.TotalLen() != 3 {
		t.Fatalf("TotalLen after reopen: got %d, want 3", q2.TotalLen())
	}
	if q2.QueueLen("a.com") != 2 {
		t.Fatalf("QueueLen(a.com) after reopen: got %d, want 2", q2.QueueLen("a.com"))
	}
	if q2.DomainCount() != 2 {
		t.Fatalf("DomainCount after reopen: got %d, want 2", q2.DomainCount())
	}
}

func TestSaveMeta_LoadMeta(t *testing.T) {
	q := openTestQueue(t)

	q.SaveMeta("example.com", []byte("hello"))
	got := q.LoadMeta("example.com")
	if string(got) != "hello" {
		t.Fatalf("LoadMeta: got %q, want %q", got, "hello")
	}

	// Non-existent key
	if q.LoadMeta("noexist.com") != nil {
		t.Fatal("expected nil for non-existent meta")
	}
}

func TestSaveMetaBatch(t *testing.T) {
	q := openTestQueue(t)

	err := q.SaveMetaBatch(func(put func(string, []byte)) {
		put("a.com", []byte("data-a"))
		put("b.com", []byte("data-b"))
		put("c.com", []byte("data-c"))
	})
	if err != nil {
		t.Fatalf("SaveMetaBatch error: %v", err)
	}

	if string(q.LoadMeta("a.com")) != "data-a" {
		t.Fatal("a.com meta mismatch")
	}
	if string(q.LoadMeta("b.com")) != "data-b" {
		t.Fatal("b.com meta mismatch")
	}
	if string(q.LoadMeta("c.com")) != "data-c" {
		t.Fatal("c.com meta mismatch")
	}
}

func TestIterMeta(t *testing.T) {
	q := openTestQueue(t)

	q.SaveMeta("a.com", []byte("1"))
	q.SaveMeta("b.com", []byte("2"))

	got := make(map[string]string)
	q.IterMeta(func(domain string, data []byte) {
		got[domain] = string(data)
	})

	if len(got) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(got))
	}
	if got["a.com"] != "1" || got["b.com"] != "2" {
		t.Fatalf("unexpected meta contents: %v", got)
	}
}

func TestDeleteMeta(t *testing.T) {
	q := openTestQueue(t)

	q.SaveMeta("example.com", []byte("data"))
	q.DeleteMeta("example.com")

	if q.LoadMeta("example.com") != nil {
		t.Fatal("expected nil after DeleteMeta")
	}
}

func TestConcurrentEnqueueDequeue(t *testing.T) {
	q := openTestQueue(t)
	const goroutines = 50
	const perGoroutine = 20

	var wg sync.WaitGroup

	// Enqueue goroutines
	for g := range goroutines {
		wg.Add(1)
		go func() {
			defer wg.Done()
			domain := fmt.Sprintf("d%d.com", g)
			for i := range perGoroutine {
				q.Enqueue(domain, fmt.Sprintf("https://%s/%d", domain, i), 0, 0, 0)
			}
		}()
	}
	wg.Wait()

	if q.TotalLen() != goroutines*perGoroutine {
		t.Fatalf("after enqueue: TotalLen=%d, want %d", q.TotalLen(), goroutines*perGoroutine)
	}

	// Dequeue goroutines
	var dequeued atomic.Int64
	for g := range goroutines {
		wg.Add(1)
		go func() {
			defer wg.Done()
			domain := fmt.Sprintf("d%d.com", g)
			for {
				_, ok := q.Dequeue(domain)
				if !ok {
					break
				}
				dequeued.Add(1)
			}
		}()
	}
	wg.Wait()

	if dequeued.Load() != goroutines*perGoroutine {
		t.Fatalf("dequeued %d, want %d", dequeued.Load(), goroutines*perGoroutine)
	}
	if q.TotalLen() != 0 {
		t.Fatalf("after dequeue: TotalLen=%d, want 0", q.TotalLen())
	}
}
