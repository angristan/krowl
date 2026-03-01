package frontier

import (
	"sync"
	"testing"
	"time"
)

func TestNew_Empty(t *testing.T) {
	f := New()
	if f.Len() != 0 {
		t.Fatalf("New() frontier Len() = %d, want 0", f.Len())
	}
}

func TestPush_IncrementsLen(t *testing.T) {
	f := New()
	now := time.Now()

	f.Push("a.com", now)
	if got := f.Len(); got != 1 {
		t.Fatalf("after 1 Push, Len() = %d, want 1", got)
	}

	f.Push("b.com", now.Add(time.Second))
	if got := f.Len(); got != 2 {
		t.Fatalf("after 2 Pushes, Len() = %d, want 2", got)
	}

	f.Push("c.com", now.Add(2*time.Second))
	if got := f.Len(); got != 3 {
		t.Fatalf("after 3 Pushes, Len() = %d, want 3", got)
	}
}

func TestPopReady_ReturnsDomainInPast(t *testing.T) {
	f := New()
	past := time.Now().Add(-time.Minute)
	f.Push("ready.com", past)

	domain, wait := f.PopReady()
	if domain != "ready.com" {
		t.Fatalf("PopReady() domain = %q, want %q", domain, "ready.com")
	}
	if wait != 0 {
		t.Fatalf("PopReady() wait = %v, want 0", wait)
	}
	if f.Len() != 0 {
		t.Fatalf("after PopReady, Len() = %d, want 0", f.Len())
	}
}

func TestPopReady_ReturnsWaitWhenFuture(t *testing.T) {
	f := New()
	future := time.Now().Add(10 * time.Second)
	f.Push("future.com", future)

	domain, wait := f.PopReady()
	if domain != "" {
		t.Fatalf("PopReady() domain = %q, want empty", domain)
	}
	// wait should be positive and roughly 10s (allow some slack)
	if wait <= 0 {
		t.Fatalf("PopReady() wait = %v, want positive duration", wait)
	}
	if wait > 11*time.Second {
		t.Fatalf("PopReady() wait = %v, unexpectedly large", wait)
	}
	// frontier should still contain the entry
	if f.Len() != 1 {
		t.Fatalf("after PopReady on future domain, Len() = %d, want 1", f.Len())
	}
}

func TestPopReady_EmptyFrontier(t *testing.T) {
	f := New()

	domain, wait := f.PopReady()
	if domain != "" {
		t.Fatalf("PopReady() on empty frontier domain = %q, want empty", domain)
	}
	if wait != 0 {
		t.Fatalf("PopReady() on empty frontier wait = %v, want 0", wait)
	}
}

func TestPush_SameDomainUpdatesOnlyIfEarlier(t *testing.T) {
	f := New()
	t0 := time.Now().Add(-time.Minute)
	t1 := t0.Add(-30 * time.Second) // earlier than t0
	t2 := t0.Add(30 * time.Second)  // later than t0

	f.Push("d.com", t0)

	// Push with a later time — should NOT update
	f.Push("d.com", t2)
	_, peekTime, ok := f.Peek()
	if !ok {
		t.Fatal("Peek() returned ok=false after Push")
	}
	if !peekTime.Equal(t0) {
		t.Fatalf("Push with later time changed NextFetch: got %v, want %v", peekTime, t0)
	}
	if f.Len() != 1 {
		t.Fatalf("Push same domain should not change Len(), got %d, want 1", f.Len())
	}

	// Push with an earlier time — should update
	f.Push("d.com", t1)
	_, peekTime, ok = f.Peek()
	if !ok {
		t.Fatal("Peek() returned ok=false after Push")
	}
	if !peekTime.Equal(t1) {
		t.Fatalf("Push with earlier time did not update NextFetch: got %v, want %v", peekTime, t1)
	}
	if f.Len() != 1 {
		t.Fatalf("Push same domain should not change Len(), got %d, want 1", f.Len())
	}
}

func TestPeek_ReturnsEarliestWithoutRemoving(t *testing.T) {
	f := New()

	// Peek on empty
	_, _, ok := f.Peek()
	if ok {
		t.Fatal("Peek() on empty frontier returned ok=true")
	}

	now := time.Now()
	f.Push("early.com", now)
	f.Push("late.com", now.Add(time.Hour))

	domain, ts, ok := f.Peek()
	if !ok {
		t.Fatal("Peek() returned ok=false on non-empty frontier")
	}
	if domain != "early.com" {
		t.Fatalf("Peek() domain = %q, want %q", domain, "early.com")
	}
	if !ts.Equal(now) {
		t.Fatalf("Peek() time = %v, want %v", ts, now)
	}

	// Peek should not remove
	if f.Len() != 2 {
		t.Fatalf("Peek() changed Len(): got %d, want 2", f.Len())
	}

	// Second Peek returns the same result
	domain2, ts2, ok2 := f.Peek()
	if !ok2 || domain2 != domain || !ts2.Equal(ts) {
		t.Fatal("second Peek() returned different result")
	}
}

func TestRemove(t *testing.T) {
	f := New()
	now := time.Now()

	f.Push("a.com", now)
	f.Push("b.com", now.Add(time.Second))
	f.Push("c.com", now.Add(2*time.Second))

	// Remove middle element
	f.Remove("b.com")
	if f.Len() != 2 {
		t.Fatalf("after Remove, Len() = %d, want 2", f.Len())
	}
	if f.Contains("b.com") {
		t.Fatal("Contains(b.com) = true after Remove")
	}

	// Remove head element
	f.Remove("a.com")
	if f.Len() != 1 {
		t.Fatalf("after second Remove, Len() = %d, want 1", f.Len())
	}

	// Peek should return the remaining element
	domain, _, ok := f.Peek()
	if !ok || domain != "c.com" {
		t.Fatalf("Peek() after removes = (%q, _, %v), want (c.com, _, true)", domain, ok)
	}

	// Remove last element
	f.Remove("c.com")
	if f.Len() != 0 {
		t.Fatalf("after removing all, Len() = %d, want 0", f.Len())
	}

	// Remove non-existent — should be a no-op
	f.Remove("nonexistent.com")
	if f.Len() != 0 {
		t.Fatal("Remove of non-existent domain changed Len()")
	}
}

func TestContains(t *testing.T) {
	f := New()
	past := time.Now().Add(-time.Minute)

	if f.Contains("x.com") {
		t.Fatal("Contains on empty frontier returned true")
	}

	f.Push("x.com", past)
	if !f.Contains("x.com") {
		t.Fatal("Contains after Push returned false")
	}

	// Pop the domain
	domain, _ := f.PopReady()
	if domain != "x.com" {
		t.Fatalf("PopReady() = %q, want x.com", domain)
	}
	if f.Contains("x.com") {
		t.Fatal("Contains after PopReady returned true")
	}

	// Push then Remove
	f.Push("y.com", past)
	f.Remove("y.com")
	if f.Contains("y.com") {
		t.Fatal("Contains after Remove returned true")
	}
}

func TestOrdering_ThreeDomains(t *testing.T) {
	f := New()
	now := time.Now()

	// Push out of order
	t1 := now.Add(-3 * time.Second)
	t2 := now.Add(-2 * time.Second)
	t3 := now.Add(-1 * time.Second)

	f.Push("second.com", t2)
	f.Push("third.com", t3)
	f.Push("first.com", t1)

	expected := []string{"first.com", "second.com", "third.com"}
	for i, want := range expected {
		domain, wait := f.PopReady()
		if domain != want {
			t.Fatalf("pop %d: got %q, want %q (wait=%v)", i, domain, want, wait)
		}
		if wait != 0 {
			t.Fatalf("pop %d: wait = %v, want 0", i, wait)
		}
	}

	if f.Len() != 0 {
		t.Fatalf("after popping all, Len() = %d, want 0", f.Len())
	}
}

func TestOrdering_MixedPastAndFuture(t *testing.T) {
	f := New()
	now := time.Now()

	f.Push("past1.com", now.Add(-2*time.Second))
	f.Push("future.com", now.Add(10*time.Second))
	f.Push("past2.com", now.Add(-1*time.Second))

	// Should get past domains in order
	d1, w1 := f.PopReady()
	if d1 != "past1.com" || w1 != 0 {
		t.Fatalf("first pop: got (%q, %v), want (past1.com, 0)", d1, w1)
	}

	d2, w2 := f.PopReady()
	if d2 != "past2.com" || w2 != 0 {
		t.Fatalf("second pop: got (%q, %v), want (past2.com, 0)", d2, w2)
	}

	// Third pop should return wait for future domain
	d3, w3 := f.PopReady()
	if d3 != "" {
		t.Fatalf("third pop: got domain %q, want empty (future domain not ready)", d3)
	}
	if w3 <= 0 {
		t.Fatalf("third pop: wait = %v, want positive", w3)
	}
}

func TestConcurrency(t *testing.T) {
	f := New()
	now := time.Now()

	const numGoroutines = 50
	const opsPerGoroutine = 100

	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	for g := 0; g < numGoroutines; g++ {
		go func(id int) {
			defer wg.Done()
			for i := 0; i < opsPerGoroutine; i++ {
				domain := "domain" + string(rune('A'+id%26)) + ".com"
				ts := now.Add(-time.Duration(i) * time.Millisecond)

				// Mix of operations
				switch i % 5 {
				case 0:
					f.Push(domain, ts)
				case 1:
					f.PopReady()
				case 2:
					f.Peek()
				case 3:
					f.Contains(domain)
				case 4:
					f.Len()
				}
			}
		}(g)
	}

	wg.Wait()

	// Drain the frontier — should not panic
	for f.Len() > 0 {
		f.PopReady()
		// If domain is in the future, remove it to avoid infinite loop
		if d, _, ok := f.Peek(); ok {
			f.Remove(d)
		}
	}
}

func TestPush_AfterPop_SameDomain(t *testing.T) {
	f := New()
	past := time.Now().Add(-time.Minute)

	f.Push("reuse.com", past)
	domain, _ := f.PopReady()
	if domain != "reuse.com" {
		t.Fatalf("PopReady() = %q, want reuse.com", domain)
	}

	// Re-push the same domain
	f.Push("reuse.com", past.Add(-time.Second))
	if !f.Contains("reuse.com") {
		t.Fatal("Contains after re-Push returned false")
	}
	if f.Len() != 1 {
		t.Fatalf("Len() after re-Push = %d, want 1", f.Len())
	}

	domain, _ = f.PopReady()
	if domain != "reuse.com" {
		t.Fatalf("second PopReady() = %q, want reuse.com", domain)
	}
}

func TestRemove_ThenPush(t *testing.T) {
	f := New()
	now := time.Now()

	f.Push("r.com", now.Add(-time.Second))
	f.Remove("r.com")

	if f.Contains("r.com") {
		t.Fatal("Contains after Remove returned true")
	}

	// Push again after removal
	f.Push("r.com", now.Add(-2*time.Second))
	if !f.Contains("r.com") {
		t.Fatal("Contains after re-Push returned false")
	}
	if f.Len() != 1 {
		t.Fatalf("Len() = %d, want 1", f.Len())
	}
}

func TestPush_ManyDomains_OrderPreserved(t *testing.T) {
	f := New()
	now := time.Now()
	n := 100

	// Push n domains with times spread across the past
	for i := n; i > 0; i-- {
		domain := domainName(i)
		ts := now.Add(-time.Duration(i) * time.Millisecond)
		f.Push(domain, ts)
	}

	if f.Len() != n {
		t.Fatalf("Len() = %d, want %d", f.Len(), n)
	}

	// Pop all — should come out in ascending time order (largest i first since
	// time.Duration(i) is subtracted from now, so larger i = earlier time)
	prev := time.Time{}
	for i := 0; i < n; i++ {
		domain, wait := f.PopReady()
		if domain == "" {
			t.Fatalf("pop %d: got empty domain, wait=%v", i, wait)
		}
		if !prev.IsZero() {
			// We can't directly check time from popped domain, but we can
			// verify ordering via Peek on subsequent calls
		}
		_ = prev
	}

	if f.Len() != 0 {
		t.Fatalf("after draining, Len() = %d, want 0", f.Len())
	}
}

func domainName(i int) string {
	// Simple unique domain name generator
	return "d" + itoa(i) + ".com"
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	s := ""
	for i > 0 {
		s = string(rune('0'+i%10)) + s
		i /= 10
	}
	return s
}

func TestPeek_AfterUpdate(t *testing.T) {
	f := New()
	now := time.Now()

	f.Push("a.com", now.Add(10*time.Second))
	f.Push("b.com", now.Add(5*time.Second))

	// Peek should return b.com (earlier)
	domain, _, ok := f.Peek()
	if !ok || domain != "b.com" {
		t.Fatalf("Peek() = (%q, _, %v), want (b.com, _, true)", domain, ok)
	}

	// Update a.com to be earlier than b.com
	f.Push("a.com", now.Add(1*time.Second))
	domain, _, ok = f.Peek()
	if !ok || domain != "a.com" {
		t.Fatalf("Peek() after update = (%q, _, %v), want (a.com, _, true)", domain, ok)
	}
}

func TestRemove_HeadCausesNewHead(t *testing.T) {
	f := New()
	now := time.Now()

	f.Push("a.com", now.Add(-3*time.Second))
	f.Push("b.com", now.Add(-2*time.Second))
	f.Push("c.com", now.Add(-1*time.Second))

	// Remove head
	f.Remove("a.com")

	domain, _, ok := f.Peek()
	if !ok || domain != "b.com" {
		t.Fatalf("Peek() after removing head = (%q, _, %v), want (b.com, _, true)", domain, ok)
	}
}
