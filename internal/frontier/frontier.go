// Package frontier implements a priority queue of domains ordered by
// their next-allowed-fetch time. This replaces the naive O(n) scan
// of all active domains with an O(log n) heap pop.
package frontier

import (
	"container/heap"
	"sync"
	"time"
)

// entryPool recycles Entry structs to reduce GC pressure.
// Frontier.Push was 12.65% of inuse_objects in profiling — each pop+push
// cycle allocated a new Entry that the GC had to scan and collect.
var entryPool = sync.Pool{
	New: func() any { return &Entry{} },
}

// Entry represents a domain in the priority queue.
type Entry struct {
	Domain    string
	NextFetch time.Time
	index     int // managed by heap.Interface
}

// Frontier is a thread-safe priority queue of domains.
type Frontier struct {
	mu  sync.Mutex
	h   domainHeap
	idx map[string]*Entry // domain -> entry for O(1) lookup/update
}

// New creates an empty frontier.
func New() *Frontier {
	f := &Frontier{
		idx: make(map[string]*Entry),
	}
	heap.Init(&f.h)
	return f
}

// Push adds or updates a domain in the frontier.
// If the domain is already present, its next fetch time is updated
// only if the new time is earlier.
func (f *Frontier) Push(domain string, nextFetch time.Time) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if e, ok := f.idx[domain]; ok {
		if nextFetch.Before(e.NextFetch) {
			e.NextFetch = nextFetch
			heap.Fix(&f.h, e.index)
		}
		return
	}

	e := entryPool.Get().(*Entry)
	e.Domain = domain
	e.NextFetch = nextFetch
	e.index = 0
	heap.Push(&f.h, e)
	f.idx[domain] = e
}

// PopReady returns the domain with the earliest next-fetch time,
// but only if that time has passed. Returns ("", waitDuration) if
// the next domain isn't ready yet, or ("", 0) if the frontier is empty.
func (f *Frontier) PopReady() (string, time.Duration) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.h.Len() == 0 {
		return "", 0
	}

	top := f.h[0]
	now := time.Now()
	if top.NextFetch.After(now) {
		return "", time.Until(top.NextFetch)
	}

	e := heap.Pop(&f.h).(*Entry)
	delete(f.idx, e.Domain)
	domain := e.Domain
	// Return entry to pool; clear references to allow GC of the domain string
	e.Domain = ""
	entryPool.Put(e)
	return domain, 0
}

// Peek returns the next-fetch time of the earliest domain without popping.
func (f *Frontier) Peek() (string, time.Time, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.h.Len() == 0 {
		return "", time.Time{}, false
	}
	return f.h[0].Domain, f.h[0].NextFetch, true
}

// Remove removes a domain from the frontier.
func (f *Frontier) Remove(domain string) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if e, ok := f.idx[domain]; ok {
		heap.Remove(&f.h, e.index)
		delete(f.idx, domain)
		e.Domain = ""
		entryPool.Put(e)
	}
}

// Len returns the number of domains in the frontier.
func (f *Frontier) Len() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.h.Len()
}

// Contains checks if a domain is in the frontier.
func (f *Frontier) Contains(domain string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	_, ok := f.idx[domain]
	return ok
}

// --- heap.Interface implementation ---

type domainHeap []*Entry

func (h domainHeap) Len() int           { return len(h) }
func (h domainHeap) Less(i, j int) bool { return h[i].NextFetch.Before(h[j].NextFetch) }

func (h domainHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].index = i
	h[j].index = j
}

func (h *domainHeap) Push(x any) {
	e := x.(*Entry)
	e.index = len(*h)
	*h = append(*h, e)
}

func (h *domainHeap) Pop() any {
	old := *h
	n := len(old)
	e := old[n-1]
	old[n-1] = nil
	e.index = -1
	*h = old[:n-1]
	return e
}
