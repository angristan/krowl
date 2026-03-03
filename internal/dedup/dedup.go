// Package dedup implements URL deduplication with a bloom filter backed
// by Pebble for persistence. On startup, all Pebble keys are loaded into
// the bloom filter. During crawling, the bloom filter handles all lookups
// (zero false negatives) and Pebble is write-only (new URLs persisted for
// restart recovery).
package dedup

import (
	"encoding/binary"

	"github.com/cockroachdb/pebble"
	"github.com/stanislas/krowl/internal/bloom"
	m "github.com/stanislas/krowl/internal/metrics"
)

// FNV-64a constants for inline hashing.
const (
	fnv64aOffset = 14695981039346656037
	fnv64aPrime  = 1099511628211
)

// Reusable value for Pebble Set calls (avoids []byte("1") alloc per write).
var pebbleOne = []byte("1")

type Dedup struct {
	bloom  *bloom.Filter
	pebble *pebble.DB
}

// New creates a two-tier dedup store.
// expectedURLs is the estimated total URLs this node will see.
func New(pebblePath string, expectedURLs int) (*Dedup, error) {
	cache := pebble.NewCache(256 * 1024 * 1024) // 256MB block cache
	defer cache.Unref()

	db, err := pebble.Open(pebblePath, &pebble.Options{
		Cache:                       cache,
		MemTableSize:                64 * 1024 * 1024, // 64MB memtable
		MemTableStopWritesThreshold: 4,
		MaxConcurrentCompactions:    func() int { return 4 },
		DisableWAL:                  true, // acceptable for dedup: lose last few ms on crash
	})
	if err != nil {
		return nil, err
	}

	bf := bloom.New(expectedURLs, 0.01) // 1% FP rate

	return &Dedup{
		bloom:  bf,
		pebble: db,
	}, nil
}

// WarmBloom scans all Pebble keys into the bloom filter.
// Must be called before crawling starts so bloom is authoritative.
func (d *Dedup) WarmBloom() (int, error) {
	iter, err := d.pebble.NewIter(nil)
	if err != nil {
		return 0, err
	}
	defer func() { _ = iter.Close() }()

	n := 0
	for iter.First(); iter.Valid(); iter.Next() {
		d.bloom.Add(iter.Key())
		n++
	}
	return n, iter.Error()
}

// IsNew returns true if the URL has not been seen before on this node.
// If the URL is new, it is automatically marked as seen in both bloom
// (for fast future lookups) and Pebble (for persistence across restarts).
// Bloom is trusted fully — no Pebble reads during crawling.
func (d *Dedup) IsNew(rawURL string) bool {
	m.DedupLookups.Inc()
	key := urlKey(rawURL)

	if d.bloom.Test(key) {
		m.DedupBloomHits.Inc()
		return false
	}

	// New URL. Mark in both bloom + Pebble.
	m.DedupNewURLs.Inc()
	d.bloom.Add(key)
	_ = d.pebble.Set(key, pebbleOne, pebble.NoSync)
	return true
}

// MarkSeen marks a URL as seen without checking. Used by the inbox
// consumer when the URL has already passed dedup on the sender side
// but we need to record it locally for future checks.
func (d *Dedup) MarkSeen(rawURL string) {
	key := urlKey(rawURL)
	d.bloom.Add(key)
	_ = d.pebble.Set(key, pebbleOne, pebble.NoSync)
}

// Metrics returns the underlying Pebble metrics.
func (d *Dedup) Metrics() *pebble.Metrics {
	return d.pebble.Metrics()
}

// Close closes the Pebble database.
func (d *Dedup) Close() error {
	return d.pebble.Close()
}

// urlKey computes FNV-64a inline to avoid fnv.New64a() interface alloc
// and []byte(rawURL) string-to-byte conversion.
func urlKey(rawURL string) []byte {
	h := uint64(fnv64aOffset)
	for i := 0; i < len(rawURL); i++ {
		h ^= uint64(rawURL[i])
		h *= fnv64aPrime
	}
	key := make([]byte, 8)
	binary.BigEndian.PutUint64(key, h)
	return key
}
