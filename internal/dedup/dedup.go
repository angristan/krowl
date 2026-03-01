// Package dedup implements two-tier URL deduplication:
//
//	Tier 0: in-memory bloom filter (fast reject, no false negatives)
//	Tier 1: local Pebble on SSD (exact, persistent)
package dedup

import (
	"encoding/binary"
	"hash/fnv"

	"github.com/cockroachdb/pebble"
	"github.com/stanislas/krowl/internal/bloom"
	m "github.com/stanislas/krowl/internal/metrics"
)

type Dedup struct {
	bloom  *bloom.Filter
	pebble *pebble.DB
}

// New creates a two-tier dedup store.
// expectedURLs is the estimated total URLs this node will see.
func New(pebblePath string, expectedURLs int) (*Dedup, error) {
	db, err := pebble.Open(pebblePath, &pebble.Options{
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

// IsNew returns true if the URL has not been seen before on this node.
// If the URL is new, it is automatically marked as seen in both tiers.
func (d *Dedup) IsNew(rawURL string) bool {
	m.DedupLookups.Inc()
	key := urlKey(rawURL)

	// Tier 0: bloom filter - fast reject
	if d.bloom.Test(key) {
		m.DedupBloomHits.Inc()
		// Probably seen. Verify with Pebble.
		_, closer, err := d.pebble.Get(key)
		if err == nil {
			closer.Close()
			m.DedupPebbleHits.Inc()
			return false // definitely seen
		}
		// Bloom false positive: not in Pebble, so it's actually new.
		m.DedupBloomFalsePositives.Inc()
	} else {
		// Definitely not in bloom, but still check Pebble
		// (bloom is lost on restart, Pebble persists)
		_, closer, err := d.pebble.Get(key)
		if err == nil {
			closer.Close()
			// In Pebble but not in bloom (post-restart). Warm bloom.
			m.DedupPebbleHits.Inc()
			d.bloom.Add(key)
			return false
		}
	}

	// New URL. Mark in both tiers.
	m.DedupNewURLs.Inc()
	d.bloom.Add(key)
	_ = d.pebble.Set(key, []byte("1"), pebble.NoSync)
	return true
}

// MarkSeen marks a URL as seen without checking. Used by the inbox
// consumer when the URL has already passed dedup on the sender side
// but we need to record it locally for future checks.
func (d *Dedup) MarkSeen(rawURL string) {
	key := urlKey(rawURL)
	d.bloom.Add(key)
	_ = d.pebble.Set(key, []byte("1"), pebble.NoSync)
}

// Metrics returns the underlying Pebble metrics.
func (d *Dedup) Metrics() *pebble.Metrics {
	return d.pebble.Metrics()
}

// Close closes the Pebble database.
func (d *Dedup) Close() error {
	return d.pebble.Close()
}

func urlKey(rawURL string) []byte {
	h := fnv.New64a()
	h.Write([]byte(rawURL))
	key := make([]byte, 8)
	binary.BigEndian.PutUint64(key, h.Sum64())
	return key
}
