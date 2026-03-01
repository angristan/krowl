// Package bloom implements a simple in-memory bloom filter for fast
// URL dedup as Tier 0 before hitting Pebble.
package bloom

import (
	"hash/fnv"
	"math"
	"sync"
)

// Filter is a thread-safe bloom filter.
type Filter struct {
	mu   sync.RWMutex
	bits []uint64 // bit array packed into uint64s
	size uint64   // total number of bits
	k    int      // number of hash functions
}

// New creates a bloom filter sized for expectedItems with the given
// false positive rate. For 100M items at 1% FP: ~120MB.
func New(expectedItems int, fpRate float64) *Filter {
	// m = -n * ln(p) / (ln2)^2
	m := uint64(-float64(expectedItems) * math.Log(fpRate) / (math.Ln2 * math.Ln2))
	// k = (m/n) * ln2
	k := int(math.Ceil(float64(m) / float64(expectedItems) * math.Ln2))

	words := (m + 63) / 64
	return &Filter{
		bits: make([]uint64, words),
		size: m,
		k:    k,
	}
}

// Add inserts a key into the filter.
func (f *Filter) Add(key []byte) {
	f.mu.Lock()
	defer f.mu.Unlock()

	h1, h2 := twoHashes(key)
	for i := 0; i < f.k; i++ {
		pos := (h1 + uint64(i)*h2) % f.size
		f.bits[pos/64] |= 1 << (pos % 64)
	}
}

// Test returns true if the key is probably in the set.
// False positives are possible; false negatives are not.
func (f *Filter) Test(key []byte) bool {
	f.mu.RLock()
	defer f.mu.RUnlock()

	h1, h2 := twoHashes(key)
	for i := 0; i < f.k; i++ {
		pos := (h1 + uint64(i)*h2) % f.size
		if f.bits[pos/64]&(1<<(pos%64)) == 0 {
			return false
		}
	}
	return true
}

// TestAndAdd atomically checks and adds. Returns true if the key was
// already probably present.
func (f *Filter) TestAndAdd(key []byte) bool {
	f.mu.Lock()
	defer f.mu.Unlock()

	h1, h2 := twoHashes(key)
	present := true
	for i := 0; i < f.k; i++ {
		pos := (h1 + uint64(i)*h2) % f.size
		if f.bits[pos/64]&(1<<(pos%64)) == 0 {
			present = false
		}
		f.bits[pos/64] |= 1 << (pos % 64)
	}
	return present
}

// twoHashes computes two independent hash values using FNV.
// We derive k hash functions via h1 + i*h2 (Kirsch-Mitzenmacher).
func twoHashes(key []byte) (uint64, uint64) {
	h := fnv.New128a()
	h.Write(key)
	sum := h.Sum(nil)
	h1 := uint64(sum[0])<<56 | uint64(sum[1])<<48 | uint64(sum[2])<<40 | uint64(sum[3])<<32 |
		uint64(sum[4])<<24 | uint64(sum[5])<<16 | uint64(sum[6])<<8 | uint64(sum[7])
	h2 := uint64(sum[8])<<56 | uint64(sum[9])<<48 | uint64(sum[10])<<40 | uint64(sum[11])<<32 |
		uint64(sum[12])<<24 | uint64(sum[13])<<16 | uint64(sum[14])<<8 | uint64(sum[15])
	return h1, h2
}

// SizeBytes returns the memory footprint of the bit array.
func (f *Filter) SizeBytes() int {
	return len(f.bits) * 8
}
