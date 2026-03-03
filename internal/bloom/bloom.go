// Package bloom implements a simple in-memory bloom filter for fast
// URL dedup as Tier 0 before hitting Pebble.
package bloom

import (
	"math"
	"math/bits"
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
	h1, h2 := twoHashes(key) // hash outside lock
	f.mu.Lock()
	for i := 0; i < f.k; i++ {
		pos := (h1 + uint64(i)*h2) % f.size
		f.bits[pos/64] |= 1 << (pos % 64)
	}
	f.mu.Unlock()
}

// Test returns true if the key is probably in the set.
// False positives are possible; false negatives are not.
func (f *Filter) Test(key []byte) bool {
	h1, h2 := twoHashes(key) // hash outside lock
	f.mu.RLock()
	for i := 0; i < f.k; i++ {
		pos := (h1 + uint64(i)*h2) % f.size
		if f.bits[pos/64]&(1<<(pos%64)) == 0 {
			f.mu.RUnlock()
			return false
		}
	}
	f.mu.RUnlock()
	return true
}

// TestAndAdd atomically checks and adds. Returns true if the key was
// already probably present.
func (f *Filter) TestAndAdd(key []byte) bool {
	h1, h2 := twoHashes(key) // hash outside lock
	f.mu.Lock()
	present := true
	for i := 0; i < f.k; i++ {
		pos := (h1 + uint64(i)*h2) % f.size
		if f.bits[pos/64]&(1<<(pos%64)) == 0 {
			present = false
		}
		f.bits[pos/64] |= 1 << (pos % 64)
	}
	f.mu.Unlock()
	return present
}

// FNV-128a constants (matches Go stdlib hash/fnv).
const (
	fnv128aOffsetHigh = 0x6c62272e07bb0142
	fnv128aOffsetLow  = 0x62b821756295c58d
	fnv128aPrimeLow   = 0x13b
	fnv128aPrimeShift = 24
)

// twoHashes computes two independent hash values using FNV-128a inline
// (avoids fnv.New128a() + Sum() heap allocations on every call).
// We derive k hash functions via h1 + i*h2 (Kirsch-Mitzenmacher).
func twoHashes(key []byte) (uint64, uint64) {
	s0 := uint64(fnv128aOffsetHigh)
	s1 := uint64(fnv128aOffsetLow)
	for _, c := range key {
		s1 ^= uint64(c)
		t0, t1 := bits.Mul64(fnv128aPrimeLow, s1)
		t0 += fnv128aPrimeLow*s0 + s1<<fnv128aPrimeShift
		s0, s1 = t0, t1
	}
	return s0, s1
}

// SizeBytes returns the memory footprint of the bit array.
func (f *Filter) SizeBytes() int {
	return len(f.bits) * 8
}
