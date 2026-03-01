package bloom

import (
	"fmt"
	"math"
	"sync"
	"testing"
)

func TestNew_Sizing(t *testing.T) {
	tests := []struct {
		name          string
		expectedItems int
		fpRate        float64
	}{
		{"small_1pct", 1000, 0.01},
		{"small_10pct", 1000, 0.10},
		{"medium_1pct", 100_000, 0.01},
		{"large_1pct", 1_000_000, 0.01},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			f := New(tc.expectedItems, tc.fpRate)

			if f == nil {
				t.Fatal("New returned nil")
			}

			sizeBytes := f.SizeBytes()
			if sizeBytes <= 0 {
				t.Fatalf("SizeBytes() = %d, want > 0", sizeBytes)
			}

			// Theoretical optimal size: m = -n * ln(p) / (ln2)^2
			mBits := -float64(tc.expectedItems) * math.Log(tc.fpRate) / (math.Ln2 * math.Ln2)
			expectedBytes := mBits / 8

			// Allow the actual size to be between 0.9x and 1.1x of theoretical
			// (rounding to uint64 words can add a bit).
			lo := expectedBytes * 0.9
			hi := expectedBytes * 1.1
			if float64(sizeBytes) < lo || float64(sizeBytes) > hi {
				t.Errorf("SizeBytes() = %d, expected roughly %.0f bytes (range [%.0f, %.0f])",
					sizeBytes, expectedBytes, lo, hi)
			}
		})
	}
}

func TestAdd_Then_Test(t *testing.T) {
	f := New(1000, 0.01)

	keys := []string{
		"https://example.com",
		"https://example.com/page",
		"hello world",
		"",
		"a",
	}

	for _, k := range keys {
		f.Add([]byte(k))
	}

	for _, k := range keys {
		if !f.Test([]byte(k)) {
			t.Errorf("Test(%q) = false after Add; want true (no false negatives)", k)
		}
	}
}

func TestTest_ReturnsFalse_ForAbsentKeys(t *testing.T) {
	f := New(1000, 0.01)

	// Add some keys.
	for i := 0; i < 100; i++ {
		f.Add([]byte(fmt.Sprintf("added-%d", i)))
	}

	// Test keys that were never added. With only 100 items in a filter
	// sized for 1000 at 1% FP, essentially none of these should match.
	falsePositives := 0
	testCount := 1000
	for i := 0; i < testCount; i++ {
		if f.Test([]byte(fmt.Sprintf("never-added-%d", i))) {
			falsePositives++
		}
	}

	// We'd expect ~0-1 false positives with such a lightly loaded filter.
	// Be generous and allow up to 5%.
	maxFP := int(float64(testCount) * 0.05)
	if falsePositives > maxFP {
		t.Errorf("got %d false positives out of %d tests (%.2f%%); want < 5%%",
			falsePositives, testCount, float64(falsePositives)/float64(testCount)*100)
	}
}

func TestTestAndAdd_FirstCallFalse_SecondCallTrue(t *testing.T) {
	f := New(1000, 0.01)

	key := []byte("test-key")

	first := f.TestAndAdd(key)
	if first {
		t.Error("TestAndAdd on fresh key returned true; want false")
	}

	second := f.TestAndAdd(key)
	if !second {
		t.Error("TestAndAdd on already-added key returned false; want true")
	}

	// Also verify Test agrees.
	if !f.Test(key) {
		t.Error("Test returned false after TestAndAdd; want true")
	}
}

func TestTestAndAdd_MultipleDistinctKeys(t *testing.T) {
	f := New(1000, 0.01)

	for i := 0; i < 50; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		if f.TestAndAdd(key) {
			t.Errorf("TestAndAdd(%q) first call returned true; want false", key)
		}
	}

	for i := 0; i < 50; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		if !f.TestAndAdd(key) {
			t.Errorf("TestAndAdd(%q) second call returned false; want true", key)
		}
	}
}

func TestFalsePositiveRate(t *testing.T) {
	const (
		numInsert = 10_000
		numProbe  = 10_000
		targetFP  = 0.01
	)

	f := New(numInsert, targetFP)

	// Insert numInsert items.
	for i := 0; i < numInsert; i++ {
		f.Add([]byte(fmt.Sprintf("insert-%d", i)))
	}

	// Probe with a completely disjoint set of keys.
	falsePositives := 0
	for i := 0; i < numProbe; i++ {
		if f.Test([]byte(fmt.Sprintf("probe-%d", i))) {
			falsePositives++
		}
	}

	observedRate := float64(falsePositives) / float64(numProbe)

	// Allow up to 5x the target FP rate as headroom (statistical variance
	// plus the approximation in hash function independence).
	// With targetFP=1%, we allow up to 5%.
	maxRate := 0.05
	if observedRate > maxRate {
		t.Errorf("false positive rate = %.4f (%d/%d); want < %.4f",
			observedRate, falsePositives, numProbe, maxRate)
	}

	t.Logf("FP rate: %.4f%% (%d/%d), target was %.2f%%",
		observedRate*100, falsePositives, numProbe, targetFP*100)
}

func TestLargeInserts_NoPanic(t *testing.T) {
	const n = 100_000
	f := New(n, 0.01)

	for i := 0; i < n; i++ {
		f.Add([]byte(fmt.Sprintf("url-%d", i)))
	}

	// Spot-check a few.
	for i := 0; i < 100; i++ {
		if !f.Test([]byte(fmt.Sprintf("url-%d", i))) {
			t.Errorf("Test(url-%d) = false after Add; no false negatives allowed", i)
		}
	}
}

func TestConcurrency(t *testing.T) {
	const (
		numItems   = 10_000
		numWriters = 4
		numReaders = 4
		itemsPerGo = numItems / numWriters
	)

	f := New(numItems, 0.01)

	var wg sync.WaitGroup

	// Writers: each goroutine adds a distinct range of keys.
	for w := 0; w < numWriters; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			base := id * itemsPerGo
			for i := 0; i < itemsPerGo; i++ {
				f.Add([]byte(fmt.Sprintf("w-%d", base+i)))
			}
		}(w)
	}

	// Readers: each goroutine tests keys (some present, some not).
	for r := 0; r < numReaders; r++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < itemsPerGo; i++ {
				// Test an existing key range (may not be inserted yet - that's fine).
				f.Test([]byte(fmt.Sprintf("w-%d", i)))
				// Test a key that was never added.
				f.Test([]byte(fmt.Sprintf("absent-%d-%d", id, i)))
			}
		}(r)
	}

	// TestAndAdd concurrently.
	for ta := 0; ta < numWriters; ta++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < itemsPerGo/2; i++ {
				f.TestAndAdd([]byte(fmt.Sprintf("ta-%d-%d", id, i)))
			}
		}(ta)
	}

	wg.Wait()

	// After all goroutines finish, every written key must be present.
	for i := 0; i < numItems; i++ {
		if !f.Test([]byte(fmt.Sprintf("w-%d", i))) {
			t.Errorf("Test(w-%d) = false after concurrent Add; no false negatives allowed", i)
		}
	}
}

func TestConcurrency_TestAndAdd_Atomicity(t *testing.T) {
	// For a single key, exactly one goroutine should observe "not present"
	// (TestAndAdd returns false), and the rest should observe "present"
	// (TestAndAdd returns true).
	f := New(1000, 0.01)
	key := []byte("shared-key")

	const numGoroutines = 100
	results := make(chan bool, numGoroutines)

	var wg sync.WaitGroup
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			results <- f.TestAndAdd(key)
		}()
	}
	wg.Wait()
	close(results)

	falseCount := 0
	for r := range results {
		if !r {
			falseCount++
		}
	}

	// Exactly one goroutine should have seen the key as absent.
	if falseCount != 1 {
		t.Errorf("expected exactly 1 goroutine to get false from TestAndAdd; got %d", falseCount)
	}
}

func TestEmptyFilter_TestReturnsFalse(t *testing.T) {
	f := New(100, 0.01)

	if f.Test([]byte("anything")) {
		t.Error("Test on empty filter returned true; want false")
	}
	if f.Test([]byte("")) {
		t.Error("Test on empty filter with empty key returned true; want false")
	}
}

func TestSizeBytes(t *testing.T) {
	// For 100M items at 1% FP the comment says ~120MB.
	f := New(100_000_000, 0.01)
	sizeMB := float64(f.SizeBytes()) / (1024 * 1024)

	// Allow 100-140 MB range.
	if sizeMB < 100 || sizeMB > 140 {
		t.Errorf("SizeBytes for 100M items at 1%% FP = %.1f MB; expected ~120 MB", sizeMB)
	}
	t.Logf("100M items at 1%% FP: %.1f MB", sizeMB)
}

func BenchmarkAdd(b *testing.B) {
	f := New(b.N, 0.01)
	keys := make([][]byte, b.N)
	for i := range keys {
		keys[i] = []byte(fmt.Sprintf("bench-key-%d", i))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		f.Add(keys[i])
	}
}

func BenchmarkTest(b *testing.B) {
	f := New(b.N, 0.01)
	keys := make([][]byte, b.N)
	for i := range keys {
		keys[i] = []byte(fmt.Sprintf("bench-key-%d", i))
		f.Add(keys[i])
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		f.Test(keys[i])
	}
}

func BenchmarkTestAndAdd(b *testing.B) {
	f := New(b.N, 0.01)
	keys := make([][]byte, b.N)
	for i := range keys {
		keys[i] = []byte(fmt.Sprintf("bench-key-%d", i))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		f.TestAndAdd(keys[i])
	}
}
