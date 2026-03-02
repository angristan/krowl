package dedup

import (
	"fmt"
	"testing"
)

func TestNew(t *testing.T) {
	dir := t.TempDir()
	d, err := New(dir, 1000)
	if err != nil {
		t.Fatalf("New() returned error: %v", err)
	}
	defer d.Close()
}

func TestIsNew_UnseenURL(t *testing.T) {
	dir := t.TempDir()
	d, err := New(dir, 1000)
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}
	defer d.Close()

	if !d.IsNew("https://example.com") {
		t.Fatal("IsNew() returned false for a never-seen URL")
	}
}

func TestIsNew_SeenURL(t *testing.T) {
	dir := t.TempDir()
	d, err := New(dir, 1000)
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}
	defer d.Close()

	url := "https://example.com/page"
	if !d.IsNew(url) {
		t.Fatal("first IsNew() should return true")
	}
	if d.IsNew(url) {
		t.Fatal("second IsNew() should return false for already-seen URL")
	}
}

func TestIsNew_ManyURLs(t *testing.T) {
	dir := t.TempDir()
	d, err := New(dir, 10000)
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}
	defer d.Close()

	const n = 500
	urls := make([]string, n)
	for i := range urls {
		urls[i] = fmt.Sprintf("https://example.com/page/%d", i)
	}

	// First pass: every URL should be new.
	for i, u := range urls {
		if !d.IsNew(u) {
			t.Fatalf("first call to IsNew(%q) at index %d returned false", u, i)
		}
	}

	// Second pass: every URL should be seen.
	for i, u := range urls {
		if d.IsNew(u) {
			t.Fatalf("second call to IsNew(%q) at index %d returned true", u, i)
		}
	}
}

func TestMarkSeen(t *testing.T) {
	dir := t.TempDir()
	d, err := New(dir, 1000)
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}
	defer d.Close()

	url := "https://example.com/marked"
	d.MarkSeen(url)

	if d.IsNew(url) {
		t.Fatal("IsNew() returned true after MarkSeen()")
	}
}

func TestClose(t *testing.T) {
	dir := t.TempDir()
	d, err := New(dir, 1000)
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}

	if err := d.Close(); err != nil {
		t.Fatalf("Close() returned error: %v", err)
	}
}

func TestPersistence(t *testing.T) {
	dir := t.TempDir()

	urls := []string{
		"https://example.com/a",
		"https://example.com/b",
		"https://example.com/c",
	}

	// First session: add URLs.
	d1, err := New(dir, 1000)
	if err != nil {
		t.Fatalf("first New() error: %v", err)
	}
	for _, u := range urls {
		if !d1.IsNew(u) {
			t.Fatalf("IsNew(%q) returned false in first session", u)
		}
	}
	// Flush memtable to disk. The production config uses DisableWAL
	// and NoSync, so unflushed data lives only in the memtable and
	// would be lost on Close without an explicit flush.
	if err := d1.pebble.Flush(); err != nil {
		t.Fatalf("Flush() error: %v", err)
	}
	if err := d1.Close(); err != nil {
		t.Fatalf("first Close() error: %v", err)
	}

	// Second session: reopen the same path. WarmBloom must reload keys
	// into the fresh bloom filter (same as main.go startup sequence).
	d2, err := New(dir, 1000)
	if err != nil {
		t.Fatalf("second New() error: %v", err)
	}
	defer d2.Close()

	n, err := d2.WarmBloom()
	if err != nil {
		t.Fatalf("WarmBloom() error: %v", err)
	}
	if n != len(urls) {
		t.Fatalf("WarmBloom() loaded %d keys, want %d", n, len(urls))
	}

	for _, u := range urls {
		if d2.IsNew(u) {
			t.Fatalf("IsNew(%q) returned true after reopen — persistence failed", u)
		}
	}

	// A URL never inserted should still be new.
	if !d2.IsNew("https://example.com/never-seen") {
		t.Fatal("fresh URL should be new after reopen")
	}
}
