package warc

import (
	"compress/gzip"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/stanislas/krowl/internal/fetch"
)

func testResult() fetch.Result {
	return fetch.Result{
		URL:            "https://example.com/page",
		Domain:         "example.com",
		Body:           []byte("<html>test</html>"),
		Status:         200,
		Headers:        http.Header{"Content-Type": {"text/html"}},
		RequestHeaders: http.Header{"User-Agent": {"test-bot"}},
		Method:         "GET",
		RequestURI:     "/page",
	}
}

func readGzipFile(t *testing.T, path string) string {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open gzip file: %v", err)
	}
	defer f.Close()
	gr, err := gzip.NewReader(f)
	if err != nil {
		t.Fatalf("create gzip reader: %v", err)
	}
	defer gr.Close()
	data, err := io.ReadAll(gr)
	if err != nil {
		t.Fatalf("read gzip: %v", err)
	}
	return string(data)
}

func warcFiles(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read dir: %v", err)
	}
	var files []string
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".warc.gz") {
			files = append(files, filepath.Join(dir, e.Name()))
		}
	}
	return files
}

func TestNewWriter(t *testing.T) {
	dir := t.TempDir()
	w, err := NewWriter(dir, 0, "test-crawler/1.0")
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	defer w.Close()

	files := warcFiles(t, dir)
	if len(files) == 0 {
		t.Fatal("expected at least one WARC file after NewWriter")
	}
}

func TestWriteResult(t *testing.T) {
	dir := t.TempDir()
	w, err := NewWriter(dir, 0, "test-crawler/1.0")
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}

	if err := w.WriteResult(testResult()); err != nil {
		t.Fatalf("WriteResult: %v", err)
	}

	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestClose(t *testing.T) {
	dir := t.TempDir()
	w, err := NewWriter(dir, 0, "test-crawler/1.0")
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestValidGzip(t *testing.T) {
	dir := t.TempDir()
	w, err := NewWriter(dir, 0, "test-crawler/1.0")
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	if err := w.WriteResult(testResult()); err != nil {
		t.Fatalf("WriteResult: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	files := warcFiles(t, dir)
	if len(files) == 0 {
		t.Fatal("no WARC files found")
	}

	content := readGzipFile(t, files[0])
	if !strings.Contains(content, "WARC/1.1") {
		t.Fatalf("decompressed content does not contain WARC/1.1:\n%s", content)
	}
}

func TestRequestAndResponseRecords(t *testing.T) {
	dir := t.TempDir()
	w, err := NewWriter(dir, 0, "test-crawler/1.0")
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	if err := w.WriteResult(testResult()); err != nil {
		t.Fatalf("WriteResult: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	content := readGzipFile(t, warcFiles(t, dir)[0])

	if !strings.Contains(content, "WARC-Type: request") {
		t.Error("missing WARC-Type: request record")
	}
	if !strings.Contains(content, "WARC-Type: response") {
		t.Error("missing WARC-Type: response record")
	}
}

func TestResponseContainsBody(t *testing.T) {
	dir := t.TempDir()
	w, err := NewWriter(dir, 0, "test-crawler/1.0")
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	if err := w.WriteResult(testResult()); err != nil {
		t.Fatalf("WriteResult: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	content := readGzipFile(t, warcFiles(t, dir)[0])
	if !strings.Contains(content, "<html>test</html>") {
		t.Error("response record does not contain the expected body")
	}
}

func TestRequestContainsMethodAndHost(t *testing.T) {
	dir := t.TempDir()
	w, err := NewWriter(dir, 0, "test-crawler/1.0")
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	if err := w.WriteResult(testResult()); err != nil {
		t.Fatalf("WriteResult: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	content := readGzipFile(t, warcFiles(t, dir)[0])
	if !strings.Contains(content, "GET /page HTTP/1.1") {
		t.Error("request record missing 'GET /page HTTP/1.1' request line")
	}
	if !strings.Contains(content, "Host: example.com") {
		t.Error("request record missing 'Host: example.com'")
	}
}

func TestConcurrentToHeaders(t *testing.T) {
	dir := t.TempDir()
	w, err := NewWriter(dir, 0, "test-crawler/1.0")
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	if err := w.WriteResult(testResult()); err != nil {
		t.Fatalf("WriteResult: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	content := readGzipFile(t, warcFiles(t, dir)[0])

	// Extract all WARC-Record-ID and WARC-Concurrent-To values
	reRecordID := regexp.MustCompile(`WARC-Record-ID: (<urn:uuid:[^>]+>)`)
	reConcurrent := regexp.MustCompile(`WARC-Concurrent-To: (<urn:uuid:[^>]+>)`)

	recordIDs := reRecordID.FindAllStringSubmatch(content, -1)
	concurrentTos := reConcurrent.FindAllStringSubmatch(content, -1)

	if len(concurrentTos) < 2 {
		t.Fatalf("expected at least 2 WARC-Concurrent-To headers, got %d", len(concurrentTos))
	}

	// Build a set of all record IDs
	idSet := make(map[string]bool)
	for _, m := range recordIDs {
		idSet[m[1]] = true
	}

	// Every Concurrent-To must reference an existing record ID
	for _, m := range concurrentTos {
		if !idSet[m[1]] {
			t.Errorf("WARC-Concurrent-To %s does not match any WARC-Record-ID", m[1])
		}
	}
}

func TestFileRotation(t *testing.T) {
	dir := t.TempDir()
	w, err := NewWriter(dir, 0, "test-crawler/1.0")
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}

	// Build a large result whose body will exceed MaxFileSize when written once
	// MaxFileSize is 1GB which is too large for a test. Instead, override the
	// writer's written counter to simulate being near the limit, then write
	// one more result to trigger rotation.
	r := testResult()

	// Write one result to the first file
	if err := w.WriteResult(r); err != nil {
		t.Fatalf("WriteResult: %v", err)
	}

	// Simulate that the file is full by setting written to MaxFileSize
	w.mu.Lock()
	w.written = MaxFileSize
	w.mu.Unlock()

	// This write should trigger rotation and create a second file
	if err := w.WriteResult(r); err != nil {
		t.Fatalf("WriteResult after rotation: %v", err)
	}

	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	files := warcFiles(t, dir)
	if len(files) < 2 {
		t.Fatalf("expected at least 2 WARC files after rotation, got %d", len(files))
	}

	// Verify the second file is also valid gzip with WARC content
	content := readGzipFile(t, files[1])
	if !strings.Contains(content, "WARC/1.1") {
		t.Error("rotated file does not contain WARC/1.1")
	}
}
