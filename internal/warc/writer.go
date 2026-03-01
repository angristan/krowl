// Package warc writes WARC 1.1 files with gzip compression and rotation.
// Files are written to JuiceFS-mounted storage.
//
// WARC (Web ARChive) format: ISO 28500:2017
// Each record is: version line, headers, payload, double CRLF.
package warc

import (
	"compress/gzip"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/stanislas/krowl/internal/fetch"
)

const (
	WARCVersion   = "WARC/1.1"
	MaxFileSize   = 1 * 1024 * 1024 * 1024 // 1GB per WARC file
	DefaultPrefix = "KROWL"
)

// Writer writes fetch results to rotating gzipped WARC files.
type Writer struct {
	mu       sync.Mutex
	dir      string
	prefix   string
	nodeID   int
	file     *os.File
	gzWriter *gzip.Writer
	written  int64
	fileNum  int
	software string
}

// NewWriter creates a WARC writer that writes to the given directory.
func NewWriter(dir string, nodeID int, software string) (*Writer, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("create WARC dir: %w", err)
	}

	w := &Writer{
		dir:      dir,
		prefix:   DefaultPrefix,
		nodeID:   nodeID,
		software: software,
	}

	if err := w.rotate(); err != nil {
		return nil, err
	}

	// Write warcinfo record
	if err := w.writeWarcinfo(); err != nil {
		return nil, err
	}

	return w, nil
}

// WriteResult writes a fetch result as a WARC response record.
func (w *Writer) WriteResult(result fetch.Result) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.written >= MaxFileSize {
		if err := w.rotate(); err != nil {
			return err
		}
		if err := w.writeWarcinfoLocked(); err != nil {
			return err
		}
	}

	return w.writeResponse(result)
}

// Close flushes and closes the current WARC file.
func (w *Writer) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.closeFile()
}

func (w *Writer) rotate() error {
	if w.file != nil {
		if err := w.closeFile(); err != nil {
			return err
		}
	}

	ts := time.Now().UTC().Format("20060102150405")
	name := fmt.Sprintf("%s-%s-node%d-%05d.warc.gz", w.prefix, ts, w.nodeID, w.fileNum)
	path := filepath.Join(w.dir, name)

	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("create WARC file: %w", err)
	}

	w.file = f
	w.gzWriter = gzip.NewWriter(f)
	w.written = 0
	w.fileNum++

	log.Printf("[warc] opened %s", name)
	return nil
}

func (w *Writer) closeFile() error {
	if w.gzWriter != nil {
		if err := w.gzWriter.Close(); err != nil {
			return err
		}
	}
	if w.file != nil {
		return w.file.Close()
	}
	return nil
}

func (w *Writer) writeResponse(result fetch.Result) error {
	recordID := fmt.Sprintf("<urn:uuid:%s>", uuid.New().String())
	date := time.Now().UTC().Format(time.RFC3339)

	// Build the HTTP response block (status line + headers + body)
	var httpBlock strings.Builder
	httpBlock.WriteString(fmt.Sprintf("HTTP/1.1 %d %s\r\n", result.Status, http.StatusText(result.Status)))
	for key, vals := range result.Headers {
		for _, val := range vals {
			httpBlock.WriteString(fmt.Sprintf("%s: %s\r\n", key, val))
		}
	}
	httpBlock.WriteString("\r\n")
	httpBlock.Write(result.Body)

	payload := httpBlock.String()

	// WARC record
	var record strings.Builder
	record.WriteString(WARCVersion + "\r\n")
	record.WriteString(fmt.Sprintf("WARC-Type: response\r\n"))
	record.WriteString(fmt.Sprintf("WARC-Date: %s\r\n", date))
	record.WriteString(fmt.Sprintf("WARC-Target-URI: %s\r\n", result.URL))
	record.WriteString(fmt.Sprintf("WARC-Record-ID: %s\r\n", recordID))
	record.WriteString(fmt.Sprintf("Content-Type: application/http;msgtype=response\r\n"))
	record.WriteString(fmt.Sprintf("Content-Length: %d\r\n", len(payload)))
	record.WriteString("\r\n")
	record.WriteString(payload)
	record.WriteString("\r\n\r\n")

	data := record.String()
	n, err := io.WriteString(w.gzWriter, data)
	w.written += int64(n)
	return err
}

func (w *Writer) writeWarcinfo() error {
	return w.writeWarcinfoLocked()
}

func (w *Writer) writeWarcinfoLocked() error {
	recordID := fmt.Sprintf("<urn:uuid:%s>", uuid.New().String())
	date := time.Now().UTC().Format(time.RFC3339)

	payload := fmt.Sprintf("software: %s\r\nformat: WARC File Format 1.1\r\n", w.software)

	var record strings.Builder
	record.WriteString(WARCVersion + "\r\n")
	record.WriteString("WARC-Type: warcinfo\r\n")
	record.WriteString(fmt.Sprintf("WARC-Date: %s\r\n", date))
	record.WriteString(fmt.Sprintf("WARC-Record-ID: %s\r\n", recordID))
	record.WriteString("Content-Type: application/warc-fields\r\n")
	record.WriteString(fmt.Sprintf("Content-Length: %d\r\n", len(payload)))
	record.WriteString("\r\n")
	record.WriteString(payload)
	record.WriteString("\r\n\r\n")

	data := record.String()
	n, err := io.WriteString(w.gzWriter, data)
	w.written += int64(n)
	return err
}
