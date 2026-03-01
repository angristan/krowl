// Package metrics defines all Prometheus metrics for krowl.
// Every metric lives here so there's a single source of truth.
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
)

const ns = "krowl"

// Histogram buckets tuned for crawler workloads.
var (
	// Durations in seconds: 1ms to 60s
	durationBuckets = []float64{
		0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5,
		1, 2.5, 5, 10, 30, 60,
	}
	// Faster durations for DNS/TLS: 1ms to 10s
	fastDurationBuckets = []float64{
		0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10,
	}
	// Page sizes in bytes: 1KB to 10MB
	sizeBuckets = []float64{
		1024, 5120, 10240, 51200, 102400, 256000, 512000,
		1048576, 2097152, 5242880, 10485760,
	}
	// Link counts per page: 0 to 5000
	linkBuckets = []float64{
		0, 1, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000,
	}
	// Batch sizes
	batchBuckets = []float64{1, 5, 10, 25, 50, 100, 250, 500}
)

// ---- Fetch / HTTP ----

var (
	// Total pages fetched, by status code bucket
	PagesFetched = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: ns, Name: "pages_fetched_total",
		Help: "Total pages fetched, by HTTP status class",
	}, []string{"status"})

	// Fetch errors by type
	FetchErrors = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: ns, Name: "fetch_errors_total",
		Help: "Fetch errors by category",
	}, []string{"reason"})

	// Fetch retries (transient errors that were retried)
	FetchRetries = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: ns, Name: "fetch_retries_total",
		Help: "Transient fetch errors that triggered a retry",
	})

	// Domains permanently abandoned
	DomainsDead = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: ns, Name: "domains_dead_total",
		Help: "Domains permanently abandoned after too many consecutive errors",
	})

	// Total response time (from request start to body read)
	FetchDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: ns, Name: "fetch_duration_seconds",
		Help:    "Total fetch duration from request to body read",
		Buckets: durationBuckets,
	})

	// DNS resolution time
	DNSDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: ns, Name: "dns_duration_seconds",
		Help:    "DNS resolution time",
		Buckets: fastDurationBuckets,
	})

	// TCP connect time
	ConnectDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: ns, Name: "connect_duration_seconds",
		Help:    "TCP connection time (after DNS)",
		Buckets: fastDurationBuckets,
	})

	// TLS handshake time
	TLSDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: ns, Name: "tls_duration_seconds",
		Help:    "TLS handshake time",
		Buckets: fastDurationBuckets,
	})

	// Time to first byte (from sending request to first response byte)
	TTFBDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: ns, Name: "ttfb_duration_seconds",
		Help:    "Time to first byte",
		Buckets: durationBuckets,
	})

	// Response body size
	ResponseSize = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: ns, Name: "response_size_bytes",
		Help:    "Response body size in bytes",
		Buckets: sizeBuckets,
	})

	// Content types seen
	ContentTypes = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: ns, Name: "content_types_total",
		Help: "Content types encountered",
	}, []string{"type"})

	// Redirects followed per request
	RedirectsFollowed = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: ns, Name: "redirects_followed",
		Help:    "Number of redirects followed per fetch",
		Buckets: []float64{0, 1, 2, 3, 4, 5},
	})

	// Robots.txt blocked
	RobotsBlocked = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: ns, Name: "robots_blocked_total",
		Help: "URLs blocked by robots.txt",
	})
)

// ---- DNS cache ----

var (
	DNSCacheHits = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: ns, Name: "dns_cache_hits_total",
		Help: "DNS cache hits",
	})
	DNSCacheMisses = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: ns, Name: "dns_cache_misses_total",
		Help: "DNS cache misses",
	})
	DNSCacheSize = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: ns, Name: "dns_cache_size",
		Help: "Current DNS cache entries",
	})
	DNSCacheEvictions = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: ns, Name: "dns_cache_evictions_total",
		Help: "DNS cache full evictions",
	})
)

// ---- Parse / Links ----

var (
	URLsDiscovered = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: ns, Name: "urls_discovered_total",
		Help: "Total URLs extracted by parser",
	})
	URLsDeduped = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: ns, Name: "urls_deduped_total",
		Help: "URLs rejected by dedup",
	})
	URLsForwarded = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: ns, Name: "urls_forwarded_total",
		Help: "URLs forwarded to peer nodes",
	})
	URLsEnqueued = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: ns, Name: "urls_enqueued_total",
		Help: "URLs enqueued locally",
	})

	// Links per page
	LinksPerPage = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: ns, Name: "links_per_page",
		Help:    "Number of links extracted per page",
		Buckets: linkBuckets,
	})

	// Soft-404 / content dedup
	ContentDedupSkipped = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: ns, Name: "content_dedup_skipped_total",
		Help: "Pages skipped due to duplicate content hash (soft-404)",
	})

	// Sitemap URLs discovered
	SitemapURLsDiscovered = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: ns, Name: "sitemap_urls_discovered_total",
		Help: "URLs discovered via sitemap.xml",
	})
)

// ---- Dedup internals ----

var (
	DedupBloomHits = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: ns, Name: "dedup_bloom_hits_total",
		Help: "Bloom filter positive results (probable match)",
	})
	DedupBloomFalsePositives = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: ns, Name: "dedup_bloom_false_positives_total",
		Help: "Bloom filter false positives (bloom said yes, pebble said no)",
	})
	DedupPebbleHits = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: ns, Name: "dedup_pebble_hits_total",
		Help: "Pebble confirmed duplicates",
	})
	DedupNewURLs = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: ns, Name: "dedup_new_urls_total",
		Help: "Genuinely new URLs seen",
	})
	DedupLookups = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: ns, Name: "dedup_lookups_total",
		Help: "Total dedup lookups",
	})
)

// ---- WARC ----

var (
	WARCBytesWritten = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: ns, Name: "warc_bytes_written_total",
		Help: "Total bytes written to WARC files (compressed)",
	})
	WARCRecordsWritten = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: ns, Name: "warc_records_written_total",
		Help: "Total WARC records written",
	})
	WARCFileRotations = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: ns, Name: "warc_file_rotations_total",
		Help: "Number of WARC file rotations",
	})
	WARCWriteErrors = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: ns, Name: "warc_write_errors_total",
		Help: "WARC write errors",
	})
	WARCCurrentFileSize = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: ns, Name: "warc_current_file_bytes",
		Help: "Current WARC file size in bytes",
	})
)

// ---- Inbox ----

var (
	InboxForwardedTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: ns, Name: "inbox_forwarded_total",
		Help: "URLs forwarded via inbox LPUSH",
	})
	InboxForwardErrors = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: ns, Name: "inbox_forward_errors_total",
		Help: "Inbox forward errors",
	})
	InboxReceivedTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: ns, Name: "inbox_received_total",
		Help: "URLs received from inbox LPOP",
	})
	InboxBatchSize = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: ns, Name: "inbox_batch_size",
		Help:    "URLs per inbox batch",
		Buckets: batchBuckets,
	})
	InboxQueueSize = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: ns, Name: "inbox_queue_size",
		Help: "Current inbox queue depth",
	})
)

// ---- Frontier / crawl status ----

var (
	FrontierSize = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: ns, Name: "frontier_size",
		Help: "Total URLs across all domain queues",
	})
	FrontierDomains = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: ns, Name: "frontier_domains",
		Help: "Domains in the frontier heap",
	})
	ActiveDomains = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: ns, Name: "active_domains",
		Help: "Domains with queued URLs",
	})
	TrackedDomains = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: ns, Name: "tracked_domains",
		Help: "Total domains with any state",
	})
	TopologyNodes = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: ns, Name: "topology_nodes",
		Help: "Number of nodes in the hash ring",
	})
)

// Register registers all krowl metrics. Go runtime and process collectors are
// already registered by the default prometheus registry, so we skip them here.
func Register() {
	// Fetch / HTTP
	prometheus.MustRegister(
		PagesFetched, FetchErrors, FetchRetries, DomainsDead, FetchDuration,
		DNSDuration, ConnectDuration, TLSDuration, TTFBDuration,
		ResponseSize, ContentTypes, RedirectsFollowed, RobotsBlocked,
	)

	// DNS cache
	prometheus.MustRegister(DNSCacheHits, DNSCacheMisses, DNSCacheSize, DNSCacheEvictions)

	// Parse / links
	prometheus.MustRegister(
		URLsDiscovered, URLsDeduped, URLsForwarded, URLsEnqueued,
		LinksPerPage, ContentDedupSkipped, SitemapURLsDiscovered,
	)

	// Dedup
	prometheus.MustRegister(
		DedupBloomHits, DedupBloomFalsePositives, DedupPebbleHits,
		DedupNewURLs, DedupLookups,
	)

	// WARC
	prometheus.MustRegister(
		WARCBytesWritten, WARCRecordsWritten, WARCFileRotations,
		WARCWriteErrors, WARCCurrentFileSize,
	)

	// Inbox
	prometheus.MustRegister(
		InboxForwardedTotal, InboxForwardErrors, InboxReceivedTotal,
		InboxBatchSize, InboxQueueSize,
	)

	// Frontier / crawl
	prometheus.MustRegister(
		FrontierSize, FrontierDomains, ActiveDomains,
		TrackedDomains, TopologyNodes,
	)
}

// StatusBucket returns a label for HTTP status grouping: "2xx", "3xx", etc.
func StatusBucket(code int) string {
	switch {
	case code < 200:
		return "1xx"
	case code < 300:
		return "2xx"
	case code < 400:
		return "3xx"
	case code < 500:
		return "4xx"
	default:
		return "5xx"
	}
}
