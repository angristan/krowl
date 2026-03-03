package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
	"unsafe"

	"github.com/KimMachineGun/automemlimit/memlimit"
	"github.com/grafana/pyroscope-go"
	consul "github.com/hashicorp/consul/api"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"

	warc "github.com/internetarchive/gowarc"

	"github.com/stanislas/krowl/internal/checkpoint"
	"github.com/stanislas/krowl/internal/dedup"
	"github.com/stanislas/krowl/internal/domain"
	"github.com/stanislas/krowl/internal/fetch"
	"github.com/stanislas/krowl/internal/frontier"
	"github.com/stanislas/krowl/internal/inbox"
	m "github.com/stanislas/krowl/internal/metrics"
	"github.com/stanislas/krowl/internal/parse"
	"github.com/stanislas/krowl/internal/ring"
)

const userAgent = "krowl/1.0 (+https://github.com/stanislas/krowl; crawl@stanislas.blog)"

func init() {
	m.Register()
}

func main() {
	var (
		standalone      = flag.Bool("standalone", false, "Single-node mode: skip Consul and Redis")
		nodeID          = flag.Int("node-id", 0, "This node's ID")
		seedFile        = flag.String("seeds", "/mnt/jfs/seeds/top100k.txt", "Path to seed domains file")
		pebblePath      = flag.String("pebble", "/mnt/jfs/data/pebble", "Path to Pebble database")
		redisAddr       = flag.String("redis", "localhost:6379", "Local Redis address")
		consulAddr      = flag.String("consul", "localhost:8500", "Consul agent address")
		metricsPort     = flag.Int("metrics-port", 9090, "Prometheus metrics port")
		expectedURLs    = flag.Int("expected-urls", 50_000_000, "Expected total URLs for bloom filter sizing")
		warcDir         = flag.String("warc-dir", "/mnt/jfs/warcs", "Directory for WARC output files")
		checkpointPath  = flag.String("checkpoint", "/mnt/jfs/data/frontier.ckpt", "Path for frontier checkpoint file")
		checkpointSec   = flag.Int("checkpoint-interval", 30, "Seconds between periodic frontier checkpoints (0 to disable)")
		fetchWorkersF   = flag.Int("fetch-workers", 200, "Number of fetcher goroutines (I/O-bound, set high)")
		parseWorkersF   = flag.Int("parse-workers", 0, "Minimum parser goroutines (0 = NumCPU)")
		parseWorkersMax = flag.Int("parse-workers-max", 16, "Maximum parser goroutines (auto-scaled based on channel backpressure)")
		warcPoolSize    = flag.Int("warc-pool-size", 4, "Number of concurrent WARC writer goroutines (gowarc pool)")
		warcSizeMB      = flag.Float64("warc-size-mb", 1024, "Max WARC file size in MB before rotation")
		warcTempDir     = flag.String("warc-temp-dir", "/tmp/krowl-warc", "Temp directory for gowarc spooled files")
		warcOnDisk      = flag.Bool("warc-on-disk", false, "Write WARC payloads to disk instead of RAM (reduces memory, slower)")
		maxFrontier     = flag.Int("max-frontier", domain.DefaultMaxFrontier, "Global cap on total queued URLs (backpressure)")
		memLimitPct     = flag.Int("mem-limit-pct", 70, "GOMEMLIMIT as percentage of cgroup/system memory (0 to disable)")
		pyroscopeAddr   = flag.String("pyroscope", "", "Pyroscope server address (e.g. http://10.100.0.6:4040), empty to disable")
		logLevel        = flag.String("log-level", "info", "Log level: debug, info, warn, error")
	)
	flag.Parse()

	// Auto-set GOMEMLIMIT from cgroup limit (systemd MemoryMax) or system memory.
	// Respects GOMEMLIMIT env var if already set.
	if *memLimitPct > 0 {
		ratio := float64(*memLimitPct) / 100
		limit, err := memlimit.SetGoMemLimitWithOpts(
			memlimit.WithRatio(ratio),
			memlimit.WithProvider(
				memlimit.ApplyFallback(
					memlimit.FromCgroup,
					memlimit.FromSystem,
				),
			),
		)
		if err != nil {
			slog.Warn("automemlimit failed", "error", err)
		} else if limit > 0 {
			slog.Info("GOMEMLIMIT set", "limit_mb", limit/(1024*1024), "ratio", ratio)
		}
	}

	mode := "distributed"
	if *standalone {
		mode = "standalone"
	}
	// Configure structured logging
	var level slog.Level
	switch strings.ToLower(*logLevel) {
	case "debug":
		level = slog.LevelDebug
	case "warn":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	default:
		level = slog.LevelInfo
	}
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level: level,
	})))
	slog.Info("krowl starting", "mode", mode, "node", *nodeID, "seeds", *seedFile)

	// Continuous profiling via Pyroscope (push mode)
	if *pyroscopeAddr != "" {
		p, err := pyroscope.Start(pyroscope.Config{
			ApplicationName: "krowl",
			ServerAddress:   *pyroscopeAddr,
			Tags:            map[string]string{"node_id": strconv.Itoa(*nodeID)},
			ProfileTypes: []pyroscope.ProfileType{
				pyroscope.ProfileCPU,
				pyroscope.ProfileAllocObjects,
				pyroscope.ProfileAllocSpace,
				pyroscope.ProfileInuseObjects,
				pyroscope.ProfileInuseSpace,
				pyroscope.ProfileGoroutines,
				pyroscope.ProfileMutexCount,
				pyroscope.ProfileMutexDuration,
				pyroscope.ProfileBlockCount,
				pyroscope.ProfileBlockDuration,
			},
		})
		if err != nil {
			slog.Warn("pyroscope failed to start", "error", err)
		} else {
			defer func() { _ = p.Stop() }()
			slog.Info("pyroscope profiling enabled", "server", *pyroscopeAddr)
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle shutdown
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigCh
		slog.Info("received signal, shutting down", "signal", sig)
		cancel()
	}()

	// --- Initialize components ---

	// Dedup (bloom + Pebble)
	dd, err := dedup.New(*pebblePath, *expectedURLs)
	if err != nil {
		slog.Error("failed to open dedup", "error", err)
		os.Exit(1)
	}
	defer func() { _ = dd.Close() }()

	// Warm bloom filter from Pebble so it's authoritative before crawling starts.
	warmStart := time.Now()
	nWarmed, err := dd.WarmBloom()
	if err != nil {
		slog.Error("failed to warm bloom filter", "error", err)
		os.Exit(1)
	}
	slog.Info("bloom filter warmed from Pebble", "urls", nWarmed, "duration", time.Since(warmStart))

	// Domain manager
	dm := domain.NewManager(userAgent, *maxFrontier)

	// Frontier priority heap (replaces O(n) domain scan with O(log n) heap)
	fr := frontier.New()
	dm.SetFrontier(fr)

	// Consistent hash ring
	hashRing := ring.New(ring.DefaultVnodes)

	// --- Mode-dependent init ---
	var (
		rdb             *redis.Client
		consulClient    *consul.Client
		sender          *inbox.Sender
		consumer        *inbox.Consumer
		consulServiceID string
	)

	if *standalone {
		// Standalone: single node owns everything, no Consul, no Redis
		hashRing.SetNodes([]ring.Node{{ID: *nodeID, Addr: "localhost"}})
		sender = inbox.NewSender(hashRing, *nodeID, dd)
		slog.Info("standalone mode: Consul and Redis disabled")
	} else {
		// Distributed: connect Redis + Consul, build topology
		rdb = redis.NewClient(&redis.Options{
			Addr:     *redisAddr,
			PoolSize: 32,
		})
		if err := rdb.Ping(ctx).Err(); err != nil {
			slog.Error("failed to connect to local Redis", "error", err)
			os.Exit(1)
		}

		consulCfg := consul.DefaultConfig()
		consulCfg.Address = *consulAddr
		cc, err := consul.NewClient(consulCfg)
		if err != nil {
			slog.Error("failed to create Consul client", "error", err)
			os.Exit(1)
		}
		consulClient = cc

		sender = inbox.NewSender(hashRing, *nodeID, dd)
		consumer = inbox.NewConsumer(rdb, dd, dm)

		// Register in Consul and ensure this node is in the hash ring
		// before loading seeds so seed distribution is correct.
		consulServiceID = registerConsulService(consulClient, *nodeID, *metricsPort)
		updateTopology(consulClient, hashRing, sender, *nodeID)
		// Consul health check may not have passed yet, so ensure self is
		// in the ring for correct seed distribution.
		hashRing.EnsureNode(ring.Node{ID: *nodeID, Addr: fmt.Sprintf("localhost:%d", *metricsPort)})
	}
	defer func() {
		if sender != nil {
			sender.Close()
		}
		if rdb != nil {
			_ = rdb.Close()
		}
	}()

	// Restore frontier from checkpoint (if any)
	ckptQueues, err := checkpoint.Load(*checkpointPath)
	if err != nil {
		slog.Warn("failed to load checkpoint, starting fresh", "error", err)
	} else if len(ckptQueues) > 0 {
		dm.RestoreQueues(ckptQueues)
	}

	// Load seed domains for this node
	loadSeeds(*seedFile, hashRing, *nodeID, dm)

	// --- Worker sizing ---
	cpus := runtime.NumCPU()
	fetchWorkers := *fetchWorkersF
	parseMin := *parseWorkersF
	if parseMin == 0 {
		parseMin = cpus
	}
	parseMax := *parseWorkersMax
	if parseMax < parseMin {
		parseMax = parseMin
	}
	slog.Info("worker pool configured",
		"fetch_workers", fetchWorkers,
		"parse_min", parseMin, "parse_max", parseMax,
		"warc_pool_size", *warcPoolSize,
		"cpus", cpus)

	// --- gowarc WARC-writing HTTP client ---
	// WARC recording happens transparently at the transport layer.
	// Every HTTP request/response is captured via TeeReader/MultiWriter
	// wrapping on the TCP connection, then written to rotating WARC files.
	rotatorSettings := &warc.RotatorSettings{
		WarcinfoContent: warc.Header{
			"software": userAgent,
		},
		Prefix:             "KROWL",
		Compression:        "gzip",
		WARCWriterPoolSize: *warcPoolSize,
		WARCSize:           *warcSizeMB,
		OutputDirectory:    *warcDir,
	}

	warcClient, err := warc.NewWARCWritingHTTPClient(warc.HTTPClientSettings{
		RotatorSettings:       rotatorSettings,
		TempDir:               *warcTempDir,
		FollowRedirects:       true,
		MaxReadBeforeTruncate: fetch.MaxBodySize,
		DecompressBody:        true,
		VerifyCerts:           false,
		FullOnDisk:            *warcOnDisk,
		DNSServers:            []string{"127.0.0.1"}, // Use local CoreDNS (port from resolv.conf, default 53)
	})
	if err != nil {
		slog.Error("failed to create WARC HTTP client", "error", err)
		os.Exit(1)
	}
	// ErrChan MUST be drained — it is unbuffered and will block gowarc otherwise.
	go func() {
		for warcErr := range warcClient.ErrChan {
			slog.Warn("gowarc error", "error", warcErr.Err, "func", warcErr.Func)
			m.WARCWriteErrors.Inc()
		}
	}()

	// Patch gowarc's transport so connections expose TLS state via
	// ConnectionState(). This makes resp.TLS non-nil, restoring TLS
	// version and cipher metrics that gowarc would otherwise hide.
	patchTransportTLS(warcClient)

	// Channels — simplified: fetch writes directly to parse channel.
	// No fan-out or WARC channel needed (gowarc records at transport layer).
	fetchResults := make(chan fetch.Result, 100)

	// Parse pool (metrics are wired directly to the centralized metrics package)
	parsePool := parse.NewPool(fetchResults, dd, dm, sender, hashRing, *nodeID, parseMin)

	// Fetch pool (writes directly to fetchResults for parsers)
	fetchPool := fetch.NewPool(dm, fr, fetchResults, warcClient, userAgent, fetchWorkers)

	// --- Start goroutines ---
	var wg sync.WaitGroup

	// Metrics + health server
	go serveHTTP(*metricsPort)

	// Periodic frontier checkpoint
	if *checkpointSec > 0 {
		go periodicCheckpoint(ctx, dm, *checkpointPath, time.Duration(*checkpointSec)*time.Second)
	}

	// Distributed-only goroutines
	if !*standalone {
		// Consul registration was done before seed loading; defer deregistration
		defer deregisterConsulService(consulClient, consulServiceID)

		go watchTopology(ctx, consulClient, hashRing, sender, *nodeID)
		go reportMetrics(ctx, dm, fr, hashRing, rdb, dd, warcClient, fetchResults)
		wg.Add(1)
		go func() {
			defer wg.Done()
			consumer.Run(ctx)
		}()
	} else {
		go reportMetrics(ctx, dm, fr, hashRing, nil, dd, warcClient, fetchResults)
	}

	// Fetchers write directly to fetchResults. WARC recording happens
	// transparently at the gowarc transport layer — no fan-out needed.
	wg.Add(1)
	go func() {
		defer wg.Done()
		fetchPool.Run(ctx)
		close(fetchResults)
	}()

	// Parse worker pool with auto-scaling based on channel backpressure.
	var parseWg sync.WaitGroup
	go runParseScaler(ctx, parsePool, fetchResults, parseMin, parseMax, &parseWg)

	// Wait for parsers to finish draining
	parseWg.Wait()

	// Wait for all goroutines to finish draining
	slog.Info("waiting for goroutines to finish")
	wg.Wait()

	// Flush gowarc WARC writers — waits for in-flight records, closes files.
	slog.Info("flushing WARC writers")
	if err := warcClient.Close(); err != nil {
		slog.Error("gowarc close error", "error", err)
	}

	// Final frontier checkpoint
	if err := checkpoint.Save(*checkpointPath, dm.Snapshot()); err != nil {
		slog.Error("failed to save final checkpoint", "error", err)
	}

	slog.Info("krowl shutdown complete")
}

// registerConsulService registers this crawler in Consul with a health check.
func registerConsulService(client *consul.Client, nodeID int, port int) string {
	serviceID := fmt.Sprintf("crawler-%d", nodeID)
	reg := &consul.AgentServiceRegistration{
		ID:   serviceID,
		Name: "crawler",
		Port: port,
		Tags: []string{"worker", "metrics"},
		Meta: map[string]string{
			"node_id": strconv.Itoa(nodeID),
		},
		Check: &consul.AgentServiceCheck{
			HTTP:                           fmt.Sprintf("http://localhost:%d/health", port),
			Interval:                       "5s",
			Timeout:                        "2s",
			DeregisterCriticalServiceAfter: "1m",
		},
	}
	if err := client.Agent().ServiceRegister(reg); err != nil {
		slog.Error("failed to register service in Consul", "error", err)
	} else {
		slog.Info("registered in Consul", "service_id", serviceID)
	}
	return serviceID
}

// deregisterConsulService removes this crawler from Consul.
func deregisterConsulService(client *consul.Client, serviceID string) {
	if err := client.Agent().ServiceDeregister(serviceID); err != nil {
		slog.Error("failed to deregister service from Consul", "error", err)
	} else {
		slog.Info("deregistered from Consul", "service_id", serviceID)
	}
}

// updateTopology rebuilds the hash ring from Consul's service catalog.
func updateTopology(client *consul.Client, hashRing *ring.Ring, sender *inbox.Sender, myID int) {
	services, _, err := client.Health().Service("crawler", "", true, nil)
	if err != nil {
		slog.Error("failed to query Consul", "error", err)
		return
	}

	nodes := make([]ring.Node, 0, len(services))
	for _, svc := range services {
		idStr := svc.Service.Meta["node_id"]
		nodeID, _ := strconv.Atoi(idStr)
		addr := svc.Service.Address
		if addr == "" {
			addr = svc.Node.Address
		}
		redisAddr := svc.Service.Meta["redis_addr"]
		if redisAddr == "" {
			redisAddr = fmt.Sprintf("%s:6379", addr)
		}

		nodes = append(nodes, ring.Node{
			ID:        nodeID,
			Addr:      fmt.Sprintf("%s:%d", addr, svc.Service.Port),
			RedisAddr: redisAddr,
		})
	}

	hashRing.SetNodes(nodes)
	sender.UpdatePeers(nodes)
	m.TopologyNodes.Set(float64(len(nodes)))
	slog.Info("topology updated", "nodes", len(nodes))
}

// watchTopology long-polls Consul for topology changes.
func watchTopology(ctx context.Context, client *consul.Client, hashRing *ring.Ring, sender *inbox.Sender, myID int) {
	var lastIndex uint64

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		services, meta, err := client.Health().Service("crawler", "", true, &consul.QueryOptions{
			WaitIndex: lastIndex,
			WaitTime:  5 * time.Minute,
		})
		if err != nil {
			slog.Error("Consul watch error", "error", err)
			time.Sleep(time.Second)
			continue
		}

		if meta.LastIndex != lastIndex {
			lastIndex = meta.LastIndex

			nodes := make([]ring.Node, 0, len(services))
			for _, svc := range services {
				idStr := svc.Service.Meta["node_id"]
				nodeID, _ := strconv.Atoi(idStr)
				addr := svc.Service.Address
				if addr == "" {
					addr = svc.Node.Address
				}
				redisAddr := svc.Service.Meta["redis_addr"]
				if redisAddr == "" {
					redisAddr = fmt.Sprintf("%s:6379", addr)
				}
				nodes = append(nodes, ring.Node{
					ID:        nodeID,
					Addr:      fmt.Sprintf("%s:%d", addr, svc.Service.Port),
					RedisAddr: redisAddr,
				})
			}

			hashRing.SetNodes(nodes)
			sender.UpdatePeers(nodes)
			m.TopologyNodes.Set(float64(len(nodes)))
			slog.Info("topology changed", "nodes", len(nodes))
		}
	}
}

// loadSeeds reads the seed file and enqueues domains owned by this node.
func loadSeeds(path string, hashRing *ring.Ring, myID int, dm *domain.Manager) {
	f, err := os.Open(path)
	if err != nil {
		slog.Error("failed to open seeds", "error", err, "path", path)
		os.Exit(1)
	}
	defer func() { _ = f.Close() }()

	scanner := bufio.NewScanner(f)
	count := 0
	for scanner.Scan() {
		d := strings.TrimSpace(scanner.Text())
		if d == "" || strings.HasPrefix(d, "#") {
			continue
		}
		if hashRing.Owner(d) == myID {
			dm.Enqueue(d, fmt.Sprintf("https://%s/", d), 0) // seeds are depth 0
			count++
		}
	}
	slog.Info("seeds loaded", "count", count, "node", myID)
}

// patchTransportTLS wraps gowarc's internal DialTLSContext so that TLS
// version and cipher suite are extracted from the utls connection and
// stored in a *fetch.TLSInfo struct passed via the request context.
//
// Go's http.Transport only populates resp.TLS for *crypto/tls.Conn
// (concrete type check), but gowarc returns *CustomConnection wrapping
// *utls.UConn — a different type. So resp.TLS is always nil with gowarc.
//
// Instead, we use reflection to call utls.UConn.ConnectionState() (which
// returns utls.ConnectionState, not crypto/tls.ConnectionState — same
// fields, different Go types) and write Version + CipherSuite into the
// context-stashed TLSInfo.
func patchTransportTLS(client *warc.CustomHTTPClient) {
	ct := reflect.ValueOf(client.Transport).Elem()
	tField := ct.FieldByName("t")
	//nolint:gosec // Accessing unexported field — required to patch DialTLSContext
	transport := (*http.Transport)(unsafe.Pointer(tField.UnsafeAddr()))

	origDialTLS := transport.DialTLSContext
	transport.DialTLSContext = func(ctx context.Context, network, addr string) (net.Conn, error) {
		conn, err := origDialTLS(ctx, network, addr)
		if err != nil {
			return conn, err
		}
		// Extract TLS state from the gowarc CustomConnection's inner utls.UConn
		// and write it into the context-stashed TLSInfo for fetch.go to read.
		if cc, ok := conn.(*warc.CustomConnection); ok {
			if info, ok := ctx.Value(fetch.TLSInfoKey).(*fetch.TLSInfo); ok {
				innerVal := reflect.ValueOf(cc.Conn)
				method := innerVal.MethodByName("ConnectionState")
				if method.IsValid() {
					results := method.Call(nil)
					if len(results) == 1 {
						csVal := results[0]
						version := csVal.FieldByName("Version")
						cipher := csVal.FieldByName("CipherSuite")
						if version.IsValid() && cipher.IsValid() {
							info.Version = uint16(version.Uint())
							info.CipherSuite = uint16(cipher.Uint())
							info.Set = true
						}
					}
				}
			}
		}
		return conn, nil
	}
	slog.Info("patched gowarc transport for TLS state visibility")
}

func serveHTTP(port int) {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	// pprof handlers (registered on DefaultServeMux by _ "net/http/pprof")
	mux.Handle("/debug/pprof/", http.DefaultServeMux)
	mux.Handle("/debug/pprof/profile", http.DefaultServeMux)
	mux.Handle("/debug/pprof/heap", http.DefaultServeMux)
	mux.Handle("/debug/pprof/goroutine", http.DefaultServeMux)
	mux.Handle("/debug/pprof/allocs", http.DefaultServeMux)
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = fmt.Fprintln(w, "ok")
	})
	slog.Info("HTTP server starting", "port", port, "endpoints", "/metrics, /health")
	if err := http.ListenAndServe(fmt.Sprintf(":%d", port), mux); err != nil {
		slog.Error("HTTP server error", "error", err)
	}
}

// periodicCheckpoint saves the frontier to disk at regular intervals.
func periodicCheckpoint(ctx context.Context, dm *domain.Manager, path string, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := checkpoint.Save(path, dm.Snapshot()); err != nil {
				slog.Error("periodic checkpoint failed", "error", err)
			}
		}
	}
}

// reportMetrics periodically updates gauge metrics that need polling.
func reportMetrics(ctx context.Context, dm *domain.Manager, fr *frontier.Frontier, hashRing *ring.Ring, rdb *redis.Client, dd *dedup.Dedup, warcClient *warc.CustomHTTPClient, chParse chan fetch.Result) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			m.FrontierSize.Set(float64(dm.TotalQueueLen()))
			m.FrontierDomains.Set(float64(fr.Len()))
			m.ActiveDomains.Set(float64(len(dm.ActiveDomains())))
			m.TrackedDomains.Set(float64(dm.DomainCount()))
			m.TopologyNodes.Set(float64(hashRing.NodeCount()))

			m.ChanParse.Set(float64(len(chParse)))

			// gowarc internal stats
			m.WARCDataBytes.Set(float64(warcClient.DataTotal.Load()))
			m.WARCInflightWriters.Set(float64(warcClient.WaitGroup.Size()))

			if rdb != nil {
				inboxLen, _ := rdb.LLen(ctx, "inbox").Result()
				m.InboxQueueSize.Set(float64(inboxLen))

				ps := rdb.PoolStats()
				m.RedisPoolHits.Set(float64(ps.Hits))
				m.RedisPoolMisses.Set(float64(ps.Misses))
				m.RedisPoolTimeouts.Set(float64(ps.Timeouts))
				m.RedisPoolTotalConns.Set(float64(ps.TotalConns))
				m.RedisPoolIdleConns.Set(float64(ps.IdleConns))
				m.RedisPoolStaleConns.Set(float64(ps.StaleConns))
			}

			// Pebble internals
			pm := dd.Metrics()
			m.PebbleDiskUsageBytes.Set(float64(pm.DiskSpaceUsage()))
			m.PebbleMemtableSizeBytes.Set(float64(pm.MemTable.Size))
			m.PebbleMemtableCount.Set(float64(pm.MemTable.Count))
			m.PebbleCompactionDebtBytes.Set(float64(pm.Compact.EstimatedDebt))
			m.PebbleL0Files.Set(float64(pm.Levels[0].NumFiles))
			m.PebbleL0Sublevels.Set(float64(pm.Levels[0].Sublevels))
			m.PebbleReadAmp.Set(float64(pm.ReadAmp()))
			var totalKeys int64
			for _, lm := range pm.Levels {
				totalKeys += int64(lm.NumFiles)
			}
			m.PebbleTotalKeys.Set(float64(totalKeys))
		}
	}
}

// runParseScaler dynamically manages parse worker goroutines based on channel
// backpressure. Same strategy as WARC scaler: scale up aggressively, scale
// down conservatively.
func runParseScaler(ctx context.Context, pool *parse.Pool, ch chan fetch.Result, minWorkers, maxWorkers int, wg *sync.WaitGroup) {
	type workerSlot struct {
		done chan struct{}
	}

	var (
		slots  []workerSlot
		nextID int
	)

	spawn := func() {
		done := make(chan struct{})
		id := nextID
		nextID++
		wg.Add(1)
		go func() {
			defer wg.Done()
			pool.Worker(ctx, done)
		}()
		slots = append(slots, workerSlot{done: done})
		m.ParseActiveWorkers.Set(float64(len(slots)))
		slog.Info("parse scaler: worker started", "id", id, "active", len(slots))
	}

	shrink := func() {
		if len(slots) <= minWorkers {
			return
		}
		last := slots[len(slots)-1]
		close(last.done)
		slots = slots[:len(slots)-1]
		m.ParseActiveWorkers.Set(float64(len(slots)))
		slog.Info("parse scaler: worker stopped", "active", len(slots))
	}

	// Spawn initial pool
	for i := 0; i < minWorkers; i++ {
		spawn()
	}

	capacity := cap(ch)
	lowTicks := 0

	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			fill := len(ch)
			pct := fill * 100 / capacity
			current := len(slots)

			switch {
			case pct > 80 && current < maxWorkers:
				n := min(4, maxWorkers-current)
				for range n {
					spawn()
				}
				lowTicks = 0
			case pct > 50 && current < maxWorkers:
				spawn()
				lowTicks = 0
			case pct < 10 && current > minWorkers:
				lowTicks++
				if lowTicks >= 6 { // 30s sustained low fill
					shrink()
					lowTicks = 0
				}
			default:
				lowTicks = 0
			}
		}
	}
}
