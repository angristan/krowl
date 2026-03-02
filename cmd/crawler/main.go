package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/KimMachineGun/automemlimit/memlimit"
	consul "github.com/hashicorp/consul/api"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"

	"github.com/stanislas/krowl/internal/checkpoint"
	"github.com/stanislas/krowl/internal/dedup"
	"github.com/stanislas/krowl/internal/domain"
	"github.com/stanislas/krowl/internal/fetch"
	"github.com/stanislas/krowl/internal/frontier"
	"github.com/stanislas/krowl/internal/inbox"
	m "github.com/stanislas/krowl/internal/metrics"
	"github.com/stanislas/krowl/internal/parse"
	"github.com/stanislas/krowl/internal/ring"
	warcwriter "github.com/stanislas/krowl/internal/warc"
)

const userAgent = "krowl/1.0 (+https://github.com/stanislas/krowl; crawl@stanislas.blog)"

func init() {
	m.Register()
}

func main() {
	var (
		standalone     = flag.Bool("standalone", false, "Single-node mode: skip Consul and Redis")
		nodeID         = flag.Int("node-id", 0, "This node's ID")
		seedFile       = flag.String("seeds", "/mnt/jfs/seeds/top10k.txt", "Path to seed domains file")
		pebblePath     = flag.String("pebble", "/mnt/jfs/data/pebble", "Path to Pebble database")
		redisAddr      = flag.String("redis", "localhost:6379", "Local Redis address")
		consulAddr     = flag.String("consul", "localhost:8500", "Consul agent address")
		metricsPort    = flag.Int("metrics-port", 9090, "Prometheus metrics port")
		expectedURLs   = flag.Int("expected-urls", 50_000_000, "Expected total URLs for bloom filter sizing")
		warcDir        = flag.String("warc-dir", "/mnt/jfs/warcs", "Directory for WARC output files")
		checkpointPath = flag.String("checkpoint", "/mnt/jfs/data/frontier.ckpt", "Path for frontier checkpoint file")
		checkpointSec  = flag.Int("checkpoint-interval", 30, "Seconds between periodic frontier checkpoints (0 to disable)")
		fetchWorkersF  = flag.Int("fetch-workers", 100, "Number of fetcher goroutines (I/O-bound, set high)")
		parseWorkersF  = flag.Int("parse-workers", 0, "Number of parser goroutines (0 = NumCPU)")
		maxFrontier    = flag.Int("max-frontier", domain.DefaultMaxFrontier, "Global cap on total queued URLs (backpressure)")
		memLimitPct    = flag.Int("mem-limit-pct", 90, "GOMEMLIMIT as percentage of cgroup/system memory (0 to disable)")
		logLevel       = flag.String("log-level", "info", "Log level: debug, info, warn, error")
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
	defer dd.Close()

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
		rdb          *redis.Client
		consulClient *consul.Client
		sender       *inbox.Sender
		consumer     *inbox.Consumer
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

		updateTopology(consulClient, hashRing, sender, *nodeID)
	}
	defer func() {
		if sender != nil {
			sender.Close()
		}
		if rdb != nil {
			rdb.Close()
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

	// WARC writer
	ww, err := warcwriter.NewWriter(*warcDir, *nodeID, userAgent)
	if err != nil {
		slog.Error("failed to create WARC writer", "error", err)
		os.Exit(1)
	}
	defer ww.Close()

	// --- Worker sizing ---
	cpus := runtime.NumCPU()
	fetchWorkers := *fetchWorkersF
	parseWorkers := *parseWorkersF
	if parseWorkers == 0 {
		parseWorkers = cpus
	}
	slog.Info("worker pool configured", "fetch_workers", fetchWorkers, "parse_workers", parseWorkers, "cpus", cpus)

	// Channels
	fetchResults := make(chan fetch.Result, 10000)
	warcResults := make(chan fetch.Result, 10000)
	fanoutResults := make(chan fetch.Result, 10000)

	// Parse pool (metrics are wired directly to the centralized metrics package)
	parsePool := parse.NewPool(fetchResults, dd, dm, sender, hashRing, *nodeID, parseWorkers)

	// Fetch pool (writes to fanoutResults, which fans out to parsers + WARC)
	fetchPool := fetch.NewPool(dm, fr, fanoutResults, userAgent, fetchWorkers)

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
		// Register this crawler in Consul (now that /health is serving)
		serviceID := registerConsulService(consulClient, *nodeID, *metricsPort)
		defer deregisterConsulService(consulClient, serviceID)

		go watchTopology(ctx, consulClient, hashRing, sender, *nodeID)
		go reportMetrics(ctx, dm, fr, hashRing, rdb, dd)
		wg.Add(1)
		go func() {
			defer wg.Done()
			consumer.Run(ctx)
		}()
	} else {
		go reportMetrics(ctx, dm, fr, hashRing, nil, dd)
	}

	// WARC writer goroutine
	var warcWg sync.WaitGroup
	warcWg.Add(1)
	go func() {
		defer warcWg.Done()
		for result := range warcResults {
			if err := ww.WriteResult(result); err != nil {
				slog.Warn("WARC write error", "error", err)
				m.WARCWriteErrors.Inc()
			}
		}
	}()

	// Fan-out: fetchers -> (parsers + WARC writer)
	wg.Add(1)
	go func() {
		defer wg.Done()
		for result := range fanoutResults {
			fetchResults <- result
			warcResults <- result
		}
		close(fetchResults)
		close(warcResults)
	}()

	// Fetchers - when ctx is cancelled, Run returns and we close fanoutResults
	wg.Add(1)
	go func() {
		defer wg.Done()
		fetchPool.Run(ctx)
		close(fanoutResults)
	}()

	// Parsers (blocks until fetchResults is closed or ctx cancelled)
	parsePool.Run(ctx)

	// Wait for all goroutines to finish draining
	slog.Info("waiting for goroutines to finish")
	wg.Wait()

	// Wait for WARC writer to flush remaining records
	slog.Info("flushing WARC writer")
	warcWg.Wait()

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
	defer f.Close()

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

func serveHTTP(port int) {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		fmt.Fprintln(w, "ok")
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
func reportMetrics(ctx context.Context, dm *domain.Manager, fr *frontier.Frontier, hashRing *ring.Ring, rdb *redis.Client, dd *dedup.Dedup) {
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
