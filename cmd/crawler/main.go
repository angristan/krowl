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

	consul "github.com/hashicorp/consul/api"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"

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
		standalone   = flag.Bool("standalone", false, "Single-node mode: skip Consul and Redis")
		nodeID       = flag.Int("node-id", 0, "This node's ID")
		seedFile     = flag.String("seeds", "/mnt/jfs/seeds/top10k.txt", "Path to seed domains file")
		pebblePath   = flag.String("pebble", "/var/data/pebble", "Path to Pebble database")
		redisAddr    = flag.String("redis", "localhost:6379", "Local Redis address")
		consulAddr   = flag.String("consul", "localhost:8500", "Consul agent address")
		metricsPort  = flag.Int("metrics-port", 9090, "Prometheus metrics port")
		expectedURLs = flag.Int("expected-urls", 50_000_000, "Expected total URLs for bloom filter sizing")
		warcDir      = flag.String("warc-dir", "/mnt/jfs/warcs", "Directory for WARC output files")
	)
	flag.Parse()

	mode := "distributed"
	if *standalone {
		mode = "standalone"
	}
	// Configure structured logging
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level: slog.LevelInfo,
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

	// Domain manager
	dm := domain.NewManager(userAgent)

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
	fetchWorkers := max(1, cpus/4)
	parseWorkers := cpus - fetchWorkers
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

	// Distributed-only goroutines
	if !*standalone {
		go watchTopology(ctx, consulClient, hashRing, sender, *nodeID)
		go reportMetrics(ctx, dm, fr, hashRing, rdb)
		wg.Add(1)
		go func() {
			defer wg.Done()
			consumer.Run(ctx)
		}()
	} else {
		go reportMetrics(ctx, dm, fr, hashRing, nil)
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

	slog.Info("krowl shutdown complete")
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
			dm.Enqueue(d, fmt.Sprintf("https://%s/", d))
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

// reportMetrics periodically updates gauge metrics that need polling.
func reportMetrics(ctx context.Context, dm *domain.Manager, fr *frontier.Frontier, hashRing *ring.Ring, rdb *redis.Client) {
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
			}
		}
	}
}
