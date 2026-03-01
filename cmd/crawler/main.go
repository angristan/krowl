package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"time"

	consul "github.com/hashicorp/consul/api"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"

	"github.com/stanislas/krowl/internal/dedup"
	"github.com/stanislas/krowl/internal/domain"
	"github.com/stanislas/krowl/internal/fetch"
	"github.com/stanislas/krowl/internal/inbox"
	"github.com/stanislas/krowl/internal/parse"
	"github.com/stanislas/krowl/internal/ring"
	warcwriter "github.com/stanislas/krowl/internal/warc"
)

const userAgent = "krowl/1.0 (+https://github.com/stanislas/krowl; crawl@stanislas.blog)"

// Prometheus metrics
var (
	pagesFetched = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "krowl_pages_fetched_total",
		Help: "Total number of pages fetched",
	})
	urlsDiscovered = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "krowl_urls_discovered_total",
		Help: "Total number of URLs discovered by parser",
	})
	urlsDeduped = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "krowl_urls_deduped_total",
		Help: "Total number of URLs rejected by dedup",
	})
	urlsForwarded = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "krowl_urls_forwarded_total",
		Help: "Total number of URLs forwarded to other nodes",
	})
	inboxSize = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "krowl_inbox_size",
		Help: "Current number of URLs in the inbox",
	})
	frontierSize = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "krowl_frontier_size",
		Help: "Total URLs across all domain frontiers",
	})
	activeDomains = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "krowl_active_domains",
		Help: "Number of domains with URLs in frontier",
	})
)

func init() {
	prometheus.MustRegister(pagesFetched, urlsDiscovered, urlsDeduped,
		urlsForwarded, inboxSize, frontierSize, activeDomains)
}

func main() {
	var (
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

	log.Printf("krowl starting: node=%d seeds=%s", *nodeID, *seedFile)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle shutdown
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigCh
		log.Printf("received signal %v, shutting down...", sig)
		cancel()
	}()

	// --- Initialize components ---

	// Dedup (bloom + Pebble)
	dd, err := dedup.New(*pebblePath, *expectedURLs)
	if err != nil {
		log.Fatalf("failed to open dedup: %v", err)
	}
	defer dd.Close()

	// Domain manager
	dm := domain.NewManager(userAgent)

	// Local Redis
	rdb := redis.NewClient(&redis.Options{
		Addr:     *redisAddr,
		PoolSize: 32,
	})
	defer rdb.Close()
	if err := rdb.Ping(ctx).Err(); err != nil {
		log.Fatalf("failed to connect to local Redis: %v", err)
	}

	// Consul client
	consulCfg := consul.DefaultConfig()
	consulCfg.Address = *consulAddr
	consulClient, err := consul.NewClient(consulCfg)
	if err != nil {
		log.Fatalf("failed to create Consul client: %v", err)
	}

	// Consistent hash ring (built from Consul)
	hashRing := ring.New(ring.DefaultVnodes)

	// Inbox sender + consumer
	sender := inbox.NewSender(hashRing, *nodeID, dd)
	defer sender.Close()
	consumer := inbox.NewConsumer(rdb, dd, dm)

	// Build initial topology from Consul
	updateTopology(consulClient, hashRing, sender, *nodeID)

	// Load seed domains for this node
	loadSeeds(*seedFile, hashRing, *nodeID, dm)

	// WARC writer
	ww, err := warcwriter.NewWriter(*warcDir, *nodeID, userAgent)
	if err != nil {
		log.Fatalf("failed to create WARC writer: %v", err)
	}
	defer ww.Close()

	// --- Worker sizing ---
	cpus := runtime.NumCPU()
	fetchWorkers := max(1, cpus/4)
	parseWorkers := cpus - fetchWorkers
	log.Printf("workers: %d fetch, %d parse (cpus=%d)", fetchWorkers, parseWorkers, cpus)

	// Channel between fetchers and parsers
	fetchResults := make(chan fetch.Result, 10000)

	// Fetch pool
	fetchPool := fetch.NewPool(dm, fetchResults, userAgent, fetchWorkers)

	// Parse pool
	parsePool := parse.NewPool(fetchResults, dd, dm, sender, hashRing, *nodeID, parseWorkers)

	// --- Start goroutines ---

	// Metrics + health server (single mux)
	go serveHTTP(*metricsPort)

	// Consul topology watcher
	go watchTopology(ctx, consulClient, hashRing, sender, *nodeID)

	// Metrics reporter
	go reportMetrics(ctx, dm, rdb)

	// Inbox consumer
	go consumer.Run(ctx)

	// WARC writer goroutine: tee from fetchResults
	// We wrap the channel so both parser and WARC writer see each result
	warcResults := make(chan fetch.Result, 10000)
	go func() {
		for result := range warcResults {
			if err := ww.WriteResult(result); err != nil {
				log.Printf("[warc] write error: %v", err)
			}
		}
	}()

	// Fan-out: fetchers -> (parsers + WARC writer)
	fanoutResults := make(chan fetch.Result, 10000)
	go func() {
		for result := range fanoutResults {
			// Send to both parser and WARC writer
			fetchResults <- result
			warcResults <- result
			pagesFetched.Inc()
		}
		close(fetchResults)
		close(warcResults)
	}()

	// Fetchers write to fanoutResults
	fetchPool = fetch.NewPool(dm, fanoutResults, userAgent, fetchWorkers)

	// Fetchers
	go fetchPool.Run(ctx)

	// Parsers (blocks until fetchResults is closed or ctx cancelled)
	parsePool.Run(ctx)

	log.Println("krowl shutdown complete")
}

// updateTopology rebuilds the hash ring from Consul's service catalog.
func updateTopology(client *consul.Client, hashRing *ring.Ring, sender *inbox.Sender, myID int) {
	services, _, err := client.Health().Service("crawler", "", true, nil)
	if err != nil {
		log.Printf("failed to query Consul: %v", err)
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
	log.Printf("topology updated: %d nodes", len(nodes))
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
			log.Printf("Consul watch error: %v", err)
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
			log.Printf("topology changed: %d nodes", len(nodes))
		}
	}
}

// loadSeeds reads the seed file and enqueues domains owned by this node.
func loadSeeds(path string, hashRing *ring.Ring, myID int, dm *domain.Manager) {
	f, err := os.Open(path)
	if err != nil {
		log.Fatalf("failed to open seeds: %v", err)
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
	log.Printf("loaded %d seed domains for node %d", count, myID)
}

func serveHTTP(port int) {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		fmt.Fprintln(w, "ok")
	})
	log.Printf("HTTP server on :%d (/metrics, /health)", port)
	if err := http.ListenAndServe(fmt.Sprintf(":%d", port), mux); err != nil {
		log.Printf("HTTP server error: %v", err)
	}
}

func reportMetrics(ctx context.Context, dm *domain.Manager, rdb *redis.Client) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			frontierSize.Set(float64(dm.TotalQueueLen()))
			activeDomains.Set(float64(len(dm.ActiveDomains())))
			inboxLen, _ := rdb.LLen(ctx, "inbox").Result()
			inboxSize.Set(float64(inboxLen))
		}
	}
}
