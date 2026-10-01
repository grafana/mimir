// SPDX-License-Identifier: AGPL-3.0-only

// parity-probe checks that ingesters owning the same partition answer alike: a reference (the Go
// TSDB ingester) and any number of compared ones (the Rust ingester, the Go ingester with another
// storage engine). It generates selectors from the tenant's own label names and values, with their
// query shard variants, and compares QueryStream samples, label names and values, series,
// cardinality and user stats.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/grafana/dskit/clusterutil"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"

	"github.com/grafana/mimir/pkg/ingester/client"
)

type stringList []string

func (s *stringList) String() string     { return strings.Join(*s, ",") }
func (s *stringList) Set(v string) error { *s = append(*s, v); return nil }

// An ingester under test, named for the report.
type target struct {
	name string
	api  client.IngesterClient
}

func parseTarget(value string) (string, string, error) {
	name, addr, ok := strings.Cut(value, "=")
	if !ok || name == "" || addr == "" {
		return "", "", fmt.Errorf("%q is not name=address", value)
	}
	return name, addr, nil
}

var clusterLabel string

func outgoing(tenant string) context.Context {
	ctx := metadata.AppendToOutgoingContext(context.Background(), "x-scope-orgid", tenant)
	if clusterLabel != "" {
		ctx = clusterutil.PutClusterIntoOutgoingContext(ctx, clusterLabel)
	}
	return ctx
}

type config struct {
	start, end        time.Time
	longStart         time.Time
	shards            []string
	unionShards       int
	verbose           bool
	generation        generationConfig
	explicitSelectors []string
}

func main() {
	var reference string
	var compared, tenants, selectors, shards stringList
	cfg := config{}
	flag.StringVar(&reference, "reference", "", "reference ingester as name=address, such as zone-a=ingester-zone-a-5.ingester-zone-a:9095")
	flag.Var(&compared, "compare", "ingester compared with the reference as name=address, owning the same partition (repeatable)")
	flag.Var(&tenants, "tenant", "tenant to compare (repeatable); defaults to the reference's largest tenants")
	flag.Var(&selectors, "selector", "series selector compared in addition to the generated ones (repeatable)")
	flag.Var(&shards, "shard", "query shard every selector is also compared with, such as 1_of_16 (repeatable; default 1_of_16 and 7_of_16)")
	topTenants := flag.Int("top-tenants", 3, "tenants to discover when -tenant is not set")
	window := flag.Duration("window", time.Minute, "QueryStream window length")
	longRange := flag.Duration("long-range", 12*time.Hour, "range of the label and series lookups compared beyond the window, within the ingesters' retention")
	settle := flag.Duration("settle", 10*time.Minute, "gap between the window end and now so every ingester has consumed it")
	flag.IntVar(&cfg.unionShards, "union-shards", 4, "shard count whose shards together must return each selector's unsharded result (0 to skip)")
	flag.Uint64Var(&cfg.generation.seed, "seed", 1, "seed of the selector generation, so runs are reproducible")
	flag.IntVar(&cfg.generation.names, "names", 8, "metric names selectors are generated for")
	flag.IntVar(&cfg.generation.maxSeries, "max-series", 2000, "most series of a metric or label value a generated selector picks, so a run stays small")
	flag.IntVar(&cfg.generation.maxSelectors, "max-selectors", 80, "most generated selectors per tenant")
	flag.BoolVar(&cfg.verbose, "verbose", false, "print every check, not only mismatches")
	flag.StringVar(&clusterLabel, "cluster-label", "", "cluster validation label expected by the ingesters, such as the namespace")
	flag.Parse()
	if reference == "" || len(compared) == 0 {
		fmt.Fprintln(os.Stderr, "-reference and at least one -compare are required")
		os.Exit(2)
	}
	if len(shards) == 0 {
		shards = stringList{"1_of_16", "7_of_16"}
	}
	cfg.shards = shards
	cfg.explicitSelectors = selectors

	ref := mustTarget(reference)
	var targets []target
	for _, value := range compared {
		targets = append(targets, mustTarget(value))
	}
	if len(tenants) == 0 {
		tenants = discoverTenants(ref.api, *topTenants)
	}

	cfg.end = time.Now().Add(-*settle).Truncate(time.Second)
	cfg.start = cfg.end.Add(-*window)
	cfg.longStart = cfg.end.Add(-*longRange)
	fmt.Printf("window=%s..%s long_range_start=%s tenants=%v reference=%s compared=%v seed=%d\n",
		cfg.start.UTC().Format(time.RFC3339), cfg.end.UTC().Format(time.RFC3339), cfg.longStart.UTC().Format(time.RFC3339),
		tenants, ref.name, names(targets), cfg.generation.seed)

	report := newReport()
	for _, tenant := range tenants {
		probeTenant(cfg, tenant, ref, targets, report)
	}
	report.print(names(targets))
	if report.failed() {
		os.Exit(1)
	}
}

func mustTarget(value string) target {
	name, addr, err := parseTarget(value)
	check(err)
	return target{name: name, api: dial(addr)}
}

func names(targets []target) []string {
	out := make([]string, len(targets))
	for i, t := range targets {
		out[i] = t.name
	}
	return out
}

func dial(addr string) client.IngesterClient {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(256<<20)))
	check(err)
	return client.NewIngesterClient(conn)
}

func discoverTenants(api client.IngesterClient, n int) []string {
	// The ingester auth interceptor rejects calls without an org ID, even though AllUserStats spans all tenants.
	ctx, cancel := context.WithTimeout(outgoing("parity-probe"), time.Minute)
	defer cancel()
	stats, err := api.AllUserStats(ctx, &client.UserStatsRequest{})
	check(err)
	sort.Slice(stats.Stats, func(i, j int) bool { return stats.Stats[i].Data.NumSeries > stats.Stats[j].Data.NumSeries })
	var tenants []string
	for _, s := range stats.Stats {
		if len(tenants) == n {
			break
		}
		tenants = append(tenants, s.UserId)
	}
	return tenants
}

func check(err error) {
	if err != nil {
		fail("%v", err)
	}
}

func fail(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}
