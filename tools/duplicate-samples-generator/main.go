// SPDX-License-Identifier: AGPL-3.0-only

// Command duplicate-samples-generator sends remote-write requests containing
// duplicate-timestamp samples, to exercise and validate Mimir's duplicate-sample
// accounting: cortex_discarded_samples_total with reason "sample_duplicate_timestamp"
// at the distributor, and "same-value-for-timestamp" / "new-value-for-timestamp" at
// the ingester.
//
// Duplicates are placed using one of five shapes:
//
//   - same-object:      two samples with the same timestamp inside one TimeSeries object
//   - same-request:     two TimeSeries objects with identical labels in one request
//   - across-requests:  the same series and timestamp sent in two separate requests
//   - ooo:              a duplicate of an out-of-order sample, in a separate request
//   - ooo-same-request: two objects with the same out-of-order timestamp in one request
//   - ooo-same-object:  two out-of-order samples with one timestamp inside one object
//
// Each shape carries either the same value (an idempotent re-send) or a different
// value (a conflict), selected with -conflict, and can be built from float samples
// or native histograms via -sample-type.
//
// The shapes that keep a duplicate inside one TimeSeries object cover the design doc's four
// distributor flows and are sent to -address; the rest cover its eight ingester flows and,
// when -ingester-address is given, are sent straight to the ingester's Push RPC over gRPC.
// That split matters because a distributor collapses within-request duplicates before they
// can reach the ingester, so its own within-request paths are otherwise unreachable.
//
// Verification is on by default: the tool scrapes the /metrics endpoints given by
// -metrics-url before and after each flow and asserts the observed counters against what
// that flow should produce, exiting non-zero on a mismatch.
package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/grafana/dskit/user"
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/histogram"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	ingesterclient "github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/mimirpb"
)

const (
	reasonDistributorDuplicate = "sample_duplicate_timestamp"
	reasonSameValue            = "same-value-for-timestamp"
	reasonNewValue             = "new-value-for-timestamp"

	// Default of -blocks-storage.tsdb.out-of-order-capacity-max, used only to warn.
	oooChunkCapacityDefault = 32

	discardedMetric  = "cortex_discarded_samples_total"
	attributedMetric = "cortex_discarded_attributed_samples_total"
	ingestedMetric   = "cortex_ingester_ingested_samples_total"
)

// distributorShapes put the duplicate inside a single TimeSeries object, which the
// distributor collapses today; they cover the four distributor flows in the design doc.
// ingesterShapes cover its eight ingester flows.
var (
	distributorShapes = []string{"same-object", "ooo-same-object"}
	ingesterShapes    = []string{"same-request", "ooo-same-request", "across-requests", "ooo"}
	allShapes         = append(append([]string{}, distributorShapes...), ingesterShapes...)
)

// withinObject reports whether a shape keeps the duplicate inside one TimeSeries object.
func withinObject(shape string) bool {
	return shape == "same-object" || shape == "ooo-same-object"
}

// labelSet is a repeatable key=value flag.
type labelSet map[string]string

func (l labelSet) String() string {
	pairs := make([]string, 0, len(l))
	for k, v := range l {
		pairs = append(pairs, k+"="+v)
	}
	sort.Strings(pairs)
	return strings.Join(pairs, ",")
}

func (l labelSet) Set(s string) error {
	for _, kv := range strings.Split(s, ",") {
		if kv == "" {
			continue
		}
		k, v, ok := strings.Cut(kv, "=")
		if !ok {
			return fmt.Errorf("expected key=value, got %q", kv)
		}
		if k == "__name__" {
			return fmt.Errorf("use -metric-name instead of setting __name__")
		}
		l[strings.TrimSpace(k)] = strings.TrimSpace(v)
	}
	return nil
}

// stringList is a comma-separated list flag.
type stringList []string

func (s *stringList) String() string { return strings.Join(*s, ",") }

func (s *stringList) Set(v string) error {
	for _, item := range strings.Split(v, ",") {
		if item = strings.TrimSpace(item); item != "" {
			*s = append(*s, item)
		}
	}
	return nil
}

type config struct {
	address         string
	ingesterAddress string
	pushPath        string
	tenantID        string
	authToken       string

	metricName  string
	extraLabels labelSet
	shape       string
	sampleType  string
	conflict    bool

	metrics          int
	samplesPerMetric int
	seriesPerRequest int
	count            int
	interval         time.Duration
	oooDelay         time.Duration

	runID string

	verify            bool
	metricsURLs       stringList
	verifyDelay       time.Duration
	replicationFactor int
}

func (c *config) registerFlags(f *flag.FlagSet) {
	f.StringVar(&c.address, "address", "http://localhost:8080", "Base URL of the Mimir endpoint to write to.")
	f.StringVar(&c.ingesterAddress, "ingester-address", "", "host:port of an ingester's gRPC endpoint (usually :9095). When set, writes go straight to that ingester's Push RPC, bypassing the distributor, and every duplicate shape is accounted for at the ingester.")
	f.StringVar(&c.pushPath, "push-path", "/api/v1/push", "Remote-write path appended to -address (use /api/prom/push for GEM gateways).")
	f.StringVar(&c.tenantID, "tenant-id", "anonymous", "Tenant ID. Sent as X-Scope-OrgID, or as the basic-auth username when -auth-token is set.")
	f.StringVar(&c.authToken, "auth-token", "", "Basic-auth token. If empty, the request is sent with X-Scope-OrgID only.")

	f.StringVar(&c.metricName, "metric-name", "duplicate_samples_generator", "Metric name prefix for the generated series.")
	f.Var(c.extraLabels, "labels", "Extra labels as key=value[,key=value]. Use a cost-attribution label here to exercise "+attributedMetric+".")
	f.StringVar(&c.shape, "shape", "all", "Where the duplicate appears: "+strings.Join(allShapes, ", ")+", or all for every shape.")
	f.StringVar(&c.sampleType, "sample-type", "both", "Sample type to generate: float, histogram, or both.")
	f.BoolVar(&c.conflict, "conflict", false, "Give the duplicate a different value (a conflict) instead of the same value.")

	f.IntVar(&c.metrics, "metrics", 50, "Number of distinct metrics (and therefore series) to generate.")
	f.IntVar(&c.samplesPerMetric, "samples-per-metric", 2, "Number of distinct timestamps per metric per iteration. Each one gets a duplicate.")
	f.IntVar(&c.seriesPerRequest, "series-per-request", 100, "Maximum number of TimeSeries objects packed into a single request.")
	f.IntVar(&c.count, "count", 5, "Number of iterations (scrapes) to send.")
	f.DurationVar(&c.interval, "interval", 0, "Pause between iterations.")
	f.DurationVar(&c.oooDelay, "ooo-delay", 5*time.Minute, "How far in the past out-of-order samples are. Must be within the tenant's OOO window.")

	f.BoolVar(&c.verify, "verify", true, "Scrape -metrics-url before and after the run and assert the discard counters. Set to false to only generate traffic.")
	f.Var(&c.metricsURLs, "metrics-url", "Comma-separated /metrics endpoints to scrape for verification. Needs the distributors (which record sample_duplicate_timestamp) as well as every ingester that could own the series. Defaults to -address with a /metrics path, which is only right for a local all-in-one Mimir.")
	f.DurationVar(&c.verifyDelay, "verify-delay", 5*time.Second, "How long to wait after the last write before the post-run scrape.")
	f.IntVar(&c.replicationFactor, "replication-factor", 1, "Replication factor, used to divide ingester-side counters summed across replicas. Use 1 when writing straight to a single ingester.")
}

func (c *config) validate() error {
	if c.shape != "all" {
		if !slices.Contains(allShapes, c.shape) {
			return fmt.Errorf("invalid -shape %q, want one of %s or all", c.shape, strings.Join(allShapes, ", "))
		}
	}
	if c.sampleType != "float" && c.sampleType != "histogram" && c.sampleType != "both" {
		return fmt.Errorf("invalid -sample-type %q, want float, histogram or both", c.sampleType)
	}
	if c.metrics < 1 {
		return fmt.Errorf("-metrics must be >= 1")
	}
	if c.samplesPerMetric < 1 {
		return fmt.Errorf("-samples-per-metric must be >= 1")
	}
	if c.count < 1 {
		return fmt.Errorf("-count must be >= 1")
	}
	if c.replicationFactor < 1 {
		return fmt.Errorf("-replication-factor must be >= 1")
	}
	// The within-request shapes put a sample and its duplicate in the same request,
	// so a request has to be able to hold both.
	if c.seriesPerRequest < 2 && !withinObject(c.shape) {
		return fmt.Errorf("-series-per-request must be >= 2 for shape %q, so a duplicate pair fits in one request", c.shape)
	}

	return nil
}

// flow is one (shape, conflict, sample type) combination to run, and the endpoint it is
// sent to.
type flow struct {
	shape      string
	conflict   bool
	sampleType string
	viaGRPC    bool
}

func (f flow) String() string {
	value := "same-value"
	if f.conflict {
		value = "conflict"
	}
	return fmt.Sprintf("%s/%s/%s", f.shape, value, f.sampleType)
}

// docFlow names the row this flow occupies in the design doc's flow tables.
func (f flow) docFlow() string {
	request := "same request"
	if f.shape == "across-requests" || f.shape == "ooo" {
		request = "across requests"
	}
	order := "in order"
	if strings.HasPrefix(f.shape, "ooo") {
		order = "out of order"
	}
	value := "same value"
	if f.conflict {
		value = "different value"
	}
	return fmt.Sprintf("%s / %s / %s", request, order, value)
}

// expectation describes what a flow should do to the discard counters.
type expectation struct {
	reason    string // the reason cortex_discarded_samples_total should record
	component string // "distributor" or "ingester", determines replication handling
}

// expect returns the accounting a flow is expected to produce.
func expect(cfg config, f flow) expectation {
	// A direct gRPC write skips the distributor entirely, so every shape lands on the
	// ingester.
	if f.viaGRPC {
		if f.conflict && (f.shape == "across-requests" || f.shape == "ooo") {
			return expectation{reason: reasonNewValue, component: "ingester"}
		}
		if f.conflict {
			return expectation{reason: reasonNewValue, component: "ingester"}
		}
		return expectation{reason: reasonSameValue, component: "ingester"}
	}

	withinRequest := withinObject(f.shape) || f.shape == "same-request" || f.shape == "ooo-same-request"

	// The distributor is stateless, so it can only collapse duplicates that share a request,
	// and today it only collapses them inside a single TimeSeries object. What it collapses
	// is accounted for there and never reaches the ingester; everything else falls through.
	// Once grafana/mimir#15550 lands the distributor will also collapse cross-object
	// duplicates, and the same-request shapes will need -ingester-address to be reached.
	if withinRequest && withinObject(f.shape) {
		return expectation{reason: reasonDistributorDuplicate, component: "distributor"}
	}

	// Everything else reaches the ingester.
	inOrderConflict := f.conflict && f.shape == "across-requests"
	if inOrderConflict {
		// A conflict against an already-committed sample is caught synchronously by
		// Append, which is the one case Mimir has always accounted for.
		return expectation{reason: reasonNewValue, component: "ingester"}
	}
	if f.conflict {
		return expectation{reason: reasonNewValue, component: "ingester"}
	}
	return expectation{reason: reasonSameValue, component: "ingester"}
}

func main() {
	cfg := config{extraLabels: labelSet{}}
	cfg.registerFlags(flag.CommandLine)
	flag.Parse()
	if err := cfg.validate(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		flag.Usage()
		os.Exit(2)
	}
	if cfg.runID == "" {
		cfg.runID = strconv.FormatInt(time.Now().UnixNano(), 36)
	}

	if cfg.ingesterAddress != "" {
		dialIngester(cfg)
	}

	if cfg.verify && len(cfg.metricsURLs) == 0 {
		cfg.metricsURLs = stringList{strings.TrimSuffix(cfg.address, "/") + "/metrics"}
		fmt.Printf("no -metrics-url given, verifying against %s\n", cfg.metricsURLs[0])
	}

	sampleTypes := []string{cfg.sampleType}
	if cfg.sampleType == "both" {
		sampleTypes = []string{"float", "histogram"}
	}

	shapes := []string{cfg.shape}
	if cfg.shape == "all" {
		shapes = allShapes
	}

	// Ingester flows go straight to the ingester when an address is given: through a
	// distributor its within-request paths are collapsed before they can be reached.
	var flows []flow
	for _, shape := range shapes {
		viaGRPC := cfg.ingesterAddress != "" && !withinObject(shape)
		for _, conflict := range conflictsFor(cfg) {
			for _, sampleType := range sampleTypes {
				flows = append(flows, flow{shape, conflict, sampleType, viaGRPC})
			}
		}
	}

	printPlan(cfg, flows, sampleTypes)

	failed := 0
	for _, f := range flows {
		if !runFlow(cfg, f) {
			failed++
		}
	}

	if cfg.verify {
		fmt.Println()
		if failed > 0 {
			fmt.Printf("FAIL: %d of %s did not match expectations\n", failed, plural(len(flows), "flow"))
			os.Exit(1)
		}
		fmt.Printf("PASS: all %s matched expectations\n", plural(len(flows), "flow"))
	}
}

// conflictsFor returns the value variants to run. A sweep covers both; an explicitly
// requested shape honours whatever -conflict was set to.
func conflictsFor(cfg config) []bool {
	if cfg.shape == "all" {
		return []bool{false, true}
	}
	return []bool{cfg.conflict}
}

// printPlan summarises what the run is about to do and where each flow is sent.
func printPlan(cfg config, flows []flow, sampleTypes []string) {
	grpcFlows := 0
	for _, f := range flows {
		if f.viaGRPC {
			grpcFlows++
		}
	}

	fmt.Printf("%s across %s\n", plural(len(flows), "flow"), strings.Join(sampleTypes, " and "))
	if grpcFlows > 0 {
		fmt.Printf("  %2d to the distributor at %s\n", len(flows)-grpcFlows, cfg.address)
		fmt.Printf("  %2d to the ingester at %s over gRPC\n\n", grpcFlows, cfg.ingesterAddress)
		return
	}

	distributorFlows := 0
	for _, f := range flows {
		if expect(cfg, f).component == "distributor" {
			distributorFlows++
		}
	}
	fmt.Printf("  all to %s: %d accounted at the distributor, %d at the ingester\n\n",
		cfg.address, distributorFlows, len(flows)-distributorFlows)
}

func runFlow(cfg config, f flow) bool {
	cfg.sampleType = f.sampleType
	exp := expect(cfg, f)

	var before snapshot
	if cfg.verify {
		var err error
		if before, err = scrape(cfg); err != nil {
			fmt.Fprintf(os.Stderr, "scrape before: %v\n", err)
			os.Exit(1)
		}
	}

	// A single monotonic base for the whole flow keeps every generated timestamp
	// distinct, so the only duplicates are the ones we deliberately create.
	base := time.Now().UnixMilli() - int64(cfg.count*cfg.samplesPerMetric)
	// Out-of-order duplicate detection is scoped by design to the current out-of-order head
	// chunk, so a duplicate whose original landed in an already-cut chunk is not recognised.
	// Keep the per-series sample count inside -blocks-storage.tsdb.out-of-order-capacity-max
	// so assertions cover what the TSDB guarantees rather than incidental chunk geometry.
	if perSeries := cfg.count * cfg.samplesPerMetric; perSeries > oooChunkCapacityDefault && strings.HasPrefix(f.shape, "ooo") {
		fmt.Fprintf(os.Stderr, "warning: %d samples per series exceeds the default out-of-order chunk capacity of %d, so %s will report one duplicate fewer per chunk boundary, which is expected rather than a fault; lower -count x -samples-per-metric, raise -metrics instead, or raise -blocks-storage.tsdb.out-of-order-capacity-max\n",
			perSeries, oooChunkCapacityDefault, f.shape)
	}
	if span := time.Duration(cfg.count*cfg.samplesPerMetric) * time.Millisecond; span > time.Hour {
		fmt.Fprintf(os.Stderr, "warning: -count x -samples-per-metric spans %s of wall clock; samples may be rejected as too old\n", span)
	}

	codes := map[int]int{}
	requests, samplesSent := 0, 0
	start := time.Now()
	for i := 0; i < cfg.count; i++ {
		if i > 0 && cfg.interval > 0 {
			time.Sleep(cfg.interval)
		}
		for _, req := range buildIteration(cfg, f, base, i) {
			code := push(cfg, f, req)
			codes[code]++
			requests++
			for _, ts := range req {
				samplesSent += len(ts.Samples) + len(ts.Histograms)
			}
		}
	}

	duplicates := cfg.count * cfg.metrics * cfg.samplesPerMetric
	fmt.Printf("%-40s %s\n", f.String(), f.docFlow())
	fmt.Printf("    %-28s %s / %s (%s) in %s, codes: %v\n", "sent",
		plural(requests, "request"), plural(samplesSent, "sample"), plural(duplicates, "duplicate"),
		time.Since(start).Round(time.Millisecond), formatCodes(codes))

	if !cfg.verify {
		return true
	}

	time.Sleep(cfg.verifyDelay)
	after, err := scrape(cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "scrape after: %v\n", err)
		os.Exit(1)
	}
	return report(cfg, f, exp, before, after, duplicates, samplesSent)
}

// buildIteration returns the requests for one iteration of a flow.
//
// Each distinct timestamp is sent as its own round of requests, the way a scrape writes
// every series at one instant. That ordering matters: batching several timestamps for one
// series into a single request and then replaying it would leave the series max time at the
// highest of them, so replaying the lower ones would be out-of-order rather than the
// in-order duplicates the across-requests shape is meant to produce.
func buildIteration(cfg config, f flow, base int64, iteration int) [][]mimirpb.PreallocTimeseries {
	var requests [][]mimirpb.PreallocTimeseries

	for k := 0; k < cfg.samplesPerMetric; k++ {
		ts := base + int64(iteration*cfg.samplesPerMetric+k)
		oooTS := ts - cfg.oooDelay.Milliseconds()

		var (
			primary   []mimirpb.PreallocTimeseries // originals, or original+duplicate when they share a request
			secondary []mimirpb.PreallocTimeseries // duplicates that go in their own request
			advance   []mimirpb.PreallocTimeseries // in-order samples that move maxt so later writes are out of order
			groupSize = 1
		)

		for m := 0; m < cfg.metrics; m++ {
			switch f.shape {
			case "same-object":
				primary = append(primary, series(cfg, f, m, value(cfg, false, ts), value(cfg, f.conflict, ts)))

			case "ooo-same-object":
				advance = append(advance, series(cfg, f, m, value(cfg, false, ts)))
				primary = append(primary, series(cfg, f, m, value(cfg, false, oooTS), value(cfg, f.conflict, oooTS)))

			case "same-request":
				primary = append(primary,
					series(cfg, f, m, value(cfg, false, ts)),
					series(cfg, f, m, value(cfg, f.conflict, ts)))
				groupSize = 2

			case "across-requests":
				primary = append(primary, series(cfg, f, m, value(cfg, false, ts)))
				secondary = append(secondary, series(cfg, f, m, value(cfg, f.conflict, ts)))

			case "ooo":
				advance = append(advance, series(cfg, f, m, value(cfg, false, ts)))
				primary = append(primary, series(cfg, f, m, value(cfg, false, oooTS)))
				secondary = append(secondary, series(cfg, f, m, value(cfg, f.conflict, oooTS)))

			case "ooo-same-request":
				advance = append(advance, series(cfg, f, m, value(cfg, false, ts)))
				primary = append(primary,
					series(cfg, f, m, value(cfg, false, oooTS)),
					series(cfg, f, m, value(cfg, f.conflict, oooTS)))
				groupSize = 2
			}
		}

		requests = append(requests, pack(advance, cfg.seriesPerRequest, 1)...)
		requests = append(requests, pack(primary, cfg.seriesPerRequest, groupSize)...)
		requests = append(requests, pack(secondary, cfg.seriesPerRequest, 1)...)
	}

	return requests
}

// pack splits objs into requests of at most maxPerRequest objects, never splitting a
// run of groupSize consecutive objects across two requests.
func pack(objs []mimirpb.PreallocTimeseries, maxPerRequest, groupSize int) [][]mimirpb.PreallocTimeseries {
	if len(objs) == 0 {
		return nil
	}
	perRequest := maxPerRequest - maxPerRequest%groupSize
	if perRequest < groupSize {
		perRequest = groupSize
	}
	var out [][]mimirpb.PreallocTimeseries
	for start := 0; start < len(objs); start += perRequest {
		end := start + perRequest
		if end > len(objs) {
			end = len(objs)
		}
		out = append(out, objs[start:end])
	}
	return out
}

// sampleValue is either a float sample or a native histogram, at a given timestamp.
type sampleValue struct {
	sample    *mimirpb.Sample
	histogram *mimirpb.Histogram
}

// testHistograms[0] is the baseline histogram and testHistograms[1] the conflicting
// one. GenerateBigTestHistograms keeps Count consistent with the bucket deltas, so
// both pass native-histogram validation.
var testHistograms = histogram.GenerateBigTestHistograms(2, 20)

func value(cfg config, conflict bool, ts int64) sampleValue {
	if cfg.sampleType == "histogram" {
		idx := 0
		if conflict {
			idx = 1
		}
		h := mimirpb.FromHistogramToHistogramProto(ts, testHistograms[idx])
		return sampleValue{histogram: &h}
	}
	v := 1.0
	if conflict {
		v = 2.0
	}
	return sampleValue{sample: &mimirpb.Sample{TimestampMs: ts, Value: v}}
}

func series(cfg config, f flow, metric int, values ...sampleValue) mimirpb.PreallocTimeseries {
	name := cfg.metricName
	if cfg.metrics > 1 {
		name = fmt.Sprintf("%s_%d", cfg.metricName, metric)
	}
	labels := []mimirpb.LabelAdapter{{Name: "__name__", Value: name}}
	for k, v := range cfg.extraLabels {
		labels = append(labels, mimirpb.LabelAdapter{Name: k, Value: v})
	}
	labels = append(labels,
		mimirpb.LabelAdapter{Name: "job", Value: "duplicate-samples-generator"},
		mimirpb.LabelAdapter{Name: "run", Value: cfg.runID},
		// Distinct per flow: sharing series across flows would let one flow's head chunks
		// decide whether another flow's duplicates are detected.
		mimirpb.LabelAdapter{Name: "flow", Value: f.String()},
	)
	sort.Slice(labels, func(i, j int) bool { return labels[i].Name < labels[j].Name })

	ts := &mimirpb.TimeSeries{Labels: labels}
	for _, v := range values {
		if v.sample != nil {
			ts.Samples = append(ts.Samples, *v.sample)
		}
		if v.histogram != nil {
			ts.Histograms = append(ts.Histograms, *v.histogram)
		}
	}
	return mimirpb.PreallocTimeseries{TimeSeries: ts}
}

// ingesterConn is the lazily dialled gRPC connection used by -ingester-address.
var ingesterConn struct {
	client ingesterclient.IngesterClient
	ctx    context.Context
}

func dialIngester(cfg config) {
	conn, err := grpc.NewClient(cfg.ingesterAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fmt.Fprintf(os.Stderr, "dial %s: %v\n", cfg.ingesterAddress, err)
		os.Exit(1)
	}
	ctx, err := user.InjectIntoGRPCRequest(user.InjectOrgID(context.Background(), cfg.tenantID))
	if err != nil {
		fmt.Fprintf(os.Stderr, "inject tenant: %v\n", err)
		os.Exit(1)
	}
	ingesterConn.client = ingesterclient.NewIngesterClient(conn)
	ingesterConn.ctx = ctx
}

// push sends one request and returns an HTTP-equivalent status code, so gRPC and
// remote-write runs report the same way.
func push(cfg config, f flow, ts []mimirpb.PreallocTimeseries) int {
	if f.viaGRPC {
		return pushGRPC(cfg, ts)
	}
	return pushHTTP(cfg, ts)
}

func pushGRPC(cfg config, ts []mimirpb.PreallocTimeseries) int {
	_, err := ingesterConn.client.Push(ingesterConn.ctx, &mimirpb.WriteRequest{Timeseries: ts})
	if err == nil {
		return http.StatusOK
	}
	code := statusCodeFor(status.Code(err))
	if code/100 == 5 {
		// A duplicate is a client-side problem; anything else is worth surfacing.
		fmt.Fprintf(os.Stderr, "push: %v\n", err)
	}
	return code
}

// statusCodeFor maps the gRPC codes the ingester's Push returns onto the HTTP codes the
// same conditions produce through the distributor.
func statusCodeFor(c codes.Code) int {
	switch c {
	case codes.OK:
		return http.StatusOK
	case codes.InvalidArgument, codes.FailedPrecondition, codes.OutOfRange:
		return http.StatusBadRequest
	case codes.ResourceExhausted:
		return http.StatusTooManyRequests
	case codes.PermissionDenied, codes.Unauthenticated:
		return http.StatusUnauthorized
	default:
		return http.StatusInternalServerError
	}
}

func pushHTTP(cfg config, ts []mimirpb.PreallocTimeseries) int {
	raw, err := proto.Marshal(&mimirpb.WriteRequest{Timeseries: ts})
	if err != nil {
		fmt.Fprintf(os.Stderr, "marshal: %v\n", err)
		os.Exit(1)
	}

	httpReq, err := http.NewRequest(http.MethodPost, strings.TrimSuffix(cfg.address, "/")+cfg.pushPath, bytes.NewReader(snappy.Encode(nil, raw)))
	if err != nil {
		fmt.Fprintf(os.Stderr, "request: %v\n", err)
		os.Exit(1)
	}
	httpReq.Header.Set("Content-Type", "application/x-protobuf")
	httpReq.Header.Set("Content-Encoding", "snappy")
	httpReq.Header.Set("X-Prometheus-Remote-Write-Version", "0.1.0")
	if cfg.authToken != "" {
		httpReq.SetBasicAuth(cfg.tenantID, cfg.authToken)
	} else {
		httpReq.Header.Set("X-Scope-OrgID", cfg.tenantID)
	}

	// Deliberately not retried: the server may have committed a request before the
	// transport failed, so a retry would create a duplicate we never meant to send.
	resp, err := http.DefaultClient.Do(httpReq)
	if err != nil {
		fmt.Fprintf(os.Stderr, "push failed: %v\n", err)
		os.Exit(1)
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		fmt.Fprintf(os.Stderr, "HTTP %d: %s\n", resp.StatusCode, strings.TrimSpace(string(msg)))
	}
	return resp.StatusCode
}

func plural(n int, noun string) string {
	if n == 1 {
		return fmt.Sprintf("%d %s", n, noun)
	}
	return fmt.Sprintf("%d %ss", n, noun)
}

func formatCodes(codes map[int]int) string {
	keys := make([]int, 0, len(codes))
	for c := range codes {
		keys = append(keys, c)
	}
	sort.Ints(keys)
	parts := make([]string, 0, len(keys))
	for _, c := range keys {
		parts = append(parts, fmt.Sprintf("%d x%d", c, codes[c]))
	}
	return strings.Join(parts, ", ")
}

// snapshot is the counter state at one instant, keyed by "metric/reason" (or just the
// metric name where there is no reason label). present distinguishes a counter that is
// genuinely zero from one the target does not expose at all.
type snapshot struct {
	totals  map[string]float64
	present map[string]bool
}

func (s snapshot) delta(other snapshot, key string) float64 {
	return s.totals[key] - other.totals[key]
}

// scrape sums the counters this tool cares about across every configured endpoint.
func scrape(cfg config) (snapshot, error) {
	snap := snapshot{totals: map[string]float64{}, present: map[string]bool{}}
	for _, url := range cfg.metricsURLs {
		resp, err := http.Get(url)
		if err != nil {
			return snapshot{}, fmt.Errorf("%s: %w", url, err)
		}
		// Parse permissively: this only ever reads back Mimir's own /metrics output.
		parser := expfmt.NewTextParser(model.UTF8Validation)
		families, err := parser.TextToMetricFamilies(resp.Body)
		resp.Body.Close()
		if err != nil {
			return snapshot{}, fmt.Errorf("%s: %w", url, err)
		}
		for _, name := range []string{discardedMetric, attributedMetric, ingestedMetric} {
			family, ok := families[name]
			if !ok {
				continue
			}
			for _, m := range family.GetMetric() {
				labels := labelsOf(m)
				if user, ok := labels["user"]; ok && user != cfg.tenantID {
					continue
				}
				key := name
				if reason, ok := labels["reason"]; ok {
					key = name + "/" + reason
				}
				snap.totals[key] += counterValue(m)
				snap.present[name] = true
			}
		}
	}
	return snap, nil
}

func labelsOf(m *dto.Metric) map[string]string {
	out := make(map[string]string, len(m.GetLabel()))
	for _, l := range m.GetLabel() {
		out[l.GetName()] = l.GetValue()
	}
	return out
}

func counterValue(m *dto.Metric) float64 {
	if c := m.GetCounter(); c != nil {
		return c.GetValue()
	}
	if u := m.GetUntyped(); u != nil {
		return u.GetValue()
	}
	return 0
}

// report prints observed against expected counters and returns whether they matched.
//
// Three things are asserted per flow: the reason the flow should have recorded gains
// exactly one count per duplicate, the other two reasons do not move, and the ingested
// counter excludes the duplicates.
func report(cfg config, f flow, exp expectation, before, after snapshot, duplicates, samplesSent int) bool {
	// Ingester counters are recorded once per replica, so summing every replica multiplies
	// them by the replication factor. A direct gRPC write reaches exactly one ingester, so
	// there is nothing to divide out. Distributor counters are recorded once, by whichever
	// distributor handled the request.
	ingesterDivisor := 1
	if !f.viaGRPC {
		ingesterDivisor = cfg.replicationFactor
	}
	// The reason's divisor depends on which component recorded it; the ingested counter is
	// always an ingester metric, whatever collapsed the duplicate.
	divisor := 1
	if exp.component == "ingester" {
		divisor = ingesterDivisor
	}

	ok := true
	for _, reason := range []string{reasonDistributorDuplicate, reasonSameValue, reasonNewValue} {
		key := discardedMetric + "/" + reason
		observed := after.delta(before, key)
		want := 0.0
		if reason == exp.reason {
			want = float64(duplicates * divisor)
		}
		if observed != want {
			ok = false
		}
		if observed == 0 && want == 0 {
			continue
		}
		fmt.Printf("    %-28s want %8.0f  observed %8.0f  %s\n", reason, want, observed, verdict(observed == want))
	}

	// The other half of the bug: a duplicate that never reached the head must not be
	// counted as ingested.
	if after.present[ingestedMetric] || before.present[ingestedMetric] {
		want := float64((samplesSent - duplicates) * ingesterDivisor)
		observed := after.delta(before, ingestedMetric)
		if observed != want {
			ok = false
		}
		fmt.Printf("    %-28s want %8.0f  observed %8.0f  %s\n", "ingested samples", want, observed, verdict(observed == want))
	} else {
		fmt.Printf("    %-28s not exposed by the scraped target, skipped\n", "ingested samples")
	}

	// Reported, not asserted: cost attribution is a per-series label the caller opts
	// into, so the expected total depends on how -labels was set.
	if len(cfg.extraLabels) > 0 && (after.present[attributedMetric] || before.present[attributedMetric]) {
		fmt.Printf("    %-28s %+.0f (reported only)\n", "attributed discards", after.delta(before, attributedMetric))
	}
	return ok
}

func verdict(ok bool) string {
	if ok {
		return "ok"
	}
	return "MISMATCH"
}
