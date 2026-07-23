// SPDX-License-Identifier: AGPL-3.0-only

// Command dupts inspects a pcap capture of write traffic into the Mimir
// distributor and reports which series carry samples that would be discarded
// with the "sample_duplicate_timestamp" reason.
//
// It reproduces exactly what pkg/distributor.validateSamples /
// validateHistograms do: within a single timeseries entry of a single
// WriteRequest, when two or more samples (or histograms) share the same
// timestamp, the first is kept and every later duplicate is dropped and counted
// against cortex_discarded_samples_total{reason="sample_duplicate_timestamp"}.
//
// The distributor receives writes on port 9095 via two gRPC paths, both handled
// here:
//
//   - /distributor.Distributor/Push - native gRPC; the message IS a
//     mimirpb.WriteRequest. The tenant is the x-scope-orgid gRPC metadata header.
//   - /httpgrpc.HTTP/Handle - dskit httpgrpc; the message is an
//     httpgrpc.HTTPRequest wrapping an HTTP push whose body is either a
//     snappy-compressed remote-write WriteRequest (/api/v1/push) or an OTLP
//     payload (/otlp/v1/metrics). The tenant is the wrapped X-Scope-OrgID header.
//
// Pipeline (two tshark passes so we can key decoding on the HTTP/2 :path):
//
//	pass A: per stream -> :path + gRPC metadata header values (for the tenant)
//	pass B: per stream -> reassembled gRPC message bytes -> decode per :path
//	                   -> per-timeseries duplicate-timestamp detection
//
// Usage:
//
//	go run ./tools/dupts -pcap /path/to/dump.pcap -tenant 1316763
package main

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/hex"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/golang/snappy"
	"github.com/grafana/dskit/httpgrpc"
	"github.com/klauspost/compress/zstd"
	"github.com/pierrec/lz4/v4"
	"github.com/prometheus/prometheus/config"
	"github.com/prometheus/prometheus/storage/remote/otlptranslator/prometheusremotewrite"
	"go.opentelemetry.io/collector/pdata/pmetric/pmetricotlp"

	"github.com/grafana/mimir/pkg/distributor/otlpappender"
	"github.com/grafana/mimir/pkg/mimirpb"
)

func main() {
	var (
		pcap       = flag.String("pcap", "", "Path to the pcap/pcapng file to analyze.")
		tsharkPath = flag.String("tshark", "/Applications/Wireshark.app/Contents/MacOS/tshark", "Path to the tshark binary.")
		port       = flag.Int("port", 9095, "Destination TCP port of the distributor gRPC server (request side).")
		hexFile    = flag.String("hexfile", "", "Optional: decode a single gRPC message from a file of hex (debug). No stream/tenant context.")
		showAll    = flag.Bool("all", false, "Print every offending series occurrence, not just the aggregated report.")
		maxTS      = flag.Int("max-timestamps", 5, "Max number of example duplicated timestamps to print per series.")
		tenant     = flag.String("tenant", "", "If set, only consider requests whose tenant (X-Scope-OrgID) matches this ID.")
		doPush     = flag.Bool("push", true, "Analyze Prometheus remote-write requests (native /distributor.Distributor/Push and httpgrpc /api/v1/push).")
		doOTLP     = flag.Bool("otlp", true, "Analyze OTLP requests (/otlp/v1/metrics), converting them with Mimir's OTLP->Prometheus converter.")
		otlpSuffix = flag.Bool("otlp-add-suffixes", true, "OTLP: add metric name suffixes (e.g. _bytes, _total), matching Mimir's default translation strategy.")
		otlpNHCB   = flag.Bool("otlp-nhcb", false, "OTLP: convert classic histograms to native histograms with custom buckets (NHCB).")
		otlpScope  = flag.Bool("otlp-promote-scope", false, "OTLP: promote OTel scope metadata to labels.")
		urls       = flag.Bool("urls", false, "Print a histogram of the request paths/URLs seen (debug).")
		doRecover  = flag.Bool("recover-unknown", false, "Best-effort decode of messages on streams whose HEADERS frame was not captured (mid-connection). Tenant is unknown for these, so they are skipped when -tenant is set.")
		dump       = flag.Bool("dump", false, "Print every decoded timeseries (labels + samples), not only the ones with duplicates. Intended for inspecting a single message via -hexfile.")
	)
	flag.Parse()

	if *pcap == "" && *hexFile == "" {
		fmt.Fprintln(os.Stderr, "error: one of -pcap or -hexfile is required")
		flag.Usage()
		os.Exit(2)
	}

	a := &analyzer{
		maxExampleTS: *maxTS,
		showAll:      *showAll,
		tenantFilter: *tenant,
		analyzePush:  *doPush,
		analyzeOTLP:  *doOTLP,
		otlpSettings: prometheusremotewrite.Settings{
			AddMetricSuffixes:         *otlpSuffix,
			ConvertHistogramsToNHCB:   *otlpNHCB,
			PromoteScopeMetadata:      *otlpScope,
			PromoteResourceAttributes: prometheusremotewrite.NewPromoteResourceAttributes(config.OTLPConfig{}),
		},
		recoverUnknownStreams: *doRecover,
		dumpSeries:            *dump,
		streams:               map[string]streamInfo{},
		stats:                 map[dupKey]*dupStat{},
	}
	if *urls {
		a.urlHist = map[string]int{}
	}

	if *hexFile != "" {
		if err := a.runHexFile(*hexFile); err != nil {
			fatalf("%v", err)
		}
		a.report(os.Stdout)
		return
	}

	// Pass A: learn each stream's :path and gRPC metadata (tenant).
	if err := a.loadStreams(*tsharkPath, *pcap, *port); err != nil {
		fatalf("load streams: %v", err)
	}
	// Pass B: decode the gRPC messages.
	if err := a.scanMessages(*tsharkPath, *pcap, *port); err != nil {
		fatalf("scan messages: %v", err)
	}
	a.report(os.Stdout)
}

// streamInfo captures what pass A learns about one HTTP/2 stream (one RPC).
type streamInfo struct {
	path       string // gRPC :path, e.g. /distributor.Distributor/Push
	tenant     string // best-effort tenant from gRPC metadata header values
	headerVals string // pipe-joined header values, for exact tenant matching
}

// loadStreams runs tshark once over the HEADERS frames and records, per
// (tcp.stream, http2.streamid), the request :path and the gRPC metadata header
// values. Header names for custom metadata (x-scope-orgid) often show up as
// <unknown> when the capture starts mid-connection and misses HPACK table
// initialization, so we keep all values and identify the tenant heuristically.
func (a *analyzer) loadStreams(bin, pcap string, port int) error {
	filter := fmt.Sprintf("tcp.dstport==%d && http2.headers.path", port)
	cmd := exec.Command(bin,
		"-r", pcap,
		"-Y", filter,
		"-T", "fields",
		"-e", "tcp.stream",
		"-e", "http2.streamid",
		"-e", "http2.headers.path",
		"-e", "http2.header.value",
		"-E", "separator=/t",
		"-E", "aggregator=|", // keep header values that contain commas intact
	)
	cmd.Stderr = os.Stderr
	out, err := cmd.StdoutPipe()
	if err != nil {
		return err
	}
	if err := cmd.Start(); err != nil {
		return err
	}
	sc := bufio.NewScanner(out)
	sc.Buffer(make([]byte, 1024*1024), 64*1024*1024)
	for sc.Scan() {
		fields := strings.Split(sc.Text(), "\t")
		if len(fields) < 3 {
			continue
		}
		key := streamKey(fields[0], fields[1])
		info := streamInfo{path: fields[2]}
		if len(fields) >= 4 {
			info.headerVals = fields[3]
			info.tenant = extractTenant(fields[3])
		}
		a.streams[key] = info
	}
	if err := sc.Err(); err != nil {
		return err
	}
	// tshark exits non-zero on truncated captures after emitting all readable
	// packets; that is expected for partial dumps, so only warn.
	if err := cmd.Wait(); err != nil {
		fmt.Fprintf(os.Stderr, "warning: tshark (pass A) exited with: %v\n", err)
	}
	return nil
}

// scanMessages runs tshark over the reassembled gRPC messages and decodes each
// according to its stream's :path.
func (a *analyzer) scanMessages(bin, pcap string, port int) error {
	filter := fmt.Sprintf("tcp.dstport==%d && grpc.message_data", port)
	cmd := exec.Command(bin,
		"-r", pcap,
		"-Y", filter,
		"-T", "fields",
		"-e", "frame.number",
		"-e", "frame.time_epoch",
		"-e", "tcp.stream",
		"-e", "http2.streamid",
		"-e", "grpc.message_data",
		"-E", "separator=/t",
	)
	cmd.Stderr = os.Stderr
	out, err := cmd.StdoutPipe()
	if err != nil {
		return err
	}
	if err := cmd.Start(); err != nil {
		return err
	}
	sc := bufio.NewScanner(out)
	// gRPC messages can be large (push/OTLP bodies up to the recv limit);
	// hex-encoded lines are ~2x that.
	sc.Buffer(make([]byte, 1024*1024), 256*1024*1024)
	for sc.Scan() {
		fields := strings.Split(sc.Text(), "\t")
		if len(fields) < 5 {
			continue
		}
		a.framesSeen++
		frame := fields[0]
		a.curFrameTime, _ = strconv.ParseFloat(strings.TrimSpace(fields[1]), 64)
		key := streamKey(fields[2], fields[3])
		// A frame may carry several completed gRPC messages (comma-joined).
		for _, h := range strings.Split(fields[4], ",") {
			if h = strings.TrimSpace(h); h != "" {
				a.processMessage(frame, key, h)
			}
		}
	}
	if err := sc.Err(); err != nil {
		return err
	}
	if err := cmd.Wait(); err != nil {
		fmt.Fprintf(os.Stderr, "warning: tshark (pass B) exited with: %v\n", err)
	}
	return nil
}

func (a *analyzer) processMessage(frame, key, h string) {
	raw, err := hex.DecodeString(h)
	if err != nil {
		a.decodeErrors++
		return
	}
	info := a.streams[key]
	if a.urlHist != nil {
		a.urlHist[info.path]++
	}
	switch {
	case strings.HasSuffix(info.path, "/httpgrpc.HTTP/Handle"):
		a.handleHTTPGRPC(frame, raw)
	case strings.HasSuffix(info.path, "Distributor/Push"):
		a.handleGRPCPush(frame, info.tenant, raw)
	case info.path == "" && a.recoverUnknownStreams:
		a.recoverUnknown(frame, raw)
	default:
		// usagetracker and other gRPC services, or unclassified streams: not
		// treated as ingestion.
		a.otherRequests++
	}
}

// recoverUnknown best-effort decodes a message on a stream whose HEADERS frame
// was not captured, so we don't know its :path. It tries an httpgrpc.HTTPRequest
// with a known ingestion URL first, then a bare mimirpb.WriteRequest. Tenant is
// unknown for these messages.
func (a *analyzer) recoverUnknown(frame string, raw []byte) {
	var req httpgrpc.HTTPRequest
	if err := req.Unmarshal(raw); err == nil && req.GetMethod() != "" {
		url := req.GetUrl()
		if i := strings.IndexByte(url, '?'); i >= 0 {
			url = url[:i]
		}
		if url == "/api/v1/push" || url == "/otlp/v1/metrics" {
			a.handleHTTPGRPC(frame, raw)
			return
		}
	}
	var wr mimirpb.WriteRequest
	if err := wr.Unmarshal(raw); err != nil || len(wr.Timeseries) == 0 {
		a.otherRequests++
		return
	}
	if a.tenantFilter != "" {
		// No tenant context for recovered messages.
		a.otherTenant++
		return
	}
	a.pushRequests++
	for _, ts := range wr.Timeseries {
		a.totalSeries++
		a.emitSeries(frame, "", "recovered", ts)
	}
}

// handleGRPCPush decodes a native /distributor.Distributor/Push message, which
// is a mimirpb.WriteRequest directly (no httpgrpc wrapper, no snappy; gRPC-level
// compression is identity in this capture).
func (a *analyzer) handleGRPCPush(frame, tenant string, raw []byte) {
	if a.tenantFilter != "" && tenant != a.tenantFilter {
		a.otherTenant++
		return
	}
	if !a.analyzePush {
		a.otherRequests++
		return
	}
	var wr mimirpb.WriteRequest
	if err := wr.Unmarshal(raw); err != nil {
		a.decodeErrors++
		return
	}
	a.pushRequests++
	for _, ts := range wr.Timeseries {
		a.totalSeries++
		a.emitSeries(frame, tenant, "grpc-push", ts)
	}
}

// handleHTTPGRPC decodes an httpgrpc.HTTPRequest and dispatches on the wrapped
// HTTP URL.
func (a *analyzer) handleHTTPGRPC(frame string, raw []byte) {
	var req httpgrpc.HTTPRequest
	if err := req.Unmarshal(raw); err != nil || req.GetMethod() == "" {
		a.decodeErrors++
		return
	}
	url := req.GetUrl()
	if i := strings.IndexByte(url, '?'); i >= 0 {
		url = url[:i]
	}
	tenant := headerValue(req.Headers, "X-Scope-OrgID")
	if a.tenantFilter != "" && tenant != a.tenantFilter {
		a.otherTenant++
		return
	}

	switch url {
	case "/api/v1/push":
		if !a.analyzePush {
			a.otherRequests++
			return
		}
		a.pushRequests++
		a.decodeRemoteWrite(frame, tenant, "httpgrpc-push", req.Body, headerValue(req.Headers, "Content-Encoding"))
	case "/otlp/v1/metrics":
		if !a.analyzeOTLP {
			a.otherRequests++
			return
		}
		a.otlpRequests++
		a.handleOTLP(frame, tenant, &req)
	default:
		// e.g. /api/v1/push/influx/write - not handled.
		a.otherRequests++
	}
}

// decodeRemoteWrite decompresses a remote-write body (snappy by default) and
// runs duplicate detection over the mimirpb.WriteRequest.
func (a *analyzer) decodeRemoteWrite(frame, tenant, source string, body []byte, contentEncoding string) {
	// Remote-write bodies are snappy block-compressed. Some encodings may be
	// explicit; fall back to raw bytes if snappy fails.
	dec, err := snappy.Decode(nil, body)
	if err != nil {
		if d2, err2 := decompress(body, contentEncoding); err2 == nil {
			dec = d2
		} else {
			dec = body
		}
	}
	var wr mimirpb.WriteRequest
	if err := wr.Unmarshal(dec); err != nil {
		a.decodeErrors++
		return
	}
	for _, ts := range wr.Timeseries {
		a.totalSeries++
		a.emitSeries(frame, tenant, source, ts)
	}
}

// handleOTLP decodes an OTLP body and converts it to Prometheus series using
// Mimir's own converter, so the resulting series and timestamps match exactly
// what the distributor validates (and therefore what it discards as
// sample_duplicate_timestamp).
func (a *analyzer) handleOTLP(frame, tenant string, req *httpgrpc.HTTPRequest) {
	body, err := decompress(req.Body, headerValue(req.Headers, "Content-Encoding"))
	if err != nil {
		a.decodeErrors++
		return
	}
	er := pmetricotlp.NewExportRequest()
	if strings.Contains(headerValue(req.Headers, "Content-Type"), "json") {
		err = er.UnmarshalJSON(body)
	} else {
		err = er.UnmarshalProto(body)
	}
	if err != nil {
		a.decodeErrors++
		return
	}

	appender := otlpappender.NewCombinedAppender()
	converter := prometheusremotewrite.NewPrometheusConverter(appender)
	if _, err := converter.FromMetrics(context.Background(), er.Metrics(), a.otlpSettings); err != nil {
		// Partial conversion errors are expected for some inputs; keep whatever
		// series were appended and only count the failure.
		a.otlpConvErr++
	}
	series, _ := appender.GetResult()
	for _, ts := range series {
		a.totalSeries++
		a.emitSeries(frame, tenant, "otlp", ts)
	}
}

// runHexFile decodes a single gRPC message with no stream context. It tries an
// httpgrpc.HTTPRequest first, then falls back to a bare mimirpb.WriteRequest.
func (a *analyzer) runHexFile(path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	raw, err := hex.DecodeString(strings.TrimSpace(string(data)))
	if err != nil {
		return fmt.Errorf("hex decode: %w", err)
	}
	a.framesSeen++
	var req httpgrpc.HTTPRequest
	if err := req.Unmarshal(raw); err == nil && req.GetMethod() != "" && req.GetUrl() != "" {
		a.handleHTTPGRPC("hexfile", raw)
		return nil
	}
	// Bare WriteRequest (as in /distributor.Distributor/Push).
	a.handleGRPCPush("hexfile", "", raw)
	return nil
}

func decompress(b []byte, enc string) ([]byte, error) {
	switch strings.ToLower(strings.TrimSpace(enc)) {
	case "", "none":
		return b, nil
	case "gzip":
		r, err := gzip.NewReader(bytes.NewReader(b))
		if err != nil {
			return nil, err
		}
		return io.ReadAll(r)
	case "zstd":
		r, err := zstd.NewReader(bytes.NewReader(b))
		if err != nil {
			return nil, err
		}
		defer r.Close()
		return io.ReadAll(r)
	case "lz4":
		return io.ReadAll(lz4.NewReader(bytes.NewReader(b)))
	default:
		return nil, fmt.Errorf("unsupported content-encoding %q", enc)
	}
}

type dupKey struct {
	tenant string
	labels string
}

type dupStat struct {
	metric     string
	labels     string
	tenant     string
	dupSamples int                 // total dropped duplicate float samples
	dupHistos  int                 // total dropped duplicate histograms
	timestamps map[int64]int       // duplicated timestamp -> number of dropped occurrences
	requests   map[string]struct{} // frames in which this series appeared with duplicates
	sources    map[string]struct{} // "grpc-push", "httpgrpc-push" and/or "otlp"
	minCap     float64             // earliest capture time (epoch seconds) of a duplicate-carrying request
	maxCap     float64             // latest capture time
}

type analyzer struct {
	maxExampleTS          int
	showAll               bool
	tenantFilter          string
	analyzePush           bool
	analyzeOTLP           bool
	recoverUnknownStreams bool
	dumpSeries            bool
	otlpSettings          prometheusremotewrite.Settings

	curFrameTime float64 // capture time (epoch seconds) of the message being processed

	framesSeen    int
	pushRequests  int
	otlpRequests  int
	otherRequests int
	otherTenant   int
	decodeErrors  int
	otlpConvErr   int
	totalSeries   int

	streams map[string]streamInfo
	urlHist map[string]int // nil unless -urls is set
	stats   map[dupKey]*dupStat
}

// emitSeries runs duplicate detection and, when -dump is set, prints the full
// timeseries content.
func (a *analyzer) emitSeries(frame, tenant, source string, ts mimirpb.PreallocTimeseries) {
	if a.dumpSeries {
		a.printSeries(tenant, source, ts)
	}
	a.checkSeries(frame, tenant, source, ts)
}

// printSeries writes one decoded timeseries (labels + samples + histograms) to
// stdout, with timestamps rendered in UTC.
func (a *analyzer) printSeries(tenant, source string, ts mimirpb.PreallocTimeseries) {
	if ts.TimeSeries == nil {
		return
	}
	fmt.Printf("\n[%s tenant=%s] %s\n", source, orNone(tenant), labelsString(ts.Labels))
	for _, s := range ts.Samples {
		fmt.Printf("    sample    t=%d (%s) v=%g\n", s.TimestampMs, msToUTC(s.TimestampMs).Format("2006-01-02 15:04:05.000 MST"), s.Value)
	}
	for i := range ts.Histograms {
		h := &ts.Histograms[i]
		kind := "int-histogram"
		if h.IsFloatHistogram() {
			kind = "float-histogram"
		}
		fmt.Printf("    %s t=%d (%s) count=%v sum=%g\n", kind, h.Timestamp, msToUTC(h.Timestamp).Format("2006-01-02 15:04:05.000 MST"), h.GetCountInt(), h.Sum)
	}
	for _, e := range ts.Exemplars {
		fmt.Printf("    exemplar  t=%d (%s) v=%g labels=%s\n", e.TimestampMs, msToUTC(e.TimestampMs).Format("2006-01-02 15:04:05.000 MST"), e.Value, labelsString(e.Labels))
	}
}

// checkSeries mirrors distributor.validateSamples / validateHistograms: it
// counts, per timeseries entry, the samples and histograms whose timestamp was
// already seen earlier in the same entry.
func (a *analyzer) checkSeries(frame, tenant, source string, ts mimirpb.PreallocTimeseries) {
	if ts.TimeSeries == nil {
		return
	}

	var dupSamples, dupHistos int
	dupTS := map[int64]int{}

	if len(ts.Samples) > 1 {
		seen := make(map[int64]struct{}, len(ts.Samples))
		for _, s := range ts.Samples {
			if _, ok := seen[s.TimestampMs]; ok {
				dupSamples++
				dupTS[s.TimestampMs]++
				continue
			}
			seen[s.TimestampMs] = struct{}{}
		}
	}
	if len(ts.Histograms) > 1 {
		seen := make(map[int64]struct{}, len(ts.Histograms))
		for _, hgram := range ts.Histograms {
			if _, ok := seen[hgram.Timestamp]; ok {
				dupHistos++
				dupTS[hgram.Timestamp]++
				continue
			}
			seen[hgram.Timestamp] = struct{}{}
		}
	}

	if dupSamples == 0 && dupHistos == 0 {
		return
	}

	labelsStr := labelsString(ts.Labels)
	key := dupKey{tenant: tenant, labels: labelsStr}
	st := a.stats[key]
	if st == nil {
		st = &dupStat{
			metric:     metricName(ts.Labels),
			labels:     labelsStr,
			tenant:     tenant,
			timestamps: map[int64]int{},
			requests:   map[string]struct{}{},
			sources:    map[string]struct{}{},
			minCap:     a.curFrameTime,
			maxCap:     a.curFrameTime,
		}
		a.stats[key] = st
	}
	st.dupSamples += dupSamples
	st.dupHistos += dupHistos
	st.requests[frame] = struct{}{}
	st.sources[source] = struct{}{}
	if a.curFrameTime > 0 {
		if st.minCap == 0 || a.curFrameTime < st.minCap {
			st.minCap = a.curFrameTime
		}
		if a.curFrameTime > st.maxCap {
			st.maxCap = a.curFrameTime
		}
	}
	for tsMs, c := range dupTS {
		st.timestamps[tsMs] += c
	}

	if a.showAll {
		fmt.Printf("frame=%s tenant=%q source=%s dup_samples=%d dup_histograms=%d series=%s\n",
			frame, tenant, source, dupSamples, dupHistos, labelsStr)
	}
}

func (a *analyzer) report(w io.Writer) {
	bw := bufio.NewWriter(w)
	defer bw.Flush()

	stats := make([]*dupStat, 0, len(a.stats))
	totalDupSamples, totalDupHistos := 0, 0
	for _, s := range a.stats {
		stats = append(stats, s)
		totalDupSamples += s.dupSamples
		totalDupHistos += s.dupHistos
	}
	sort.Slice(stats, func(i, j int) bool {
		di := stats[i].dupSamples + stats[i].dupHistos
		dj := stats[j].dupSamples + stats[j].dupHistos
		if di != dj {
			return di > dj
		}
		return stats[i].labels < stats[j].labels
	})

	fmt.Fprintln(bw, "==================== dupts summary ====================")
	fmt.Fprintf(bw, "streams (RPCs) seen:         %d\n", len(a.streams))
	fmt.Fprintf(bw, "gRPC messages processed:     %d\n", a.framesSeen)
	if a.tenantFilter != "" {
		fmt.Fprintf(bw, "tenant filter:               %s\n", a.tenantFilter)
		fmt.Fprintf(bw, "requests, other tenant:      %d (skipped)\n", a.otherTenant)
	}
	fmt.Fprintf(bw, "remote-write push requests:  %d\n", a.pushRequests)
	fmt.Fprintf(bw, "OTLP requests:               %d\n", a.otlpRequests)
	fmt.Fprintf(bw, "other/ignored messages:      %d\n", a.otherRequests)
	fmt.Fprintf(bw, "undecodable messages:        %d\n", a.decodeErrors)
	fmt.Fprintf(bw, "OTLP conversion errors:      %d\n", a.otlpConvErr)
	fmt.Fprintf(bw, "timeseries inspected:        %d (post-conversion for OTLP)\n", a.totalSeries)
	fmt.Fprintf(bw, "series with duplicate ts:    %d\n", len(stats))
	fmt.Fprintf(bw, "duplicate float samples:     %d (discarded as sample_duplicate_timestamp)\n", totalDupSamples)
	fmt.Fprintf(bw, "duplicate histograms:        %d (discarded as sample_duplicate_timestamp)\n", totalDupHistos)
	fmt.Fprintln(bw, "=======================================================")

	if a.urlHist != nil {
		type uc struct {
			url string
			n   int
		}
		ucs := make([]uc, 0, len(a.urlHist))
		for u, n := range a.urlHist {
			ucs = append(ucs, uc{u, n})
		}
		sort.Slice(ucs, func(i, j int) bool { return ucs[i].n > ucs[j].n })
		fmt.Fprintln(bw, "\nrequest :path histogram:")
		for _, u := range ucs {
			fmt.Fprintf(bw, "  %6d  %s\n", u.n, orNone(u.url))
		}
	}

	if len(stats) == 0 {
		fmt.Fprintln(bw, "\nNo series with duplicated timestamps found in this capture.")
		return
	}

	a.reportByTenant(bw, stats)

	fmt.Fprintln(bw, "\nOffending series (most duplicates first):")
	for _, s := range stats {
		fmt.Fprintf(bw, "\n- tenant=%s metric=%s source=%s\n", orNone(s.tenant), orNone(s.metric), sourcesString(s.sources))
		fmt.Fprintf(bw, "  labels: %s\n", s.labels)
		fmt.Fprintf(bw, "  dropped: %d duplicate samples, %d duplicate histograms across %d request(s)\n",
			s.dupSamples, s.dupHistos, len(s.requests))
		fmt.Fprintf(bw, "  duplicated timestamps (ms): %s\n", exampleTimestamps(s.timestamps, a.maxExampleTS))
	}
}

// reportByTenant prints, per tenant, the number of offending series and the UTC
// windows both for when the duplicates were detected (packet capture time) and
// for the duplicated sample timestamps themselves.
func (a *analyzer) reportByTenant(bw io.Writer, stats []*dupStat) {
	type tstat struct {
		tenant                   string
		series                   int
		dupSamples, dupHistos    int
		minCap, maxCap           float64
		minSampleTS, maxSampleTS int64
	}
	byTenant := map[string]*tstat{}
	for _, s := range stats {
		t := byTenant[s.tenant]
		if t == nil {
			t = &tstat{tenant: s.tenant, minSampleTS: -1}
			byTenant[s.tenant] = t
		}
		t.series++
		t.dupSamples += s.dupSamples
		t.dupHistos += s.dupHistos
		if s.minCap > 0 && (t.minCap == 0 || s.minCap < t.minCap) {
			t.minCap = s.minCap
		}
		if s.maxCap > t.maxCap {
			t.maxCap = s.maxCap
		}
		for ts := range s.timestamps {
			if t.minSampleTS == -1 || ts < t.minSampleTS {
				t.minSampleTS = ts
			}
			if ts > t.maxSampleTS {
				t.maxSampleTS = ts
			}
		}
	}

	ts := make([]*tstat, 0, len(byTenant))
	for _, t := range byTenant {
		ts = append(ts, t)
	}
	sort.Slice(ts, func(i, j int) bool {
		if ts[i].dupSamples+ts[i].dupHistos != ts[j].dupSamples+ts[j].dupHistos {
			return ts[i].dupSamples+ts[i].dupHistos > ts[j].dupSamples+ts[j].dupHistos
		}
		return ts[i].tenant < ts[j].tenant
	})

	fmt.Fprintln(bw, "\nTenants with duplicates (detection = packet capture time, UTC):")
	for _, t := range ts {
		fmt.Fprintf(bw, "\n- tenant=%s: %d offending series, %d duplicate samples, %d duplicate histograms\n",
			orNone(t.tenant), t.series, t.dupSamples, t.dupHistos)
		fmt.Fprintf(bw, "    detected (capture) window: %s\n", utcWindow(epochToUTC(t.minCap), epochToUTC(t.maxCap)))
		fmt.Fprintf(bw, "    duplicated sample-ts window: %s\n", utcWindow(msToUTC(t.minSampleTS), msToUTC(t.maxSampleTS)))
	}
}

func epochToUTC(sec float64) time.Time {
	if sec <= 0 {
		return time.Time{}
	}
	return time.Unix(0, int64(sec*1e9)).UTC()
}

func msToUTC(ms int64) time.Time {
	if ms <= 0 {
		return time.Time{}
	}
	return time.UnixMilli(ms).UTC()
}

func utcWindow(from, to time.Time) string {
	if from.IsZero() || to.IsZero() {
		return "<unknown>"
	}
	const layout = "2006-01-02 15:04:05.000 MST"
	if from.Equal(to) {
		return from.Format(layout) + " (single instant)"
	}
	return from.Format(layout) + "  →  " + to.Format(layout) +
		fmt.Sprintf("  (span %s)", to.Sub(from).Round(time.Millisecond))
}

func exampleTimestamps(m map[int64]int, max int) string {
	type kv struct {
		ts    int64
		count int
	}
	kvs := make([]kv, 0, len(m))
	for ts, c := range m {
		kvs = append(kvs, kv{ts, c})
	}
	sort.Slice(kvs, func(i, j int) bool {
		if kvs[i].count != kvs[j].count {
			return kvs[i].count > kvs[j].count
		}
		return kvs[i].ts < kvs[j].ts
	})
	var b strings.Builder
	for i, e := range kvs {
		if i >= max {
			fmt.Fprintf(&b, " ... (+%d more)", len(kvs)-max)
			break
		}
		if i > 0 {
			b.WriteString(", ")
		}
		fmt.Fprintf(&b, "%d(x%d)", e.ts, e.count+1) // +1: original occurrence plus dropped ones
	}
	return b.String()
}

// extractTenant picks the most likely X-Scope-OrgID from a pipe-joined list of
// HTTP/2 header values. Tenant IDs in this environment are purely numeric,
// while grpc-timeout ends in a unit letter, trace headers contain dashes/colons
// and the authority contains dots - so the all-digit value is the tenant.
func extractTenant(joined string) string {
	for _, v := range strings.Split(joined, "|") {
		v = strings.TrimSpace(v)
		if len(v) >= 2 && isAllDigits(v) {
			return v
		}
	}
	return ""
}

func isAllDigits(s string) bool {
	for _, r := range s {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}

func streamKey(tcpStream, streamID string) string {
	// A frame carrying several http2 frames for the same stream reports the
	// streamid more than once, joined by the field aggregator (',' by default,
	// '|' in pass A). Take the leading run of digits so both passes agree.
	return firstUint(tcpStream) + ":" + firstUint(streamID)
}

func firstUint(s string) string {
	for i, r := range s {
		if r < '0' || r > '9' {
			return s[:i]
		}
	}
	return s
}

func sourcesString(m map[string]struct{}) string {
	ss := make([]string, 0, len(m))
	for s := range m {
		ss = append(ss, s)
	}
	sort.Strings(ss)
	return strings.Join(ss, "+")
}

func headerValue(headers []*httpgrpc.Header, name string) string {
	for _, h := range headers {
		if strings.EqualFold(h.GetKey(), name) {
			if vs := h.GetValues(); len(vs) > 0 {
				return vs[0]
			}
		}
	}
	return ""
}

// metricNameLabel is the reserved label holding a series' metric name.
const metricNameLabel = "__name__"

func metricName(ls []mimirpb.LabelAdapter) string {
	for _, l := range ls {
		if l.Name == metricNameLabel {
			return l.Value
		}
	}
	return ""
}

func labelsString(ls []mimirpb.LabelAdapter) string {
	var b strings.Builder
	b.WriteByte('{')
	for i, l := range ls {
		if i > 0 {
			b.WriteString(", ")
		}
		fmt.Fprintf(&b, "%s=%q", l.Name, l.Value)
	}
	b.WriteByte('}')
	return b.String()
}

func orNone(s string) string {
	if s == "" {
		return "<none>"
	}
	return s
}

func fatalf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "dupts: "+format+"\n", args...)
	os.Exit(1)
}
