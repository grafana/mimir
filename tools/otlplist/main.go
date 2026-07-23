// SPDX-License-Identifier: AGPL-3.0-only

// Command otlplist decodes a single httpgrpc.HTTPRequest carrying an OTLP
// /otlp/v1/metrics push (as extracted from a pcap via tshark grpc.message_data)
// and lists the metrics it contains. It also flags, per metric and per
// attribute set (i.e. per resulting Prometheus series), OTLP data points that
// share the same timestamp - those are what would be dropped as
// sample_duplicate_timestamp after conversion in the distributor.
//
// Usage:
//
//	go run ./tools/otlplist -hexfile message.hex
package main

import (
	"bytes"
	"compress/gzip"
	"encoding/hex"
	"flag"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"

	"github.com/klauspost/compress/zstd"
	"github.com/pierrec/lz4/v4"

	"github.com/grafana/dskit/httpgrpc"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/pmetric/pmetricotlp"
)

func main() {
	hexFile := flag.String("hexfile", "", "File containing the hex of a single gRPC message (httpgrpc.HTTPRequest).")
	flag.Parse()
	if *hexFile == "" {
		fmt.Fprintln(os.Stderr, "error: -hexfile is required")
		os.Exit(2)
	}

	data, err := os.ReadFile(*hexFile)
	if err != nil {
		fatalf("read hexfile: %v", err)
	}
	raw, err := hex.DecodeString(strings.TrimSpace(string(data)))
	if err != nil {
		fatalf("hex decode: %v", err)
	}

	var req httpgrpc.HTTPRequest
	if err := req.Unmarshal(raw); err != nil {
		fatalf("unmarshal httpgrpc.HTTPRequest: %v", err)
	}

	fmt.Printf("method:  %s\n", req.GetMethod())
	fmt.Printf("url:     %s\n", req.GetUrl())
	tenant := header(req.Headers, "X-Scope-OrgID")
	contentType := header(req.Headers, "Content-Type")
	contentEnc := header(req.Headers, "Content-Encoding")
	fmt.Printf("tenant:  %s\n", tenant)
	fmt.Printf("content-type: %s   content-encoding: %s   body-bytes: %d\n", contentType, orNone(contentEnc), len(req.Body))

	body, err := decompress(req.Body, contentEnc)
	if err != nil {
		fatalf("decompress (%s): %v", contentEnc, err)
	}

	er := pmetricotlp.NewExportRequest()
	if strings.Contains(contentType, "json") {
		err = er.UnmarshalJSON(body)
	} else {
		err = er.UnmarshalProto(body)
	}
	if err != nil {
		fatalf("unmarshal OTLP ExportRequest: %v", err)
	}

	inventory(er.Metrics())
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

// seriesStat aggregates data points for one (metric, attribute-set) combination,
// which is what maps to a single Prometheus series after conversion.
type seriesStat struct {
	metric   string
	mtype    string
	attrs    string
	points   int
	tsCounts map[int64]int
}

func inventory(md pmetric.Metrics) {
	type metricStat struct {
		name       string
		typ        string
		unit       string
		dataPoints int
	}
	var metrics []metricStat
	series := map[string]*seriesStat{}

	rms := md.ResourceMetrics()
	fmt.Printf("\nresource_metrics=%d\n", rms.Len())
	for i := 0; i < rms.Len(); i++ {
		rm := rms.At(i)
		sms := rm.ScopeMetrics()
		for j := 0; j < sms.Len(); j++ {
			ms := sms.At(j).Metrics()
			for k := 0; k < ms.Len(); k++ {
				m := ms.At(k)
				typ := m.Type().String()
				dps := dataPointCount(m)
				metrics = append(metrics, metricStat{m.Name(), typ, m.Unit(), dps})
				collectSeries(series, m)
			}
		}
	}

	sort.Slice(metrics, func(i, j int) bool {
		if metrics[i].dataPoints != metrics[j].dataPoints {
			return metrics[i].dataPoints > metrics[j].dataPoints
		}
		return metrics[i].name < metrics[j].name
	})

	fmt.Printf("distinct metrics=%d\n\n", len(metrics))
	fmt.Printf("%-8s  %-22s  %-6s  %s\n", "DPOINTS", "TYPE", "UNIT", "METRIC")
	for _, m := range metrics {
		fmt.Printf("%-8d  %-22s  %-6s  %s\n", m.dataPoints, m.typ, orNone(m.unit), m.name)
	}

	// Report series (metric + attributes) carrying duplicate timestamps.
	var dup []*seriesStat
	for _, s := range series {
		for _, c := range s.tsCounts {
			if c > 1 {
				dup = append(dup, s)
				break
			}
		}
	}
	fmt.Printf("\n==== duplicate-timestamp series (metric + attribute set) ====\n")
	if len(dup) == 0 {
		fmt.Println("none")
		return
	}
	sort.Slice(dup, func(i, j int) bool { return dup[i].metric < dup[j].metric })
	for _, s := range dup {
		var dropped int
		var tsList []string
		for ts, c := range s.tsCounts {
			if c > 1 {
				dropped += c - 1
				tsList = append(tsList, fmt.Sprintf("%d(x%d)", ts, c))
			}
		}
		sort.Strings(tsList)
		fmt.Printf("\n- metric=%s type=%s\n  attrs=%s\n  points=%d dropped_duplicates=%d\n  timestamps=%s\n",
			s.metric, s.mtype, orNone(s.attrs), s.points, dropped, strings.Join(tsList, ", "))
	}
}

func dataPointCount(m pmetric.Metric) int {
	switch m.Type() {
	case pmetric.MetricTypeGauge:
		return m.Gauge().DataPoints().Len()
	case pmetric.MetricTypeSum:
		return m.Sum().DataPoints().Len()
	case pmetric.MetricTypeHistogram:
		return m.Histogram().DataPoints().Len()
	case pmetric.MetricTypeExponentialHistogram:
		return m.ExponentialHistogram().DataPoints().Len()
	case pmetric.MetricTypeSummary:
		return m.Summary().DataPoints().Len()
	default:
		return 0
	}
}

func collectSeries(series map[string]*seriesStat, m pmetric.Metric) {
	add := func(attrs pcommon.Map, ts int64) {
		as := attrString(attrs)
		key := m.Name() + "\x00" + as
		s := series[key]
		if s == nil {
			s = &seriesStat{metric: m.Name(), mtype: m.Type().String(), attrs: as, tsCounts: map[int64]int{}}
			series[key] = s
		}
		s.points++
		s.tsCounts[ts]++
	}
	switch m.Type() {
	case pmetric.MetricTypeGauge:
		dps := m.Gauge().DataPoints()
		for i := 0; i < dps.Len(); i++ {
			add(dps.At(i).Attributes(), int64(dps.At(i).Timestamp()))
		}
	case pmetric.MetricTypeSum:
		dps := m.Sum().DataPoints()
		for i := 0; i < dps.Len(); i++ {
			add(dps.At(i).Attributes(), int64(dps.At(i).Timestamp()))
		}
	case pmetric.MetricTypeHistogram:
		dps := m.Histogram().DataPoints()
		for i := 0; i < dps.Len(); i++ {
			add(dps.At(i).Attributes(), int64(dps.At(i).Timestamp()))
		}
	case pmetric.MetricTypeExponentialHistogram:
		dps := m.ExponentialHistogram().DataPoints()
		for i := 0; i < dps.Len(); i++ {
			add(dps.At(i).Attributes(), int64(dps.At(i).Timestamp()))
		}
	case pmetric.MetricTypeSummary:
		dps := m.Summary().DataPoints()
		for i := 0; i < dps.Len(); i++ {
			add(dps.At(i).Attributes(), int64(dps.At(i).Timestamp()))
		}
	}
}

func attrString(m pcommon.Map) string {
	type kv struct{ k, v string }
	kvs := make([]kv, 0, m.Len())
	m.Range(func(k string, v pcommon.Value) bool {
		kvs = append(kvs, kv{k, v.AsString()})
		return true
	})
	sort.Slice(kvs, func(i, j int) bool { return kvs[i].k < kvs[j].k })
	var b strings.Builder
	for i, e := range kvs {
		if i > 0 {
			b.WriteString(", ")
		}
		fmt.Fprintf(&b, "%s=%q", e.k, e.v)
	}
	return b.String()
}

func header(hs []*httpgrpc.Header, name string) string {
	for _, h := range hs {
		if strings.EqualFold(h.GetKey(), name) {
			if v := h.GetValues(); len(v) > 0 {
				return v[0]
			}
		}
	}
	return ""
}

func orNone(s string) string {
	if s == "" {
		return "<none>"
	}
	return s
}

func fatalf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "otlplist: "+format+"\n", args...)
	os.Exit(1)
}
