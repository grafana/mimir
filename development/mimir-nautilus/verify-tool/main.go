// SPDX-License-Identifier: AGPL-3.0-only
//
// verify-tool is a tiny CLI used by the nautilus dev-cell verify.sh
// harness to push and query samples using Mimir's vendored
// remote-write libraries. Keeping it in-tree (rather than relying on
// a Python snappy install on the developer machine) makes the smoke
// loop hermetic.
//
// Subcommands:
//
//	verify-tool push -url ... -tenant ... -metric ... -value ... -timestamp-ms ...
//	verify-tool query -url ... -tenant ... -query ...
//
// `query` exits 0 if the response contains the literal value passed
// via -expect-value, 1 otherwise; the raw response body is always
// printed to stdout so the harness can log it on failure.

package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/prometheus/prometheus/prompb"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: verify-tool push|query|spike [flags]")
		os.Exit(2)
	}

	switch os.Args[1] {
	case "push":
		os.Exit(cmdPush(os.Args[2:]))
	case "query":
		os.Exit(cmdQuery(os.Args[2:]))
	case "spike":
		os.Exit(cmdSpike(os.Args[2:]))
	default:
		fmt.Fprintf(os.Stderr, "unknown subcommand %q\n", os.Args[1])
		os.Exit(2)
	}
}

func cmdPush(args []string) int {
	fs := flag.NewFlagSet("push", flag.ExitOnError)
	urlFlag := fs.String("url", "http://localhost:8000", "distributor base URL")
	tenant := fs.String("tenant", "", "tenant ID for X-Scope-OrgID")
	metric := fs.String("metric", "verify_metric", "metric name")
	value := fs.Float64("value", 1, "sample value")
	tsMS := fs.Int64("timestamp-ms", time.Now().UnixMilli(), "sample timestamp in ms")
	_ = fs.Parse(args)

	if *tenant == "" {
		fmt.Fprintln(os.Stderr, "push: -tenant is required")
		return 2
	}

	req := &prompb.WriteRequest{
		Timeseries: []prompb.TimeSeries{{
			Labels: []prompb.Label{{Name: "__name__", Value: *metric}},
			Samples: []prompb.Sample{{
				Value:     *value,
				Timestamp: *tsMS,
			}},
		}},
	}
	raw, err := proto.Marshal(req)
	if err != nil {
		fmt.Fprintf(os.Stderr, "push: marshal: %v\n", err)
		return 1
	}
	body := snappy.Encode(nil, raw)

	httpReq, err := http.NewRequest(http.MethodPost, strings.TrimRight(*urlFlag, "/")+"/api/v1/push", bytes.NewReader(body))
	if err != nil {
		fmt.Fprintf(os.Stderr, "push: build request: %v\n", err)
		return 1
	}
	httpReq.Header.Set("Content-Type", "application/x-protobuf")
	httpReq.Header.Set("Content-Encoding", "snappy")
	httpReq.Header.Set("X-Prometheus-Remote-Write-Version", "0.1.0")
	httpReq.Header.Set("X-Scope-OrgID", *tenant)

	httpClient := &http.Client{Timeout: 10 * time.Second}
	resp, err := httpClient.Do(httpReq)
	if err != nil {
		fmt.Fprintf(os.Stderr, "push: do: %v\n", err)
		return 1
	}
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(resp.Body)
	if resp.StatusCode/100 != 2 {
		fmt.Fprintf(os.Stderr, "push: status=%d body=%s\n", resp.StatusCode, string(respBody))
		return 1
	}
	fmt.Println("push_ok")
	return 0
}

func cmdQuery(args []string) int {
	fs := flag.NewFlagSet("query", flag.ExitOnError)
	urlFlag := fs.String("url", "http://localhost:8007", "query-frontend base URL")
	tenant := fs.String("tenant", "", "tenant ID for X-Scope-OrgID")
	q := fs.String("query", "", "instant query string (e.g. verify_metric)")
	expect := fs.String("expect-value", "", "if set, exit 0 only when this string appears as a sample value")
	_ = fs.Parse(args)

	if *tenant == "" || *q == "" {
		fmt.Fprintln(os.Stderr, "query: -tenant and -query are required")
		return 2
	}

	u := strings.TrimRight(*urlFlag, "/") + "/prometheus/api/v1/query"
	vals := url.Values{"query": []string{*q}}
	httpReq, err := http.NewRequest(http.MethodGet, u+"?"+vals.Encode(), nil)
	if err != nil {
		fmt.Fprintf(os.Stderr, "query: build request: %v\n", err)
		return 1
	}
	httpReq.Header.Set("X-Scope-OrgID", *tenant)

	httpClient := &http.Client{Timeout: 10 * time.Second}
	resp, err := httpClient.Do(httpReq)
	if err != nil {
		fmt.Fprintf(os.Stderr, "query: do: %v\n", err)
		return 1
	}
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(resp.Body)
	fmt.Print(string(respBody))
	if !bytes.HasSuffix(respBody, []byte("\n")) {
		fmt.Println()
	}
	if resp.StatusCode/100 != 2 {
		fmt.Fprintf(os.Stderr, "query: status=%d\n", resp.StatusCode)
		return 1
	}
	if *expect == "" {
		return 0
	}

	// Walk the JSON looking for {"value":[<ts>, "<expect>"]} entries.
	// We don't shape-check exhaustively; the verify harness just
	// wants to know whether the sample exists.
	var parsed struct {
		Status string `json:"status"`
		Data   struct {
			ResultType string `json:"resultType"`
			Result     []struct {
				Value [2]any `json:"value"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(respBody, &parsed); err != nil {
		fmt.Fprintf(os.Stderr, "query: parse: %v\n", err)
		return 1
	}
	for _, r := range parsed.Data.Result {
		if s, ok := r.Value[1].(string); ok && s == *expect {
			return 0
		}
	}
	fmt.Fprintf(os.Stderr, "query: expected value %q not in response\n", *expect)
	return 1
}

// cmdSpike pushes N distinct time series in a single remote-write
// request, each with a unique value of the "id" label. Used to
// drive a head-series spike under one (or a few) metric name(s) so
// the rebalancer sees uneven per-partition active-series counts and
// hash-range movement becomes observable.
//
// Multiple metric names spread load across hash ranges if Mimir
// shards by metric name; for the dev cell we want a controlled
// number of series per metric so the readcache's per-partition
// totals diverge enough to overrun the slicer's movement budget.
func cmdSpike(args []string) int {
	fs := flag.NewFlagSet("spike", flag.ExitOnError)
	urlFlag := fs.String("url", "http://localhost:8000", "distributor base URL")
	tenant := fs.String("tenant", "", "tenant ID for X-Scope-OrgID")
	metric := fs.String("metric", "spike_metric", "metric name (or prefix when -metrics > 1)")
	metrics := fs.Int("metrics", 1, "number of distinct metric names; series are split evenly across them")
	count := fs.Int("count", 5000, "total number of distinct series to push")
	tsMS := fs.Int64("timestamp-ms", time.Now().UnixMilli(), "sample timestamp in ms")
	idPrefix := fs.String("id-prefix", "s", "prefix for the per-series id label (controls hash distribution)")
	_ = fs.Parse(args)

	if *tenant == "" {
		fmt.Fprintln(os.Stderr, "spike: -tenant is required")
		return 2
	}
	if *count <= 0 || *metrics <= 0 {
		fmt.Fprintln(os.Stderr, "spike: -count and -metrics must be > 0")
		return 2
	}

	req := &prompb.WriteRequest{
		Timeseries: make([]prompb.TimeSeries, 0, *count),
	}
	for i := 0; i < *count; i++ {
		name := *metric
		if *metrics > 1 {
			name = fmt.Sprintf("%s_%d", *metric, i%*metrics)
		}
		req.Timeseries = append(req.Timeseries, prompb.TimeSeries{
			Labels: []prompb.Label{
				{Name: "__name__", Value: name},
				{Name: "id", Value: fmt.Sprintf("%s%06d", *idPrefix, i)},
			},
			Samples: []prompb.Sample{{Value: float64(i), Timestamp: *tsMS}},
		})
	}

	raw, err := proto.Marshal(req)
	if err != nil {
		fmt.Fprintf(os.Stderr, "spike: marshal: %v\n", err)
		return 1
	}
	body := snappy.Encode(nil, raw)

	httpReq, err := http.NewRequest(http.MethodPost, strings.TrimRight(*urlFlag, "/")+"/api/v1/push", bytes.NewReader(body))
	if err != nil {
		fmt.Fprintf(os.Stderr, "spike: build request: %v\n", err)
		return 1
	}
	httpReq.Header.Set("Content-Type", "application/x-protobuf")
	httpReq.Header.Set("Content-Encoding", "snappy")
	httpReq.Header.Set("X-Prometheus-Remote-Write-Version", "0.1.0")
	httpReq.Header.Set("X-Scope-OrgID", *tenant)

	httpClient := &http.Client{Timeout: 30 * time.Second}
	resp, err := httpClient.Do(httpReq)
	if err != nil {
		fmt.Fprintf(os.Stderr, "spike: do: %v\n", err)
		return 1
	}
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(resp.Body)
	if resp.StatusCode/100 != 2 {
		fmt.Fprintf(os.Stderr, "spike: status=%d body=%s\n", resp.StatusCode, string(respBody))
		return 1
	}
	fmt.Printf("spike_ok count=%d metrics=%d\n", *count, *metrics)
	return 0
}
