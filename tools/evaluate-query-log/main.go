// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"context"
	"flag"
	"fmt"
	"net/textproto"
	"os"
	"path"
	"strconv"
	"time"

	"github.com/go-logfmt/logfmt"
	"github.com/weaveworks/common/httpgrpc"
	"github.com/weaveworks/common/user"

	"github.com/grafana/mimir/pkg/ruler"
	util_log "github.com/grafana/mimir/pkg/util/log"
)

type config struct {
	logFile             string
	orgID               string
	outputDir           string
	queryFrontendConfig ruler.QueryFrontendConfig
}

// Run this with a command line like the following:
// go run . --log-file /tmp/log.txt --org-id 10248 --output-dir /tmp/queries/ --ruler.query-frontend.address localhost:9095
//
// Query Loki for relevant query logs with a query like the following:
// {namespace="<<NAMESPACE>>",name=~"query-frontend.*"} |= "query stats" != "/api/v1/read" | logfmt | status="success" | user="10428"
// Note that downloaded logs from Grafana have each line prefixed with the timestamp, which needs to be removed before it can be used by this tool.
func main() {
	cfg := config{}
	cfg.queryFrontendConfig.RegisterFlags(flag.CommandLine)
	flag.StringVar(&cfg.logFile, "log-file", "", "Path to log file containing query stats log lines")
	flag.StringVar(&cfg.orgID, "org-id", "", "Org ID to use when evaluating queries")
	flag.StringVar(&cfg.outputDir, "output-dir", "", "Path to directory to write query results to")
	flag.Parse()

	if cfg.logFile == "" {
		fmt.Println("Missing required command line flag: log file")
		os.Exit(1)
	}

	if cfg.orgID == "" {
		fmt.Println("Missing required command line flag: org ID")
		os.Exit(1)
	}

	if cfg.outputDir == "" {
		fmt.Println("Missing required command line flag: output directory")
		os.Exit(1)
	}

	queries, rangeQueries, err := readQueriesFromLog(cfg.logFile)

	if err != nil {
		fmt.Printf("Error reading queries from log file: %v\n", err)
		os.Exit(1)
	}

	if err := os.RemoveAll(cfg.outputDir); err != nil {
		fmt.Printf("Error cleaning output directory: %v\n", err)
		os.Exit(1)
	}

	if err := os.MkdirAll(cfg.outputDir, 0700); err != nil {
		fmt.Printf("Error creating output directory: %v\n", err)
		os.Exit(1)
	}

	client, err := DialQueryFrontend(cfg.queryFrontendConfig, cfg.orgID)
	if err != nil {
		fmt.Printf("Error creating query frontend client: %v\n", err)
		os.Exit(1)
	}

	querier := NewRemoteQuerier(client, 30*time.Second, "/prometheus", util_log.Logger, WithOrgIDMiddleware(cfg.orgID))
	failures := 0

	for i, q := range queries {
		fmt.Printf("Evaluating query %v...\n", i+1)
		response, err := querier.Query(context.Background(), q.queryExpr, q.time, util_log.Logger)

		if err != nil {
			fmt.Printf("  Evaluation failed: %v\n", err)
			failures++
			continue
		}

		if err := os.WriteFile(path.Join(cfg.outputDir, fmt.Sprintf("query-%v.json", i+1)), response, 0700); err != nil {
			fmt.Printf("Writing response to file failed: %v\n", err)
			os.Exit(1)
		}
	}

	for i, q := range rangeQueries {
		fmt.Printf("Evaluating range query %v...\n", i+1)
		response, err := querier.QueryRange(context.Background(), q.queryExpr, q.start, q.end, q.step, util_log.Logger)

		if err != nil {
			fmt.Printf("  Evaluation failed: %v\n", err)
			failures++
			continue
		}

		if err := os.WriteFile(path.Join(cfg.outputDir, fmt.Sprintf("range-query-%v.json", i+1)), response, 0700); err != nil {
			fmt.Printf("Writing response to file failed: %v\n", err)
			os.Exit(1)
		}
	}

	fmt.Printf("Completed with %v query failures.\n", failures)
}

type query struct {
	queryExpr string
	time      time.Time
}

type rangeQuery struct {
	queryExpr string
	start     time.Time
	end       time.Time
	step      time.Duration
}

// HACK: this is not particularly efficient or robust, but it does the job.
func readQueriesFromLog(file string) ([]query, []rangeQuery, error) {
	f, err := os.Open(file)
	if err != nil {
		return nil, nil, err
	}

	d := logfmt.NewDecoder(f)
	lineNumber := 0
	var queries []query
	var rangeQueries []rangeQuery

	for d.ScanRecord() {
		rec := map[string]string{}
		lineNumber++

		for d.ScanKeyval() {
			key := string(d.Key())
			val := string(d.Value())

			rec[key] = val
		}

		if d.Err() != nil {
			return nil, nil, d.Err()
		}

		switch rec["path"] {
		case "/prometheus/api/v1/query":
			q, err := toQuery(rec)
			if err != nil {
				return nil, nil, fmt.Errorf("could not parse log line %v as query: %w", lineNumber, err)
			}

			queries = append(queries, q)
		case "/prometheus/api/v1/query_range":
			q, err := toRangeQuery(rec)
			if err != nil {
				return nil, nil, fmt.Errorf("could not parse log line %v as range query: %w", lineNumber, err)
			}

			rangeQueries = append(rangeQueries, q)

		default:
			return nil, nil, fmt.Errorf("unknown path %v", rec["path"])
		}

	}

	if d.Err() != nil {
		return nil, nil, d.Err()
	}

	return queries, rangeQueries, nil
}

func toQuery(rec map[string]string) (query, error) {
	t, err := parseTime(rec["param_time"])
	if err != nil {
		return query{}, fmt.Errorf("could not parse time: %w", err)
	}

	return query{
		queryExpr: rec["param_query"],
		time:      t,
	}, nil
}

func toRangeQuery(rec map[string]string) (rangeQuery, error) {
	start, err := parseTime(rec["param_start"])
	if err != nil {
		return rangeQuery{}, fmt.Errorf("could not parse start time: %w", err)
	}

	end, err := parseTime(rec["param_end"])
	if err != nil {
		return rangeQuery{}, fmt.Errorf("could not parse end time: %w", err)
	}

	step, err := parseStep(rec["param_step"])
	if err != nil {
		return rangeQuery{}, fmt.Errorf("could not parse step: %w", err)
	}

	return rangeQuery{
		queryExpr: rec["param_query"],
		start:     start,
		end:       end,
		step:      step,
	}, nil
}

func parseTime(s string) (time.Time, error) {
	unix, err := strconv.ParseFloat(s, 64)

	if err == nil {
		seconds := int64(unix)
		nanoseconds := int64(unix*1e9) % 1e9
		return time.Unix(seconds, nanoseconds).UTC(), nil
	}

	return time.Parse(time.RFC3339Nano, s)
}

func parseStep(s string) (time.Duration, error) {
	seconds, err := strconv.ParseFloat(s, 64)

	if err == nil {
		nanoseconds := int64(seconds * 1e9)
		return time.Duration(nanoseconds) * time.Nanosecond, nil
	}

	return time.ParseDuration(s)
}

func WithOrgIDMiddleware(orgID string) func(ctx context.Context, req *httpgrpc.HTTPRequest) error {
	return func(ctx context.Context, req *httpgrpc.HTTPRequest) error {
		req.Headers = append(req.Headers, &httpgrpc.Header{
			Key:    textproto.CanonicalMIMEHeaderKey(user.OrgIDHeaderName),
			Values: []string{orgID},
		})
		return nil
	}
}
