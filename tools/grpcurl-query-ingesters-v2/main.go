package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"sync"
	"time"

	"github.com/grafana/dskit/flagext"
	"github.com/grafana/dskit/ring"
	"github.com/grafana/dskit/user"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser"

	ingester_client "github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/mimirpb"
)

type IngesterStats struct {
	successQueries int
	failureQueries int
}

func main() {
	var port int
	flag.IntVar(&port, "port", 9095, "Port to port-forward.")
	var pods flagext.StringSlice
	flag.Var(&pods, "pod", "Pod to query, can be provided multiple times.")
	var namespace string
	flag.StringVar(&namespace, "namespace", "", "Namespace of the pods.")

	var fromUnix, toUnix int64
	flag.Int64Var(&fromUnix, "from", time.Now().Add(-10*time.Minute).Unix(), "Start time in Unix timestamp. Default is 10 minutes ago.")
	flag.Int64Var(&toUnix, "to", time.Now().Unix(), "End time in Unix timestamp. Default is now.")

	var matchersString string
	flag.StringVar(&matchersString, "matchers", "", "Matchers to query the ingester. Format: {foo=\"bar\",...}")

	var orgID string
	flag.StringVar(&orgID, "org", "", "Organization ID to query the ingester.")

	flag.Parse()

	if orgID == "" {
		log.Fatal("orgID is required")
	}

	if namespace == "" {
		log.Fatal("namespace is required")
	}

	if len(pods) == 0 {
		log.Fatal("at least one pod is required")
	}

	matchers, err := parser.ParseMetricSelector(matchersString)
	if err != nil {
		log.Fatalf("failed to parse matchers: %v", err)
	}

	from := model.Time(fromUnix * 1000)
	to := model.Time(toUnix * 1000)
	ctx := user.InjectOrgID(context.Background(), orgID)

	// Keep track of query stats.
	statsMx := sync.Mutex{}
	stats := make(map[string]*IngesterStats)

	process := func(pod string, localPort int) {
		addr := fmt.Sprintf("localhost:%d", localPort)

		// Query continuously.
		for {
			err := queryIngesterAndCheckMatchersCorrectness(ctx, addr, from, to, matchers...)
			if err != nil {
				log.Printf("failed to check ingester %s: %v", pod, err)
			}

			// Keep track of stats.
			statsMx.Lock()
			data, ok := stats[pod]
			if !ok {
				data = &IngesterStats{}
				stats[pod] = data
			}

			if err == nil {
				data.successQueries++
			} else {
				data.failureQueries++
			}
			statsMx.Unlock()
		}
	}

	// WaitGroup to wait for all goroutines to finish
	var wg sync.WaitGroup

	for _, pod := range pods {
		wg.Add(1)
		go func(pod string) {
			defer wg.Done()
			if err := processPortForwarded(pod, namespace, port, process); err != nil {
				log.Printf("Error processing pod %s in namespace %s: %v", pod, namespace, err)
			}
		}(pod)
	}

	// Periodically print stats.
	wg.Add(1)
	go func() {
		defer wg.Done()

		for {
			// Print stats.
			statsMx.Lock()

			successes := 0
			failures := 0
			minQueriesPerIngester := -1
			maxQueriesPerIngester := 0

			for _, stat := range stats {
				successes += stat.successQueries
				failures += stat.failureQueries

				total := stat.successQueries + stat.failureQueries
				if minQueriesPerIngester < 0 || total < minQueriesPerIngester {
					minQueriesPerIngester = total
				}
				if total > maxQueriesPerIngester {
					maxQueriesPerIngester = total
				}
			}

			if minQueriesPerIngester < 0 {
				minQueriesPerIngester = 0
			}

			log.Printf("Stats - Total ingesters: %d Successes: %d Failures: %d - Min / max queries per ingester: %d / %d", len(stats), successes, failures, minQueriesPerIngester, maxQueriesPerIngester)
			statsMx.Unlock()

			// Throttle.
			time.Sleep(5 * time.Second)
		}
	}()

	wg.Wait()
}

func queryIngesterAndCheckMatchersCorrectness(ctx context.Context, addr string, from, to model.Time, matchers ...*labels.Matcher) error {
	fetchedSeries, err := queryIngester(ctx, addr, from, to, matchers...)
	if err != nil {
		return err
	}

	for _, series := range fetchedSeries {
		seriesLabels := mimirpb.FromLabelAdaptersToLabels(series.Labels)

		// Ensure the matchers match all series labels.
		for _, m := range matchers {
			val := seriesLabels.Get(m.Name)
			if !m.Matches(val) {
				return fmt.Errorf("received series %s but it doesn't match the matcher %s", seriesLabels.String(), m.String())
			}
		}
	}

	return nil
}

func queryIngester(ctx context.Context, addr string, from, to model.Time, matchers ...*labels.Matcher) (_ map[string]ingester_client.TimeSeriesChunk, returnErr error) {
	req, err := ingester_client.ToQueryRequest(from, to, matchers)
	if err != nil {
		return nil, err
	}

	// To keep it simple, create a gRPC client each time.
	clientMetrics := ingester_client.NewMetrics(nil)
	clientConfig := ingester_client.Config{}
	flagext.DefaultValues(&clientConfig)

	client, err := ingester_client.MakeIngesterClient(ring.InstanceDesc{Addr: addr}, clientConfig, clientMetrics)
	if err != nil {
		return nil, err
	}

	// Ensure to close the client once done.
	defer func() {
		if closeErr := client.Close(); closeErr != nil && returnErr == nil {
			returnErr = closeErr
		}
	}()

	stream, err := client.QueryStream(ctx, req)
	if err != nil {
		return nil, err
	}

	// Fetch all series.
	fetchedSeries := make(map[string]ingester_client.TimeSeriesChunk)

	for {
		resp, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			return nil, err
		}

		if len(resp.Timeseries) > 0 {
			panic("Not expected to receive timeseries")
		} else if len(resp.StreamingSeries) > 0 {
			panic("Not expected to receive streaming series")
		} else if len(resp.Chunkseries) > 0 {
			for _, series := range resp.Chunkseries {
				// Serialize the series labels and use it as map key.
				key := mimirpb.FromLabelAdaptersToString(series.Labels)

				data, ok := fetchedSeries[key]
				if !ok {
					fetchedSeries[key] = series
				} else {
					data = fetchedSeries[key]
					data.Chunks = append(fetchedSeries[key].Chunks, series.Chunks...)
					fetchedSeries[key] = data
				}
			}
		}
	}

	return fetchedSeries, nil
}
