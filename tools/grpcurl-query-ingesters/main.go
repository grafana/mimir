// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"net"
	"os"
	"os/exec"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/alecthomas/kingpin/v2"
	"github.com/prometheus/prometheus/model/labels"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"

	"github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/mimirpb"
)

const (
	goroutineStartDelay = 250 * time.Millisecond
	minPort             = 16384
	maxPortRange        = 49152 - 16384
	defaultTimeout      = 30 * time.Second
	ingesterPort        = 9095
)

func main() {
	app := kingpin.New("grpcurl-query-ingesters", "Query Kubernetes ingesters and dump chunk content")

	k8sContext := app.Arg("context", "Kubernetes context").Required().String()
	k8sNamespace := app.Arg("namespace", "Kubernetes namespace").Required().String()
	tenantID := app.Arg("tenant-id", "Mimir tenant ID").Required().String()
	queryFile := app.Arg("query-file", "JSON query file").Required().String()

	kingpin.MustParse(app.Parse(os.Args[1:]))

	if err := queryAndDumpIngesters(*k8sContext, *k8sNamespace, *tenantID, *queryFile); err != nil {
		fmt.Printf("Error: %v\n", err)
		os.Exit(1)
	}
}

func queryAndDumpIngesters(k8sContext, k8sNamespace, tenantID, queryFile string) error {
	queryReq, err := loadQueryRequest(queryFile)
	if err != nil {
		return fmt.Errorf("failed to load query request: %w", err)
	}

	pods, err := getIngesterPods(k8sContext, k8sNamespace)
	if err != nil {
		return fmt.Errorf("failed to get ingester pods: %w", err)
	}

	fmt.Printf("Found %d ingester pods\n", len(pods))

	var wg sync.WaitGroup
	var failures []string
	failuresMutex := &sync.Mutex{}

	for _, pod := range pods {
		wg.Add(1)
		go func(podName string) {
			defer wg.Done()

			fmt.Printf("\n=== Querying %s ===\n", podName)
			err := queryAndDumpIngester(k8sContext, k8sNamespace, podName, tenantID, queryReq)
			if err != nil {
				printFailure(fmt.Sprintf("Failed to query %s: %v", podName, err))
				failuresMutex.Lock()
				failures = append(failures, podName)
				failuresMutex.Unlock()
			} else {
				printSuccess(fmt.Sprintf("Successfully queried %s", podName))
			}
		}(pod)

		time.Sleep(goroutineStartDelay)
	}

	wg.Wait()

	if len(failures) > 0 {
		fmt.Printf("\nFailed to query %d ingesters:\n", len(failures))
		for _, failure := range failures {
			fmt.Printf("- %s\n", failure)
		}
		return fmt.Errorf("failed to query %d out of %d ingesters", len(failures), len(pods))
	}

	printSuccess("\nSuccessfully queried all ingesters")
	return nil
}

func loadQueryRequest(queryFile string) (*client.QueryRequest, error) {
	data, err := os.ReadFile(queryFile)
	if err != nil {
		return nil, err
	}

	var queryData struct {
		StartTimestampMs int64 `json:"start_timestamp_ms"`
		EndTimestampMs   int64 `json:"end_timestamp_ms"`
		Matchers         []struct {
			Type  int    `json:"type"`
			Name  string `json:"name"`
			Value string `json:"value"`
		} `json:"matchers"`
	}

	if err := json.Unmarshal(data, &queryData); err != nil {
		return nil, err
	}

	req := &client.QueryRequest{
		StartTimestampMs: queryData.StartTimestampMs,
		EndTimestampMs:   queryData.EndTimestampMs,
	}

	for _, m := range queryData.Matchers {
		req.Matchers = append(req.Matchers, &client.LabelMatcher{
			Type:  client.MatchType(m.Type),
			Name:  m.Name,
			Value: m.Value,
		})
	}

	return req, nil
}

func queryAndDumpIngester(k8sContext, k8sNamespace, podName, tenantID string, queryReq *client.QueryRequest) error {
	// Set up port forwarding
	localPort := minPort + rand.Intn(maxPortRange)
	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()

	portForwardCmd := exec.CommandContext(ctx, "kubectl", "port-forward",
		"--context", k8sContext, "-n", k8sNamespace, podName,
		fmt.Sprintf("%d:%d", localPort, ingesterPort))

	err := portForwardCmd.Start()
	if err != nil {
		return fmt.Errorf("failed to start port-forward: %w", err)
	}

	defer func() {
		if portForwardCmd.Process != nil {
			_ = portForwardCmd.Process.Kill()
			_ = portForwardCmd.Wait()
		}
	}()

	// Wait for port forward to be ready
	if err := waitForPortForward(localPort, 10*time.Second); err != nil {
		return fmt.Errorf("port-forward not ready: %w", err)
	}

	// Connect to ingester
	conn, err := grpc.NewClient(fmt.Sprintf("localhost:%d", localPort),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("failed to connect to ingester: %w", err)
	}
	defer conn.Close()

	// Create client and query
	ingesterClient := client.NewIngesterClient(conn)

	// Add tenant ID to context
	queryCtx := metadata.AppendToOutgoingContext(ctx, "X-Scope-OrgID", tenantID)

	stream, err := ingesterClient.QueryStream(queryCtx, queryReq)
	if err != nil {
		return fmt.Errorf("failed to create query stream: %w", err)
	}

	// Process responses and dump immediately
	agg := &responseAggregator{}
	for {
		resp, err := stream.Recv()
		if err != nil {
			if err.Error() == "EOF" {
				break
			}
			return fmt.Errorf("failed to receive response: %w", err)
		}

		agg.addResponse(resp)
	}

	return nil
}

func waitForPortForward(port int, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", fmt.Sprintf("localhost:%d", port), time.Second)
		if err == nil {
			conn.Close()
			return nil
		}
		time.Sleep(100 * time.Millisecond)
	}
	return fmt.Errorf("port-forward not ready after %v", timeout)
}

// responseAggregator collects streaming series and their chunks across multiple responses.
type responseAggregator struct {
	series []client.QueryStreamSeries
}

func (a *responseAggregator) addResponse(resp *client.QueryStreamResponse) {
	// Collect series metadata
	a.series = append(a.series, resp.StreamingSeries...)

	// Dump chunks as they arrive
	for _, seriesChunks := range resp.StreamingSeriesChunks {
		if int(seriesChunks.SeriesIndex) >= len(a.series) {
			fmt.Printf("Warning: chunk references unknown series index %d\n", seriesChunks.SeriesIndex)
			continue
		}

		series := a.series[seriesChunks.SeriesIndex]
		lbls := labelsFromAdapter(series.Labels)
		fmt.Printf("%s\n", lbls.String())

		for _, chunk := range seriesChunks.Chunks {
			startTime := time.UnixMilli(chunk.StartTimestampMs).UTC()
			endTime := time.UnixMilli(chunk.EndTimestampMs).UTC()

			fmt.Printf("- Chunk: %s - %s\n",
				startTime.Format(time.RFC3339),
				endTime.Format(time.RFC3339))

			// Decode and iterate through chunk data
			if err := dumpChunkSamples(chunk.Data, int(chunk.Encoding)); err != nil {
				fmt.Printf("  Error decoding chunk: %v\n", err)
			}
		}
	}
}

func labelsFromAdapter(adapters []mimirpb.LabelAdapter) labels.Labels {
	lblPairs := make([]labels.Label, 0, len(adapters))
	for _, lbl := range adapters {
		lblPairs = append(lblPairs, labels.Label{
			Name:  lbl.Name,
			Value: lbl.Value,
		})
	}
	slices.SortFunc(lblPairs, func(a, b labels.Label) int {
		return strings.Compare(a.Name, b.Name)
	})
	return labels.New(lblPairs...)
}

func dumpChunkSamples(data mimirpb.UnsafeByteSlice, encoding int) error {
	// This is a simplified version - you might need to implement proper chunk decoding
	// based on the encoding type from the Mimir chunk package
	fmt.Printf("  - Chunk data: %d bytes (encoding: %d)\n", len(data), encoding)
	// TODO: Implement proper chunk sample iteration using Mimir's chunk package
	return nil
}

func getIngesterPods(k8sContext, k8sNamespace string) ([]string, error) {
	cmd := exec.Command("kubectl", "--context", k8sContext, "-n", k8sNamespace,
		"get", "pods", "--no-headers", "-o", "custom-columns=NAME:.metadata.name")
	output, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("failed to get pods: %w", err)
	}

	lines := strings.Split(strings.TrimSpace(string(output)), "\n")
	var ingesterPods []string

	for _, line := range lines {
		if strings.Contains(line, "ingester") {
			ingesterPods = append(ingesterPods, line)
		}
	}

	return ingesterPods, nil
}

func printSuccess(message string) {
	fmt.Printf("\033[0;32m%s\033[0m\n", message)
}

func printFailure(message string) {
	fmt.Printf("\033[0;31m%s\033[0m\n", message)
}
