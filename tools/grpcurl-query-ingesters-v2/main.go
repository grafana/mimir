package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os/exec"
	"sync"

	"github.com/grafana/dskit/flagext"
	"github.com/grafana/dskit/ring"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"

	ingester_client "github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/mimirpb"
)

func main() {
	var port int
	flag.IntVar(&port, "port", 80, "Port to port-forward.")
	var pods flagext.StringSlice
	flag.Var(&pods, "pod", "Pod to query, can be provided multiple times.")
	var namespace string
	flag.StringVar(&namespace, "namespace", "", "Namespace of the pods.")
	flag.Parse()

	if namespace == "" {
		log.Fatal("namespace is required")
	}

	if len(pods) == 0 {
		log.Fatal("at least one pod is required")
	}

	process := func(pod string, localPort int) {

		// Send HTTP GET request to /metrics
		url := fmt.Sprintf("http://localhost:%d/metrics", localPort)
		resp, err := http.Get(url)
		if err != nil {
			log.Printf("failed to GET %s: %v", url, err)
			return
		}
		defer resp.Body.Close()

		// Read and process the response
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			log.Printf("failed to read response: %v", err)
			return
		}
		fmt.Printf("Response from pod %s in namespace %s:\n%s\n", pod, namespace, string(body))
	}

	// WaitGroup to wait for all goroutines to finish
	var wg sync.WaitGroup

	for _, pod := range pods {
		wg.Add(1)
		go func(pod string) {
			defer wg.Done()
			if err := processPod(pod, namespace, port, process); err != nil {
				log.Printf("Error processing pod %s in namespace %s: %v", pod, namespace, err)
			}
		}(pod)
	}

	wg.Wait()
}

// processPod handles port-forwarding, HTTP request, and cleanup for a single pod
func processPod(pod string, namespace string, podPort int, process func(pod string, localPort int)) error {
	localPort, err := getFreePort()
	if err != nil {
		return fmt.Errorf("failed to get free port: %v", err)
	}

	// Start port-forwarding
	cmd := exec.Command("kubectl", "port-forward", "--namespace", namespace,
		fmt.Sprintf("pod/%s", pod), fmt.Sprintf("%d:%d", localPort, podPort))

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return fmt.Errorf("failed to get stdout pipe: %v", err)
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return fmt.Errorf("failed to get stderr pipe: %v", err)
	}

	if err := cmd.Start(); err != nil {
		return fmt.Errorf("failed to start kubectl port-forward: %v", err)
	}

	defer cmd.Process.Kill()

	// Wait for port-forward to be ready
	if err := waitForPortForward(stdout, stderr); err != nil {
		return fmt.Errorf("port-forward failed: %v", err)
	}

	process(pod, localPort)

	// Stop the port-forward
	if err := cmd.Process.Kill(); err != nil {
		return fmt.Errorf("failed to kill port-forward process: %v", err)
	}

	return nil
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
