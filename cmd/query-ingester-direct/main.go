package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"time"

	"github.com/grafana/dskit/user"
	"github.com/grafana/mimir/pkg/ingester/client"
	"github.com/prometheus/prometheus/promql/parser"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	var (
		tenantId string
		metricSelector string
	)

	flag.StringVar(&tenantId, "tenantId", "", "Tenant ID")
	flag.StringVar(&metricSelector, "metricSelector", "", "Metric selector")
	flag.Parse()

	if tenantId == "" {
		log.Fatalf("tenantId is required")
		os.Exit(2)
	}
	if metricSelector == "" {
		log.Fatalf("metricSelector is required")
		os.Exit(2)
	}

	var opts []grpc.DialOption
	opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))

	conn, err := grpc.Dial("localhost:9095", opts...)
	if err != nil {
		log.Fatalf("failed to dial: %v", err)
	}
	fmt.Println("Connected to Mimir")

	ingester := client.NewIngesterClient(conn)
	fmt.Println("Created client")

	matchers, err := parser.ParseMetricSelector(metricSelector)
	if err != nil {
		log.Fatalf("failed to parse selector: %v", err)
	}
	//fmt.Printf("Parsed selector: %v\n", matchers)

	clientMatchers, err := client.ToLabelMatchers(matchers)
	if err != nil {
		log.Fatalf("failed to convert matchers: %v", err)
	}

	now := time.Now().UnixMilli()
	req := client.QueryRequest{
		StartTimestampMs: now - 30*1000,
		EndTimestampMs: now,
		Matchers: clientMatchers,
	}
	req.StreamingChunksBatchSize = 0

	ctx := context.Background()
	ctx = user.InjectOrgID(ctx, tenantId)
	ctx, err = user.InjectIntoGRPCRequest(ctx)
	if err != nil {
		log.Fatalf("failed to inject orgID: %v", err)
	}

	streamClient, err := ingester.QueryStream(ctx, &req)
	if err != nil {
		log.Fatalf("failed to query: %v", err)
	}

	exitCode := 0

	for {
		resp, err := streamClient.Recv()
		if errors.Is(err, io.EOF) {
			fmt.Println("EOF")
			break
		}
		if err != nil {
			log.Fatalf("failed to receive: %v", err)
		}

		fmt.Printf("Timeseries: %v Chunks: %v StreamSeries: %v IsEndofSeries: %v\n", len(resp.Timeseries), len(resp.Chunkseries), len(resp.StreamingSeries), resp.IsEndOfSeriesStream)
		for _, chk := range resp.Chunkseries {
			//fmt.Printf("Labels: %v\n", chk.Labels)
			for _, label := range chk.Labels {
				if label.Name == "environment" && (label.Value == "test" || label.Value == "stage") {
					fmt.Printf("Found unexpected labels: %v\n", chk.Labels)
					exitCode = 1
				}
			}
		}
	}
	os.Exit(exitCode)
	defer conn.Close()
}
