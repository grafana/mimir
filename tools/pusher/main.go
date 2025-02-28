package main

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/golang/snappy"
	"github.com/grafana/dskit/httpgrpc"
	"github.com/grafana/dskit/user"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/grafana/mimir/pkg/mimirpb"
)

func main() {
	wg := sync.WaitGroup{}
	const workers = 10

	conn, err := grpc.Dial("localhost:12000", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		panic(err)
	}
	grpcClient := httpgrpc.NewHTTPClient(conn)

	for i := 0; i < workers; i++ {
		wg.Add(1)

		go func(workerID int) {
			defer wg.Done()

			ctx := user.InjectOrgID(context.Background(), "anonymous")

			for {
				req := &mimirpb.WriteRequest{
					Timeseries: []mimirpb.PreallocTimeseries{
						{
							TimeSeries: &mimirpb.TimeSeries{
								Labels:  []mimirpb.LabelAdapter{{Name: "__name__", Value: fmt.Sprintf("test_series_%d", workerID)}},
								Samples: []mimirpb.Sample{{TimestampMs: time.Now().UnixMilli(), Value: float64(workerID)}},
							},
						},
					},
					Source:                   0,
					Metadata:                 nil,
					SkipLabelValidation:      false,
					SkipLabelCountValidation: false,
				}

				reqData, err := req.Marshal()
				if err != nil {
					panic(err)
				}

				reqDataCompressed := snappy.Encode(reqData, nil)

				httpReq := httpgrpc.HTTPRequest{
					Method: http.MethodPost,
					Url:    "/api/v1/push",
					Body:   reqDataCompressed,
					Headers: []*httpgrpc.Header{
						{Key: "Content-Encoding", Values: []string{"snappy"}},
						{Key: "Content-Type", Values: []string{"application/x-protobuf"}},
						{Key: "X-Prometheus-Remote-Write-Version", Values: []string{"0.1.0"}},
					},
				}

				// Make gRPC request
				start := time.Now()
				res, err := grpcClient.Handle(ctx, &httpReq)

				if err == nil {
					fmt.Println("Worker:", workerID, "Push status:", res.Code, "err", err, "elapsed:", time.Since(start))
				} else {
					fmt.Println("Worker:", workerID, "Push err:", "elapsed:", time.Since(start))
				}

				// Throttle in case of errors.
				if err != nil {
					time.Sleep(500 * time.Millisecond)
				}
			}
		}(i)
	}

	wg.Wait()
}
