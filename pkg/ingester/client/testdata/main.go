package main

import (
	"context"
	"flag"
	"strconv"
	"strings"
	"sync"

	"github.com/grafana/dskit/grpcclient"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/weaveworks/common/user"

	"github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/mimirpb"
)

func main() {
	flag.Parse()
	address := flag.Arg(0)
	numRequests, err := strconv.Atoi(flag.Arg(1))
	if err != nil {
		panic(err)
	}
	client, err := client.MakeIngesterClient(address, client.Config{GRPCClientConfig: grpcclient.Config{MaxSendMsgSize: 100000000}}, client.NewMetrics(prometheus.NewRegistry()))
	if err != nil {
		panic(err)
	}
	defer client.Close()

	writeRequest := &mimirpb.WriteRequest{
		Timeseries: mimirpb.PreallocTimeseriesSliceFromPool(),
	}
	writeRequest.Timeseries = writeRequest.Timeseries[:cap(writeRequest.Timeseries)]
	for j := range writeRequest.Timeseries {
		writeRequest.Timeseries[j].TimeSeries = &mimirpb.TimeSeries{
			Labels:  mimirpb.FromLabelsToLabelAdapters(labels.FromStrings(strings.Repeat("a", 20), strings.Repeat("b", 40))),
			Samples: make([]mimirpb.Sample, 1),
		}
	}
	wg := &sync.WaitGroup{}
	ctx, err := user.InjectIntoGRPCRequest(user.InjectOrgID(context.Background(), "test"))
	if err != nil {
		panic(err)
	}
	for i := 0; i < numRequests; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := client.Push(ctx, writeRequest)
			if err == nil {
				panic("error is nil")
			} else if !strings.Contains(err.Error(), "not implemented") {
				panic("error doesn't contain 'not implemented': " + err.Error())
			}
		}()
	}
	wg.Wait()
}
