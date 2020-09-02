package commands

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/prometheus/client_golang/api"
	v1 "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/prometheus/common/config"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/prompb"
	"github.com/prometheus/prometheus/storage/remote"
	"github.com/sirupsen/logrus"
	"gopkg.in/alecthomas/kingpin.v2"
)

var writeRequestDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
	Name:    "write_request_duration_seconds",
	Buckets: prometheus.DefBuckets,
}, []string{"success"})

var queryRequestDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
	Name:    "query_request_duration_seconds",
	Buckets: prometheus.DefBuckets,
}, []string{"success"})

type LoadgenCommand struct {
	writeURL       string
	activeSeries   int
	scrapeInterval time.Duration
	parallelism    int
	batchSize      int
	writeTimeout   time.Duration

	queryURL         string
	query            string
	queryParallelism int
	queryTimeout     time.Duration
	queryDuration    time.Duration

	metricsListenAddress string

	// Runtime stuff.
	wg          sync.WaitGroup
	writeClient remote.WriteClient
	queryClient v1.API
}

func (c *LoadgenCommand) Register(app *kingpin.Application) {
	loadgenCommand := &LoadgenCommand{}
	cmd := app.Command("loadgen", "Simple load generator for Cortex.").Action(loadgenCommand.run)
	cmd.Flag("write-url", "").
		Required().StringVar(&loadgenCommand.writeURL)
	cmd.Flag("active-series", "number of active series to send").
		Default("1000").IntVar(&loadgenCommand.activeSeries)
	cmd.Flag("scrape-interval", "period to send metrics").
		Default("15s").DurationVar(&loadgenCommand.scrapeInterval)
	cmd.Flag("parallelism", "how many metrics to send simultaneously").
		Default("10").IntVar(&loadgenCommand.parallelism)
	cmd.Flag("batch-size", "how big a batch to send").
		Default("100").IntVar(&loadgenCommand.batchSize)
	cmd.Flag("write-timeout", "timeout for write requests").
		Default("500ms").DurationVar(&loadgenCommand.writeTimeout)

	cmd.Flag("query-url", "").
		Required().StringVar(&loadgenCommand.queryURL)
	cmd.Flag("query", "query to run").
		Default("sum(node_cpu_seconds_total)").StringVar(&loadgenCommand.query)
	cmd.Flag("query-parallelism", "number of queries to run in parallel").
		Default("10").IntVar(&loadgenCommand.queryParallelism)
	cmd.Flag("query-timeout", "").
		Default("20s").DurationVar(&loadgenCommand.queryTimeout)
	cmd.Flag("query-duration", "length of query").
		Default("1h").DurationVar(&loadgenCommand.queryDuration)

	cmd.Flag("metrics-listen-address", "address to serve metrics on").
		Default(":8080").StringVar(&loadgenCommand.metricsListenAddress)
}

func (c *LoadgenCommand) run(k *kingpin.ParseContext) error {
	writeURL, err := url.Parse(c.writeURL)
	if err != nil {
		return err
	}

	writeClient, err := remote.NewWriteClient("loadgen", &remote.ClientConfig{
		URL:     &config.URL{URL: writeURL},
		Timeout: model.Duration(c.writeTimeout),
	})
	if err != nil {
		return err
	}
	c.writeClient = writeClient

	queryClient, err := api.NewClient(api.Config{
		Address: c.queryURL,
	})
	if err != nil {
		return err
	}
	c.queryClient = v1.NewAPI(queryClient)

	http.Handle("/metrics", promhttp.Handler())
	go func() {
		err := http.ListenAndServe(c.metricsListenAddress, nil)
		if err != nil {
			logrus.WithError(err).Errorln("metrics listener failed")
		}
	}()

	c.wg.Add(c.parallelism)
	c.wg.Add(c.queryParallelism)

	metricsPerShard := c.activeSeries / c.parallelism
	for i := 0; i < c.activeSeries; i += metricsPerShard {
		go c.runWriteShard(i, i+metricsPerShard)
	}

	for i := 0; i < c.queryParallelism; i++ {
		go c.runQueryShard()
	}

	c.wg.Wait()
	return nil
}

func (c *LoadgenCommand) runWriteShard(from, to int) {
	defer c.wg.Done()
	ticker := time.NewTicker(c.scrapeInterval)
	c.runScrape(from, to)
	for range ticker.C {
		c.runScrape(from, to)
	}
}

func (c *LoadgenCommand) runScrape(from, to int) {
	for i := from; i < to; i += c.batchSize {
		if err := c.runBatch(i, i+c.batchSize); err != nil {
			log.Printf("error sending batch: %v", err)
		}
	}
	fmt.Printf("sent %d samples\n", to-from)
}

func (c *LoadgenCommand) runBatch(from, to int) error {
	var (
		req = prompb.WriteRequest{
			Timeseries: make([]prompb.TimeSeries, 0, to-from),
		}
		now = time.Now().UnixNano() / int64(time.Millisecond)
	)

	for i := from; i < to; i++ {
		timeseries := prompb.TimeSeries{
			Labels: []prompb.Label{
				{Name: "__name__", Value: "node_cpu_seconds_total"},
				{Name: "job", Value: "node_exporter"},
				{Name: "instance", Value: fmt.Sprintf("instance%000d", i)},
				{Name: "cpu", Value: "0"},
				{Name: "mode", Value: "idle"},
			},
			Samples: []prompb.Sample{{
				Timestamp: now,
				Value:     rand.Float64(),
			}},
		}
		req.Timeseries = append(req.Timeseries, timeseries)
	}

	data, err := proto.Marshal(&req)
	if err != nil {
		return err
	}

	compressed := snappy.Encode(nil, data)

	start := time.Now()
	if err := c.writeClient.Store(context.Background(), compressed); err != nil {
		writeRequestDuration.WithLabelValues("error").Observe(time.Since(start).Seconds())
		return err
	}
	writeRequestDuration.WithLabelValues("success").Observe(time.Since(start).Seconds())

	return nil
}

func (c *LoadgenCommand) runQueryShard() {
	defer c.wg.Done()
	for {
		c.runQuery()
	}
}

func (c *LoadgenCommand) runQuery() {
	ctx, cancel := context.WithTimeout(context.Background(), c.queryTimeout)
	defer cancel()
	r := v1.Range{
		Start: time.Now().Add(-c.queryDuration),
		End:   time.Now(),
		Step:  time.Minute,
	}
	start := time.Now()
	_, _, err := c.queryClient.QueryRange(ctx, c.query, r)
	if err != nil {
		queryRequestDuration.WithLabelValues("error").Observe(time.Since(start).Seconds())
		log.Printf("error doing query: %v", err)
		return
	}
	queryRequestDuration.WithLabelValues("success").Observe(time.Since(start).Seconds())
}
