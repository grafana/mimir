package bench

import (
	"context"
	"flag"
	"math/rand"
	"net/url"
	"strconv"
	"sync"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/common/config"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/prompb"
	"github.com/prometheus/prometheus/storage/remote"
	"github.com/thanos-io/thanos/pkg/discovery/dns"
	"github.com/thanos-io/thanos/pkg/extprom"
)

type WriteBenchConfig struct {
	Enabled           bool   `yaml:"enabled"`
	Endpoint          string `yaml:"endpoint"`
	BasicAuthUsername string `yaml:"basic_auth_username"`
	BasicAuthPasword  string `yaml:"basic_auth_password"`
}

func (cfg *WriteBenchConfig) RegisterFlags(f *flag.FlagSet) {
	f.BoolVar(&cfg.Enabled, "bench.write.enabled", false, "enable write benchmarking")
	f.StringVar(&cfg.Endpoint, "bench.write.endpoint", "", "Remote write endpoint.")
	f.StringVar(&cfg.BasicAuthUsername, "bench.write.basic-auth-username", "", "Set the basic auth username on remote write requests.")
	f.StringVar(&cfg.BasicAuthPasword, "bench.write.basic-auth-password", "", "Set the basic auth password on remote write requests.")
}

type WriteBenchmarkRunner struct {
	id         string
	tenantName string
	cfg        WriteBenchConfig

	// Do DNS client side load balancing if configured
	remoteMtx  sync.Mutex
	addresses  []string
	clientPool map[string]*writeClient

	dnsProvider *dns.Provider

	workload *writeWorkload

	reg    prometheus.Registerer
	logger log.Logger

	requestDuration  *prometheus.HistogramVec
	missedIterations prometheus.Counter
}

func NewWriteBenchmarkRunner(id string, tenantName string, cfg WriteBenchConfig, workload *writeWorkload, logger log.Logger, reg prometheus.Registerer) (*WriteBenchmarkRunner, error) {
	writeBench := &WriteBenchmarkRunner{
		id:         id,
		tenantName: tenantName,
		cfg:        cfg,

		workload: workload,
		dnsProvider: dns.NewProvider(
			logger,
			extprom.WrapRegistererWithPrefix("benchtool_", reg),
			dns.GolangResolverType,
		),
		clientPool: map[string]*writeClient{},
		logger:     logger,
		reg:        reg,
		requestDuration: promauto.With(reg).NewHistogramVec(
			prometheus.HistogramOpts{
				Namespace: "benchtool",
				Name:      "write_request_duration_seconds",
				Buckets:   []float64{0.001, 0.01, 0.1, 0.3, 0.6, 1, 3, 6, 9, 20, 30, 60, 120},
			},
			[]string{"code"},
		),
		missedIterations: promauto.With(reg).NewCounter(
			prometheus.CounterOpts{
				Namespace: "benchtool",
				Name:      "write_iterations_late_total",
				Help:      "Number of write intervals started late because the previous interval did not complete in time.",
			},
		),
	}

	// Resolve an initial set of distributor addresses
	err := writeBench.resolveAddrs()
	if err != nil {
		return nil, errors.Wrap(err, "unable to resolve enpoints")
	}

	return writeBench, nil
}

func (w *WriteBenchmarkRunner) getRandomWriteClient() (*writeClient, error) {
	w.remoteMtx.Lock()
	defer w.remoteMtx.Unlock()

	if len(w.addresses) == 0 {
		return nil, errors.New("no addresses found")
	}
	randomIndex := rand.Intn(len(w.addresses))
	pick := w.addresses[randomIndex]

	var cli *writeClient
	var exists bool

	if cli, exists = w.clientPool[pick]; !exists {
		u, err := url.Parse("http://" + pick + "/api/v1/push")
		if err != nil {
			return nil, err
		}
		cli, err = newWriteClient("bench-"+pick, w.tenantName, &remote.ClientConfig{
			URL:     &config.URL{URL: u},
			Timeout: model.Duration(w.workload.options.Timeout),

			HTTPClientConfig: config.HTTPClientConfig{
				BasicAuth: &config.BasicAuth{
					Username: w.cfg.BasicAuthUsername,
					Password: config.Secret(w.cfg.BasicAuthPasword),
				},
			},
		}, w.logger, w.requestDuration)
		if err != nil {
			return nil, err
		}
		w.clientPool[pick] = cli
	}

	return cli, nil
}

// Run starts a loop that forwards metrics to the configured remote write endpoint
func (w *WriteBenchmarkRunner) Run(ctx context.Context) error {
	// Start a loop to re-resolve addresses every 5 minutes
	go w.resolveAddrsLoop(ctx)

	batchChan := make(chan batchReq, 10)
	for i := 0; i < w.workload.replicas*10; i++ {
		go w.writeWorker(batchChan)
	}

	ticker := time.NewTicker(w.workload.options.Interval)
	for {
		select {
		case <-ctx.Done():
			close(batchChan)
			return nil
		case now := <-ticker.C:
			timeseries := w.workload.generateTimeSeries(w.id, now)
			batchSize := w.workload.options.BatchSize
			var batches [][]prompb.TimeSeries
			if batchSize < len(timeseries) {
				batches = make([][]prompb.TimeSeries, 0, (len(timeseries)+batchSize-1)/batchSize)

				level.Info(w.logger).Log("msg", "sending timeseries", "num_series", strconv.Itoa(len(timeseries)))
				for batchSize < len(timeseries) {
					timeseries, batches = timeseries[batchSize:], append(batches, timeseries[0:batchSize:batchSize])
				}
			} else {
				batches = [][]prompb.TimeSeries{timeseries}
			}

			wg := &sync.WaitGroup{}
			for _, batch := range batches {
				reqBatch := batch
				wg.Add(1)
				batchChan <- batchReq{reqBatch, wg}
			}

			wg.Wait()
			if time.Since(now) > w.workload.options.Interval {
				w.missedIterations.Inc()
			}
		}
	}
}

type batchReq struct {
	batch []prompb.TimeSeries
	wg    *sync.WaitGroup
}

func (w *WriteBenchmarkRunner) writeWorker(batchChan chan batchReq) {
	for batchReq := range batchChan {
		err := w.sendBatch(context.Background(), batchReq.batch)
		if err != nil {
			level.Warn(w.logger).Log("msg", "unable to send batch", "err", err)
		}
		batchReq.wg.Done()
	}
}

func (w *WriteBenchmarkRunner) sendBatch(ctx context.Context, batch []prompb.TimeSeries) error {
	level.Debug(w.logger).Log("msg", "sending timeseries batch", "num_series", strconv.Itoa(len(batch)))
	cli, err := w.getRandomWriteClient()
	if err != nil {
		return errors.Wrap(err, "unable to get remote-write client")
	}
	req := prompb.WriteRequest{
		Timeseries: batch,
	}

	data, err := proto.Marshal(&req)
	if err != nil {
		return errors.Wrap(err, "failed to marshal remote-write request")
	}

	compressed := snappy.Encode(nil, data)

	err = cli.Store(ctx, compressed)

	if err != nil {
		return errors.Wrap(err, "remote-write request failed")
	}

	return nil
}

func (w *WriteBenchmarkRunner) resolveAddrsLoop(ctx context.Context) {
	ticker := time.NewTicker(time.Minute * 5)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			err := w.resolveAddrs()
			if err != nil {
				level.Warn(w.logger).Log("msg", "failed update remote write servers list", "err", err)
			}
		case <-ctx.Done():
			return
		}
	}
}

func (w *WriteBenchmarkRunner) resolveAddrs() error {
	// Resolve configured addresses with a reasonable timeout.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// If some of the dns resolution fails, log the error.
	if err := w.dnsProvider.Resolve(ctx, []string{w.cfg.Endpoint}); err != nil {
		level.Error(w.logger).Log("msg", "failed to resolve addresses", "err", err)
	}

	// Fail in case no server address is resolved.
	servers := w.dnsProvider.Addresses()
	if len(servers) == 0 {
		return errors.New("no server address resolved")
	}

	w.remoteMtx.Lock()
	w.addresses = servers
	w.remoteMtx.Unlock()

	return nil
}
