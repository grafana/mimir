package bench

import (
	"context"
	"flag"
	"math/rand"
	"net/http"
	"sync"
	"time"

	"github.com/grafana/mimir/pkg/util/spanlogger"
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/api"
	v1 "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	config_util "github.com/prometheus/common/config"
	"github.com/thanos-io/thanos/pkg/discovery/dns"
	"github.com/thanos-io/thanos/pkg/extprom"
)

type QueryConfig struct {
	Enabled           bool   `yaml:"enabled"`
	Endpoint          string `yaml:"endpoint"`
	BasicAuthUsername string `yaml:"basic_auth_username"`
	BasicAuthPasword  string `yaml:"basic_auth_password"`
}

func (cfg *QueryConfig) RegisterFlags(f *flag.FlagSet) {
	f.BoolVar(&cfg.Enabled, "bench.query.enabled", false, "enable query benchmarking")
	f.StringVar(&cfg.Endpoint, "bench.query.endpoint", "", "Remote query endpoint.")
	f.StringVar(&cfg.BasicAuthUsername, "bench.query.basic-auth-username", "", "Set the basic auth username on remote query requests.")
	f.StringVar(&cfg.BasicAuthPasword, "bench.query.basic-auth-password", "", "Set the basic auth password on remote query requests.")
}

type queryRunner struct {
	id         string
	tenantName string
	cfg        QueryConfig

	// Do DNS client side load balancing if configured
	dnsProvider *dns.Provider
	addressMtx  sync.Mutex
	addresses   []string
	clientPool  map[string]v1.API

	workload *queryWorkload

	reg    prometheus.Registerer
	logger log.Logger

	requestDuration *prometheus.HistogramVec
}

func newQueryRunner(id string, tenantName string, cfg QueryConfig, workload *queryWorkload, logger log.Logger, reg prometheus.Registerer) (*queryRunner, error) {
	runner := &queryRunner{
		id:         id,
		tenantName: tenantName,
		cfg:        cfg,

		workload:   workload,
		clientPool: map[string]v1.API{},
		dnsProvider: dns.NewProvider(
			logger,
			extprom.WrapRegistererWithPrefix("benchtool_query_", reg),
			dns.GolangResolverType,
		),

		logger: logger,
		reg:    reg,
		requestDuration: promauto.With(reg).NewHistogramVec(
			prometheus.HistogramOpts{
				Namespace: "benchtool",
				Name:      "query_request_duration_seconds",
				Buckets:   []float64{0.001, 0.01, 0.1, 0.3, 0.6, 1, 3, 6, 9, 20, 30, 40, 50, 60, 70, 80, 90, 100, 110, 120, 200},
			},
			[]string{"code", "type"},
		),
	}

	return runner, nil
}

// jitterUp adds random jitter to the duration.
//
// This adds or subtracts time from the duration within a given jitter fraction.
// For example for 10s and jitter 0.1, it will return a time within [9s, 11s])
//
// Reference: https://godoc.org/github.com/grpc-ecosystem/go-grpc-middleware/util/backoffutils
func jitterUp(duration time.Duration, jitter float64) time.Duration {
	multiplier := jitter * (rand.Float64()*2 - 1)
	return time.Duration(float64(duration) * (1 + multiplier))
}

func (q *queryRunner) Run(ctx context.Context) error {
	go q.resolveAddrsLoop(ctx)

	queryChan := make(chan query, 1000)
	for i := 0; i < 100; i++ {
		go q.queryWorker(queryChan)
	}
	for _, queryReq := range q.workload.queries {
		// every query has a ticker and a Go loop...
		// not sure if this is a good idea but it should be fine
		go func(req query) {
			// issue the initial query with a jitter
			firstWait := jitterUp(req.interval, 0.4)
			time.Sleep(firstWait)
			queryChan <- req

			ticker := time.NewTicker(req.interval)
			defer ticker.Stop()
			for {
				select {
				case <-ticker.C:
					queryChan <- req
				case <-ctx.Done():
					return
				}
			}
		}(queryReq)
	}
	for {
		<-ctx.Done()
		close(queryChan)
		return nil

	}
}

func (q *queryRunner) queryWorker(queryChan chan query) {
	for queryReq := range queryChan {
		err := q.executeQuery(context.Background(), queryReq)
		if err != nil {
			level.Warn(q.logger).Log("msg", "unable to execute query", "err", err)
		}
	}
}

type tenantIDRoundTripper struct {
	tenantName string
	next       http.RoundTripper
}

func (r *tenantIDRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	if r.tenantName != "" {
		req.Header.Set("X-Scope-OrgID", r.tenantName)
	}
	return r.next.RoundTrip(req)
}

func newQueryClient(url, tenantName, username, password string) (v1.API, error) {
	apiClient, err := api.NewClient(api.Config{
		Address: url,
		RoundTripper: &tenantIDRoundTripper{
			tenantName: tenantName,
			next:       config_util.NewBasicAuthRoundTripper(username, config_util.Secret(password), "", api.DefaultRoundTripper),
		},
	})

	if err != nil {
		return nil, err
	}
	return v1.NewAPI(apiClient), nil
}

func (q *queryRunner) getRandomAPIClient() (v1.API, error) {
	q.addressMtx.Lock()
	defer q.addressMtx.Unlock()

	if len(q.addresses) == 0 {
		return nil, errors.New("no addresses found")
	}

	randomIndex := rand.Intn(len(q.addresses))
	pick := q.addresses[randomIndex]

	var cli v1.API
	var exists bool
	var err error

	if cli, exists = q.clientPool[pick]; !exists {
		cli, err = newQueryClient("http://"+pick+"/prometheus", q.tenantName, q.cfg.BasicAuthUsername, q.cfg.BasicAuthPasword)
		if err != nil {
			return nil, err
		}
		q.clientPool[pick] = cli
	}

	return cli, nil
}

func (q *queryRunner) executeQuery(ctx context.Context, queryReq query) error {
	spanLog, ctx := spanlogger.New(ctx, "queryRunner.executeQuery")
	defer spanLog.Span.Finish()
	apiClient, err := q.getRandomAPIClient()
	if err != nil {
		return err
	}

	// Create a timestamp for use when creating the requests and observing latency
	now := time.Now()

	var (
		queryType string = "instant"
		status    string = "success"
	)
	if queryReq.timeRange > 0 {
		queryType = "range"
		level.Debug(q.logger).Log("msg", "sending range query", "expr", queryReq.expr, "range", queryReq.timeRange)
		r := v1.Range{
			Start: now.Add(-queryReq.timeRange),
			End:   now,
			Step:  time.Minute,
		}
		_, _, err = apiClient.QueryRange(ctx, queryReq.expr, r)
	} else {
		level.Debug(q.logger).Log("msg", "sending instant query", "expr", queryReq.expr)
		_, _, err = apiClient.Query(ctx, queryReq.expr, now)
	}

	if err != nil {
		status = "failure"
	}

	q.requestDuration.WithLabelValues(status, queryType).Observe(time.Since(now).Seconds())
	return err
}

func (q *queryRunner) resolveAddrsLoop(ctx context.Context) {
	err := q.resolveAddrs()
	if err != nil {
		level.Warn(q.logger).Log("msg", "failed update remote write servers list", "err", err)
	}
	ticker := time.NewTicker(time.Minute * 5)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			err := q.resolveAddrs()
			if err != nil {
				level.Warn(q.logger).Log("msg", "failed update remote write servers list", "err", err)
			}
		case <-ctx.Done():
			return
		}
	}
}

func (q *queryRunner) resolveAddrs() error {
	// Resolve configured addresses with a reasonable timeout.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// If some of the dns resolution fails, log the error.
	if err := q.dnsProvider.Resolve(ctx, []string{q.cfg.Endpoint}); err != nil {
		level.Error(q.logger).Log("msg", "failed to resolve addresses", "err", err)
	}

	// Fail in case no server address is resolved.
	servers := q.dnsProvider.Addresses()
	if len(servers) == 0 {
		return errors.New("no server address resolved")
	}

	q.addressMtx.Lock()
	q.addresses = servers
	q.addressMtx.Unlock()

	return nil
}
