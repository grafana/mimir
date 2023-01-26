package aggregator

import (
	"context"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/services"
	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/weaveworks/common/user"
)

type Push func(ctx context.Context, req *mimirpb.WriteRequest) (*mimirpb.WriteResponse, error)

type Batcher struct {
	services.Service
	cfg                Config
	logger             log.Logger
	shutdownCh         chan struct{}
	aggregationResults chan aggregationResultWithUser
	userBatches        userBatches
	push               Push
}

type aggregationResultWithUser struct {
	user   string
	result aggregationResult
}

type aggregationResult struct {
	aggregatedLabels string
	sample           mimirpb.Sample
}

func NewBatcher(cfg Config, push Push, logger log.Logger) *Batcher {
	b := &Batcher{
		cfg:                cfg,
		shutdownCh:         make(chan struct{}),
		logger:             logger,
		aggregationResults: make(chan aggregationResultWithUser, cfg.ResultChanSize),
		userBatches:        newUserBatches(),
		push:               push,
	}
	b.Service = services.NewBasicService(b.starting, b.running, b.stopping)
	return b
}

func (b *Batcher) starting(ctx context.Context) error {
	go b.run()

	return nil
}

func (b *Batcher) running(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return nil
	case <-b.shutdownCh:
		return nil
	}
}

func (b *Batcher) stopping(_ error) error {
	close(b.shutdownCh)
	return nil
}

func (b *Batcher) run() {
	ticker := time.NewTicker(b.cfg.FlushInterval)
	for {
		select {
		case result := <-b.aggregationResults:
			err := b.userBatches.addResult(result)
			if err != nil {
				level.Error(b.logger).Log("msg", "failed to handle aggregation result", "err", err, "result", result)
			}
		case <-ticker.C:
			b.flush()
		case <-b.shutdownCh:
			ticker.Stop()
			return
		}
	}
}

func (b *Batcher) flush() {
	for _, username := range b.userBatches.users() {
		ctx := user.InjectOrgID(context.Background(), username)
		writeReq := b.userBatches.writeRequest(username)
		if writeReq == nil {
			level.Info(b.logger).Log("msg", "skipping empty request", "user", username)
			continue
		}
		level.Info(b.logger).Log("msg", "pushing request", "req", writeReq, "user", username)
		_, err := b.push(ctx, writeReq)
		if err != nil {
			level.Error(b.logger).Log("msg", "failed to push aggregation result", "err", err)
		}
		mimirpb.ReuseSlice(writeReq.Timeseries)
	}
}

func (b *Batcher) AddSample(user, aggregatedLabels string, sample mimirpb.Sample) {
	b.aggregationResults <- aggregationResultWithUser{
		user: user,
		result: aggregationResult{
			aggregatedLabels: aggregatedLabels,
			sample:           sample,
		},
	}
}

type userBatches map[string]*batch

func newUserBatches() userBatches {
	return make(userBatches)
}

func (u userBatches) users() []string {
	users := make([]string, 0, len(u))
	for user := range u {
		users = append(users, user)
	}
	return users
}

func (u userBatches) writeRequest(user string) *mimirpb.WriteRequest {
	return u[user].writeReq(user)
}

func (u userBatches) addResult(result aggregationResultWithUser) error {
	userBatch, ok := u[result.user]
	if !ok {
		userBatch = &batch{}
	}
	err := userBatch.addResult(result.result)
	u[result.user] = userBatch
	return err
}

type batch struct {
	labels  []labels.Labels
	samples []mimirpb.Sample
}

func (b *batch) addResult(result aggregationResult) error {
	lbls, err := parser.ParseMetric(result.aggregatedLabels)
	if err != nil {
		return errors.Wrapf(err, "failed to parse aggregated labels %s", result.aggregatedLabels)
	}

	b.labels = append(b.labels, lbls)
	b.samples = append(b.samples, result.sample)

	return nil
}

func (b *batch) writeReq(user string) *mimirpb.WriteRequest {
	if len(b.samples) == 0 {
		return nil
	}

	req := &mimirpb.WriteRequest{
		Timeseries: mimirpb.PreallocTimeseriesSliceFromPool(),
		Source:     mimirpb.AGGREGATOR,
	}

	for i, s := range b.samples {
		ts := mimirpb.TimeseriesFromPool()
		ts.Labels = ts.Labels[:0]
		ts.Samples = ts.Samples[:0]

		// Copy labels because we want to reuse b.labels.
		for _, label := range b.labels[i] {
			ts.Labels = append(ts.Labels, mimirpb.LabelAdapter{
				Name:  label.Name,
				Value: label.Value,
			})
		}
		ts.Samples = append(ts.Samples, s)

		req.Timeseries = append(req.Timeseries, mimirpb.PreallocTimeseries{TimeSeries: ts})
	}

	b.labels = b.labels[:0]
	b.samples = b.samples[:0]

	return req
}
