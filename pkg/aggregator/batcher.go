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
		shutdownCh:         make(chan struct{}),
		logger:             logger,
		aggregationResults: make(chan aggregationResultWithUser, cfg.ResultChanSize),
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
				level.Error(b.logger).Log("msg", "failed to handle aggregation result", "err", err)
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
	userBatch := u[result.user]
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
	req := mimirpb.ToWriteRequest(b.labels, b.samples, nil, nil, mimirpb.API)
	b.labels = b.labels[:0]
	b.samples = b.samples[:0]
	return req
}
