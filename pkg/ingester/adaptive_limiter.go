package ingester

import (
	"context"

	"github.com/platinummonkey/go-concurrency-limits/core"
	"github.com/platinummonkey/go-concurrency-limits/limit"
	"github.com/platinummonkey/go-concurrency-limits/limiter"
	"github.com/platinummonkey/go-concurrency-limits/strategy"
	"github.com/platinummonkey/go-concurrency-limits/strategy/matchers"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type ConcurrencyLimiter struct {
	concurrencyLimiter *limiter.DefaultLimiter
}

func NewConcurrencyLimiter() *ConcurrencyLimiter {
	return &ConcurrencyLimiter{}
}

func (cl *ConcurrencyLimiter) InitConcurrencyLimiter(tenants map[string]*userTSDB, total int64) {
	partitions := make(map[string]*strategy.LookupPartition)

	for user, userDB := range tenants {

		// here to update the limiter with percentage of the active series. And total active series is in ingester.seriesCount
		partition := strategy.NewLookupPartitionWithMetricRegistry(
			user,
			float64(userDB.Head().NumSeries())/float64(total),
			1,
			core.EmptyMetricRegistryInstance,
		)
		promauto.NewGaugeFunc(
			prometheus.GaugeOpts{
				Name:        "ad_tenant_limit",
				ConstLabels: map[string]string{"tenant": user},
			},
			func() float64 {
				return float64(partition.Limit())
			},
		)
		promauto.NewGaugeFunc(
			prometheus.GaugeOpts{
				Name:        "ad_tenant_concurrency",
				ConstLabels: map[string]string{"tenant": user},
			},
			func() float64 {
				return float64(partition.BusyCount())
			},
		)
		partitions[user] = partition
	}

	partition, _ := strategy.NewLookupPartitionStrategyWithMetricRegistry(partitions, nil, 10, core.EmptyMetricRegistryInstance)
	adaptiveLimit := limit.NewDefaultVegasLimit("vegas", limit.BuiltinLimitLogger{}, core.EmptyMetricRegistryInstance)
	partitionedLimiter, _ := limiter.NewDefaultLimiter(
		adaptiveLimit,
		int64(1e9),
		int64(1e9),
		int64(1e5),
		100,
		partition,
		limit.BuiltinLimitLogger{},
		core.EmptyMetricRegistryInstance,
	)

	cl.concurrencyLimiter = partitionedLimiter
}

func (cl *ConcurrencyLimiter) UpdateConcurrencyLimiter(tenants map[string]*userTSDB, total int64) {
	if cl.concurrencyLimiter == nil {
		cl.InitConcurrencyLimiter(tenants, total)
		return
	}
	// Here to update the limiter instead of creating a new one
}

func (l *ConcurrencyLimiter) Acquire(user string) (bool, core.Listener) {
	ctx := context.WithValue(context.Background(), matchers.LookupPartitionContextKey, user)
	token, ok := l.concurrencyLimiter.Acquire(ctx)
	if !ok {
		return false, nil // 430 indicates a concurrency limiter rejection
	}

	return true, token
}
