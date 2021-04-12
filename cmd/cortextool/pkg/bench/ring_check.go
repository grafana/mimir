package bench

import (
	"context"
	"flag"
	"fmt"
	"sort"
	"strings"
	"time"

	ingester_client "github.com/cortexproject/cortex/pkg/ingester/client"
	"github.com/cortexproject/cortex/pkg/ring"
	"github.com/cortexproject/cortex/pkg/ring/kv/codec"
	"github.com/cortexproject/cortex/pkg/ring/kv/memberlist"
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/prompb"
)

type RingCheckConfig struct {
	Enabled       bool                `yaml:"enabled"`
	MemberlistKV  memberlist.KVConfig `yaml:"memberlist"`
	RingConfig    ring.Config         `yaml:"ring"`
	CheckInterval time.Duration       `yaml:"check_interval"`
}

func (cfg *RingCheckConfig) RegisterFlagsWithPrefix(prefix string, f *flag.FlagSet) {
	f.BoolVar(&cfg.Enabled, prefix+"enabled", true, "enable ring check module")
	cfg.MemberlistKV.RegisterFlags(f, prefix)
	cfg.RingConfig.RegisterFlagsWithPrefix(prefix, f)

	f.DurationVar(&cfg.CheckInterval, prefix+"check-interval", 5*time.Minute, "Interval at which the current ring will be compared with the configured workload")
}

type RingChecker struct {
	id           string
	instanceName string
	cfg          RingCheckConfig

	Ring         *ring.Ring
	MemberlistKV *memberlist.KVInitService
	workload     *writeWorkload
	logger       log.Logger
}

func NewRingChecker(id string, instanceName string, cfg RingCheckConfig, workload *writeWorkload, logger log.Logger) (*RingChecker, error) {
	r := RingChecker{
		id:           id,
		instanceName: instanceName,
		cfg:          cfg,

		logger:   logger,
		workload: workload,
	}
	cfg.MemberlistKV.MetricsRegisterer = prometheus.DefaultRegisterer
	cfg.MemberlistKV.Codecs = []codec.Codec{
		ring.GetCodec(),
	}
	r.MemberlistKV = memberlist.NewKVInitService(&cfg.MemberlistKV, logger)
	cfg.RingConfig.KVStore.MemberlistKV = r.MemberlistKV.GetMemberlistKV

	var err error
	r.Ring, err = ring.New(cfg.RingConfig, "ingester", ring.IngesterRingKey, prometheus.DefaultRegisterer)
	if err != nil {
		return nil, err
	}

	return &r, nil
}

func (r *RingChecker) Run(ctx context.Context) error {
	err := r.Ring.Service.StartAsync(ctx)
	if err != nil {
		return fmt.Errorf("unable to start ring, %w", err)
	}
	ticker := time.NewTicker(r.cfg.CheckInterval)
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			r.check()
		}
	}
}

func (r *RingChecker) check() {
	timeseries := r.workload.generateTimeSeries(r.id, time.Now())

	addrMap := map[string]int{}
	for _, s := range timeseries {
		sort.Slice(s.Labels, func(i, j int) bool {
			return strings.Compare(s.Labels[i].Name, s.Labels[j].Name) < 0
		})

		token := shardByAllLabels(r.instanceName, s.Labels)

		rs, err := r.Ring.Get(token, ring.Write, []ring.InstanceDesc{}, nil, nil)

		if err != nil {
			level.Warn(r.logger).Log("msg", "unable to get token for metric", "err", err)
			continue
		}

		rs.GetAddresses()
		for _, addr := range rs.GetAddresses() {
			_, exists := addrMap[addr]
			if !exists {
				addrMap[addr] = 0
			}
			addrMap[addr]++
		}
	}

	fmt.Println("ring check:")
	for addr, tokensTotal := range addrMap {
		fmt.Printf("  %s,%d\n", addr, tokensTotal)
	}
}

func shardByUser(userID string) uint32 {
	h := ingester_client.HashNew32()
	h = ingester_client.HashAdd32(h, userID)
	return h
}

// This function generates different values for different order of same labels.
func shardByAllLabels(userID string, labels []prompb.Label) uint32 {
	h := shardByUser(userID)
	for _, label := range labels {
		h = ingester_client.HashAdd32(h, label.Name)
		h = ingester_client.HashAdd32(h, label.Value)
	}
	return h
}
