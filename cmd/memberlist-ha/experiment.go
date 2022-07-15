// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"context"
	"strconv"
	"time"

	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/kv"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/grafana/mimir/pkg/util"
	util_log "github.com/grafana/mimir/pkg/util/log"
)

const key = "key"

func runExperiment(ctx context.Context, store kv.Client) {
	level.Info(util_log.Logger).Log("msg", "starting experiment")
	go watchCurrentValue(ctx, store)
	go doCAS(ctx, store)
}

func doCAS(ctx context.Context, store kv.Client) {
	if !isLeader {
		level.Info(util_log.Logger).Log("msg", "not leader, not going to do CAS")
		return
	}
	t := time.NewTicker(util.DurationWithJitter(time.Duration(keyChangeInterval), 0.1))

	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			err := store.CAS(ctx, key, func(in interface{}) (interface{}, bool, error) {
				level.Info(util_log.Logger).Log("msg", "doing CAS")

				intVal, ok := in.(*value)
				if !ok {
					level.Error(util_log.Logger).Log("msg", "received a non-int value", "value", in)
					intVal = &value{}
				}
				intVal.number = time.Now().UnixMilli()
				return intVal, true, nil
			})
			if err != nil {
				level.Error(util_log.Logger).Log("msg", "couldn't CAS", "err", err)
			}
		}
	}
}

func watchCurrentValue(ctx context.Context, store kv.Client) {
	currentValueLatency := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name:      "current_value_latency_milliseconds",
		Namespace: "experiment",
	}, []string{"is_leader"})
	prometheus.MustRegister(currentValueLatency)

	store.WatchKey(ctx, key, func(val interface{}) bool {
		intVal, ok := val.(*value)
		if !ok {
			level.Error(util_log.Logger).Log("msg", "received a non-int value", "value", val)
			return true
		}
		latency := time.Now().UnixMilli() - intVal.number
		currentValueLatency.WithLabelValues(strconv.FormatBool(isLeader)).Set(float64(latency))
		level.Info(util_log.Logger).Log("msg", "value changed", "new_value", val, "latency_millis", latency)

		return true
	})
}
