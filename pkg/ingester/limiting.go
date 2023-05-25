// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"net/http"
	"time"

	"github.com/go-kit/log/level"
	"github.com/prometheus/procfs"
	"github.com/weaveworks/common/httpgrpc"
)

// This is the closest fitting Prometheus API error code for requests rejected due to limiting.
const queryLimitingCode = http.StatusServiceUnavailable

func (i *Ingester) updateResourceUtilization() {
	if !i.cfg.UtilizationBasedLimitingEnabled {
		return
	}

	lastUpdate := i.lastResourceUtilizationUpdate.Load()

	p, err := procfs.Self()
	if err != nil {
		level.Warn(i.logger).Log("msg", "couldn't update CPU/memory utilization - failed to get process info",
			"err", err.Error())
		return
	}
	ps, err := p.Stat()
	if err != nil {
		level.Warn(i.logger).Log("msg", "couldn't update CPU/memory utilization - failed to get process stats",
			"err", err.Error())
		return
	}

	i.memoryUtilization.Store(uint64(ps.ResidentMemory()))

	now := time.Now().UTC()

	lastCPUTime := i.lastCPUTime.Load()
	i.lastCPUTime.Store(ps.CPUTime())

	i.lastResourceUtilizationUpdate.Store(now)

	if lastUpdate.IsZero() {
		return
	}

	cpuUtil := (ps.CPUTime() - lastCPUTime) / now.Sub(lastUpdate).Seconds()
	i.movingAvg.Add(cpuUtil)
	cpuA := i.movingAvg.Value()
	i.cpuUtilization.Store(cpuA)

	level.Info(i.logger).Log("msg", "process resource utilization", "memory_utilization", ps.ResidentMemory(),
		"smoothed_cpu_utilization", cpuA, "raw_cpu_utilization", cpuUtil)
}

// checkReadOverloaded checks whether the ingester read path is overloaded wrt. CPU and/or memory.
func (i *Ingester) checkReadOverloaded() error {
	if !i.cfg.UtilizationBasedLimitingEnabled {
		return nil
	}

	memUtil := i.memoryUtilization.Load()
	cpuUtil := i.cpuUtilization.Load()
	lastUpdate := i.lastResourceUtilizationUpdate.Load()
	if lastUpdate.IsZero() {
		return nil
	}

	memPercent := 100 * (float64(memUtil) / float64(i.readPathMemoryThreshold))
	cpuPercent := 100 * (cpuUtil / i.readPathCPUThreshold)

	level.Info(i.logger).Log("msg", "read path resource based limiting",
		"memory_threshold", i.readPathMemoryThreshold, "memory_percentage_of_threshold", memPercent,
		"cpu_threshold", i.readPathCPUThreshold, "cpu_percentage_of_threshold", cpuPercent,
		"memory_utilization", memUtil, "cpu_utilization", cpuUtil)

	if memPercent >= 100 {
		return httpgrpc.Errorf(queryLimitingCode, "the ingester is currently too busy to process queries, try again later")
	}
	if cpuPercent >= 100 {
		return httpgrpc.Errorf(queryLimitingCode, "the ingester is currently too busy to process queries, try again later")
	}

	return nil
}
