// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"net/http"
	"os"
	"time"

	"github.com/go-kit/log/level"
	"github.com/shirou/gopsutil/v3/process"
	"github.com/weaveworks/common/httpgrpc"
)

func (i *Ingester) updateResourceUtilization() {
	if !i.cfg.UtilizationBasedLimitingEnabled {
		return
	}

	lastUpdate := i.lastResourceUtilizationUpdate.Load()

	proc, err := process.NewProcess(int32(os.Getpid()))
	if err != nil {
		level.Warn(i.logger).Log("msg", "couldn't update CPU/memory utilization - failed to get process info",
			"err", err.Error())
		return
	}

	memInfo, err := proc.MemoryInfo()
	if err != nil {
		level.Warn(i.logger).Log("msg", "couldn't update CPU/memory utilization - failed to get memory info",
			"err", err.Error())
		return
	}
	i.memoryUtilization.Store(memInfo.RSS)

	cput, err := proc.Times()
	if err != nil {
		level.Warn(i.logger).Log("msg", "couldn't update CPU/memory utilization - failed to get process times",
			"err", err.Error())
		return
	}

	now := time.Now().UTC()

	lastCPUTime := i.lastCPUTime.Load()
	// TODO: Should Guest and GuestNice be ignored, as potentially duplicated in User?
	// https://github.com/mackerelio/go-osstat/blob/54b9216143f9aa087151258f103dbd5c381f7915/cpu/cpu_linux.go#L70-L74
	cpuTime := cput.User + cput.System + cput.Idle + cput.Nice + cput.Iowait + cput.Irq +
		cput.Softirq + cput.Steal + cput.Guest + cput.GuestNice
	i.lastCPUTime.Store(cpuTime)

	i.lastResourceUtilizationUpdate.Store(now)

	if lastUpdate.IsZero() {
		return
	}

	cpuUtil := (cpuTime - lastCPUTime) / now.Sub(lastUpdate).Seconds()
	i.movingAvg.Add(cpuUtil)
	cpuA := i.movingAvg.Value()
	i.cpuUtilization.Store(cpuA)

	level.Info(i.logger).Log("msg", "process resource utilization", "memory_utilization", memInfo.RSS,
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
		return httpgrpc.Errorf(http.StatusTooManyRequests, "the ingester is currently too busy to process queries, try again later")
	}
	if cpuPercent >= 100 {
		return httpgrpc.Errorf(http.StatusTooManyRequests, "the ingester is currently too busy to process queries, try again later")
	}

	return nil
}

// checkWriteOverloaded checks whether the ingester write path is overloaded wrt. CPU and/or memory.
func (i *Ingester) checkWriteOverloaded() error {
	if !i.cfg.UtilizationBasedLimitingEnabled {
		return nil
	}

	memUtil := i.memoryUtilization.Load()
	cpuUtil := i.cpuUtilization.Load()
	lastUpdate := i.lastResourceUtilizationUpdate.Load()
	if lastUpdate.IsZero() {
		return nil
	}

	memPercent := 100 * (float64(memUtil) / float64(i.writePathMemoryThreshold))
	cpuPercent := 100 * (cpuUtil / i.writePathCPUThreshold)

	level.Info(i.logger).Log("msg", "write path resource based limiting",
		"memory_threshold", i.writePathMemoryThreshold, "memory_percentage_of_threshold", memPercent,
		"cpu_threshold", i.writePathCPUThreshold, "cpu_percentage_of_threshold", cpuPercent,
		"memory_utilization", memUtil, "cpu_utilization", cpuUtil)

	if memPercent >= 100 {
		// TODO: This should be 5xx, to allow for retries?
		return httpgrpc.Errorf(http.StatusTooManyRequests, "the ingester is currently too busy to process writes, try again later")
	}
	if cpuPercent >= 100 {
		// TODO: This should be 5xx, to allow for retries?
		return httpgrpc.Errorf(http.StatusTooManyRequests, "the ingester is currently too busy to process writes, try again later")
	}

	return nil
}
