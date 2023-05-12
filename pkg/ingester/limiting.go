package ingester

import (
	"net/http"
	"os"
	"time"

	"github.com/go-kit/log/level"
	"github.com/pkg/errors"
	"github.com/shirou/gopsutil/v3/process"
	"github.com/weaveworks/common/httpgrpc"
)

// checkReadOverloaded checks whether the ingester read path is overloaded wrt. CPU and/or memory.
func (i *Ingester) checkReadOverloaded() error {
	// TODO: Determine memory usage as percentage of threshold (80% of memory request)

	proc, err := process.NewProcess(int32(os.Getpid()))
	if err != nil {
		return errors.Wrap(err, "failed getting process info")
	}
	crtTime, err := proc.CreateTime()
	if err != nil {
		return errors.Wrap(err, "failed getting process creation time")
	}
	created := time.Unix(0, crtTime*int64(time.Millisecond))
	// Time elapsed since process creation
	timeAlive := time.Since(created).Seconds()

	memInfo, err := proc.MemoryInfo()
	if err != nil {
		return errors.Wrap(err, "failed getting process memory info")
	}
	memPercent := 100 * (memInfo.RSS / i.readPathMemoryThreshold)

	// Calculate CPU utilization percentage by dividing CPU time (in seconds) spent by process by time the process has been alive
	// This should give us the percentage of a core the process has used
	var cpuUtilPercent float64
	if timeAlive > 0 {
		cput, err := proc.Times()
		if err != nil {
			return errors.Wrap(err, "failed getting process times")
		}
		cpuUtilPercent = 100 * cput.Total() / timeAlive
	}
	cpuPercent := 100 * (cpuUtilPercent / (float64(i.readPathCPUThreshold) / 1000))

	level.Info(i.logger).Log("msg", "resource utilization",
		"memory_threshold", i.readPathMemoryThreshold, "memory_percentage", memPercent,
		"cpu_threshold", i.readPathCPUThreshold, "cpu_percentage", cpuPercent)

	if memPercent >= 100 {
		return httpgrpc.Errorf(http.StatusTooManyRequests, "the ingester is currently too busy to process queries, try again later")
	}

	if cpuPercent >= 100 {
		return httpgrpc.Errorf(http.StatusTooManyRequests, "the ingester is currently too busy to process queries, try again later")
	}

	return nil
}
