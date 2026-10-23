// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"context"
	"os"
	"strconv"
	"strings"
	"syscall"
	"time"
)

// ReplayCPU is an ingester's CPU over the part of the replay that comes after the backfill's first scrape interval,
// once every series exists: what ingesting costs on the unchanged path, per replayed sample. The 5s recorder can't
// resolve it.
type ReplayCPU struct {
	CPUSeconds float64 `json:"cpu_seconds"`
	Samples    float64 `json:"samples"`
	// The CPU seconds per sample, 0 when the ingester was ready before the bench saw it past the first interval.
	CPUSecondsPerSample float64 `json:"cpu_seconds_per_sample"`
	// The share of the backfill's samples past the first interval that Samples are: a short replay is mostly over by
	// the time the bench first sees the ingester, and a measurement of little of it says little.
	Coverage float64 `json:"coverage"`
}

// Counters are an ingester's counters of what changes the cost of ingesting and reading, but not with the load:
// compactions, checkpoints, truncations and head snapshots, and reads the read path's utilization limit rejected.
type Counters struct {
	Ready       map[string]float64 `json:"ready"`
	WindowStart map[string]float64 `json:"window_start"`
	WindowEnd   map[string]float64 `json:"window_end"`
}

// DriverCPU is the bench's own CPU, in cores, which has to stay below its CPUs for it not to be the bottleneck.
type DriverCPU struct {
	ReplayCores float64 `json:"replay_cores"`
	WindowCores float64 `json:"window_cores"`
}

// StealShare is the share of the host's CPU time the hypervisor took, from /proc/stat, -1 where that isn't there.
type StealShare struct {
	Replay float64 `json:"replay"`
	Window float64 `json:"window"`
}

// burn keeps n CPUs busy until the process is stopped.
func burn(n int) {
	for range n - 1 {
		go spin()
	}
	spin()
}

func spin() {
	for x := uint64(1); ; x = x*6364136223846793005 + 1442695040888963407 {
	}
}

// firstIntervalMs is the longest scrape interval of the world's metrics: every series has a sample in the first one.
func (w *world) firstIntervalMs() int64 {
	var longest int64 = 1000
	for _, t := range w.tenants {
		for _, m := range t.metrics {
			longest = max(longest, m.intervalMs)
		}
	}
	return longest
}

const consumedOffsetMetric = "cortex_ingest_storage_reader_last_consumed_offset{partition=0}"

// measureReplay returns the ingester's CPU and ingested samples once it consumed past offset, which it polls for, and
// whether it saw that before ctx was done. It stops polling then, so that its scrapes don't add to what it measures.
func measureReplay(ctx context.Context, ing *ingesterProcess, offset int64) (cpu, samples float64, ok bool) {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return 0, 0, false
		case <-ticker.C:
		}
		m, err := ing.metrics()
		if err != nil {
			continue
		}
		if consumed, found := m[consumedOffsetMetric]; found && int64(consumed) >= offset {
			return m["process_cpu_seconds_total"], m["cortex_ingester_ingested_samples_total"], true
		}
	}
}

// replayCPU is the replay's CPU from the snapshot past the first interval to the one at /ready, of the backfill's
// replayed samples past the first interval.
func replayCPU(startCPU, startSamples float64, ok bool, ready map[string]float64, replayed int64) ReplayCPU {
	if !ok {
		return ReplayCPU{}
	}
	r := ReplayCPU{CPUSeconds: ready["process_cpu_seconds_total"] - startCPU, Samples: ready["cortex_ingester_ingested_samples_total"] - startSamples}
	if r.Samples > 0 {
		r.CPUSecondsPerSample = r.CPUSeconds / r.Samples
	}
	if replayed > 0 {
		r.Coverage = r.Samples / float64(replayed)
	}
	return r
}

// counters picks Counters' counters out of an ingester's metrics.
func counters(m map[string]float64) map[string]float64 {
	out := map[string]float64{}
	for name, v := range m {
		base, _, _ := strings.Cut(name, "{")
		if !strings.HasSuffix(base, "_total") {
			continue
		}
		for _, kind := range []string{"compaction", "checkpoint", "truncation", "head_snapshots", "utilization_limited_read_requests"} {
			if strings.Contains(base, kind) {
				out[name] = v
				break
			}
		}
	}
	return out
}

// selfCPUSeconds is the bench's own user and system CPU so far.
func selfCPUSeconds() float64 {
	var usage syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &usage); err != nil {
		return 0
	}
	return float64(usage.Utime.Nano()+usage.Stime.Nano()) / 1e9
}

// hostCPUTimes are the host's steal and total CPU time so far, in ticks, from /proc/stat; ok is false without it.
func hostCPUTimes() (steal, total uint64, ok bool) {
	data, err := os.ReadFile("/proc/stat")
	if err != nil {
		return 0, 0, false
	}
	line, _, _ := strings.Cut(string(data), "\n")
	fields := strings.Fields(line)
	// cpu user nice system idle iowait irq softirq steal guest guest_nice; guest time is in user's already.
	if len(fields) < 9 || fields[0] != "cpu" {
		return 0, 0, false
	}
	for i, field := range fields[1:9] {
		v, err := strconv.ParseUint(field, 10, 64)
		if err != nil {
			return 0, 0, false
		}
		total += v
		if i == 7 {
			steal = v
		}
	}
	return steal, total, true
}

// cpuInterval measures the bench's CPU and the host's steal from when it's made to when end is called.
type cpuInterval struct {
	at          time.Time
	self        float64
	steal, host uint64
	hostOK      bool
}

func startCPUInterval() cpuInterval {
	steal, host, ok := hostCPUTimes()
	return cpuInterval{at: time.Now(), self: selfCPUSeconds(), steal: steal, host: host, hostOK: ok}
}

// end returns the bench's CPU in cores and the steal share over the interval, -1 without /proc/stat.
func (c cpuInterval) end() (cores, steal float64) {
	if elapsed := time.Since(c.at).Seconds(); elapsed > 0 {
		cores = (selfCPUSeconds() - c.self) / elapsed
	}
	steal = -1
	if s, host, ok := hostCPUTimes(); ok && c.hostOK && host > c.host {
		steal = float64(s-c.steal) / float64(host-c.host)
	}
	return cores, steal
}
