// SPDX-License-Identifier: AGPL-3.0-only

package usagestats

import (
	"expvar"
	"runtime"
	"strings"
	"time"

	prom "github.com/prometheus/prometheus/web/api/v1"

	"github.com/grafana/mimir/pkg/util/version"
)

// Report is the JSON object sent to the stats server
type Report struct {
	// ClusterID is the unique Mimir cluster ID.
	ClusterID string `json:"clusterID"`

	// CreatedAt is when the cluster was created.
	CreatedAt time.Time `json:"createdAt"`

	// Interval is when the report was created (value is aligned across all replicas of the same Mimir cluster).
	Interval time.Time `json:"interval"`

	// IntervalPeriod is how frequently the report is sent, in seconds.
	IntervalPeriod float64 `json:"intervalPeriod"`

	// Target used to run Mimir.
	Target string `json:"target"`

	// Version holds information about the Mimir version.
	Version prom.PrometheusVersion `json:"version"`

	// Os is the operating system where Mimir is running.
	Os string `json:"os"`

	// Arch is the CPU architecture where Mimir is running.
	Arch string `json:"arch"`

	// Edition is the Mimir edition ("oss" or "enterprise").
	Edition string `json:"edition"`

	// Mode is the Mimir architecture mode ("classic" or "ingest_storage").
	Mode string `json:"mode"`

	// Metrics holds custom metrics tracked by Mimir. Can contain nested objects.
	Metrics map[string]interface{} `json:"metrics"`
}

// buildReport builds the report to be sent to the stats server.
func buildReport(seed ClusterSeed, reportAt time.Time, reportInterval time.Duration) *Report {
	var (
		targetName  string
		editionName string
		modeName    string
	)
	if target := expvar.Get(statsPrefix + targetKey); target != nil {
		if target, ok := target.(*expvar.String); ok {
			targetName = target.Value()
		}
	}
	if edition := expvar.Get(statsPrefix + editionKey); edition != nil {
		if edition, ok := edition.(*expvar.String); ok {
			editionName = edition.Value()
		}
	}
	if val := expvar.Get(statsPrefix + modeKey); val != nil {
		if name, ok := val.(*expvar.String); ok {
			modeName = name.Value()
		}
	}

	return &Report{
		ClusterID:      seed.UID,
		CreatedAt:      seed.CreatedAt,
		Version:        buildVersion(),
		Interval:       reportAt,
		IntervalPeriod: reportInterval.Seconds(),
		Os:             runtime.GOOS,
		Arch:           runtime.GOARCH,
		Target:         targetName,
		Edition:        editionName,
		Mode:           modeName,
		Metrics:        buildMetrics(),
	}
}

// buildMetrics builds the metrics part of the report to be sent to the stats server.
func buildMetrics() map[string]interface{} {
	result := map[string]interface{}{
		"memstats":      buildMemstats(),
		"num_goroutine": runtime.NumGoroutine(),
	}
	defer cpuUsage.Set(0)

	expvar.Do(func(kv expvar.KeyValue) {
		if !strings.HasPrefix(kv.Key, statsPrefix) {
			return
		}

		key := strings.TrimPrefix(kv.Key, statsPrefix)

		// Exclude reserved keys; they are reported on the root level of the Report.
		switch key {
		case targetKey,
			editionKey,
			modeKey:
			return
		}

		var value any
		switch v := kv.Value.(type) {
		case *expvar.Int:
			value = v.Value()
		case *expvar.String:
			value = v.Value()
		case *expvar.Float:
			value = v.Value()
		case *Counter:
			v.updateRate()
			value = v.Value()
			v.reset()
		default:
			// Unsupported.
			return
		}

		result[key] = value
	})

	return result
}

func buildMemstats() interface{} {
	stats := new(runtime.MemStats)
	runtime.ReadMemStats(stats)

	return map[string]interface{}{
		"alloc":           stats.Alloc,
		"total_alloc":     stats.TotalAlloc,
		"sys":             stats.Sys,
		"heap_alloc":      stats.HeapAlloc,
		"heap_inuse":      stats.HeapInuse,
		"stack_inuse":     stats.StackInuse,
		"pause_total_ns":  stats.PauseTotalNs,
		"num_gc":          stats.NumGC,
		"gc_cpu_fraction": stats.GCCPUFraction,
	}
}

func buildVersion() prom.PrometheusVersion {
	return prom.PrometheusVersion{
		Version:   version.Version,
		Revision:  version.Revision,
		Branch:    version.Branch,
		GoVersion: version.GoVersion,
	}
}
