// SPDX-License-Identifier: AGPL-3.0-only

package flusher

import (
	"flag"
	"time"
)

type Config struct {
	WALDir            string        `yaml:"wal_dir"`
	ConcurrentFlushes int           `yaml:"concurrent_flushes"`
	FlushOpTimeout    time.Duration `yaml:"flush_op_timeout"`
	ExitAfterFlush    bool          `yaml:"exit_after_flush"`
}

const (
	postFlushSleepTime = 1 * time.Minute
)

func (cfg *Config) RegisterFlags(f *flag.FlagSet) {
	f.StringVar(&cfg.WALDir, "flusher.wal-dir", "wal", "Directory to read WAL from (chunks storage engine only).")
	f.IntVar(&cfg.ConcurrentFlushes, "flusher.concurrent-flushes", 50, "Number of concurrent goroutines flushing to storage (chunks storage engine only).")
	f.DurationVar(&cfg.FlushOpTimeout, "flusher.flush-op-timeout", 2*time.Minute, "Timeout for individual flush operations (chunks storage engine only).")
	f.BoolVar(&cfg.ExitAfterFlush, "flusher.exit-after-flush", true, "Stop Cortex after flush has finished. If false, Cortex process will keep running, doing nothing.")
}
