// SPDX-License-Identifier: AGPL-3.0-only

package cache

import (
	"flag"
)

type BackgroundConfig struct {
	WriteBackGoroutines int `yaml:"writeback_goroutines"`
	WriteBackBuffer     int `yaml:"writeback_buffer"`
}

func (cfg *BackgroundConfig) RegisterFlagsWithPrefix(prefix string, description string, f *flag.FlagSet) {
	f.IntVar(&cfg.WriteBackGoroutines, prefix+"background.write-back-concurrency", 10, description+"At what concurrency to write back to cache.")
	f.IntVar(&cfg.WriteBackBuffer, prefix+"background.write-back-buffer", 10000, description+"How many key batches to buffer for background write-back.")
}
