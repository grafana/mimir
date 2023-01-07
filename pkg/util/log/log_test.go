package log_test

import (
	"testing"

	"github.com/go-kit/log/level"
	"github.com/weaveworks/common/server"

	"github.com/grafana/mimir/pkg/util/log"
)

// Check the overhead of debug logging which gets filtered out.
func BenchmarkDebugLog(b *testing.B) {
	cfg := server.Config{}
	_ = cfg.LogLevel.Set("info")
	log.InitLogger(&cfg)
	b.ResetTimer()
	dl := level.Debug(log.Logger)
	for i := 0; i < b.N; i++ {
		dl.Log("something", "happened")
	}
}
