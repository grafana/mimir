// SPDX-License-Identifier: AGPL-3.0-only

package ingest

import (
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/twmb/franz-go/pkg/kgo"
)

type KafkaLogger struct {
	logger log.Logger
}

func NewKafkaLogger(logger log.Logger) *KafkaLogger {
	return &KafkaLogger{
		logger: log.With(logger, "component", "kafka_client"),
	}
}

func (l *KafkaLogger) Level() kgo.LogLevel {
	// The Kafka client calls Level() to check whether debug level is enabled or not.
	// To keep it simple, we always return Info, so the Kafka client will never try
	// to log expensive debug messages.
	return kgo.LogLevelInfo
}

func (l *KafkaLogger) Log(lev kgo.LogLevel, msg string, keyvals ...any) {
	keyvals = append([]any{"msg", msg}, keyvals...)
	switch lev {
	case kgo.LogLevelDebug:
		level.Debug(l.logger).Log(keyvals...)
	case kgo.LogLevelInfo:
		level.Info(l.logger).Log(keyvals...)
	case kgo.LogLevelWarn:
		level.Warn(l.logger).Log(keyvals...)
	case kgo.LogLevelError:
		level.Error(l.logger).Log(keyvals...)
	}
}
