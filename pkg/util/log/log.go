// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/util/log/log.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package log

import (
	"context"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	dslog "github.com/grafana/dskit/log"
	"github.com/grafana/dskit/server"
)

var (
	// Logger is a shared go-kit logger.
	// TODO: Change all components to take a non-global logger via their constructors.
	// Prefer accepting a non-global logger as an argument.
	Logger         = log.NewNopLogger()
	bufferedLogger *dslog.BufferedLogger
)

// InitLogger initialises the global gokit logger (util_log.Logger) and overrides the
// default logger for the server.
func InitLogger(cfg *server.Config, buffered bool) {
	l := newBasicLogger(cfg.LogFormat, buffered)

	// when using util_log.Logger, skip 5 stack frames.
	logger := log.With(l, "caller", log.Caller(5))
	// Must put the level filter last for efficiency.
	Logger = level.NewFilter(logger, cfg.LogLevel.Gokit)

	// cfg.Log wraps log function, skip 6 stack frames to get caller information.
	cfg.Log = dslog.GoKit(level.NewFilter(log.With(l, "caller", log.Caller(6)), cfg.LogLevel.Gokit))
}

func newBasicLogger(format dslog.Format, buffered bool) log.Logger {
	var logger log.Logger
	var writer io.Writer = os.Stderr

	if buffered {
		var (
			logEntries    uint32 = 256                    // buffer up to 256 log lines in memory before flushing to a write(2) syscall
			logBufferSize uint32 = 10e6                   // 10MB
			flushTimeout         = 100 * time.Millisecond // flush the buffer after 100ms regardless of how full it is, to prevent losing many logs in case of ungraceful termination
		)

		// retain a reference to this logger because it doesn't conform to the standard Logger interface,
		// and we can't unwrap it to get the underlying logger when we flush on shutdown
		bufferedLogger = dslog.NewBufferedLogger(writer, logEntries,
			dslog.WithFlushPeriod(flushTimeout),
			dslog.WithPrellocatedBuffer(logBufferSize),
		)

		writer = bufferedLogger
	} else {
		writer = log.NewSyncWriter(writer)
	}

	if format.String() == "json" {
		logger = log.NewJSONLogger(writer)
	} else {
		logger = log.NewLogfmtLogger(writer)
	}

	// return a Logger without filter or caller information, shouldn't use directly
	return log.With(logger, "ts", log.DefaultTimestampUTC)
}

// NewDefaultLogger creates a new gokit logger with the configured level and format
func NewDefaultLogger(l dslog.Level, format dslog.Format) log.Logger {
	logger := newBasicLogger(format, false)
	return level.NewFilter(log.With(logger, "ts", log.DefaultTimestampUTC), l.Gokit)
}

// CheckFatal prints an error and exits with error code 1 if err is non-nil
func CheckFatal(location string, err error) {
	if err != nil {
		logger := level.Error(Logger)
		if location != "" {
			logger = log.With(logger, "msg", "error "+location)
		}
		// %+v gets the stack trace from errors using github.com/pkg/errors
		logger.Log("err", fmt.Sprintf("%+v", err))

		if err = Flush(); err != nil {
			fmt.Fprintln(os.Stderr, "Could not flush logger", err)
		}
		os.Exit(1)
	}
}

// Flush forces the buffered logger, if configured, to flush to the underlying writer
// This is typically only called when the application is shutting down.
func Flush() error {
	if bufferedLogger != nil {
		return bufferedLogger.Flush()
	}

	return nil
}

type DoNotLogError struct{ Err error }

func (i DoNotLogError) Error() string                                     { return i.Err.Error() }
func (i DoNotLogError) Unwrap() error                                     { return i.Err }
func (i DoNotLogError) ShouldLog(_ context.Context, _ time.Duration) bool { return false }
