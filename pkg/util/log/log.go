// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/util/log/log.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package log

import (
	"fmt"
	"io"
	"os"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/weaveworks/common/logging"
	"github.com/weaveworks/common/server"
)

var (
	// Logger is a shared go-kit logger.
	// TODO: Change all components to take a non-global logger via their constructors.
	// Prefer accepting a non-global logger as an argument.
	Logger         = log.NewNopLogger()
	bufferedLogger *LineBufferedLogger
)

// InitLogger initialises the global gokit logger (util_log.Logger) and overrides the
// default logger for the server.
func InitLogger(cfg *server.Config, buffered bool, sync bool) {
	l := newBasicLogger(cfg.LogFormat, buffered, sync)

	// when using util_log.Logger, skip 5 stack frames.
	logger := log.With(l, "caller", log.Caller(5))
	// Must put the level filter last for efficiency.
	Logger = level.NewFilter(logger, cfg.LogLevel.Gokit)

	// cfg.Log wraps log function, skip 6 stack frames to get caller information.
	cfg.Log = logging.GoKit(level.NewFilter(log.With(l, "caller", log.Caller(6)), cfg.LogLevel.Gokit))
}

func newBasicLogger(format logging.Format, buffered bool, sync bool) log.Logger {
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
		bufferedLogger = NewLineBufferedLogger(writer, logEntries,
			WithFlushPeriod(flushTimeout),
			WithPrellocatedBuffer(logBufferSize),
		)

		writer = bufferedLogger
	}

	if sync {
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
func NewDefaultLogger(l logging.Level, format logging.Format) log.Logger {
	logger := newBasicLogger(format, false, true)
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
