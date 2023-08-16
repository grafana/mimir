// Provenance-includes-location: https://github.com/weaveworks/common/blob/main/logging/gokit.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: Weaveworks Ltd.

package log

import (
	"fmt"
	"io"
	"os"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
)

const (
	LogfmtFormat = "logfmt"
	JSONFormat   = "json"
)

// NewGoKit creates a new GoKit logger with the given format and writer.
// If the given writer is nil, os.Stderr is used.
// If the given format is nil, logfmt is used.
// No additional fields nor filters are added to the created logger, and
// if they are required, the caller is expected to add them.
func NewGoKit(format string, writer io.Writer) log.Logger {
	if writer == nil {
		writer = log.NewSyncWriter(os.Stderr)
	}
	if format == JSONFormat {
		return log.NewJSONLogger(writer)
	}
	return log.NewLogfmtLogger(writer)
}

// NewGoKitWithLevel creates a new GoKit logger with the given level, format and writer.
// If the given writer is nil, os.Stderr is used.
// If the given format is nil, logfmt is used.
func NewGoKitWithLevel(lvl Level, format string, writer io.Writer) log.Logger {
	logger := NewGoKit(format, writer)
	return level.NewFilter(logger, lvl.Option)
}

// stand-alone for test purposes
func addStandardFields(logger log.Logger) log.Logger {
	return log.With(logger, "ts", log.DefaultTimestampUTC, "caller", log.Caller(5))
}

type Sprintf struct {
	format string
	args   []interface{}
}

func LazySprintf(format string, args ...interface{}) *Sprintf {
	return &Sprintf{
		format: format,
		args:   args,
	}
}

func (s *Sprintf) String() string {
	return fmt.Sprintf(s.format, s.args...)
}
