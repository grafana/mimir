// SPDX-License-Identifier: AGPL-3.0-only

package log

import (
	"os"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	dslog "github.com/grafana/dskit/log"
)

// NewLogger creates a configured logger with the specified level.
func NewLogger(levelStr string) (log.Logger, error) {
	var levelOption level.Option
	// Handle "fatal" level which is not supported by dskit/log but was supported
	// by the previous logrus-based implementation. Map it to level.AllowNone()
	// which is the most restrictive level (no leveled logs pass through).
	if levelStr == "fatal" {
		levelOption = level.AllowNone()
	} else {
		var logLevel dslog.Level
		if err := logLevel.Set(levelStr); err != nil {
			return nil, err
		}
		levelOption = logLevel.Option
	}

	base := log.NewLogfmtLogger(log.NewSyncWriter(os.Stderr))
	base = log.With(base, "ts", log.DefaultTimestampUTC)
	return level.NewFilter(base, levelOption), nil
}
