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
	var logLevel dslog.Level
	if err := logLevel.Set(levelStr); err != nil {
		return nil, err
	}

	base := log.NewLogfmtLogger(log.NewSyncWriter(os.Stderr))
	base = log.With(base, "ts", log.DefaultTimestampUTC)
	return level.NewFilter(base, logLevel.Option), nil
}
