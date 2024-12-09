// SPDX-License-Identifier: AGPL-3.0-only

package log

import (
	"log/slog"

	"github.com/go-kit/log"
	slgk "github.com/tjhop/slog-gokit"
)

// SlogFromGoKit returns slog adapter for logger.
func SlogFromGoKit(logger log.Logger) *slog.Logger {
	var sl slog.Level
	x, ok := logger.(leveledLogger)
	if !ok {
		sl = slog.LevelDebug
	} else {
		switch x.level() {
		case infoLevel:
			sl = slog.LevelInfo
		case warnLevel:
			sl = slog.LevelWarn
		case errorLevel:
			sl = slog.LevelError
		default:
			sl = slog.LevelDebug
		}
	}

	lvl := slog.LevelVar{}
	lvl.Set(sl)
	return slog.New(slgk.NewGoKitHandler(logger, &lvl))
}
