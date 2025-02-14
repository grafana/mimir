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
	x := privateLevelDetector{
		string:   "This struct is expected to be used with levelLogger only",
		logLevel: debugLevel,
	}
	logger.Log("test", &x)
	switch x.logLevel {
	case infoLevel:
		sl = slog.LevelInfo
	case warnLevel:
		sl = slog.LevelWarn
	case errorLevel:
		sl = slog.LevelError
	default:
		sl = slog.LevelDebug
	}

	lvl := slog.LevelVar{}
	lvl.Set(sl)
	return slog.New(slgk.NewGoKitHandler(logger, &lvl))
}
