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
		string:   "internal message: if you see this, probably log initialization has gone wrong",
		logLevel: debugLevel,
	}
	// This is a probing message; it should reach levelFilter.Log() no matter how many levels of wrapper are around logger.
	logger.Log("probe", &x)
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
