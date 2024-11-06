// SPDX-License-Identifier: AGPL-3.0-only

package log

import (
	"context"
	"log/slog"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
)

// SlogFromGoKit returns slog adapter for logger.
func SlogFromGoKit(logger log.Logger) *slog.Logger {
	return slog.New(goKitHandler{logger: logger})
}

var _ slog.Handler = goKitHandler{}

type goKitHandler struct {
	logger log.Logger
	group  string
}

func (h goKitHandler) Enabled(_ context.Context, level slog.Level) bool {
	x, ok := h.logger.(leveledLogger)
	if !ok {
		return true
	}

	l := x.level()
	switch level {
	case slog.LevelInfo:
		return l <= infoLevel
	case slog.LevelWarn:
		return l <= warnLevel
	case slog.LevelError:
		return l <= errorLevel
	default:
		return l <= debugLevel
	}
}

func (h goKitHandler) Handle(_ context.Context, record slog.Record) error {
	var logger log.Logger
	switch record.Level {
	case slog.LevelInfo:
		logger = level.Info(h.logger)
	case slog.LevelWarn:
		logger = level.Warn(h.logger)
	case slog.LevelError:
		logger = level.Error(h.logger)
	default:
		logger = level.Debug(h.logger)
	}

	if h.group == "" {
		pairs := make([]any, 0, record.NumAttrs()+2)
		pairs = append(pairs, "msg", record.Message)
		record.Attrs(func(attr slog.Attr) bool {
			pairs = append(pairs, attr.Key, attr.Value)
			return true
		})
		return logger.Log(pairs...)
	}

	pairs := make([]any, 0, record.NumAttrs())
	record.Attrs(func(attr slog.Attr) bool {
		pairs = append(pairs, attr.Key, attr.Value)
		return true
	})
	g := slog.Group(h.group, pairs...)
	pairs = []any{"msg", record.Message, g.Key, g.Value}
	return logger.Log(pairs...)
}

func (h goKitHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	pairs := make([]any, 0, len(attrs))
	for _, attr := range attrs {
		pairs = append(pairs, attr.Key, attr.Value)
	}

	if h.group == "" {
		return goKitHandler{logger: log.With(h.logger, pairs...)}
	}

	g := slog.Group(h.group, pairs...)
	return goKitHandler{logger: log.With(h.logger, g.Key, g.Value)}
}

func (h goKitHandler) WithGroup(name string) slog.Handler {
	if name == "" {
		return h
	}

	h.group = name
	return h
}
