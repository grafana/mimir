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

// For tests only:
// Enable this to skip adding slog's default timestamp kv pair into the kv
// pairs passed to the handler's underlying go-kit logger. This is needed so
// that non-deterministic output can be stripped from the log calls and play
// nicely with Mock.
var dropTimestamp = false

type goKitHandler struct {
	logger       log.Logger
	preformatted []any
	group        string
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

	// 1 slog.Attr == 1 key and 1 value, set capacity >= (2 * num attrs).
	//
	// Note: this could probably be (micro)-optimized further -- we know we
	// need to also append on a timestamp from the record, the message, the
	// preformatted vals, all things we more or less know the size of at
	// creation time here.
	pairs := make([]any, 0, (2 * record.NumAttrs()))
	if !record.Time.IsZero() && !dropTimestamp {
		pairs = append(pairs, "time", record.Time)
	}
	pairs = append(pairs, "msg", record.Message)
	pairs = append(pairs, h.preformatted...)

	record.Attrs(func(a slog.Attr) bool {
		pairs = appendPair(pairs, h.group, a)
		return true
	})

	return logger.Log(pairs...)
}

func (h goKitHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	pairs := make([]any, 0, 2*len(attrs))
	for _, a := range attrs {
		pairs = appendPair(pairs, h.group, a)
	}

	if h.preformatted != nil {
		pairs = append(h.preformatted, pairs...)
	}

	return goKitHandler{logger: h.logger, preformatted: pairs, group: h.group}
}

func (h goKitHandler) WithGroup(name string) slog.Handler {
	if name == "" {
		return h
	}

	g := name
	if h.group != "" {
		g = h.group + "." + g
	}

	return goKitHandler{logger: h.logger, preformatted: h.preformatted, group: g}
}

func appendPair(pairs []any, groupPrefix string, attr slog.Attr) []any {
	if attr.Equal(slog.Attr{}) {
		return pairs
	}

	switch attr.Value.Kind() {
	case slog.KindGroup:
		if attr.Key != "" {
			groupPrefix = groupPrefix + "." + attr.Key
		}
		for _, a := range attr.Value.Group() {
			pairs = appendPair(pairs, groupPrefix, a)
		}
	default:
		key := attr.Key
		if groupPrefix != "" {
			key = groupPrefix + "." + key
		}

		pairs = append(pairs, key, attr.Value)
	}

	return pairs
}
