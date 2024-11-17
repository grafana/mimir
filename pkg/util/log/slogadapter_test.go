// SPDX-License-Identifier: AGPL-3.0-only

package log

import (
	"log/slog"
	"testing"

	"github.com/go-kit/log/level"
	dslog "github.com/grafana/dskit/log"
	"github.com/stretchr/testify/require"
)

func init() {
	// Remove timestamps from all test log calls to avoid non-deterministic
	// output and play nicely with Mock.
	dropTimestamp = true
}

func TestSlogFromGoKit(t *testing.T) {
	var lvl dslog.Level
	require.NoError(t, lvl.Set("debug"))
	mLogger := &mockLogger{}
	logger := newFilter(mLogger, lvl)
	slogger := SlogFromGoKit(logger)

	levels := []level.Value{
		level.DebugValue(),
		level.InfoValue(),
		level.WarnValue(),
		level.ErrorValue(),
	}

	for _, l := range levels {
		mLogger.On("Log", level.Key(), l, "msg", "test", "attr", slog.StringValue("value")).Times(1).Return(nil)
	}

	for _, l := range levels {
		attrs := []any{"attr", "value"}
		switch l {
		case level.DebugValue():
			slogger.Debug("test", attrs...)
		case level.InfoValue():
			slogger.Info("test", attrs...)
		case level.WarnValue():
			slogger.Warn("test", attrs...)
		case level.ErrorValue():
			slogger.Error("test", attrs...)
		}
	}

	t.Run("warn level", func(t *testing.T) {
		var lvl dslog.Level
		require.NoError(t, lvl.Set("warn"))
		mLogger := &mockLogger{}
		logger := newFilter(mLogger, lvl)
		slogger := SlogFromGoKit(logger)

		levels := []level.Value{
			level.DebugValue(),
			level.InfoValue(),
			level.WarnValue(),
			level.ErrorValue(),
		}

		mLogger.On("Log", level.Key(), level.WarnValue(), "msg", "test", "attr", slog.StringValue("value")).Times(1).Return(nil)
		mLogger.On("Log", level.Key(), level.ErrorValue(), "msg", "test", "attr", slog.StringValue("value")).Times(1).Return(nil)

		for _, l := range levels {
			attrs := []any{"attr", "value"}
			switch l {
			case level.DebugValue():
				slogger.Debug("test", attrs...)
			case level.InfoValue():
				slogger.Info("test", attrs...)
			case level.WarnValue():
				slogger.Warn("test", attrs...)
			case level.ErrorValue():
				slogger.Error("test", attrs...)
			}
		}
	})

	t.Run("with attrs", func(t *testing.T) {
		var lvl dslog.Level
		require.NoError(t, lvl.Set("debug"))
		mLogger := &mockLogger{}
		logger := newFilter(mLogger, lvl)
		slogger := SlogFromGoKit(logger).With("extra", "attr")

		mLogger.On(
			"Log",
			level.Key(), level.DebugValue(),
			"msg", "test",
			"extra", slog.StringValue("attr"),
			"attr", slog.StringValue("value"),
		).Times(1).Return(nil)

		slogger.Debug("test", "attr", "value")
	})

	t.Run("with non-empty group", func(t *testing.T) {
		var lvl dslog.Level
		require.NoError(t, lvl.Set("debug"))
		mLogger := &mockLogger{}
		logger := newFilter(mLogger, lvl)
		slogger := SlogFromGoKit(logger)

		mLogger.On(
			"Log",
			level.Key(), level.DebugValue(),
			"msg", "test",
			"test-group.attr", slog.StringValue("value"),
		).Times(1).Return(nil)

		slogger.WithGroup("test-group").Debug("test", "attr", "value")
	})
}
