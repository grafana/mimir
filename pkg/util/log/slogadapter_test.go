// SPDX-License-Identifier: AGPL-3.0-only

package log

import (
	"context"
	"fmt"
	"log/slog"
	"testing"

	"github.com/go-kit/log/level"
	dslog "github.com/grafana/dskit/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestSlogFromGoKit(t *testing.T) {
	levels := []level.Value{
		level.DebugValue(),
		level.InfoValue(),
		level.WarnValue(),
		level.ErrorValue(),
	}
	slogLevels := []slog.Level{
		slog.LevelDebug,
		slog.LevelInfo,
		slog.LevelWarn,
		slog.LevelError,
	}

	t.Run("enabled for the right slog levels when go-kit level configured", func(t *testing.T) {
		for i, l := range levels {
			var lvl dslog.Level
			switch i {
			case 0:
				require.NoError(t, lvl.Set("debug"))
			case 1:
				require.NoError(t, lvl.Set("info"))
			case 2:
				require.NoError(t, lvl.Set("warn"))
			case 3:
				require.NoError(t, lvl.Set("error"))
			default:
				panic(fmt.Errorf("unhandled level %d", i))
			}

			mLogger := &mockLogger{}
			logger := newFilter(mLogger, lvl)
			slogger := SlogFromGoKit(logger)

			for j, sl := range slogLevels {
				if j >= i {
					assert.Truef(t, slogger.Enabled(context.Background(), sl), "slog logger should be enabled for go-kit level %v / slog level %v", l, sl)
				} else {
					assert.Falsef(t, slogger.Enabled(context.Background(), sl), "slog logger should not be enabled for go-kit level %v / slog level %v", l, sl)
				}
			}
		}
	})

	t.Run("enabled for the right slog levels when go-kit level not configured", func(t *testing.T) {
		mLogger := &mockLogger{}
		slogger := SlogFromGoKit(mLogger)

		for _, sl := range slogLevels {
			assert.Truef(t, slogger.Enabled(context.Background(), sl), "slog logger should be enabled for level %v", sl)
		}
	})

	t.Run("wraps go-kit logger", func(*testing.T) {
		mLogger := &mockLogger{}
		slogger := SlogFromGoKit(mLogger)

		for _, l := range levels {
			mLogger.On("Log", level.Key(), l, "caller", mock.AnythingOfType("string"), "time", mock.AnythingOfType("time.Time"), "msg", "test", "attr", slog.StringValue("value")).Times(1).Return(nil)
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
			default:
				panic(fmt.Errorf("unrecognized level %v", l))
			}
		}
	})
}
