// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/grafana/cortex-tools/blob/main/pkg/commands/logger.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package commands

import (
	"fmt"

	"github.com/alecthomas/kingpin/v2"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"

	mimirlog "github.com/grafana/mimir/pkg/mimirtool/log"
)

type LoggerConfig struct {
	Level  string
	logger log.Logger
}

// Logger returns the configured logger. If not yet initialized, initializes with the configured level.
func (l *LoggerConfig) Logger() log.Logger {
	if l.logger == nil {
		// Initialize logger lazily if PreAction wasn't called (e.g., when using default log level).
		lvl := l.Level
		if lvl == "" {
			lvl = "info"
		}
		logger, err := mimirlog.NewLogger(lvl)
		if err != nil {
			// Fall back to nop logger if initialization fails.
			return log.NewNopLogger()
		}
		l.logger = logger
	}
	return l.logger
}

func (l *LoggerConfig) registerLogLevel(_ *kingpin.ParseContext) error {
	logger, err := mimirlog.NewLogger(l.Level)
	if err != nil {
		return fmt.Errorf("log level %s is not valid: %w", l.Level, err)
	}
	l.logger = logger
	level.Info(l.logger).Log("msg", fmt.Sprintf("log level set to %s", l.Level))
	return nil
}

// Register configures log related flags.
func (l *LoggerConfig) Register(app *kingpin.Application, _ EnvVarNames) {
	app.Flag("log.level", "set level of the logger").Default("info").PreAction(l.registerLogLevel).StringVar(&l.Level)
}
