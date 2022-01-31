// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/grafana/cortex-tools/blob/main/pkg/commands/logger.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package commands

import (
	"fmt"

	"github.com/sirupsen/logrus"
	"gopkg.in/alecthomas/kingpin.v2"
)

type LoggerConfig struct {
	Level string
}

func (l *LoggerConfig) registerLogLevel(pc *kingpin.ParseContext) error {
	var logLevel logrus.Level
	switch l.Level {
	case "debug":
		logLevel = logrus.DebugLevel
	case "info":
		logLevel = logrus.InfoLevel
	case "warn":
		logLevel = logrus.WarnLevel
	case "error":
		logLevel = logrus.ErrorLevel
	case "fatal":
		logLevel = logrus.FatalLevel
	default:
		return fmt.Errorf("log level %s is not valid", l.Level)
	}
	logrus.SetLevel(logLevel)
	logrus.Infof("log level set to %s", l.Level)
	return nil
}

// Register configures log related flags
func (l *LoggerConfig) Register(app *kingpin.Application, _ EnvVarNames) {
	app.Flag("log.level", "set level of the logger").Default("info").PreAction(l.registerLogLevel).StringVar(&l.Level)
}
