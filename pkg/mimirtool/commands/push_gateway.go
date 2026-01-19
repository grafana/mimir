// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/grafana/cortex-tools/blob/main/pkg/commands/push_gateway.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package commands

import (
	"net/url"
	"time"

	"github.com/alecthomas/kingpin/v2"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/push"
)

// PushGatewayConfig configures the pushgateway
type PushGatewayConfig struct {
	Endpoint *url.URL
	JobName  string
	Interval time.Duration

	logger     log.Logger
	pusher     *push.Pusher
	done       chan struct{}
	terminated chan struct{}
}

// Register configures log related flags
func (l *PushGatewayConfig) Register(app *kingpin.Application, _ EnvVarNames, logConfig *LoggerConfig) {
	app.PreAction(func(_ *kingpin.ParseContext) error {
		l.logger = logConfig.Logger()
		return l.setup()
	})
	app.Flag("push-gateway.endpoint", "url for the push-gateway to register metrics").URLVar(&l.Endpoint)
	app.Flag("push-gateway.job", "job name to register metrics").StringVar(&l.JobName)
	app.Flag("push-gateway.interval", "interval to forward metrics to the push gateway").Default("1m").DurationVar(&l.Interval)
}

func (l *PushGatewayConfig) setup() error {
	if l.Endpoint == nil || l.JobName == "" {
		level.Debug(l.logger).Log("msg", "push-gateway not configured")
		return nil
	}

	level.Debug(l.logger).Log("msg", "push-gateway enabled", "endpoint", l.Endpoint, "job_name", l.JobName, "interval", l.Interval.String())

	l.pusher = push.New(l.Endpoint.String(), l.JobName).Gatherer(prometheus.DefaultGatherer)
	err := l.pusher.Push()
	if err != nil {
		level.Error(l.logger).Log("msg", "unable to forward metrics to pushgateway", "err", err)
	}

	l.done = make(chan struct{})
	l.terminated = make(chan struct{})

	go l.loop()

	return nil
}

func (l *PushGatewayConfig) loop() {
	timer := time.NewTicker(l.Interval)
	defer timer.Stop()
	defer close(l.terminated)

	for {
		select {
		case <-l.done:
			err := l.pusher.Add()
			if err != nil {
				level.Error(l.logger).Log("msg", "unable to forward metrics to pushgateway", "err", err)
			}
			return
		case <-timer.C:
			err := l.pusher.Add()
			if err != nil {
				level.Error(l.logger).Log("msg", "unable to forward metrics to pushgateway", "err", err)
			}
		}
	}
}

// Stop shutsdown the pushgateway
func (l *PushGatewayConfig) Stop() {
	if l.done == nil {
		return
	}
	close(l.done)
	<-l.terminated
}
