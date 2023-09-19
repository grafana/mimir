// Copyright The OpenTelemetry Authors
// Copyright (c) 2021 The Jaeger Authors.
// Copyright (c) 2017 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package jaegerremote // import "go.opentelemetry.io/contrib/samplers/jaegerremote"

import (
	"time"

	"github.com/go-logr/logr"

	"go.opentelemetry.io/otel/sdk/trace"
)

type config struct {
	sampler                 trace.Sampler
	samplingServerURL       string
	samplingRefreshInterval time.Duration
	samplingFetcher         samplingStrategyFetcher
	samplingParser          samplingStrategyParser
	updaters                []samplerUpdater
	posParams               perOperationSamplerParams
	logger                  logr.Logger
}

// newConfig returns an appropriately configured config.
func newConfig(options ...Option) config {
	c := config{
		sampler:                 newProbabilisticSampler(0.001),
		samplingServerURL:       defaultSamplingServerURL,
		samplingRefreshInterval: defaultSamplingRefreshInterval,
		samplingFetcher:         newHTTPSamplingStrategyFetcher(defaultSamplingServerURL),
		samplingParser:          new(samplingStrategyParserImpl),
		updaters: []samplerUpdater{
			new(probabilisticSamplerUpdater),
			new(rateLimitingSamplerUpdater),
		},
		posParams: perOperationSamplerParams{
			MaxOperations:            defaultSamplingMaxOperations,
			OperationNameLateBinding: defaultSamplingOperationNameLateBinding,
		},
		logger: logr.Discard(),
	}
	for _, option := range options {
		option.apply(&c)
	}
	c.updaters = append([]samplerUpdater{&perOperationSamplerUpdater{
		MaxOperations:            c.posParams.MaxOperations,
		OperationNameLateBinding: c.posParams.OperationNameLateBinding,
	}}, c.updaters...)
	return c
}

// Option applies configuration settings to a Sampler.
type Option interface {
	apply(*config)
}

type optionFunc func(*config)

func (fn optionFunc) apply(c *config) {
	fn(c)
}

// WithInitialSampler creates a Option that sets the initial sampler
// to use before a remote sampler is created and used.
func WithInitialSampler(sampler trace.Sampler) Option {
	return optionFunc(func(c *config) {
		c.sampler = sampler
	})
}

// WithSamplingServerURL creates a Option that sets the sampling server url
// of the local agent that contains the sampling strategies.
func WithSamplingServerURL(samplingServerURL string) Option {
	return optionFunc(func(c *config) {
		c.samplingServerURL = samplingServerURL
		// The default port of jaeger agent is 5778, but there are other ports specified by the user, so the sampling address and fetch address are strongly bound
		c.samplingFetcher = newHTTPSamplingStrategyFetcher(samplingServerURL)
	})
}

// WithMaxOperations creates a Option that sets the maximum number of
// operations the sampler will keep track of.
func WithMaxOperations(maxOperations int) Option {
	return optionFunc(func(c *config) {
		c.posParams.MaxOperations = maxOperations
	})
}

// WithOperationNameLateBinding creates a Option that sets the respective
// field in the perOperationSamplerParams.
func WithOperationNameLateBinding(enable bool) Option {
	return optionFunc(func(c *config) {
		c.posParams.OperationNameLateBinding = enable
	})
}

// WithSamplingRefreshInterval creates a Option that sets how often the
// sampler will poll local agent for the appropriate sampling strategy.
func WithSamplingRefreshInterval(samplingRefreshInterval time.Duration) Option {
	return optionFunc(func(c *config) {
		c.samplingRefreshInterval = samplingRefreshInterval
	})
}

// WithLogger configures the sampler to log operation and debug information with logger.
func WithLogger(logger logr.Logger) Option {
	return optionFunc(func(c *config) {
		c.logger = logger
	})
}

// samplingStrategyFetcher creates a Option that initializes sampling strategy fetcher.
func withSamplingStrategyFetcher(fetcher samplingStrategyFetcher) Option {
	return optionFunc(func(c *config) {
		c.samplingFetcher = fetcher
	})
}

// samplingStrategyParser creates a Option that initializes sampling strategy parser.
func withSamplingStrategyParser(parser samplingStrategyParser) Option {
	return optionFunc(func(c *config) {
		c.samplingParser = parser
	})
}

// withUpdaters creates a Option that initializes sampler updaters.
func withUpdaters(updaters ...samplerUpdater) Option {
	return optionFunc(func(c *config) {
		c.updaters = updaters
	})
}
