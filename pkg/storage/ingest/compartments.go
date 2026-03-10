// SPDX-License-Identifier: AGPL-3.0-only

package ingest

import (
	"errors"
	"flag"
	"fmt"
	"strings"

	"github.com/grafana/mimir/pkg/mimirpb"
)

const (
	compartmentIDPlaceholder = "<compartment-id>"
)

var (
	ErrCompartmentsInvalidNumCompartments = errors.New("compartments num_compartments must be greater than 0 when compartments are enabled")
	ErrCompartmentsInvalidTopicFormat     = fmt.Errorf("compartments topic_format must contain the %q placeholder when compartments are enabled", compartmentIDPlaceholder)
)

// CompartmentsConfig holds the configuration for read compartments.
type CompartmentsConfig struct {
	Enabled         bool   `yaml:"enabled"`
	NumCompartments int    `yaml:"num_compartments"`
	TopicFormat     string `yaml:"topic_format"`
}

func (cfg *CompartmentsConfig) RegisterFlagsWithPrefix(prefix string, f *flag.FlagSet) {
	f.BoolVar(&cfg.Enabled, prefix+"enabled", false, "Whether compartments are enabled. When enabled, series are sharded across multiple Kafka topics based on metric name.")
	f.IntVar(&cfg.NumCompartments, prefix+"num-compartments", 0, "The number of read compartments. Each compartment uses a dedicated Kafka topic.")
	f.StringVar(&cfg.TopicFormat, prefix+"topic-format", "", fmt.Sprintf("The topic name template with a %q placeholder that gets replaced with the compartment ID (e.g. mimir-read-comp-%s).", compartmentIDPlaceholder, compartmentIDPlaceholder))
}

// Validate returns an error if the config is invalid.
func (cfg *CompartmentsConfig) Validate() error {
	if !cfg.Enabled {
		return nil
	}
	if cfg.NumCompartments <= 0 {
		return ErrCompartmentsInvalidNumCompartments
	}
	if !strings.Contains(cfg.TopicFormat, compartmentIDPlaceholder) {
		return ErrCompartmentsInvalidTopicFormat
	}
	return nil
}

// CompartmentRouter assigns series to compartments based on a hash of the user ID and metric name.
type CompartmentRouter struct {
	numCompartments int
	topics          []string
}

// NewCompartmentRouter creates a new CompartmentRouter that pre-computes topic names for each compartment.
func NewCompartmentRouter(cfg CompartmentsConfig) *CompartmentRouter {
	topics := make([]string, cfg.NumCompartments)
	for i := range topics {
		topics[i] = strings.ReplaceAll(cfg.TopicFormat, compartmentIDPlaceholder, fmt.Sprintf("%d", i))
	}
	return &CompartmentRouter{
		numCompartments: cfg.NumCompartments,
		topics:          topics,
	}
}

// CompartmentForMetric returns the compartment index for a given tenant's metric name.
// The metric is assigned to a compartment by hashing userID + metric name
// and taking modulo numCompartments.
func (r *CompartmentRouter) CompartmentForMetric(userID, metricName string) int {
	hash := mimirpb.ShardByMetricName(userID, metricName)
	return int(hash % uint32(r.numCompartments))
}

// TopicForMetric returns the topic for a given tenant's metric name.
func (r *CompartmentRouter) TopicForMetric(userID, metricName string) string {
	return r.topics[r.CompartmentForMetric(userID, metricName)]
}

// NumCompartments returns the number of compartments.
func (r *CompartmentRouter) NumCompartments() int {
	return r.numCompartments
}

// Topic returns the topic for the given compartment index.
func (r *CompartmentRouter) Topic(compartmentID int) string {
	return r.topics[compartmentID]
}
