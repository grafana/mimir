// SPDX-License-Identifier: AGPL-3.0-only

package ingest

import (
	"errors"
	"flag"
	"fmt"
	"strings"

	"github.com/grafana/dskit/flagext"

	"github.com/grafana/mimir/pkg/mimirpb"
)

const (
	compartmentIDPlaceholder = "<compartment-id>"
)

var (
	ErrCompartmentsInvalidNumCompartments = errors.New("compartments num_compartments must be greater than 0 when compartments are enabled")
)

// CompartmentsConfig holds the configuration for read compartments.
type CompartmentsConfig struct {
	Enabled         bool `yaml:"enabled"`
	NumCompartments int  `yaml:"num_compartments"`
}

// RegisterFlagsWithPrefix registers the flags for CompartmentsConfig with the given prefix.
func (cfg *CompartmentsConfig) RegisterFlagsWithPrefix(prefix string, f *flag.FlagSet) {
	f.BoolVar(&cfg.Enabled, prefix+"enabled", false, "Whether compartments are enabled. When enabled, series are sharded across different Kafka backends based on metric name. Each compartment is fully parametrized via the <compartment-id> placeholder in the standard Kafka config fields (address, topic, sasl-username, sasl-password).")
	f.IntVar(&cfg.NumCompartments, prefix+"num-compartments", 0, "The number of read compartments. Each compartment uses a dedicated Kafka backend.")
}

// Validate returns an error if the config is invalid.
func (cfg *CompartmentsConfig) Validate() error {
	if !cfg.Enabled {
		return nil
	}
	if cfg.NumCompartments <= 0 {
		return ErrCompartmentsInvalidNumCompartments
	}
	return nil
}

// CompartmentRouter assigns series to compartments based on a hash of the user ID and metric name.
type CompartmentRouter struct {
	numCompartments int
}

// NewCompartmentRouter creates a new CompartmentRouter.
func NewCompartmentRouter(cfg CompartmentsConfig) *CompartmentRouter {
	return &CompartmentRouter{
		numCompartments: cfg.NumCompartments,
	}
}

// CompartmentForMetric returns the compartment index for a given tenant's metric name.
// The metric is assigned to a compartment by hashing userID + metric name
// and taking modulo numCompartments.
func (r *CompartmentRouter) CompartmentForMetric(userID, metricName string) int {
	hash := mimirpb.ShardByMetricName(userID, metricName)
	return int(hash % uint32(r.numCompartments))
}

// NumCompartments returns the number of compartments.
func (r *CompartmentRouter) NumCompartments() int {
	return r.numCompartments
}

// KafkaConfigForCompartment returns a copy of the base KafkaConfig with the <compartment-id>
// placeholder replaced by the given compartment ID in address, topic, SASL username and password.
func KafkaConfigForCompartment(base KafkaConfig, compartmentID int) KafkaConfig {
	cfg := base
	idStr := fmt.Sprintf("%d", compartmentID)

	// Replace in address slice (create new slice to avoid aliasing).
	newAddress := make(flagext.StringSliceCSV, len(cfg.Address))
	for i, addr := range cfg.Address {
		newAddress[i] = strings.ReplaceAll(addr, compartmentIDPlaceholder, idStr)
	}
	cfg.Address = newAddress

	// Replace in topic.
	cfg.Topic = strings.ReplaceAll(cfg.Topic, compartmentIDPlaceholder, idStr)

	// Replace in SASL credentials.
	cfg.SASL.Username = strings.ReplaceAll(cfg.SASL.Username, compartmentIDPlaceholder, idStr)
	cfg.SASL.Password = flagext.SecretWithValue(strings.ReplaceAll(cfg.SASL.Password.String(), compartmentIDPlaceholder, idStr))

	return cfg
}
