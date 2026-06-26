// SPDX-License-Identifier: AGPL-3.0-only

package blockbuilder

import (
	"github.com/grafana/mimir/pkg/compartments"
	"github.com/grafana/mimir/pkg/storage/ingest"
)

// wcClientConfigs returns the Kafka config for each write compartment (WC) the
// block-builder must read from. In non-compartment mode it returns the single
// base config. With compartments enabled it returns one config per write
// compartment, each targeting that compartment's cluster (address and SASL are
// templated by WriteCompartmentConfig). The topic is left as the base topic: the
// builder consumes the read compartment topic carried in each job spec, not the
// config's topic, so it doesn't need resolving here.
func wcClientConfigs(base ingest.KafkaConfig, comp compartments.Config) []ingest.KafkaConfig {
	if !comp.Enabled {
		return []ingest.KafkaConfig{base}
	}
	out := make([]ingest.KafkaConfig, comp.Write.NumCompartments)
	for wc := range out {
		out[wc] = base.WriteCompartmentConfig(wc)
	}
	return out
}
