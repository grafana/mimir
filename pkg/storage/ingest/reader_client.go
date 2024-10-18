// SPDX-License-Identifier: AGPL-3.0-only

package ingest

import (
	"github.com/go-kit/log"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/plugin/kprom"
)

// NewKafkaReaderClient returns the kgo.Client that should be used by the Reader.
func NewKafkaReaderClient(cfg KafkaConfig, metrics *kprom.Metrics, logger log.Logger, opts ...kgo.Opt) (*kgo.Client, error) {
	const fetchMaxBytes = 100_000_000
	clientOpts := commonKafkaClientOptions(cfg, metrics, logger)
	clientOpts = append(clientOpts,
		// Fetch configuration is unused when using concurrent fetchers.
		kgo.FetchMinBytes(1),
		kgo.FetchMaxBytes(fetchMaxBytes),
		kgo.FetchMaxWait(defaultMinBytesMaxWaitTime),
		kgo.FetchMaxPartitionBytes(50_000_000),

		// BrokerMaxReadBytes sets the maximum response size that can be read from
		// Kafka. This is a safety measure to avoid OOMing on invalid responses.
		// franz-go recommendation is to set it 2x FetchMaxBytes.
		// With concurrent fetchers we set FetchMaxBytes and FetchMaxPartitionBytes on a per-request basis, so here we put a high enough limit that should work for those requests.
		kgo.BrokerMaxReadBytes(1_000_000_000),
	)
	// Allow the caller to override our defaults.
	clientOpts = append(clientOpts, opts...)
	client, err := kgo.NewClient(clientOpts...)
	if err != nil {
		return nil, errors.Wrap(err, "creating kafka client")
	}

	return client, nil
}

func NewKafkaReaderClientMetrics(component string, reg prometheus.Registerer) *kprom.Metrics {
	return kprom.NewMetrics("cortex_ingest_storage_reader",
		kprom.Registerer(prometheus.WrapRegistererWith(prometheus.Labels{"component": component}, reg)),
		// Do not export the client ID, because we use it to specify options to the backend.
		kprom.FetchAndProduceDetail(kprom.Batches, kprom.Records, kprom.CompressedBytes, kprom.UncompressedBytes))
}
