// SPDX-License-Identifier: AGPL-3.0-only

package usagetracker

import (
	"flag"

	"github.com/pkg/errors"

	"github.com/grafana/mimir/pkg/storage/ingest"
)

type EventsStorageConfig struct {
	TopicName string             `yaml:"topic_name"`
	Writer    ingest.KafkaConfig `yaml:"writer"`
	Reader    ingest.KafkaConfig `yaml:"reader"`
}

func (c *EventsStorageConfig) RegisterFlags(f *flag.FlagSet) {
	f.StringVar(&c.TopicName, "usage-tracker.events-storage.topic-name", "usage-tracker-events-storage", "Kafka topic name for usage tracker events")
	c.Writer.RegisterFlagsWithPrefix("usage-tracker.events-storage.writer", f)
	c.Reader.RegisterFlagsWithPrefix("usage-tracker.events-storage.reader", f)
}

func (c *EventsStorageConfig) Validate() error {
	if err := c.Writer.Validate(); err != nil {
		return errors.Wrap(err, "Kafka writer")
	}
	if err := c.Reader.Validate(); err != nil {
		return errors.Wrap(err, "Kafka reader")
	}

	return nil
}
