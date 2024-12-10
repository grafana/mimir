// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"strings"
	"testing"

	"github.com/alecthomas/kingpin/v2"
	"github.com/twmb/franz-go/pkg/kgo"
)

func createKafkaClientProvider(t *testing.T, kafkaAddress, kafkaClientID string) (func() *kgo.Client, error) {
	client, err := CreateKafkaClient(kafkaAddress, kafkaClientID, nil)
	if err != nil {
		return nil, err
	}

	// Close the Kafka client once the test is done.
	t.Cleanup(client.Close)

	return func() *kgo.Client {
		return client
	}, nil
}

func newTestApp() *kingpin.Application {
	return kingpin.New("test", "").Terminate(nil)
}

func expectedLineRegexpByComponents(components ...string) string {
	return strings.Join(components, `\s+`)
}
