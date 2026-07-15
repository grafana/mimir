// SPDX-License-Identifier: AGPL-3.0-only

package ingest

import (
	"bytes"
	"testing"

	"github.com/go-kit/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestKafkaLogger_Log(t *testing.T) {
	tests := map[string]struct {
		level         kgo.LogLevel
		expectedLevel string
	}{
		"debug": {level: kgo.LogLevelDebug, expectedLevel: "level=debug"},
		"info":  {level: kgo.LogLevelInfo, expectedLevel: "level=info"},
		"warn":  {level: kgo.LogLevelWarn, expectedLevel: "level=warn"},
		"error": {level: kgo.LogLevelError, expectedLevel: "level=error"},
	}

	for name, testData := range tests {
		t.Run(name, func(t *testing.T) {
			var buf bytes.Buffer
			l := NewKafkaLogger(log.NewLogfmtLogger(&buf))

			l.Log(testData.level, "the message", "key", "value")

			out := buf.String()
			assert.Contains(t, out, testData.expectedLevel)
			assert.Contains(t, out, "component=kafka_client")
			assert.Contains(t, out, `msg="the message"`)
			assert.Contains(t, out, "key=value")
		})
	}
}

func TestKafkaLogger_Level(t *testing.T) {
	l := NewKafkaLogger(log.NewNopLogger())

	// Always Info so the Kafka client never emits (expensive) debug logs.
	require.Equal(t, kgo.LogLevelInfo, l.Level())
}
