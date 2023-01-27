package aggregator

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPartitions(t *testing.T) {
	type testcase struct {
		cfg Config

		expError      string
		expPartitions []int
	}

	for name, tc := range map[string]testcase{
		"explicit partitions": {
			cfg:           Config{KafkaPartitions: "1,2,5-10,20"},
			expPartitions: []int{1, 2, 5, 6, 7, 8, 9, 10, 20},
		},

		"8 partitions, 3 readers, reader-0": {
			cfg: Config{
				KafkaPartitionsTotal:   8,
				KafkaPartitionsReaders: 3,
				KafkaReaderID:          "reader-0",
			},
			expPartitions: []int{0, 3, 6},
		},
		"8 partitions, 3 readers, reader-1": {
			cfg: Config{
				KafkaPartitionsTotal:   8,
				KafkaPartitionsReaders: 3,
				KafkaReaderID:          "reader-1",
			},
			expPartitions: []int{1, 4, 7},
		},
		"8 partitions, 3 readers, reader-2": {
			cfg: Config{
				KafkaPartitionsTotal:   8,
				KafkaPartitionsReaders: 3,
				KafkaReaderID:          "reader-2",
			},
			expPartitions: []int{2, 5},
		},

		"8 partitions, 4 readers, reader-0": {
			cfg: Config{
				KafkaPartitionsTotal:   8,
				KafkaPartitionsReaders: 4,
				KafkaReaderID:          "reader-0",
			},
			expPartitions: []int{0, 4},
		},

		"4 readers, too high reader id": {
			cfg: Config{
				KafkaPartitionsTotal:   8,
				KafkaPartitionsReaders: 4,
				KafkaReaderID:          "reader-10",
			},
			expError: "invalid aggregator.kafka-reader-id: reader-10 (readers: 4)",
		},
	} {
		t.Run(name, func(t *testing.T) {
			tc.cfg.KafkaTopic = "topic"
			err := tc.cfg.Validate()

			if tc.expError == "" {
				require.NoError(t, err)
				require.Equal(t, tc.expPartitions, tc.cfg.kafkaPartitions)
			} else {
				require.EqualError(t, err, tc.expError)
			}
		})
	}
}
