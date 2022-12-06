package main

import (
	"testing"
	"time"

	"github.com/grafana/dskit/flagext"
	"github.com/stretchr/testify/require"
)

func TestGetRandomRequestTimeRange(t *testing.T) {
	const numRuns = 1000

	tests := map[string]struct {
		cfg *Config
	}{
		"the configured max range is less than the configured max - min time": {
			cfg: &Config{
				TesterRequestMinTime:  flagext.Time(mustParseTime(time.RFC3339, "2022-12-01T00:00:00Z")),
				TesterRequestMaxTime:  flagext.Time(mustParseTime(time.RFC3339, "2022-12-01T01:00:00Z")),
				TesterRequestMinRange: 1 * time.Minute,
				TesterRequestMaxRange: 5 * time.Minute,
			},
		},
		"the configured max range is greater than the configured max - min time": {
			cfg: &Config{
				TesterRequestMinTime:  flagext.Time(mustParseTime(time.RFC3339, "2022-12-01T00:00:00Z")),
				TesterRequestMaxTime:  flagext.Time(mustParseTime(time.RFC3339, "2022-12-01T01:00:00Z")),
				TesterRequestMinRange: 30 * time.Minute,
				TesterRequestMaxRange: 90 * time.Minute,
			},
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			for r := 0; r < numRuns; r++ {
				actualStart, actualEnd := getRandomRequestTimeRange(testData.cfg)

				require.GreaterOrEqual(t, actualStart, time.Time(testData.cfg.TesterRequestMinTime).UnixMilli())
				require.LessOrEqual(t, actualEnd, time.Time(testData.cfg.TesterRequestMaxTime).UnixMilli())

				require.GreaterOrEqual(t, actualEnd-actualStart, testData.cfg.TesterRequestMinRange.Milliseconds())
				require.LessOrEqual(t, actualEnd-actualStart, testData.cfg.TesterRequestMaxRange.Milliseconds())
			}
		})
	}
}

func mustParseTime(layout, value string) time.Time {
	t, err := time.Parse(layout, value)
	if err != nil {
		panic(err)
	}

	return t
}
