// SPDX-License-Identifier: AGPL-3.0-only

package continuoustest

import (
	"math"
	"strconv"
	"time"

	"github.com/prometheus/prometheus/prompb"
)

func alignTimestampToInterval(ts time.Time, interval time.Duration) time.Time {
	return time.Unix(0, (ts.UnixNano()/int64(interval))*int64(interval))
}

func generateSineWaveSeries(name string, t time.Time, numSeries int) []prompb.TimeSeries {
	out := make([]prompb.TimeSeries, 0, numSeries)
	value := generateSineWaveValue(t)

	for i := 0; i < numSeries; i++ {
		out = append(out, prompb.TimeSeries{
			Labels: []prompb.Label{{
				Name:  "__name__",
				Value: name,
			}, {
				Name:  "series_id",
				Value: strconv.Itoa(i),
			}},
			Samples: []prompb.Sample{{
				Value:     value,
				Timestamp: t.UnixMilli(),
			}},
		})
	}

	return out
}

func generateSineWaveValue(t time.Time) float64 {
	// With a 15-second scrape interval this gives a ten-minute period
	period := float64(40 * (15 * time.Second))
	radians := float64(t.UnixNano()) / period * 2 * math.Pi
	return math.Sin(radians)
}
