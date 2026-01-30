// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/util/time.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package util

import (
	"math"
	"math/rand"
	"strconv"
	"time"

	"github.com/efficientgo/core/errors"
	"github.com/prometheus/common/model"
)

func TimeToMillis(t time.Time) int64 {
	return t.Unix()*1000 + int64(t.Nanosecond())/int64(time.Millisecond)
}

// TimeFromMillis is a helper to turn milliseconds -> time.Time
func TimeFromMillis(ms int64) time.Time {
	return time.Unix(ms/1000, (ms%1000)*int64(time.Millisecond)).UTC()
}

// FormatTimeMillis returns a human-readable version of the input time (in milliseconds).
func FormatTimeMillis(ms int64) string {
	return TimeFromMillis(ms).String()
}

// FormatTimeModel returns a human-readable version of the input time.
func FormatTimeModel(t model.Time) string {
	return TimeFromMillis(int64(t)).String()
}

// ParseTime parses the string into an int64 time, unix milliseconds since epoch.
func ParseTime(s string) (int64, error) {
	if t, err := strconv.ParseFloat(s, 64); err == nil {
		s, ns := math.Modf(t)
		ns = math.Round(ns*1000) / 1000
		tm := time.Unix(int64(s), int64(ns*float64(time.Second)))
		return TimeToMillis(tm), nil
	}
	if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
		return TimeToMillis(t), nil
	}
	return 0, errors.Newf("cannot parse %q to a valid timestamp", s)
}

// ParseDurationMS parses the string into an int64 duration, the elapsed nanoseconds between two instants
func ParseDurationMS(s string) (int64, error) {
	if d, err := strconv.ParseFloat(s, 64); err == nil {
		ts := d * float64(time.Second/time.Millisecond)
		if ts > float64(math.MaxInt64) || ts < float64(math.MinInt64) {
			return 0, errors.Newf("cannot parse %q to a valid duration. It overflows int64", s)
		}
		return int64(ts), nil
	}
	if d, err := model.ParseDuration(s); err == nil {
		return int64(d) / int64(time.Millisecond/time.Nanosecond), nil
	}
	return 0, errors.Newf("cannot parse %q to a valid duration", s)
}

// DurationWithJitter returns random duration from "input - input*variance" to "input + input*variance" interval.
func DurationWithJitter(input time.Duration, variancePerc float64) time.Duration {
	variance := int64(float64(input) * variancePerc)
	if variance <= 0 {
		return input
	}

	jitter := rand.Int63n(variance*2) - variance

	return input + time.Duration(jitter)
}

// DurationWithPositiveJitter returns random duration from "input" to "input + input*variance" interval.
func DurationWithPositiveJitter(input time.Duration, variancePerc float64) time.Duration {
	variance := int64(float64(input) * variancePerc)
	if variance <= 0 {
		return input
	}

	jitter := rand.Int63n(variance)

	return input + time.Duration(jitter)
}

// DurationWithNegativeJitter returns random duration from "input - input*variance" to "input" interval.
func DurationWithNegativeJitter(input time.Duration, variancePerc float64) time.Duration {
	variance := int64(float64(input) * variancePerc)
	if variance <= 0 {
		return input
	}

	jitter := rand.Int63n(variance)

	return input - time.Duration(jitter)
}

// NewDisableableTicker essentially wraps NewTicker but allows the ticker to be disabled by passing
// zero duration as the interval. Returns a function for stopping the ticker, and the ticker channel.
func NewDisableableTicker(interval time.Duration) (func(), <-chan time.Time) {
	if interval == 0 {
		return func() {}, nil
	}

	tick := time.NewTicker(interval)
	return func() { tick.Stop() }, tick.C
}

// UnixSeconds is Unix timestamp with seconds precision.
type UnixSeconds int64

func UnixSecondsFromTime(t time.Time) UnixSeconds {
	return UnixSeconds(t.Unix())
}

func (t UnixSeconds) Time() time.Time {
	return time.Unix(int64(t), 0)
}
