// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"context"
	"time"
)

// Carrying is what carrying per-series metadata costs a profile's records, which the on/off pairs leave out by design,
// as their records carry it either way: bytes per sample with the series' own metadata, and without it, as today's
// senders' records go, with their families' metadata still as stand-ins.
type Carrying struct {
	WithBytesPerSample    float64 `json:"with_bytes_per_sample"`
	WithoutBytesPerSample float64 `json:"without_bytes_per_sample"`
	Increase              float64 `json:"increase"`
}

// carryingCost produces the seconds of the profile's records twice, with and without the series' own metadata, and
// compares their bytes. The feed is deterministic, so both are of the same series and samples.
func carryingCost(profile Profile, series int, seconds int64) (Carrying, error) {
	start := time.Now().UnixMilli()/60_000*60_000 - 3_600_000
	bytesPerSample := func(without bool) (float64, error) {
		f, err := newFeed(newWorld(profile.Series, series, 1), "", 0)
		if err != nil {
			return 0, err
		}
		defer f.close()
		f.withoutSeriesMetadata = without
		if err := f.produceSeconds(context.Background(), start, start+seconds*1000); err != nil {
			return 0, err
		}
		return float64(f.recordBytes.Load()) / float64(f.samples.Load()), nil
	}
	with, err := bytesPerSample(false)
	if err != nil {
		return Carrying{}, err
	}
	without, err := bytesPerSample(true)
	if err != nil {
		return Carrying{}, err
	}
	return Carrying{WithBytesPerSample: with, WithoutBytesPerSample: without, Increase: with/without - 1}, nil
}
