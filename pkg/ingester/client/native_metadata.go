// SPDX-License-Identifier: AGPL-3.0-only

package client

import (
	"cmp"
	"errors"
	"maps"
	"slices"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// The native metadata lookups' errors that a querier tells apart from failures: a tenant whose native metadata is off
// in an ingester, and a lookup that more series matched than its cap. Both are FailedPrecondition, told apart by their
// messages, which no other ingester error starts with.
const (
	nativeMetadataDisabledMessage = "native metadata is disabled for the tenant"
	nativeLookupTooManyMessage    = "the native metadata lookup matched more series than its cap"
)

// ErrNativeMetadataDisabled is the status of a native metadata lookup of a tenant whose native metadata the ingester
// doesn't keep.
var ErrNativeMetadataDisabled = status.Error(codes.FailedPrecondition, nativeMetadataDisabledMessage)

// NativeLookupTooManySeriesError is the status of a native metadata lookup that more series matched than its cap.
func NativeLookupTooManySeriesError(maxSeries int) error {
	return status.Errorf(codes.FailedPrecondition, "%s of %d", nativeLookupTooManyMessage, maxSeries)
}

// IsNativeMetadataDisabled reports whether err is an ingester's ErrNativeMetadataDisabled.
func IsNativeMetadataDisabled(err error) bool {
	return hasStatusMessage(err, nativeMetadataDisabledMessage)
}

// IsNativeLookupTooManySeries reports whether err is an ingester's NativeLookupTooManySeriesError.
func IsNativeLookupTooManySeries(err error) bool {
	return hasStatusMessage(err, nativeLookupTooManyMessage)
}

func hasStatusMessage(err error, prefix string) bool {
	for ; err != nil; err = errors.Unwrap(err) {
		if s, ok := status.FromError(err); ok && s.Code() == codes.FailedPrecondition && strings.HasPrefix(s.Message(), prefix) {
			return true
		}
	}
	return false
}

// The lookups' versions are change points, oldest first: each is in force over [MinTimeMs, MaxTimeMs], until the next
// one starts, or for the last one, until the newest sample of its series. Answers about the same series, from the
// ingesters of a partition, and about the same target, from the series of every partition, merge into one sequence
// of change points by time.

// MergeNativeMetricMetadataVersions merges answers about one series' metric metadata into one sequence of versions.
func MergeNativeMetricMetadataVersions(answers [][]NativeMetricMetadataVersion) []NativeMetricMetadataVersion {
	return mergeVersions(answers, nativeMetricMetadataVersionOps)
}

// MergeNativeResourceVersions merges answers about one series' resources, or about the series of one target, into one
// sequence of versions.
func MergeNativeResourceVersions(answers [][]NativeResourceVersion) []NativeResourceVersion {
	return mergeVersions(answers, nativeResourceVersionOps)
}

type versionOps[V any] struct {
	minTime func(*V) int64
	maxTime func(*V) int64
	setTime func(v *V, minTime, maxTime int64)
	same    func(a, b *V) bool
	// compare orders versions by content, 0 exactly when same.
	compare func(a, b *V) int
}

var nativeMetricMetadataVersionOps = versionOps[NativeMetricMetadataVersion]{
	minTime: func(v *NativeMetricMetadataVersion) int64 { return v.MinTimeMs },
	maxTime: func(v *NativeMetricMetadataVersion) int64 { return v.MaxTimeMs },
	setTime: func(v *NativeMetricMetadataVersion, minTime, maxTime int64) {
		v.MinTimeMs, v.MaxTimeMs = minTime, maxTime
	},
	same: func(a, b *NativeMetricMetadataVersion) bool {
		return a.Type == b.Type && a.Unit == b.Unit && a.Help == b.Help
	},
	compare: func(a, b *NativeMetricMetadataVersion) int {
		return cmp.Or(cmp.Compare(a.Type, b.Type), strings.Compare(a.Unit, b.Unit), strings.Compare(a.Help, b.Help))
	},
}

var nativeResourceVersionOps = versionOps[NativeResourceVersion]{
	minTime: func(v *NativeResourceVersion) int64 { return v.MinTimeMs },
	maxTime: func(v *NativeResourceVersion) int64 { return v.MaxTimeMs },
	setTime: func(v *NativeResourceVersion, minTime, maxTime int64) { v.MinTimeMs, v.MaxTimeMs = minTime, maxTime },
	same: func(a, b *NativeResourceVersion) bool {
		return maps.Equal(a.Identifying, b.Identifying) && maps.Equal(a.Descriptive, b.Descriptive)
	},
	compare: func(a, b *NativeResourceVersion) int {
		return cmp.Or(compareAttributes(a.Identifying, b.Identifying), compareAttributes(a.Descriptive, b.Descriptive))
	},
}

// compareAttributes orders attributes by their sorted keys and values, a nil map as an empty one, as maps.Equal does.
func compareAttributes(a, b map[string]string) int {
	if maps.Equal(a, b) {
		return 0
	}
	aKeys, bKeys := slices.Sorted(maps.Keys(a)), slices.Sorted(maps.Keys(b))
	for i := range min(len(aKeys), len(bKeys)) {
		if c := cmp.Or(strings.Compare(aKeys[i], bKeys[i]), strings.Compare(a[aKeys[i]], b[bKeys[i]])); c != 0 {
			return c
		}
	}
	return cmp.Compare(len(aKeys), len(bKeys))
}

// mergeVersions merges answers' versions by their change points' times. At the same time, the answer with the newest
// sample wins: its ingester has read further. Of answers as new, the least content wins, so that the merge doesn't
// depend on the answers' order. Neighbours of the same content become one, since each answer sees a change at its own
// series' next sample. Each version then ends where the next starts, and the last at the newest sample of any answer.
func mergeVersions[V any](answers [][]V, ops versionOps[V]) []V {
	type changePoint struct {
		version V
		// The newest sample of the answer's series, which its last version ends at.
		newest int64
	}
	var points []changePoint
	newest := int64(0)
	haveNewest := false
	for _, answer := range answers {
		if len(answer) == 0 {
			continue
		}
		answerNewest := ops.maxTime(&answer[len(answer)-1])
		if !haveNewest || answerNewest > newest {
			newest, haveNewest = answerNewest, true
		}
		for _, v := range answer {
			points = append(points, changePoint{version: v, newest: answerNewest})
		}
	}
	if len(points) == 0 {
		return nil
	}
	slices.SortStableFunc(points, func(a, b changePoint) int {
		if c := cmp.Compare(ops.minTime(&a.version), ops.minTime(&b.version)); c != 0 {
			return c
		}
		return cmp.Compare(b.newest, a.newest)
	})
	merged := make([]V, 0, len(points))
	for i := 0; i < len(points); {
		// The points at one time, the newest answers' first: one of those wins, and every other point at the time lost
		// to it, also when the winner continues the version before.
		winner, next := i, i+1
		for ; next < len(points) && ops.minTime(&points[next].version) == ops.minTime(&points[i].version); next++ {
			if points[next].newest == points[i].newest && !ops.same(&points[next].version, &points[winner].version) && ops.compare(&points[next].version, &points[winner].version) < 0 {
				winner = next
			}
		}
		v := points[winner].version
		i = next
		// A neighbour of the same content continues the last version.
		if n := len(merged); n > 0 && ops.same(&v, &merged[n-1]) {
			continue
		}
		merged = append(merged, v)
	}
	for i := range merged {
		end := newest
		if i+1 < len(merged) {
			end = ops.minTime(&merged[i+1]) - 1
		}
		minTime := ops.minTime(&merged[i])
		ops.setTime(&merged[i], minTime, max(end, minTime))
	}
	return merged
}
