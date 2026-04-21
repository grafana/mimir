// SPDX-License-Identifier: AGPL-3.0-only

package compactor

// SizeBucketLabel is the metric label name used to bucket compaction jobs by their input size.
// The scheduler-mode autoscaler uses this to compute a per-bucket sec/byte and predict drain
// time more accurately than a single global rate, because end-to-end duration scales
// sublinearly with bytes.
const SizeBucketLabel = "size_bucket"

// Size bucket label values. Boundaries are log-spaced (~×4) and fixed so label values
// are stable over time. To change the boundaries, edit SizeBucket and AllSizeBuckets
// together.
const (
	sizeBucketXS  = "xs"  // < 100 MiB
	sizeBucketS   = "s"   // 100 MiB – 1 GiB
	sizeBucketM   = "m"   // 1 GiB – 4 GiB
	sizeBucketL   = "l"   // 4 GiB – 16 GiB
	sizeBucketXL  = "xl"  // 16 GiB – 64 GiB
	sizeBucketXXL = "xxl" // >= 64 GiB
)

// SizeBucket returns the size bucket label value for the given number of bytes.
func SizeBucket(bytes uint64) string {
	switch {
	case bytes < 100<<20:
		return sizeBucketXS
	case bytes < 1<<30:
		return sizeBucketS
	case bytes < 4<<30:
		return sizeBucketM
	case bytes < 16<<30:
		return sizeBucketL
	case bytes < 64<<30:
		return sizeBucketXL
	default:
		return sizeBucketXXL
	}
}

// AllSizeBuckets returns all size bucket label values in ascending order of size. Callers
// use this to pre-initialize metric time series so they exist with value zero.
func AllSizeBuckets() []string {
	return []string{sizeBucketXS, sizeBucketS, sizeBucketM, sizeBucketL, sizeBucketXL, sizeBucketXXL}
}
