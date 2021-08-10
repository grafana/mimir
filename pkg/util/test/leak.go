// SPDX-License-Identifier: AGPL-3.0-only

package test

import (
	"testing"

	"go.uber.org/goleak"
)

func VerifyNoLeak(t testing.TB) {
	opts := []goleak.Option{
		// Ignore opencensus default worker because it's started in a init() function.
		goleak.IgnoreTopFunction("go.opencensus.io/stats/view.(*worker).start"),

		// The store-gateway BucketStore starts a goroutine in the index-header readers pool and
		// it gets closed when we close the BucketStore. However, we currently don't close BucketStore
		// on store-gateway termination so it never gets terminated.
		goleak.IgnoreTopFunction("github.com/thanos-io/thanos/pkg/block/indexheader.NewReaderPool.func1"),
	}

	// Run it as a cleanup function so that "last added, first called" ordering execution is guaranteed.
	t.Cleanup(func() {
		goleak.VerifyNone(t, opts...)
	})
}
