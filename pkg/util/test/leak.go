// SPDX-License-Identifier: AGPL-3.0-only

package test

import (
	"testing"

	"go.uber.org/goleak"
)

func VerifyNoLeak(t testing.TB) {
	// Run it as a cleanup function so that "last added, first called" ordering execution is guaranteed.
	t.Cleanup(func() {
		goleak.VerifyNone(t, goLeakOptions()...)
	})
}

func VerifyNoLeakTestMain(m *testing.M) {
	goleak.VerifyTestMain(m, goLeakOptions()...)
}

func goLeakOptions() []goleak.Option {
	return []goleak.Option{
		// Ignore opencensus default worker because it's started in a init() function.
		goleak.IgnoreTopFunction("go.opencensus.io/stats/view.(*worker).start"),

		// The store-gateway BucketStore starts a goroutine in the index-header readers pool and
		// it gets closed when we close the BucketStore. However, we currently don't close BucketStore
		// on store-gateway termination so it never gets terminated.
		goleak.IgnoreTopFunction("github.com/grafana/mimir/pkg/storegateway/indexheader.NewReaderPool.func1"),
		// Similar to the one above, store-gateway BucketStore starts a goroutine in snapshotter
		// that manages lazy-loaded snapshots. It stops the snapshotter on BucketStore close.
		goleak.IgnoreTopFunction("github.com/grafana/mimir/pkg/storegateway/indexheader.(*Snapshotter).Start.func1"),

		// The FastRegexMatcher uses a global instance of ristretto.Cache which is never stopped,
		// so we ignore its gouroutines and then ones from glog which is a ristretto dependency.
		goleak.IgnoreTopFunction("github.com/dgraph-io/ristretto.(*defaultPolicy).processItems"),
		goleak.IgnoreTopFunction("github.com/dgraph-io/ristretto.(*Cache).processItems"),
		goleak.IgnoreTopFunction("github.com/golang/glog.(*fileSink).flushDaemon"),
	}
}
