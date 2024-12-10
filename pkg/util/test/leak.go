// SPDX-License-Identifier: AGPL-3.0-only

package test

import (
	"testing"

	"go.uber.org/goleak"
)

func VerifyNoLeak(t testing.TB, extraOpts ...goleak.Option) {
	// Run it as a cleanup function so that "last added, first called" ordering execution is guaranteed.
	t.Cleanup(func() {
		goleak.VerifyNone(t, append(goLeakOptions(), extraOpts...)...)
	})
}

func VerifyNoLeakTestMain(m *testing.M, extraOpts ...goleak.Option) {
	goleak.VerifyTestMain(m, append(goLeakOptions(), extraOpts...)...)
}

func goLeakOptions() []goleak.Option {
	return []goleak.Option{
		// Ignore opencensus default worker because it's started in a init() function.
		goleak.IgnoreTopFunction("go.opencensus.io/stats/view.(*worker).start"),

		// The FastRegexMatcher uses a global instance of ristretto.Cache which is never stopped,
		// so we ignore its gouroutines and then ones from glog which is a ristretto dependency.
		goleak.IgnoreTopFunction("github.com/dgraph-io/ristretto.(*defaultPolicy).processItems"),
		goleak.IgnoreTopFunction("github.com/dgraph-io/ristretto.(*Cache).processItems"),
		goleak.IgnoreTopFunction("github.com/golang/glog.(*fileSink).flushDaemon"),
	}
}
