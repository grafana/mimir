// SPDX-License-Identifier: AGPL-3.0-only

package log

import (
	"bytes"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/stretchr/testify/require"
)

func TestDedupLogger(t *testing.T) {
	tcs := map[string]struct {
		repeatTimes         int
		kvs                 []interface{}
		callerPrefix        string
		targetKeys          []string
		maxDedupCount       int
		expireEntriesAfter  time.Duration
		collectEntriesEvery time.Duration
		waitBeforeAssert    time.Duration
		expectedOutput      string
	}{
		"empty entry": {
			repeatTimes:         1,
			kvs:                 nil,
			callerPrefix:        "push.go",
			targetKeys:          []string{"code", "user", "msg"},
			maxDedupCount:       maxDedupCount,
			expireEntriesAfter:  expireEntriesAfter,
			collectEntriesEvery: garbageCollectEvery,
			expectedOutput:      "",
		},
		"non-matching caller prefix": {
			repeatTimes:         1,
			kvs:                 []interface{}{"level", "info", "caller", "file.go:89", "msg", "caller prefix does not match"},
			callerPrefix:        "push.go",
			targetKeys:          []string{"level", "code", "user", "msg"},
			maxDedupCount:       maxDedupCount,
			expireEntriesAfter:  expireEntriesAfter,
			collectEntriesEvery: garbageCollectEvery,
			expectedOutput:      `level=info caller=file.go:89 msg="caller prefix does not match"` + "\n",
		},
		"missing target key": {
			repeatTimes:         1,
			kvs:                 []interface{}{"level", "info", "caller", "push.go:89", "code", 400, "msg", "missing target key"},
			callerPrefix:        "push.go",
			targetKeys:          []string{"level", "code", "user", "msg"},
			maxDedupCount:       maxDedupCount,
			expireEntriesAfter:  expireEntriesAfter,
			collectEntriesEvery: garbageCollectEvery,
			expectedOutput:      `level=info caller=push.go:89 code=400 msg="missing target key"` + "\n",
		},
		"forward entry after reaching max dedup count": {
			repeatTimes:         10,
			kvs:                 []interface{}{"level", "info", "caller", "push.go:89", "code", 400, "user", 21378, "msg", "max dedup reached"},
			callerPrefix:        "push.go",
			targetKeys:          []string{"level", "code", "user", "msg"},
			maxDedupCount:       10,
			expireEntriesAfter:  expireEntriesAfter,
			collectEntriesEvery: garbageCollectEvery,
			expectedOutput:      `level=info caller=push.go:89 code=400 user=21378 msg="max dedup reached" dedup=10` + "\n",
		},
		"forward entry after expiring": {
			repeatTimes:         2,
			kvs:                 []interface{}{"level", "info", "caller", "push.go:89", "code", 400, "user", 21378, "msg", "log entry expired"},
			callerPrefix:        "push.go",
			targetKeys:          []string{"level", "code", "user", "msg"},
			maxDedupCount:       maxDedupCount,
			expireEntriesAfter:  time.Millisecond * 500,
			collectEntriesEvery: time.Second,
			waitBeforeAssert:    time.Millisecond * 1500,
			expectedOutput:      `level=info caller=push.go:89 code=400 user=21378 msg="log entry expired" dedup=2` + "\n",
		},
		"forward entry after stopping": {
			repeatTimes:         2,
			kvs:                 []interface{}{"level", "info", "caller", "push.go:89", "code", 400, "msg", "deduper stopped"},
			callerPrefix:        "push.go",
			targetKeys:          []string{"level", "code", "msg"},
			maxDedupCount:       maxDedupCount,
			expireEntriesAfter:  expireEntriesAfter,
			collectEntriesEvery: garbageCollectEvery,
			expectedOutput:      `level=info caller=push.go:89 code=400 msg="deduper stopped" dedup=2` + "\n",
		},
		"odd key-values entry": {
			repeatTimes:         1,
			kvs:                 []interface{}{"level", "info", "caller", "push.go:89", "code", 400, "user", 21378, "msg"},
			callerPrefix:        "push.go",
			targetKeys:          []string{"level", "code", "user", "msg"},
			maxDedupCount:       maxDedupCount,
			expireEntriesAfter:  time.Millisecond * 500,
			collectEntriesEvery: time.Second,
			waitBeforeAssert:    time.Millisecond * 1500,
			expectedOutput:      `level=info caller=push.go:89 code=400 user=21378 msg=null` + "\n",
		},
	}
	for tn, tc := range tcs {
		t.Run(tn, func(t *testing.T) {
			buf := bytes.NewBuffer(nil)

			next := log.NewLogfmtLogger(buf)

			dd := newDedupLogger(
				tc.callerPrefix,
				tc.targetKeys,
				next,
				maxEntries,
				tc.maxDedupCount,
				tc.expireEntriesAfter,
				tc.collectEntriesEvery,
			)
			for i := 0; i < tc.repeatTimes; i++ {
				require.NoError(t, dd.Log(tc.kvs...))
			}
			if tc.waitBeforeAssert > 0 {
				time.Sleep(tc.waitBeforeAssert)
			}
			dd.Stop()

			require.Equal(t, tc.expectedOutput, buf.String())
		})
	}
}
