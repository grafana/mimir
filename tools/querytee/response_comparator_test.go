// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/tools/querytee/response_comparator_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package querytee

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/require"
)

func TestCompareMatrix(t *testing.T) {
	for _, tc := range []struct {
		name     string
		expected json.RawMessage
		actual   json.RawMessage
		err      string
	}{
		{
			name:     "no metrics",
			expected: json.RawMessage(`[]`),
			actual:   json.RawMessage(`[]`),
		},
		{
			name: "no metrics in actual response",
			expected: json.RawMessage(`[
							{"metric":{"foo":"bar"},"values":[[1,"1"]]}
						]`),
			actual: json.RawMessage(`[]`),
			err:    "expected 1 metrics but got 0",
		},
		{
			name: "extra metric in actual response",
			expected: json.RawMessage(`[
							{"metric":{"foo":"bar"},"values":[[1,"1"]]}
						]`),
			actual: json.RawMessage(`[
							{"metric":{"foo":"bar"},"values":[[1,"1"]]},
							{"metric":{"foo1":"bar1"},"values":[[1,"1"]]}
						]`),
			err: "expected 1 metrics but got 2",
		},
		{
			name: "same number of metrics but with different labels",
			expected: json.RawMessage(`[
							{"metric":{"foo":"bar"},"values":[[1,"1"]]}
						]`),
			actual: json.RawMessage(`[
							{"metric":{"foo1":"bar1"},"values":[[1,"1"]]}
						]`),
			err: `expected metric {foo="bar"} missing from actual response`,
		},
		{
			name: "difference in number of float samples",
			expected: json.RawMessage(`[
							{"metric":{"foo":"bar"},"values":[[1,"1"],[2,"2"]]}
						]`),
			actual: json.RawMessage(`[
							{"metric":{"foo":"bar"},"values":[[1,"1"]]}
						]`),
			err: `expected 2 float sample(s) and 0 histogram sample(s) for metric {foo="bar"} but got 1 float sample(s) and 0 histogram sample(s)
Expected result for series:
{foo="bar"} =>
1 @[1]
2 @[2]

Actual result for series:
{foo="bar"} =>
1 @[1]`,
		},
		{
			name: "difference in float sample timestamp",
			expected: json.RawMessage(`[
							{"metric":{"foo":"bar"},"values":[[1,"1"],[2,"2"]]}
						]`),
			actual: json.RawMessage(`[
							{"metric":{"foo":"bar"},"values":[[1,"1"],[3,"2"]]}
						]`),
			err: `float sample pair does not match for metric {foo="bar"}: expected timestamp 2 (1970-01-01T00:00:02Z) but got 3 (1970-01-01T00:00:03Z)
Expected result for series:
{foo="bar"} =>
1 @[1]
2 @[2]

Actual result for series:
{foo="bar"} =>
1 @[1]
2 @[3]`,
		},
		{
			name: "difference in float sample value",
			expected: json.RawMessage(`[
							{"metric":{"foo":"bar"},"values":[[1,"1"],[2,"2"]]}
						]`),
			actual: json.RawMessage(`[
							{"metric":{"foo":"bar"},"values":[[1,"1"],[2,"3"]]}
						]`),
			err: `float sample pair does not match for metric {foo="bar"}: expected value 2 for timestamp 2 (1970-01-01T00:00:02Z) but got 3
Expected result for series:
{foo="bar"} =>
1 @[1]
2 @[2]

Actual result for series:
{foo="bar"} =>
1 @[1]
3 @[2]`,
		},
		{
			name: "actual float samples match expected",
			expected: json.RawMessage(`[
							{"metric":{"foo":"bar"},"values":[[1,"1"],[2,"2"]]}
						]`),
			actual: json.RawMessage(`[
							{"metric":{"foo":"bar"},"values":[[1,"1"],[2,"2"]]}
						]`),
		},
		{
			name: "first series match but later series has difference in sample value",
			expected: json.RawMessage(`[
							{"metric":{"foo":"bar"},"values":[[1,"1"],[2,"2"]]},
							{"metric":{"oops":"bar"},"values":[[1,"1"],[2,"2"]]}
						]`),
			actual: json.RawMessage(`[
							{"metric":{"foo":"bar"},"values":[[1,"1"],[2,"2"]]},
							{"metric":{"oops":"bar"},"values":[[1,"1"],[2,"3"]]}
						]`),
			err: `float sample pair does not match for metric {oops="bar"}: expected value 2 for timestamp 2 (1970-01-01T00:00:02Z) but got 3
Expected result for series:
{oops="bar"} =>
1 @[1]
2 @[2]

Actual result for series:
{oops="bar"} =>
1 @[1]
3 @[2]`,
		},
		{
			name: "single expected sample has histogram but actual sample has float value",
			expected: json.RawMessage(`[
							{
								"metric": {"foo":"bar"},
								"histograms": [
									[1, {
										"count": "2", 
										"sum": "3", 
										"buckets": [
											[1,"0","2","2"]
										]
									}]
								]
							}
						]`),
			actual: json.RawMessage(`[
							{
								"metric": {"foo":"bar"},
								"values": [[1,"1"]]
							}
						]`),
			err: `expected 0 float sample(s) and 1 histogram sample(s) for metric {foo="bar"} but got 1 float sample(s) and 0 histogram sample(s)
Expected result for series:
{foo="bar"} =>
Count: 2.000000, Sum: 3.000000, Buckets: [[0,2):2] @[1]

Actual result for series:
{foo="bar"} =>
1 @[1]`,
		},
		{
			name: "single expected sample has float value but actual sample has histogram",
			expected: json.RawMessage(`[
							{
								"metric": {"foo":"bar"},
								"values": [[1,"1"]]
							}
						]`),
			actual: json.RawMessage(`[
							{
								"metric": {"foo":"bar"},
								"histograms": [
									[2, {
										"count": "2", 
										"sum": "3", 
										"buckets": [
											[1,"0","2","2"]
										]
									}]
								]
							}
						]`),
			err: `expected 1 float sample(s) and 0 histogram sample(s) for metric {foo="bar"} but got 0 float sample(s) and 1 histogram sample(s)
Expected result for series:
{foo="bar"} =>
1 @[1]

Actual result for series:
{foo="bar"} =>
Count: 2.000000, Sum: 3.000000, Buckets: [[0,2):2] @[2]`,
		},
		{
			name: "difference in histogram sample timestamp",
			expected: json.RawMessage(`[
							{
								"metric": {"foo":"bar"},
								"histograms": [
									[1, {
										"count": "2", 
										"sum": "3", 
										"buckets": [
											[1,"0","2","2"]
										]
									}]
								]
							}
						]`),
			actual: json.RawMessage(`[
							{
								"metric": {"foo":"bar"},
								"histograms": [
									[2, {
										"count": "2", 
										"sum": "3", 
										"buckets": [
											[1,"0","2","2"]
										]
									}]
								]
							}
						]`),
			err: `histogram sample pair does not match for metric {foo="bar"}: expected timestamp 1 but got 2
Expected result for series:
{foo="bar"} =>
Count: 2.000000, Sum: 3.000000, Buckets: [[0,2):2] @[1]

Actual result for series:
{foo="bar"} =>
Count: 2.000000, Sum: 3.000000, Buckets: [[0,2):2] @[2]`,
		},
		{
			name: "difference in histogram sample count",
			expected: json.RawMessage(`[
							{
								"metric": {"foo":"bar"},
								"histograms": [
									[1, {
										"count": "2", 
										"sum": "3", 
										"buckets": [
											[1,"0","2","2"]
										]
									}]
								]
							}
						]`),
			actual: json.RawMessage(`[
							{
								"metric": {"foo":"bar"},
								"histograms": [
									[1, {
										"count": "5", 
										"sum": "3", 
										"buckets": [
											[1,"0","2","2"]
										]
									}]
								]
							}
						]`),
			err: `histogram sample pair does not match for metric {foo="bar"}: expected count 2 for timestamp 1 but got 5
Expected result for series:
{foo="bar"} =>
Count: 2.000000, Sum: 3.000000, Buckets: [[0,2):2] @[1]

Actual result for series:
{foo="bar"} =>
Count: 5.000000, Sum: 3.000000, Buckets: [[0,2):2] @[1]`,
		},
		{
			name: "difference in histogram sample sum",
			expected: json.RawMessage(`[
							{
								"metric": {"foo":"bar"},
								"histograms": [
									[1, {
										"count": "2", 
										"sum": "3", 
										"buckets": [
											[1,"0","2","2"]
										]
									}]
								]
							}
						]`),
			actual: json.RawMessage(`[
							{
								"metric": {"foo":"bar"},
								"histograms": [
									[1, {
										"count": "2", 
										"sum": "5", 
										"buckets": [
											[1,"0","2","2"]
										]
									}]
								]
							}
						]`),
			err: `histogram sample pair does not match for metric {foo="bar"}: expected sum 3 for timestamp 1 but got 5
Expected result for series:
{foo="bar"} =>
Count: 2.000000, Sum: 3.000000, Buckets: [[0,2):2] @[1]

Actual result for series:
{foo="bar"} =>
Count: 2.000000, Sum: 5.000000, Buckets: [[0,2):2] @[1]`,
		},
		{
			name: "difference in histogram sample buckets length",
			expected: json.RawMessage(`[
							{
								"metric": {"foo":"bar"},
								"histograms": [
									[1, {
										"count": "2", 
										"sum": "3", 
										"buckets": [
											[1,"0","2","2"]
										]
									}]
								]
							}
						]`),
			actual: json.RawMessage(`[
							{
								"metric": {"foo":"bar"},
								"histograms": [
									[1, {
										"count": "2", 
										"sum": "3", 
										"buckets": [
											[1,"0","2","2"], 
											[1,"2","4","2"]
										]
									}]
								]
							}
						]`),
			err: `histogram sample pair does not match for metric {foo="bar"}: expected buckets [[0,2):2] for timestamp 1 but got [[0,2):2 [2,4):2]
Expected result for series:
{foo="bar"} =>
Count: 2.000000, Sum: 3.000000, Buckets: [[0,2):2] @[1]

Actual result for series:
{foo="bar"} =>
Count: 2.000000, Sum: 3.000000, Buckets: [[0,2):2 [2,4):2] @[1]`,
		},
		{
			name: "difference in histogram sample buckets boundaries",
			expected: json.RawMessage(`[
							{
								"metric": {"foo":"bar"},
								"histograms": [
									[1, {
										"count": "2", 
										"sum": "3", 
										"buckets": [
											[1,"0","2","2"]
										]
									}]
								]
							}
						]`),
			actual: json.RawMessage(`[
							{
								"metric": {"foo":"bar"},
								"histograms": [
									[1, {
										"count": "2", 
										"sum": "3", 
										"buckets": [
											[2,"0","2","2"]
										]
									}]
								]
							}
						]`),
			err: `histogram sample pair does not match for metric {foo="bar"}: expected buckets [[0,2):2] for timestamp 1 but got [(0,2):2]
Expected result for series:
{foo="bar"} =>
Count: 2.000000, Sum: 3.000000, Buckets: [[0,2):2] @[1]

Actual result for series:
{foo="bar"} =>
Count: 2.000000, Sum: 3.000000, Buckets: [(0,2):2] @[1]`,
		},
		{
			name: "difference in histogram sample buckets lower boundary",
			expected: json.RawMessage(`[
							{
								"metric": {"foo":"bar"},
								"histograms": [
									[1, {
										"count": "2", 
										"sum": "3", 
										"buckets": [
											[1,"0","2","2"]
										]
									}]
								]
							}
						]`),
			actual: json.RawMessage(`[
							{
								"metric": {"foo":"bar"},
								"histograms": [
									[1, {
										"count": "2", 
										"sum": "3", 
										"buckets": [
											[1,"1","2","2"]
										]
									}]
								]
							}
						]`),
			err: `histogram sample pair does not match for metric {foo="bar"}: expected buckets [[0,2):2] for timestamp 1 but got [[1,2):2]
Expected result for series:
{foo="bar"} =>
Count: 2.000000, Sum: 3.000000, Buckets: [[0,2):2] @[1]

Actual result for series:
{foo="bar"} =>
Count: 2.000000, Sum: 3.000000, Buckets: [[1,2):2] @[1]`,
		},
		{
			name: "difference in histogram sample buckets upper boundary",
			expected: json.RawMessage(`[
							{
								"metric": {"foo":"bar"},
								"histograms": [
									[1, {
										"count": "2", 
										"sum": "3", 
										"buckets": [
											[1,"0","2","2"]
										]
									}]
								]
							}
						]`),
			actual: json.RawMessage(`[
							{
								"metric": {"foo":"bar"},
								"histograms": [
									[1, {
										"count": "2", 
										"sum": "3", 
										"buckets": [
											[1,"0","3","2"]
										]
									}]
								]
							}
						]`),
			err: `histogram sample pair does not match for metric {foo="bar"}: expected buckets [[0,2):2] for timestamp 1 but got [[0,3):2]
Expected result for series:
{foo="bar"} =>
Count: 2.000000, Sum: 3.000000, Buckets: [[0,2):2] @[1]

Actual result for series:
{foo="bar"} =>
Count: 2.000000, Sum: 3.000000, Buckets: [[0,3):2] @[1]`,
		},
		{
			name: "difference in histogram sample buckets count",
			expected: json.RawMessage(`[
							{
								"metric": {"foo":"bar"},
								"histograms": [
									[1, {
										"count": "2", 
										"sum": "3", 
										"buckets": [
											[1,"0","2","2"]
										]
									}]
								]
							}
						]`),
			actual: json.RawMessage(`[
							{
								"metric": {"foo":"bar"},
								"histograms": [
									[1, {
										"count": "2", 
										"sum": "3", 
										"buckets": [
											[1,"0","2","3"]
										]
									}]
								]
							}
						]`),
			err: `histogram sample pair does not match for metric {foo="bar"}: expected buckets [[0,2):2] for timestamp 1 but got [[0,2):3]
Expected result for series:
{foo="bar"} =>
Count: 2.000000, Sum: 3.000000, Buckets: [[0,2):2] @[1]

Actual result for series:
{foo="bar"} =>
Count: 2.000000, Sum: 3.000000, Buckets: [[0,2):3] @[1]`,
		},
		{
			name: "single actual histogram value matches expected",
			expected: json.RawMessage(`[
							{
								"metric": {"foo":"bar"},
								"histograms": [
									[1, {
										"count": "2", 
										"sum": "3", 
										"buckets": [
											[1,"0","2","2"]
										]
									}]
								]
							}
						]`),
			actual: json.RawMessage(`[
							{
								"metric": {"foo":"bar"},
								"histograms": [
									[1, {
										"count": "2", 
										"sum": "3", 
										"buckets": [
											[1,"0","2","2"]
										]
									}]
								]
							}
						]`),
		},
		{
			name: "multiple actual histogram values match expected",
			expected: json.RawMessage(`[
							{
								"metric": {"foo":"bar"},
								"histograms": [
									[1, {
										"count": "2", 
										"sum": "3", 
										"buckets": [
											[1,"0","2","2"]
										]
									}],
									[2, {
										"count": "4", 
										"sum": "5", 
										"buckets": [
											[1,"0","2","4"]
										]
									}]
								]
							}
						]`),
			actual: json.RawMessage(`[
							{
								"metric": {"foo":"bar"},
								"histograms": [
									[1, {
										"count": "2", 
										"sum": "3", 
										"buckets": [
											[1,"0","2","2"]
										]
									}],
									[2, {
										"count": "4", 
										"sum": "5", 
										"buckets": [
											[1,"0","2","4"]
										]
									}]
								]
							}
						]`),
		},
		{
			name: "multiple histogram samples, but one is different",
			expected: json.RawMessage(`[
							{
								"metric": {"foo":"bar"},
								"histograms": [
									[1, {
										"count": "2", 
										"sum": "3", 
										"buckets": [
											[1,"0","2","2"]
										]
									}],
									[2, {
										"count": "4", 
										"sum": "5", 
										"buckets": [
											[1,"0","2","4"]
										]
									}]
								]
							}
						]`),
			actual: json.RawMessage(`[
							{
								"metric": {"foo":"bar"},
								"histograms": [
									[1, {
										"count": "2", 
										"sum": "3", 
										"buckets": [
											[1,"0","2","2"]
										]
									}],
									[2, {
										"count": "4", 
										"sum": "6", 
										"buckets": [
											[1,"0","2","4"]
										]
									}]
								]
							}
						]`),
			err: `histogram sample pair does not match for metric {foo="bar"}: expected sum 5 for timestamp 2 but got 6
Expected result for series:
{foo="bar"} =>
Count: 2.000000, Sum: 3.000000, Buckets: [[0,2):2] @[1]
Count: 4.000000, Sum: 5.000000, Buckets: [[0,2):4] @[2]

Actual result for series:
{foo="bar"} =>
Count: 2.000000, Sum: 3.000000, Buckets: [[0,2):2] @[1]
Count: 4.000000, Sum: 6.000000, Buckets: [[0,2):4] @[2]`,
		},
		{
			name: "actual result has different number of histogram samples to expected result",
			expected: json.RawMessage(`[
							{
								"metric": {"foo":"bar"},
								"histograms": [
									[1, {
										"count": "2", 
										"sum": "3", 
										"buckets": [
											[1,"0","2","2"]
										]
									}],
									[2, {
										"count": "4", 
										"sum": "5", 
										"buckets": [
											[1,"0","2","4"]
										]
									}]
								]
							}
						]`),
			actual: json.RawMessage(`[
							{
								"metric": {"foo":"bar"},
								"histograms": [
									[1, {
										"count": "2", 
										"sum": "3", 
										"buckets": [
											[1,"0","2","2"]
										]
									}]
								]
							}
						]`),
			err: `expected 0 float sample(s) and 2 histogram sample(s) for metric {foo="bar"} but got 0 float sample(s) and 1 histogram sample(s)
Expected result for series:
{foo="bar"} =>
Count: 2.000000, Sum: 3.000000, Buckets: [[0,2):2] @[1]
Count: 4.000000, Sum: 5.000000, Buckets: [[0,2):4] @[2]

Actual result for series:
{foo="bar"} =>
Count: 2.000000, Sum: 3.000000, Buckets: [[0,2):2] @[1]`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := compareMatrix(tc.expected, tc.actual, time.Now(), SampleComparisonOptions{})
			if tc.err == "" {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			require.EqualError(t, err, tc.err)
		})
	}
}

func TestCompareVector(t *testing.T) {
	for _, tc := range []struct {
		name     string
		expected json.RawMessage
		actual   json.RawMessage
		err      error
	}{
		{
			name:     "no metrics",
			expected: json.RawMessage(`[]`),
			actual:   json.RawMessage(`[]`),
		},
		{
			name: "no metrics in actual response",
			expected: json.RawMessage(`[
							{"metric":{"foo":"bar"},"value":[1,"1"]}
						]`),
			actual: json.RawMessage(`[]`),
			err:    errors.New("expected 1 metrics but got 0"),
		},
		{
			name: "extra metric in actual response",
			expected: json.RawMessage(`[
							{"metric":{"foo":"bar"},"value":[1,"1"]}
						]`),
			actual: json.RawMessage(`[
							{"metric":{"foo":"bar"},"value":[1,"1"]},
							{"metric":{"foo1":"bar1"},"value":[1,"1"]}
						]`),
			err: errors.New("expected 1 metrics but got 2"),
		},
		{
			name: "same number of metrics but with different labels",
			expected: json.RawMessage(`[
							{"metric":{"foo":"bar"},"value":[1,"1"]}
						]`),
			actual: json.RawMessage(`[
							{"metric":{"foo1":"bar1"},"value":[1,"1"]}
						]`),
			err: errors.New(`expected metric {foo="bar"} missing from actual response`),
		},
		{
			name: "difference in float sample timestamp",
			expected: json.RawMessage(`[
							{"metric":{"foo":"bar"},"value":[1,"1"]}
						]`),
			actual: json.RawMessage(`[
							{"metric":{"foo":"bar"},"value":[2,"1"]}
						]`),
			err: errors.New(`float sample pair does not match for metric {foo="bar"}: expected timestamp 1 (1970-01-01T00:00:01Z) but got 2 (1970-01-01T00:00:02Z)`),
		},
		{
			name: "difference in float sample value",
			expected: json.RawMessage(`[
							{"metric":{"foo":"bar"},"value":[1,"1"]}
						]`),
			actual: json.RawMessage(`[
							{"metric":{"foo":"bar"},"value":[1,"2"]}
						]`),
			err: errors.New(`float sample pair does not match for metric {foo="bar"}: expected value 1 for timestamp 1 (1970-01-01T00:00:01Z) but got 2`),
		},
		{
			name: "correct float samples",
			expected: json.RawMessage(`[
							{"metric":{"foo":"bar"},"value":[1,"1"]}
						]`),
			actual: json.RawMessage(`[
							{"metric":{"foo":"bar"},"value":[1,"1"]}
						]`),
		},
		{
			name: "expected sample has histogram but actual sample has float value",
			expected: json.RawMessage(`[
							{
								"metric": {"foo":"bar"},
								"histogram": [1, {
									"count": "2", 
									"sum": "3", 
									"buckets": [
										[1,"0","2","2"]
									]
								}]
							}
						]`),
			actual: json.RawMessage(`[
							{
								"metric": {"foo":"bar"},
								"value":[1,"1"]
							}
						]`),
			err: errors.New(`sample pair does not match for metric {foo="bar"}: expected histogram but got float value`),
		},
		{
			name: "expected sample has float value but actual sample has histogram",
			expected: json.RawMessage(`[
							{
								"metric": {"foo":"bar"},
								"value":[1,"1"]
							}
						]`),
			actual: json.RawMessage(`[
							{
								"metric": {"foo":"bar"},
								"histogram": [2, {
									"count": "2", 
									"sum": "3", 
									"buckets": [
										[1,"0","2","2"]
									]
								}]
							}
						]`),
			err: errors.New(`sample pair does not match for metric {foo="bar"}: expected float value but got histogram`),
		},
		{
			name: "difference in histogram sample timestamp",
			expected: json.RawMessage(`[
							{
								"metric": {"foo":"bar"},
								"histogram": [1, {
									"count": "2", 
									"sum": "3", 
									"buckets": [
										[1,"0","2","2"]
									]
								}]
							}
						]`),
			actual: json.RawMessage(`[
							{
								"metric": {"foo":"bar"},
								"histogram": [2, {
									"count": "2", 
									"sum": "3", 
									"buckets": [
										[1,"0","2","2"]
									]
								}]
							}
						]`),
			err: errors.New(`histogram sample pair does not match for metric {foo="bar"}: expected timestamp 1 but got 2`),
		},
		{
			name: "difference in histogram sample count",
			expected: json.RawMessage(`[
							{
								"metric": {"foo":"bar"},
								"histogram": [1, {
									"count": "2", 
									"sum": "3", 
									"buckets": [
										[1,"0","2","2"]
									]
								}]
							}
						]`),
			actual: json.RawMessage(`[
							{
								"metric": {"foo":"bar"},
								"histogram": [1, {
									"count": "5", 
									"sum": "3", 
									"buckets": [
										[1,"0","2","2"]
									]
								}]
							}
						]`),
			err: errors.New(`histogram sample pair does not match for metric {foo="bar"}: expected count 2 for timestamp 1 but got 5`),
		},
		{
			name: "difference in histogram sample sum",
			expected: json.RawMessage(`[
							{
								"metric": {"foo":"bar"},
								"histogram": [1, {
									"count": "2", 
									"sum": "3", 
									"buckets": [
										[1,"0","2","2"]
									]
								}]
							}
						]`),
			actual: json.RawMessage(`[
							{
								"metric": {"foo":"bar"},
								"histogram": [1, {
									"count": "2", 
									"sum": "5", 
									"buckets": [
										[1,"0","2","2"]
									]
								}]
							}
						]`),
			err: errors.New(`histogram sample pair does not match for metric {foo="bar"}: expected sum 3 for timestamp 1 but got 5`),
		},
		{
			name: "difference in histogram sample buckets length",
			expected: json.RawMessage(`[
							{
								"metric": {"foo":"bar"},
								"histogram": [1, {
									"count": "2", 
									"sum": "3", 
									"buckets": [
										[1,"0","2","2"]
									]
								}]
							}
						]`),
			actual: json.RawMessage(`[
							{
								"metric": {"foo":"bar"},
								"histogram": [1, {
									"count": "2", 
									"sum": "3", 
									"buckets": [
										[1,"0","2","2"], 
										[1,"2","4","2"]
									]
								}]
							}
						]`),
			err: errors.New(`histogram sample pair does not match for metric {foo="bar"}: expected buckets [[0,2):2] for timestamp 1 but got [[0,2):2 [2,4):2]`),
		},
		{
			name: "difference in histogram sample buckets boundaries",
			expected: json.RawMessage(`[
							{
								"metric": {"foo":"bar"},
								"histogram": [1, {
									"count": "2", 
									"sum": "3", 
									"buckets": [
										[1,"0","2","2"]
									]
								}]
							}
						]`),
			actual: json.RawMessage(`[
							{
								"metric": {"foo":"bar"},
								"histogram": [1, {
									"count": "2", 
									"sum": "3", 
									"buckets": [
										[2,"0","2","2"]
									]
								}]
							}
						]`),
			err: errors.New(`histogram sample pair does not match for metric {foo="bar"}: expected buckets [[0,2):2] for timestamp 1 but got [(0,2):2]`),
		},
		{
			name: "difference in histogram sample buckets lower boundary",
			expected: json.RawMessage(`[
							{
								"metric": {"foo":"bar"},
								"histogram": [1, {
									"count": "2", 
									"sum": "3", 
									"buckets": [
										[1,"0","2","2"]
									]
								}]
							}
						]`),
			actual: json.RawMessage(`[
							{
								"metric": {"foo":"bar"},
								"histogram": [1, {
									"count": "2", 
									"sum": "3", 
									"buckets": [
										[1,"1","2","2"]
									]
								}]
							}
						]`),
			err: errors.New(`histogram sample pair does not match for metric {foo="bar"}: expected buckets [[0,2):2] for timestamp 1 but got [[1,2):2]`),
		},
		{
			name: "difference in histogram sample buckets upper boundary",
			expected: json.RawMessage(`[
							{
								"metric": {"foo":"bar"},
								"histogram": [1, {
									"count": "2", 
									"sum": "3", 
									"buckets": [
										[1,"0","2","2"]
									]
								}]
							}
						]`),
			actual: json.RawMessage(`[
							{
								"metric": {"foo":"bar"},
								"histogram": [1, {
									"count": "2", 
									"sum": "3", 
									"buckets": [
										[1,"0","3","2"]
									]
								}]
							}
						]`),
			err: errors.New(`histogram sample pair does not match for metric {foo="bar"}: expected buckets [[0,2):2] for timestamp 1 but got [[0,3):2]`),
		},
		{
			name: "difference in histogram sample buckets count",
			expected: json.RawMessage(`[
							{
								"metric": {"foo":"bar"},
								"histogram": [1, {
									"count": "2", 
									"sum": "3", 
									"buckets": [
										[1,"0","2","2"]
									]
								}]
							}
						]`),
			actual: json.RawMessage(`[
							{
								"metric": {"foo":"bar"},
								"histogram": [1, {
									"count": "2", 
									"sum": "3", 
									"buckets": [
										[1,"0","2","3"]
									]
								}]
							}
						]`),
			err: errors.New(`histogram sample pair does not match for metric {foo="bar"}: expected buckets [[0,2):2] for timestamp 1 but got [[0,2):3]`),
		},
		{
			name: "actual histogram value matches expected",
			expected: json.RawMessage(`[
							{
								"metric": {"foo":"bar"},
								"histogram": [1, {
									"count": "2", 
									"sum": "3", 
									"buckets": [
										[1,"0","2","2"]
									]
								}]
							}
						]`),
			actual: json.RawMessage(`[
							{
								"metric": {"foo":"bar"},
								"histogram": [1, {
									"count": "2", 
									"sum": "3", 
									"buckets": [
										[1,"0","2","2"]
									]
								}]
							}
						]`),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := compareVector(tc.expected, tc.actual, time.Now(), SampleComparisonOptions{})
			if tc.err == nil {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			require.Equal(t, tc.err.Error(), err.Error())
		})
	}
}

func TestCompareScalar(t *testing.T) {
	for _, tc := range []struct {
		name     string
		expected json.RawMessage
		actual   json.RawMessage
		err      error
	}{
		{
			name:     "difference in timestamp",
			expected: json.RawMessage(`[1,"1"]`),
			actual:   json.RawMessage(`[2,"1"]`),
			err:      errors.New("expected timestamp 1 (1970-01-01T00:00:01Z) but got 2 (1970-01-01T00:00:02Z)"),
		},
		{
			name:     "difference in value",
			expected: json.RawMessage(`[1,"1"]`),
			actual:   json.RawMessage(`[1,"2"]`),
			err:      errors.New("expected value 1 for timestamp 1 (1970-01-01T00:00:01Z) but got 2"),
		},
		{
			name:     "correct values",
			expected: json.RawMessage(`[1,"1"]`),
			actual:   json.RawMessage(`[1,"1"]`),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := compareScalar(tc.expected, tc.actual, time.Now(), SampleComparisonOptions{})
			if tc.err == nil {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			require.Equal(t, tc.err.Error(), err.Error())
		})
	}
}

func TestCompareSamplesResponse(t *testing.T) {
	nowT := model.TimeFromUnixNano(time.Date(2099, 1, 2, 3, 4, 5, 6, time.UTC).UnixNano())
	now := nowT.String()
	overAnHourAgo := nowT.Add(-61 * time.Minute).String()

	for _, tc := range []struct {
		name              string
		tolerance         float64
		expected          json.RawMessage
		actual            json.RawMessage
		err               error
		useRelativeError  bool
		skipRecentSamples time.Duration
		skipSamplesBefore int64 // In unix milliseconds
	}{
		{
			name: "difference in response status",
			expected: json.RawMessage(`{
							"status": "success",
							"data": {"resultType":"scalar","result":[1,"1"]}
						}`),
			actual: json.RawMessage(`{
							"status": "fail"
						}`),
			err: errors.New("expected status success but got fail"),
		},
		{
			name: "same error and error type",
			expected: json.RawMessage(`{
							"status": "error",
							"errorType": "something",
							"error": "something went wrong"
						}`),
			actual: json.RawMessage(`{
							"status": "error",
							"errorType": "something",
							"error": "something went wrong"
						}`),
			err: nil,
		},
		{
			name: "difference in response error type",
			expected: json.RawMessage(`{
							"status": "error",
							"errorType": "something"
						}`),
			actual: json.RawMessage(`{
							"status": "error",
							"errorType": "something-else"
						}`),
			err: errors.New("expected error type 'something' but got 'something-else'"),
		},
		{
			name: "difference in response error",
			expected: json.RawMessage(`{
							"status": "error",
							"error": "something went wrong"
						}`),
			actual: json.RawMessage(`{
							"status": "error",
							"error": "something else went wrong"
						}`),
			err: errors.New("expected error 'something went wrong' but got 'something else went wrong'"),
		},
		{
			name: "response error is different but equivalent, and matches the same pattern in the equivalence class",
			expected: json.RawMessage(`{
							"status": "error",
							"error": "found duplicate series for the match group {foo=\"bar\"} on the right hand-side of the operation: [{foo=\"bar\", env=\"test\"}, {foo=\"bar\", env=\"prod\"}];many-to-many matching not allowed: matching labels must be unique on one side"
						}`),
			actual: json.RawMessage(`{
							"status": "error",
							"error": "found duplicate series for the match group {foo=\"blah\"} on the right hand-side of the operation: [{foo=\"blah\", env=\"test\"}, {foo=\"blah\", env=\"prod\"}];many-to-many matching not allowed: matching labels must be unique on one side"
						}`),
			err: nil,
		},
		{
			name: "response error is different but equivalent, and matches a different pattern in the equivalence class",
			expected: json.RawMessage(`{
							"status": "error",
							"error": "found duplicate series for the match group {foo=\"blah\"} on the right hand-side of the operation: [{foo=\"blah\", env=\"test\"}, {foo=\"blah\", env=\"prod\"}];many-to-many matching not allowed: matching labels must be unique on one side"
						}`),
			actual: json.RawMessage(`{
							"status": "error",
							"error": "found duplicate series for the match group {foo=\"bar\"} on the right side of the operation at timestamp 1970-01-01T00:00:00.123Z: {foo=\"bar\", env=\"test\"} and {foo=\"bar\", env=\"prod\"}"
						}`),
			err: nil,
		},
		{
			name: "response errors match equivalence classes, but errors are not from the same equivalence class",
			expected: json.RawMessage(`{
							"status": "error",
							"error": "found duplicate series for the match group {foo=\"bar\"} on the right hand-side of the operation: [{foo=\"bar\", env=\"test\"}, {foo=\"bar\", env=\"prod\"}];many-to-many matching not allowed: matching labels must be unique on one side"
						}`),
			actual: json.RawMessage(`{
							"status": "error",
							"error": "invalid parameter \"query\": invalid expression type \"range vector\" for range query, must be Scalar or instant Vector"
						}`),
			err: errors.New(`expected error 'found duplicate series for the match group {foo="bar"} on the right hand-side of the operation: [{foo="bar", env="test"}, {foo="bar", env="prod"}];many-to-many matching not allowed: matching labels must be unique on one side' but got 'invalid parameter "query": invalid expression type "range vector" for range query, must be Scalar or instant Vector'`),
		},
		{
			name: "expected response error matches an equivalence class, but actual response error does not",
			expected: json.RawMessage(`{
							"status": "error",
							"error": "invalid parameter \"query\": invalid expression type \"range vector\" for range query, must be Scalar or instant Vector"
						}`),
			actual: json.RawMessage(`{
							"status": "error",
							"error": "something went wrong"
						}`),
			err: errors.New(`expected error 'invalid parameter "query": invalid expression type "range vector" for range query, must be Scalar or instant Vector' but got 'something went wrong'`),
		},
		{
			name: "actual response error matches an equivalence class, but expected response error does not",
			expected: json.RawMessage(`{
							"status": "error",
							"error": "something went wrong"
						}`),
			actual: json.RawMessage(`{
							"status": "error",
							"error": "invalid parameter \"query\": invalid expression type \"range vector\" for range query, must be Scalar or instant Vector"
						}`),
			err: errors.New(`expected error 'something went wrong' but got 'invalid parameter "query": invalid expression type "range vector" for range query, must be Scalar or instant Vector'`),
		},
		{
			name: "difference in resultType",
			expected: json.RawMessage(`{
							"status": "success",
							"data": {"resultType":"scalar","result":[1,"1"]}
						}`),
			actual: json.RawMessage(`{
							"status": "success",
							"data": {"resultType":"vector","result":[{"metric":{"foo":"bar"},"value":[1,"1"]}]}
						}`),
			err: errors.New("expected resultType scalar but got vector"),
		},
		{
			name: "unregistered resultType",
			expected: json.RawMessage(`{
							"status": "success",
							"data": {"resultType":"new-scalar","result":[1,"1"]}
						}`),
			actual: json.RawMessage(`{
							"status": "success",
							"data": {"resultType":"new-scalar","result":[1,"1"]}
						}`),
			err: errors.New("resultType new-scalar not registered for comparison"),
		},
		{
			name: "valid scalar response",
			expected: json.RawMessage(`{
							"status": "success",
							"data": {"resultType":"scalar","result":[1,"1"]}
						}`),
			actual: json.RawMessage(`{
							"status": "success",
							"data": {"resultType":"scalar","result":[1,"1"]}
						}`),
		},
		{
			name:      "should pass if values are slightly different but within the tolerance",
			tolerance: 0.000001,
			expected: json.RawMessage(`{
							"status": "success",
							"data": {"resultType":"vector","result":[{"metric":{"foo":"bar"},"value":[1,"773054.5916666666"]}]}
						}`),
			actual: json.RawMessage(`{
							"status": "success",
							"data": {"resultType":"vector","result":[{"metric":{"foo":"bar"},"value":[1,"773054.59166667"]}]}
						}`),
		},
		{
			name:      "should correctly compare NaN values with tolerance is disabled",
			tolerance: 0,
			expected: json.RawMessage(`{
							"status": "success",
							"data": {"resultType":"vector","result":[{"metric":{"foo":"bar"},"value":[1,"NaN"]}]}
						}`),
			actual: json.RawMessage(`{
							"status": "success",
							"data": {"resultType":"vector","result":[{"metric":{"foo":"bar"},"value":[1,"NaN"]}]}
						}`),
		},
		{
			name:      "should correctly compare NaN values with tolerance is enabled",
			tolerance: 0.000001,
			expected: json.RawMessage(`{
							"status": "success",
							"data": {"resultType":"vector","result":[{"metric":{"foo":"bar"},"value":[1,"NaN"]}]}
						}`),
			actual: json.RawMessage(`{
							"status": "success",
							"data": {"resultType":"vector","result":[{"metric":{"foo":"bar"},"value":[1,"NaN"]}]}
						}`),
		},
		{
			name: "should correctly compare Inf values",
			expected: json.RawMessage(`{
							"status": "success",
							"data": {"resultType":"vector","result":[{"metric":{"foo":"bar"},"value":[1,"Inf"]}]}
						}`),
			actual: json.RawMessage(`{
							"status": "success",
							"data": {"resultType":"vector","result":[{"metric":{"foo":"bar"},"value":[1,"Inf"]}]}
						}`),
		},
		{
			name: "should correctly compare -Inf values",
			expected: json.RawMessage(`{
							"status": "success",
							"data": {"resultType":"vector","result":[{"metric":{"foo":"bar"},"value":[1,"-Inf"]}]}
						}`),
			actual: json.RawMessage(`{
							"status": "success",
							"data": {"resultType":"vector","result":[{"metric":{"foo":"bar"},"value":[1,"-Inf"]}]}
						}`),
		},
		{
			name: "should correctly compare +Inf values",
			expected: json.RawMessage(`{
							"status": "success",
							"data": {"resultType":"vector","result":[{"metric":{"foo":"bar"},"value":[1,"+Inf"]}]}
						}`),
			actual: json.RawMessage(`{
							"status": "success",
							"data": {"resultType":"vector","result":[{"metric":{"foo":"bar"},"value":[1,"+Inf"]}]}
						}`),
		},
		{
			name:      "should fail if values are significantly different, over the tolerance",
			tolerance: 0.000001,
			expected: json.RawMessage(`{
							"status": "success",
							"data": {"resultType":"vector","result":[{"metric":{"foo":"bar"},"value":[1,"773054.5916666666"]}]}
						}`),
			actual: json.RawMessage(`{
							"status": "success",
							"data": {"resultType":"vector","result":[{"metric":{"foo":"bar"},"value":[1,"773054.789"]}]}
						}`),
			err: errors.New(`float sample pair does not match for metric {foo="bar"}: expected value 773054.5916666666 for timestamp 1 (1970-01-01T00:00:01Z) but got 773054.789`),
		},
		{
			name:      "should fail if large values are significantly different, over the tolerance without using relative error",
			tolerance: 1e-14,
			expected: json.RawMessage(`{
							"status": "success",
							"data": {"resultType":"vector","result":[{"metric":{"foo":"bar"},"value":[1,"4.923488536785282e+41"]}]}
						}`),
			actual: json.RawMessage(`{
							"status": "success",
							"data": {"resultType":"vector","result":[{"metric":{"foo":"bar"},"value":[1,"4.923488536785281e+41"]}]}
						}`),
			err: errors.New(`float sample pair does not match for metric {foo="bar"}: expected value 492348853678528200000000000000000000000000 for timestamp 1 (1970-01-01T00:00:01Z) but got 492348853678528100000000000000000000000000`),
		},
		{
			name:      "should not fail if large values are significantly different, over the tolerance using relative error",
			tolerance: 1e-14,
			expected: json.RawMessage(`{
							"status": "success",
							"data": {"resultType":"vector","result":[{"metric":{"foo":"bar"},"value":[1,"4.923488536785282e+41"]}]}
						}`),
			actual: json.RawMessage(`{
							"status": "success",
							"data": {"resultType":"vector","result":[{"metric":{"foo":"bar"},"value":[1,"4.923488536785281e+41"]}]}
						}`),
			useRelativeError: true,
		},
		{
			name: "should not fail when the float sample in a vector is recent and configured to skip recent samples",
			expected: json.RawMessage(`{
							"status": "success",
							"data": {"resultType":"vector","result":[{"metric":{"foo":"bar"},"value":[` + now + `,"10"]}]}
						}`),
			actual: json.RawMessage(`{
							"status": "success",
							"data": {"resultType":"vector","result":[{"metric":{"foo":"bar"},"value":[` + now + `,"5"]}]}
						}`),
			skipRecentSamples: time.Hour,
		},
		{
			name: "should not fail when the histogram sample in a vector is recent and configured to skip recent samples",
			expected: json.RawMessage(`{
							"status": "success",
							"data": {
								"resultType": "vector",
								"result": [
									{
										"metric": {"foo":"bar"},
										"histogram": [
											` + now + `, 
											{
												"count": "2", 
												"sum": "3", 
												"buckets": [
													[1,"0","2","2"]
												]
											}
										]
									}
								]
							}
						}`),
			actual: json.RawMessage(`{
							"status": "success",
							"data": {
								"resultType": "vector",
								"result": [
									{
										"metric": {"foo":"bar"},
										"histogram": [
											` + now + `, 
											{
												"count": "5", 
												"sum": "3", 
												"buckets": [
													[1,"0","2","5"]
												]
											}
										]
									}
								]
							}
						}`),
			skipRecentSamples: time.Hour,
		},
		{
			name: "should not fail when there are a different number of series in a vector, and all float samples are within the configured skip recent samples interval",
			expected: json.RawMessage(`{
							"status": "success",
							"data": {"resultType":"vector","result":[{"metric":{"foo":"bar"},"value":[` + now + `,"10"]}]}
						}`),
			actual: json.RawMessage(`{
							"status": "success",
							"data": {"resultType":"vector","result":[{"metric":{"foo":"bar"},"value":[` + now + `,"10"]}, {"metric":{"foo":"baz"},"value":[` + now + `,"10"]}]}
						}`),
			skipRecentSamples: time.Hour,
		},
		{
			name: "should not fail when there are a different number of series in a vector, and all histogram samples are within the configured skip recent samples interval",
			expected: json.RawMessage(`{
							"status": "success",
							"data": {
								"resultType": "vector",
								"result": [
									{
										"metric": {"foo":"bar"},
										"histogram": [
											` + now + `, 
											{
												"count": "2", 
												"sum": "3", 
												"buckets": [
													[1,"0","2","2"]
												]
											}
										]
									}
								]
							}
						}`),
			actual: json.RawMessage(`{
							"status": "success",
							"data": {
								"resultType": "vector",
								"result": [
									{
										"metric": {"foo":"bar"},
										"histogram": [
											` + now + `, 
											{
												"count": "5", 
												"sum": "3", 
												"buckets": [
													[1,"0","2","5"]
												]
											}
										]
									},
									{
										"metric": {"foo":"baz"},
										"histogram": [
											` + now + `, 
											{
												"count": "5", 
												"sum": "20", 
												"buckets": [
													[1,"0","2","5"]
												]
											}
										]
									}
								]
							}
						}`),
			skipRecentSamples: time.Hour,
		},
		{
			name: "should not fail when there are different series in a vector, and all float samples are within the configured skip recent samples interval",
			expected: json.RawMessage(`{
							"status": "success",
							"data": {"resultType":"vector","result":[{"metric":{"foo":"bar"},"value":[` + now + `,"10"]}]}
						}`),
			actual: json.RawMessage(`{
							"status": "success",
							"data": {"resultType":"vector","result":[{"metric":{"foo":"other-bar"},"value":[` + now + `,"11"]}]}
						}`),
			skipRecentSamples: time.Hour,
		},
		{
			name: "should not fail when there are different series in a vector, and all histogram samples are within the configured skip recent samples interval",
			expected: json.RawMessage(`{
							"status": "success",
							"data": {
								"resultType": "vector",
								"result": [
									{
										"metric": {"foo":"bar"},
										"histogram": [
											` + now + `, 
											{
												"count": "2", 
												"sum": "3", 
												"buckets": [
													[1,"0","2","2"]
												]
											}
										]
									}
								]
							}
						}`),
			actual: json.RawMessage(`{
							"status": "success",
							"data": {
								"resultType": "vector",
								"result": [
									{
										"metric": {"foo":"other-bar"},
										"histogram": [
											` + now + `, 
											{
												"count": "5", 
												"sum": "3", 
												"buckets": [
													[1,"0","2","5"]
												]
											}
										]
									}
								]
							}
						}`),
			skipRecentSamples: time.Hour,
		},
		{
			name: "should fail when there are a different number of series in a vector, and some float samples are outside the configured skip recent samples interval",
			expected: json.RawMessage(`{
							"status": "success",
							"data": {"resultType":"vector","result":[{"metric":{"foo":"bar"},"value":[` + now + `,"10"]}]}
						}`),

			// It's not expected that a vector will have samples with different timestamps, but if it happens, we should still behave in a predictable way.
			actual: json.RawMessage(`{
							"status": "success",
							"data": {"resultType":"vector","result":[{"metric":{"foo":"bar"},"value":[` + now + `,"10"]}, {"metric":{"foo":"baz"},"value":[` + overAnHourAgo + `,"10"]}]}
						}`),
			skipRecentSamples: time.Hour,
			err:               errors.New("expected 1 metrics but got 2"),
		},
		{
			name: "should fail when there are a different number of series in a vector, and some histogram samples are outside the configured skip recent samples interval",
			expected: json.RawMessage(`{
							"status": "success",
							"data": {
								"resultType": "vector",
								"result": [
									{
										"metric": {"foo":"bar"},
										"histogram": [
											` + now + `, 
											{
												"count": "2", 
												"sum": "3", 
												"buckets": [
													[1,"0","2","2"]
												]
											}
										]
									}
								]
							}
						}`),

			// It's not expected that a vector will have samples with different timestamps, but if it happens, we should still behave in a predictable way.
			actual: json.RawMessage(`{
							"status": "success",
							"data": {
								"resultType": "vector",
								"result": [
									{
										"metric": {"foo":"bar"},
										"histogram": [
											` + now + `, 
											{
												"count": "5", 
												"sum": "3", 
												"buckets": [
													[1,"0","2","5"]
												]
											}
										]
									},
									{
										"metric": {"foo":"baz"},
										"histogram": [
											` + overAnHourAgo + `, 
											{
												"count": "5", 
												"sum": "20", 
												"buckets": [
													[1,"0","2","5"]
												]
											}
										]
									}
								]
							}
						}`),
			skipRecentSamples: time.Hour,
			err:               errors.New("expected 1 metrics but got 2"),
		},
		{
			name: "should not fail when the float sample in a matrix is recent and configured to skip recent samples",
			expected: json.RawMessage(`{
							"status": "success",
							"data": {"resultType":"matrix","result":[{"metric":{"foo":"bar"},"values":[[` + now + `,"10"]]}]}
						}`),
			actual: json.RawMessage(`{
							"status": "success",
							"data": {"resultType":"matrix","result":[{"metric":{"foo":"bar"},"values":[[` + now + `,"5"]]}]}
						}`),
			skipRecentSamples: time.Hour,
		},
		{
			name: "should not fail when the histogram sample in a matrix is recent and configured to skip recent samples",
			expected: json.RawMessage(`{
							"status": "success",
							"data": {
								"resultType": "matrix",
								"result": [
									{
										"metric": {"foo":"bar"},
										"histograms": [
											[
												` + now + `, 
												{
													"count": "2", 
													"sum": "3", 
													"buckets": [
														[1,"0","2","2"]
													]
												}
											]
										]
									}
								]
							}
						}`),
			actual: json.RawMessage(`{
							"status": "success",
							"data": {
								"resultType": "matrix",
								"result": [
									{
										"metric": {"foo":"bar"},
										"histograms": [
											[
												` + now + `, 
												{
													"count": "5", 
													"sum": "3", 
													"buckets": [
														[1,"0","2","5"]
													]
												}
											]
										]
									}
								]
							}
						}`),
			skipRecentSamples: time.Hour,
		},
		{
			name: "should not fail when there are a different number of series in a matrix, and all float samples are within the configured skip recent samples interval",
			expected: json.RawMessage(`{
							"status": "success",
							"data": {"resultType":"matrix","result":[{"metric":{"foo":"bar"},"values":[[` + now + `,"10"]]}]}
						}`),
			actual: json.RawMessage(`{
							"status": "success",
							"data": {"resultType":"matrix","result":[{"metric":{"foo":"bar"},"values":[[` + now + `,"10"]]}, {"metric":{"foo":"baz"},"values":[[` + now + `,"10"]]}]}
						}`),
			skipRecentSamples: time.Hour,
		},
		{
			name: "should not fail when there are a different number of series in a matrix, and all histogram samples are within the configured skip recent samples interval",
			expected: json.RawMessage(`{
							"status": "success",
							"data": {
								"resultType": "matrix",
								"result": [
									{
										"metric": {"foo":"bar"},
										"histograms": [[
											` + now + `, 
											{
												"count": "2", 
												"sum": "3", 
												"buckets": [
													[1,"0","2","2"]
												]
											}
										]]
									}
								]
							}
						}`),
			actual: json.RawMessage(`{
							"status": "success",
							"data": {
								"resultType": "matrix",
								"result": [
									{
										"metric": {"foo":"bar"},
										"histogram": [
											` + now + `, 
											{
												"count": "5", 
												"sum": "3", 
												"buckets": [
													[1,"0","2","5"]
												]
											}
										]
									},
									{
										"metric": {"foo":"baz"},
										"histogram": [
											` + now + `, 
											{
												"count": "5", 
												"sum": "20", 
												"buckets": [
													[1,"0","2","5"]
												]
											}
										]
									}
								]
							}
						}`),
			skipRecentSamples: time.Hour,
		},
		{
			name: "should not fail when there are different series in a matrix, and all float samples are within the configured skip recent samples interval",
			expected: json.RawMessage(`{
							"status": "success",
							"data": {"resultType":"matrix","result":[{"metric":{"foo":"bar"},"values":[[` + now + `,"10"]]}]}
						}`),
			actual: json.RawMessage(`{
							"status": "success",
							"data": {"resultType":"matrix","result":[{"metric":{"foo":"other-bar"},"values":[[` + now + `,"11"]]}]}
						}`),
			skipRecentSamples: time.Hour,
		},
		{
			name: "should not fail when there are different number series in a matrix, and all histogram samples are within the configured skip recent samples interval",
			expected: json.RawMessage(`{
							"status": "success",
							"data": {
								"resultType": "matrix",
								"result": [
									{
										"metric": {"foo":"bar"},
										"histograms": [[
											` + now + `, 
											{
												"count": "2", 
												"sum": "3", 
												"buckets": [
													[1,"0","2","2"]
												]
											}
										]]
									}
								]
							}
						}`),
			actual: json.RawMessage(`{
							"status": "success",
							"data": {
								"resultType": "matrix",
								"result": [
									{
										"metric": {"foo":"other-bar"},
										"histogram": [
											` + now + `, 
											{
												"count": "5", 
												"sum": "3", 
												"buckets": [
													[1,"0","2","5"]
												]
											}
										]
									}
								]
							}
						}`),
			skipRecentSamples: time.Hour,
		},
		{
			name: "should fail when there are a different number of series in a matrix, and some float samples are outside the configured skip recent samples interval",
			expected: json.RawMessage(`{
							"status": "success",
							"data": {"resultType":"matrix","result":[{"metric":{"foo":"bar"},"values":[[` + now + `,"10"]]}]}
						}`),

			// It's not expected that a matrix will have samples with different timestamps, but if it happens, we should still behave in a predictable way.
			actual: json.RawMessage(`{
							"status": "success",
							"data": {"resultType":"matrix","result":[{"metric":{"foo":"bar"},"values":[[` + now + `,"10"]]}, {"metric":{"foo":"baz"},"values":[[` + overAnHourAgo + `,"10"]]}]}
						}`),
			skipRecentSamples: time.Hour,
			err:               errors.New("expected 1 metrics but got 2"),
		},
		{
			name: "should fail when there are a different number of series in a matrix, and some histogram samples are outside the configured skip recent samples interval",
			expected: json.RawMessage(`{
							"status": "success",
							"data": {
								"resultType": "matrix",
								"result": [
									{
										"metric": {"foo":"bar"},
										"histograms": [
											[
												` + now + `, 
												{
													"count": "2", 
													"sum": "3", 
													"buckets": [
														[1,"0","2","2"]
													]
												}
											]
										]
									}
								]
							}
						}`),

			// It's not expected that a matrix will have samples with different timestamps, but if it happens, we should still behave in a predictable way.
			actual: json.RawMessage(`{
							"status": "success",
							"data": {
								"resultType": "matrix",
								"result": [
									{
										"metric": {"foo":"bar"},
										"histograms": [
											[
												` + now + `, 
												{
													"count": "5", 
													"sum": "3", 
													"buckets": [
														[1,"0","2","5"]
													]
												}
											]
										]
									},
									{
										"metric": {"foo":"baz"},
										"histograms": [
											[
												` + overAnHourAgo + `, 
												{
													"count": "5", 
													"sum": "20", 
													"buckets": [
														[1,"0","2","5"]
													]
												}
											]
										]
									}
								]
							}
						}`),
			skipRecentSamples: time.Hour,
			err:               errors.New("expected 1 metrics but got 2"),
		},
		{
			name: "should not fail when there is different number of samples in a series, but non-matching float samples are more recent than configured skip recent samples interval",
			expected: json.RawMessage(`{
							"status": "success",
							"data": {"resultType":"matrix","result":[{"metric":{"foo":"bar"},"values":[[` + overAnHourAgo + `,"10"],[` + now + `,"20"]]}]}
						}`),

			actual: json.RawMessage(`{
							"status": "success",
							"data": {"resultType":"matrix","result":[{"metric":{"foo":"bar"},"values":[[` + overAnHourAgo + `,"10"]]}]}
						}`),
			skipRecentSamples: time.Hour,
		},
		{
			name: "should not fail when there is different number of samples in a series, but non-matching histogram samples are more recent than configured skip recent samples interval",
			expected: json.RawMessage(`{
							"status": "success",
							"data": {
								"resultType": "matrix",
								"result": [
									{
										"metric": {"foo":"bar"},
										"histograms": [
											[
												` + overAnHourAgo + `,
												{
													"count": "2",
													"sum": "3",
													"buckets": [
														[1,"0","2","2"]
													]
												}
											],
											[
												` + now + `,
												{
													"count": "2",
													"sum": "3",
													"buckets": [
														[1,"0","2","2"]
													]
												}
											]
										]
									}
								]
							}
						}`),

			actual: json.RawMessage(`{
							"status": "success",
							"data": {
								"resultType": "matrix",
								"result": [
									{
										"metric": {"foo":"bar"},
										"histograms": [
											[
												` + overAnHourAgo + `,
												{
													"count": "2",
													"sum": "3",
													"buckets": [
														[1,"0","2","2"]
													]
												}
											]
										]
									}
								]
							}
						}`),
			skipRecentSamples: time.Hour,
		},
		{
			name: "should not fail if both results have an empty set of warnings",
			expected: json.RawMessage(`{
							"status": "success",
							"data": {"resultType":"scalar","result":[1,"1"]},
							"warnings": []
						}`),
			actual: json.RawMessage(`{
							"status": "success",
							"data": {"resultType":"scalar","result":[1,"1"]},
							"warnings": []
						}`),
		},
		{
			name: "should not fail if both results have the same warnings in the same order",
			expected: json.RawMessage(`{
							"status": "success",
							"data": {"resultType":"scalar","result":[1,"1"]},
							"warnings": ["warning #1", "warning #2"]
						}`),
			actual: json.RawMessage(`{
							"status": "success",
							"data": {"resultType":"scalar","result":[1,"1"]},
							"warnings": ["warning #1", "warning #2"]
						}`),
		},
		{
			name: "should not fail if both results have the same warnings in a different order",
			expected: json.RawMessage(`{
							"status": "success",
							"data": {"resultType":"scalar","result":[1,"1"]},
							"warnings": ["warning #1", "warning #2"]
						}`),
			actual: json.RawMessage(`{
							"status": "success",
							"data": {"resultType":"scalar","result":[1,"1"]},
							"warnings": ["warning #2", "warning #1"]
						}`),
		},
		{
			name: "should fail if both results do not have the same warnings",
			expected: json.RawMessage(`{
							"status": "success",
							"data": {"resultType":"scalar","result":[1,"1"]},
							"warnings": ["warning #1"]
						}`),
			actual: json.RawMessage(`{
							"status": "success",
							"data": {"resultType":"scalar","result":[1,"1"]},
							"warnings": ["warning #1", "warning #2"]
						}`),
			err: errors.New(`expected warning annotations ["warning #1"] but got ["warning #1", "warning #2"]`),
		},
		{
			name: "should fail if both results have the same warnings but one is duplicated",
			expected: json.RawMessage(`{
							"status": "success",
							"data": {"resultType":"scalar","result":[1,"1"]},
							"warnings": ["warning #1"]
						}`),
			actual: json.RawMessage(`{
							"status": "success",
							"data": {"resultType":"scalar","result":[1,"1"]},
							"warnings": ["warning #1", "warning #1"]
						}`),
			err: errors.New(`expected warning annotations ["warning #1"] but got ["warning #1", "warning #1"]`),
		},
		{
			name: "should fail with a correctly escaped message if warnings differ and contain double quotes",
			expected: json.RawMessage(`{
							"status": "success",
							"data": {"resultType":"scalar","result":[1,"1"]},
							"warnings": ["\"warning\" #1"]
						}`),
			actual: json.RawMessage(`{
							"status": "success",
							"data": {"resultType":"scalar","result":[1,"1"]},
							"warnings": ["\"warning\" #2"]
						}`),
			err: errors.New(`expected warning annotations ["\"warning\" #1"] but got ["\"warning\" #2"]`),
		},
		{
			name: "should not fail if both results have an empty set of info annotations",
			expected: json.RawMessage(`{
							"status": "success",
							"data": {"resultType":"scalar","result":[1,"1"]},
							"infos": []
						}`),
			actual: json.RawMessage(`{
							"status": "success",
							"data": {"resultType":"scalar","result":[1,"1"]},
							"infos": []
						}`),
		},
		{
			name: "should not fail if both results have the same info annotations in the same order",
			expected: json.RawMessage(`{
							"status": "success",
							"data": {"resultType":"scalar","result":[1,"1"]},
							"infos": ["info #1", "info #2"]
						}`),
			actual: json.RawMessage(`{
							"status": "success",
							"data": {"resultType":"scalar","result":[1,"1"]},
							"infos": ["info #1", "info #2"]
						}`),
		},
		{
			name: "should not fail if both results have the same info annotations in a different order",
			expected: json.RawMessage(`{
							"status": "success",
							"data": {"resultType":"scalar","result":[1,"1"]},
							"infos": ["info #1", "info #2"]
						}`),
			actual: json.RawMessage(`{
							"status": "success",
							"data": {"resultType":"scalar","result":[1,"1"]},
							"infos": ["info #2", "info #1"]
						}`),
		},
		{
			name: "should fail if both results do not have the same info annotations",
			expected: json.RawMessage(`{
							"status": "success",
							"data": {"resultType":"scalar","result":[1,"1"]},
							"infos": ["info #1"]
						}`),
			actual: json.RawMessage(`{
							"status": "success",
							"data": {"resultType":"scalar","result":[1,"1"]},
							"infos": ["info #1", "info #2"]
						}`),
			err: errors.New(`expected info annotations ["info #1"] but got ["info #1", "info #2"]`),
		},
		{
			name: "should fail if both results have the same info annotations but one is duplicated",
			expected: json.RawMessage(`{
							"status": "success",
							"data": {"resultType":"scalar","result":[1,"1"]},
							"infos": ["info #1"]
						}`),
			actual: json.RawMessage(`{
							"status": "success",
							"data": {"resultType":"scalar","result":[1,"1"]},
							"infos": ["info #1", "info #1"]
						}`),
			err: errors.New(`expected info annotations ["info #1"] but got ["info #1", "info #1"]`),
		},
		{
			name: "should fail with a correctly escaped message if info annotations differ and contain double quotes",
			expected: json.RawMessage(`{
							"status": "success",
							"data": {"resultType":"scalar","result":[1,"1"]},
							"infos": ["\"info\" #1"]
						}`),
			actual: json.RawMessage(`{
							"status": "success",
							"data": {"resultType":"scalar","result":[1,"1"]},
							"infos": ["\"info\" #2"]
						}`),
			err: errors.New(`expected info annotations ["\"info\" #1"] but got ["\"info\" #2"]`),
		},
		{
			name: "should not fail when we skip samples from the beginning of a matrix for expected and actual - float",
			expected: json.RawMessage(`{
							"status": "success",
							"data": {"resultType":"matrix","result":[{"metric":{"foo":"bar"},"values":[[90,"9"], [100,"10"]]}, {"metric":{"foo":"bar2"},"values":[[100,"10"]]}]}
						}`),
			actual: json.RawMessage(`{
							"status": "success",
							"data": {"resultType":"matrix","result":[{"metric":{"foo":"bar"},"values":[[100,"10"]]}, {"metric":{"foo":"bar2"},"values":[[80,"9"], [100,"10"]]}]}
						}`),
			skipSamplesBefore: 95 * 1000,
		},
		{
			name: "should not fail when some series is entirely skipped and rest matches - float",
			expected: json.RawMessage(`{
							"status": "success",
							"data": {"resultType":"matrix","result":[{"metric":{"foo":"bar"},"values":[[90,"9"], [100,"10"]]}, {"metric":{"foo":"bar2"},"values":[[90,"10"]]}]}
						}`),
			actual: json.RawMessage(`{
							"status": "success",
							"data": {"resultType":"matrix","result":[{"metric":{"foo":"bar"},"values":[[100,"10"]]}]}
						}`),
			skipSamplesBefore: 95 * 1000,
		},
		{
			name: "fail when all series from expected are filtered out - float",
			expected: json.RawMessage(`{
							"status": "success",
							"data": {"resultType":"matrix","result":[{"metric":{"foo":"bar"},"values":[[90,"9"], [93,"10"]]}, {"metric":{"foo":"bar2"},"values":[[90,"10"]]}]}
						}`),
			actual: json.RawMessage(`{
							"status": "success",
							"data": {"resultType":"matrix","result":[{"metric":{"foo":"bar"},"values":[[90,"10"], [93,"10"], [100,"10"]]}]}
						}`),
			skipSamplesBefore: 95 * 1000,
			err:               errors.New(`expected 0 metrics but got 1 (also, some series were completely filtered out from the expected response due to the 'skip samples before')`),
		},
		{
			name: "should not fail when we skip all samples starting from the beginning - float",
			expected: json.RawMessage(`{
							"status": "success",
							"data": {"resultType":"matrix","result":[{"metric":{"foo":"bar"},"values":[[90,"9"], [100,"10"]]}]}
						}`),
			actual: json.RawMessage(`{
							"status": "success",
							"data": {"resultType":"matrix","result":[{"metric":{"foo":"bar"},"values":[[100,"10"]]}]}
						}`),
			skipSamplesBefore: 105 * 1000,
		},
		{
			name: "should fail when we skip partial samples in the beginning but compare some other - float",
			expected: json.RawMessage(`{
							"status": "success",
							"data": {"resultType":"matrix","result":[{"metric":{"foo":"bar"},"values":[[90,"9"], [97,"7"], [100,"10"]]}, {"metric":{"foo":"bar2"},"values":[[100,"10"]]}]}
						}`),
			actual: json.RawMessage(`{
							"status": "success",
							"data": {"resultType":"matrix","result":[{"metric":{"foo":"bar"},"values":[[100,"10"]]}, {"metric":{"foo":"bar2"},"values":[[90,"10"], [100,"10"]]}]}
						}`),
			skipSamplesBefore: 95 * 1000,
			// 9 @[90] is not compared. foo=bar2 does not fail.
			err: errors.New(`float sample pair does not match for metric {foo="bar"}: expected timestamp 97 (1970-01-01T00:01:37Z) but got 100 (1970-01-01T00:01:40Z)
Expected result for series:
{foo="bar"} =>
7 @[97]
10 @[100]

Actual result for series:
{foo="bar"} =>
10 @[100]`),
		},
		{
			name: "should not fail when we skip samples from the beginning of a matrix for expected and actual - histogram",
			expected: json.RawMessage(`{
							"status": "success",
							"data": {"resultType":"matrix","result":[
								{"metric":{"foo":"bar"},"histograms":[[90,{"count": "2","sum": "4","buckets": [[1,"0","2","2"]]}], [100,{"count":"2","sum":"3","buckets":[[1,"0","2","2"]]}]]},
								{"metric":{"foo":"bar2"},"histograms":[[100,{"count":"2","sum":"3","buckets":[[1,"0","2","2"]]}]]}]}
						}`),
			actual: json.RawMessage(`{
							"status": "success",
							"data": {"resultType":"matrix","result":[
								{"metric":{"foo":"bar"},"histograms":[[100,{"count":"2","sum":"3","buckets":[[1,"0","2","2"]]}]]}, 
								{"metric":{"foo":"bar2"},"histograms":[[80,{"count": "2","sum": "4","buckets": [[1,"0","2","2"]]}], [100,{"count":"2","sum":"3","buckets":[[1,"0","2","2"]]}]]}]}
						}`),
			skipSamplesBefore: 95 * 1000,
		},
		{
			name: "should not fail when we skip all samples starting from the beginning - histogram",
			expected: json.RawMessage(`{
							"status": "success",
							"data": {"resultType":"matrix","result":[{"metric":{"foo":"bar"},"histograms":[[90,{"count":"2","sum":"3","buckets":[[1,"0","2","2"]]}], [100,{"count":"2","sum":"3","buckets":[[1,"0","2","2"]]}]]}]}
						}`),
			actual: json.RawMessage(`{
							"status": "success",
							"data": {"resultType":"matrix","result":[{"metric":{"foo":"bar"},"histograms":[[100,{"count":"2","sum":"3","buckets":[[1,"0","2","2"]]}]]}]}
						}`),
			skipSamplesBefore: 105 * 1000,
		},
		{
			name: "should fail when we skip partial samples in the beginning but compare some other - histogram",
			expected: json.RawMessage(`{
							"status": "success",
							"data": {"resultType":"matrix","result":[
								{"metric":{"foo":"bar"},"histograms":[[90,{"count":"2","sum":"3","buckets":[[1,"0","2","2"]]}], [97,{"count": "2","sum": "33","buckets": [[1,"0","2","2"]]}], [100,{"count":"2","sum":"3","buckets":[[1,"0","2","2"]]}]]},
								{"metric":{"foo":"bar2"},"histograms":[[100,{"count":"2","sum":"3","buckets":[[1,"0","2","2"]]}]]}]}
						}`),
			actual: json.RawMessage(`{
							"status": "success",
							"data": {"resultType":"matrix","result":[
								{"metric":{"foo":"bar"},"histograms":[[100,{"count":"2","sum":"3","buckets":[[1,"0","2","2"]]}]]},
								{"metric":{"foo":"bar2"},"histograms":[[90,{"count": "2","sum": "44","buckets": [[1,"0","2","2"]]}], [100,{"count":"2","sum":"3","buckets":[[1,"0","2","2"]]}]]}]}
						}`),
			skipSamplesBefore: 95 * 1000,
			// @[90] is not compared. foo=bar2 does not fail.
			err: errors.New(`histogram sample pair does not match for metric {foo="bar"}: expected timestamp 97 but got 100
Expected result for series:
{foo="bar"} =>
Count: 2.000000, Sum: 33.000000, Buckets: [[0,2):2] @[97]
Count: 2.000000, Sum: 3.000000, Buckets: [[0,2):2] @[100]

Actual result for series:
{foo="bar"} =>
Count: 2.000000, Sum: 3.000000, Buckets: [[0,2):2] @[100]`),
		},
		{
			name: "should not fail when skipped samples properly for vectors",
			expected: json.RawMessage(`{
							"status": "success",
							"data": {"resultType":"vector","result":[{"metric":{"foo":"bar"},"value":[90,"1"]}]}
						}`),
			actual: json.RawMessage(`{
							"status": "success",
							"data": {"resultType":"vector","result":[{"metric":{"foo":"bar"},"value":[95,"1"]}]}
						}`),
			skipSamplesBefore: 100 * 1000,
		},
		{
			name: "should not fail when skipped samples properly for vectors - histogram",
			expected: json.RawMessage(`{
							"status": "success",
							"data": {"resultType":"vector","result":[{"metric":{"foo":"bar"},"histogram":[90,{"count":"2","sum":"3","buckets":[[1,"0","2","2"]]}]}]}
						}`),
			actual: json.RawMessage(`{
							"status": "success",
							"data": {"resultType":"vector","result":[{"metric":{"foo":"bar"},"histogram":[95,{"count":"2","sum":"3","buckets":[[1,"0","2","2"]]}]}]}
						}`),
			skipSamplesBefore: 100 * 1000,
		},
		{
			name: "should fail when skipped samples only for expected vector",
			expected: json.RawMessage(`{
							"status": "success",
							"data": {"resultType":"vector","result":[{"metric":{"foo":"bar"},"value":[90,"1"]}]}
						}`),
			actual: json.RawMessage(`{
							"status": "success",
							"data": {"resultType":"vector","result":[{"metric":{"foo":"bar"},"value":[105,"1"]}]}
						}`),
			skipSamplesBefore: 100 * 1000,
			err:               errors.New(`expected 0 metrics but got 1 (also, some samples were filtered out from the expected response due to the 'skip samples before'; if all samples have been filtered out, this could cause the check on the expected number of metrics to fail)`),
		},
		{
			name: "should fail when skipped samples only for actual vector",
			expected: json.RawMessage(`{
							"status": "success",
							"data": {"resultType":"vector","result":[{"metric":{"foo":"bar"},"value":[105,"1"]}]}
						}`),
			actual: json.RawMessage(`{
							"status": "success",
							"data": {"resultType":"vector","result":[{"metric":{"foo":"bar"},"value":[95,"1"]}]}
						}`),
			skipSamplesBefore: 100 * 1000,
			err:               errors.New(`expected 1 metrics but got 0 (also, some samples were filtered out from the actual response due to the 'skip samples before'; if all samples have been filtered out, this could cause the check on the expected number of metrics to fail)`),
		},
		{
			name: "should skip properly when there are multiple series in a vector",
			expected: json.RawMessage(`{
							"status": "success",
							"data": {"resultType":"vector","result":[{"metric":{"foo":"bar"},"value":[90,"1"]}, {"metric":{"foo":"bar2"},"value":[105,"1"]}]}
						}`),
			actual: json.RawMessage(`{
							"status": "success",
							"data": {"resultType":"vector","result":[{"metric":{"foo":"bar"},"value":[95,"1"]}, {"metric":{"foo":"bar2"},"value":[105,"1"]}]}
						}`),
			skipSamplesBefore: 100 * 1000,
		},
		{
			name: "should skip properly when there are multiple series in a vector - histogram",
			expected: json.RawMessage(`{
							"status": "success",
							"data": {"resultType":"vector","result":[
								{"metric":{"foo":"bar"},"histogram":[90,{"count":"2","sum":"333","buckets":[[1,"0","2","2"]]}]}, 
								{"metric":{"foo":"bar2"},"histogram":[105,{"count":"2","sum":"3","buckets":[[1,"0","2","2"]]}]}]}
						}`),
			actual: json.RawMessage(`{
							"status": "success",
							"data": {"resultType":"vector","result":[
								{"metric":{"foo":"bar"},"histogram":[95,{"count":"2","sum":"3","buckets":[[1,"0","2","2"]]}]}, 
								{"metric":{"foo":"bar2"},"histogram":[105,{"count":"2","sum":"3","buckets":[[1,"0","2","2"]]}]}]}
						}`),
			skipSamplesBefore: 100 * 1000,
		},
		{
			name: "different series skipped in expected and actual, causing an error",
			expected: json.RawMessage(`{
							"status": "success",
							"data": {"resultType":"vector","result":[{"metric":{"foo":"bar"},"value":[105,"1"]}, {"metric":{"foo":"bar2"},"value":[90,"1"]}]}
						}`),
			actual: json.RawMessage(`{
							"status": "success",
							"data": {"resultType":"vector","result":[{"metric":{"foo":"bar"},"value":[95,"1"]}, {"metric":{"foo":"bar2"},"value":[105,"1"]}]}
						}`),
			skipSamplesBefore: 100 * 1000,
			err:               errors.New(`expected metric {foo="bar"} missing from actual response (also, some samples were filtered out from the expected and actual response due to the 'skip samples before'; if all samples have been filtered out, this could cause the check on the expected number of metrics to fail)`),
		},
		{
			name: "different series skipped in expected and actual, causing an error - histogram",
			expected: json.RawMessage(`{
							"status": "success",
							"data": {"resultType":"vector","result":[
								{"metric":{"foo":"bar"},"histogram":[105,{"count":"2","sum":"3","buckets":[[1,"0","2","2"]]}]}, 
								{"metric":{"foo":"bar2"},"histogram":[90,{"count":"2","sum":"3","buckets":[[1,"0","2","2"]]}]}]}
						}`),
			actual: json.RawMessage(`{
							"status": "success",
							"data": {"resultType":"vector","result":[
								{"metric":{"foo":"bar"},"histogram":[95,{"count":"2","sum":"3","buckets":[[1,"0","2","2"]]}]}, 
								{"metric":{"foo":"bar2"},"histogram":[105,{"count":"2","sum":"3","buckets":[[1,"0","2","2"]]}]}]}
						}`),
			skipSamplesBefore: 100 * 1000,
			err:               errors.New(`expected metric {foo="bar"} missing from actual response (also, some samples were filtered out from the expected and actual response due to the 'skip samples before'; if all samples have been filtered out, this could cause the check on the expected number of metrics to fail)`),
		},
		{
			name: "expected is skippable but not the actual, causing an error",
			expected: json.RawMessage(`{
							"status": "success",
							"data": {"resultType":"scalar","result":[90,"1"]}
						}`),
			actual: json.RawMessage(`{
							"status": "success",
							"data": {"resultType":"scalar","result":[100,"1"]}
						}`),
			skipSamplesBefore: 95 * 1000,
			err:               errors.New(`expected timestamp 90 (1970-01-01T00:01:30Z) but got 100 (1970-01-01T00:01:40Z)`),
		},
		{
			name: "actual is skippable but not the expected, causing an error",
			expected: json.RawMessage(`{
							"status": "success",
							"data": {"resultType":"scalar","result":[100,"1"]}
						}`),
			actual: json.RawMessage(`{
							"status": "success",
							"data": {"resultType":"scalar","result":[90,"1"]}
						}`),
			skipSamplesBefore: 95 * 1000,
			err:               errors.New(`expected timestamp 100 (1970-01-01T00:01:40Z) but got 90 (1970-01-01T00:01:30Z)`),
		},
		{
			name: "both expected and actual are skippable",
			expected: json.RawMessage(`{
							"status": "success",
							"data": {"resultType":"scalar","result":[95,"1"]}
						}`),
			actual: json.RawMessage(`{
							"status": "success",
							"data": {"resultType":"scalar","result":[90,"1"]}
						}`),
			skipSamplesBefore: 100 * 1000,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			samplesComparator := NewSamplesComparator(SampleComparisonOptions{
				Tolerance:         tc.tolerance,
				UseRelativeError:  tc.useRelativeError,
				SkipRecentSamples: tc.skipRecentSamples,
				SkipSamplesBefore: model.Time(tc.skipSamplesBefore),
			})
			result, err := samplesComparator.Compare(tc.expected, tc.actual, nowT.Time())
			if tc.err == nil {
				require.NoError(t, err)
				require.Equal(t, ComparisonSuccess, result)
				return
			}
			require.Error(t, err)
			require.Equal(t, tc.err.Error(), err.Error())
			require.Equal(t, result, ComparisonFailed)
		})
	}
}

func TestCompareSampleHistogramPair(t *testing.T) {
	bucketsErrorMessageGenerator := func(expected, actual *model.SampleHistogramPair) string {
		return fmt.Sprintf("expected buckets %s for timestamp %v but got %s", expected.Histogram.Buckets, expected.Timestamp, actual.Histogram.Buckets)
	}

	fields := map[string]struct {
		mutator                       func(*model.SampleHistogram, model.FloatString)
		expectedErrorMessageGenerator func(expected, actual *model.SampleHistogramPair) string
	}{
		"sum": {
			mutator: func(h *model.SampleHistogram, v model.FloatString) {
				h.Sum = v
			},
			expectedErrorMessageGenerator: func(expected, actual *model.SampleHistogramPair) string {
				return fmt.Sprintf("expected sum %s for timestamp %v but got %s", expected.Histogram.Sum, expected.Timestamp, actual.Histogram.Sum)
			},
		},
		"count": {
			mutator: func(h *model.SampleHistogram, v model.FloatString) {
				h.Count = v
			},
			expectedErrorMessageGenerator: func(expected, actual *model.SampleHistogramPair) string {
				return fmt.Sprintf("expected count %s for timestamp %v but got %s", expected.Histogram.Count, expected.Timestamp, actual.Histogram.Count)
			},
		},
		"bucket lower boundary": {
			mutator: func(h *model.SampleHistogram, v model.FloatString) {
				h.Buckets[0].Lower = v
			},
			expectedErrorMessageGenerator: bucketsErrorMessageGenerator,
		},
		"bucket upper boundary": {
			mutator: func(h *model.SampleHistogram, v model.FloatString) {
				h.Buckets[0].Upper = v
			},
			expectedErrorMessageGenerator: bucketsErrorMessageGenerator,
		},
		"bucket count": {
			mutator: func(h *model.SampleHistogram, v model.FloatString) {
				h.Buckets[0].Count = v
			},
			expectedErrorMessageGenerator: bucketsErrorMessageGenerator,
		},
	}

	testCases := map[string]struct {
		tolerance        float64
		useRelativeError bool
		expected         float64
		actual           float64
		shouldFail       bool
	}{
		"no tolerance, values equal": {
			expected:   123.4,
			actual:     123.4,
			shouldFail: false,
		},
		"no tolerance, values not equal": {
			expected:   123.4,
			actual:     123.5,
			shouldFail: true,
		},
		"no tolerance, values are both NaN": {
			expected:   math.NaN(),
			actual:     math.NaN(),
			shouldFail: false,
		},
		"no tolerance, values are both +Inf": {
			expected:   math.Inf(1),
			actual:     math.Inf(1),
			shouldFail: false,
		},
		"no tolerance, values are both -Inf": {
			expected:   math.Inf(-1),
			actual:     math.Inf(-1),
			shouldFail: false,
		},
		"non-zero tolerance, values differ by less than tolerance": {
			tolerance:  0.1,
			expected:   1.2,
			actual:     1.2999999,
			shouldFail: false,
		},
		"non-zero tolerance, values differ by more than tolerance": {
			tolerance:  0.1,
			expected:   1.2,
			actual:     1.3000001,
			shouldFail: true,
		},
		"non-zero tolerance, values are both NaN": {
			tolerance:  0.1,
			expected:   math.NaN(),
			actual:     math.NaN(),
			shouldFail: false,
		},
		"non-zero tolerance, values are both +Inf": {
			tolerance:  0.1,
			expected:   math.Inf(1),
			actual:     math.Inf(1),
			shouldFail: false,
		},
		"non-zero tolerance, values are both -Inf": {
			tolerance:  0.1,
			expected:   math.Inf(-1),
			actual:     math.Inf(-1),
			shouldFail: false,
		},
		"non-zero relative tolerance, values differ by less than tolerance": {
			tolerance:        0.1,
			useRelativeError: true,
			expected:         1.31,
			actual:           1.2,
			shouldFail:       false,
		},
		"non-zero relative tolerance, values differ by more than tolerance": {
			tolerance:        0.1,
			useRelativeError: true,
			expected:         0.551,
			actual:           0.5,
			shouldFail:       true,
		},
	}

	newHistogramPair := func() model.SampleHistogramPair {
		return model.SampleHistogramPair{
			Timestamp: 2,
			Histogram: &model.SampleHistogram{
				Count: 3,
				Sum:   4,
				Buckets: model.HistogramBuckets{
					{
						Boundaries: 1,
						Lower:      5,
						Upper:      6,
						Count:      7,
					},
				},
			},
		}

	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			opts := SampleComparisonOptions{
				Tolerance:        testCase.tolerance,
				UseRelativeError: testCase.useRelativeError,
			}

			for fieldName, field := range fields {
				t.Run(fieldName, func(t *testing.T) {
					expected := newHistogramPair()
					actual := newHistogramPair()

					field.mutator(expected.Histogram, model.FloatString(testCase.expected))
					field.mutator(actual.Histogram, model.FloatString(testCase.actual))

					err := compareSampleHistogramPair(expected, actual, time.Now(), opts)

					if testCase.shouldFail {
						expectedError := field.expectedErrorMessageGenerator(&expected, &actual)
						require.EqualError(t, err, expectedError)
					} else {
						require.NoError(t, err)
					}
				})
			}
		})
	}
}
