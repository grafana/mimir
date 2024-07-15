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
							{"metric":{"foo":"bar"},"values":[[1,"1"]]}
						]`),
			actual: json.RawMessage(`[]`),
			err:    errors.New("expected 1 metrics but got 0"),
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
			err: errors.New("expected 1 metrics but got 2"),
		},
		{
			name: "same number of metrics but with different labels",
			expected: json.RawMessage(`[
							{"metric":{"foo":"bar"},"values":[[1,"1"]]}
						]`),
			actual: json.RawMessage(`[
							{"metric":{"foo1":"bar1"},"values":[[1,"1"]]}
						]`),
			err: errors.New(`expected metric {foo="bar"} missing from actual response`),
		},
		{
			name: "difference in number of float samples",
			expected: json.RawMessage(`[
							{"metric":{"foo":"bar"},"values":[[1,"1"],[2,"2"]]}
						]`),
			actual: json.RawMessage(`[
							{"metric":{"foo":"bar"},"values":[[1,"1"]]}
						]`),
			err: errors.New(`expected 2 float sample(s) and 0 histogram sample(s) for metric {foo="bar"} but got 1 float sample(s) and 0 histogram sample(s)`),
		},
		{
			name: "difference in float sample timestamp",
			expected: json.RawMessage(`[
							{"metric":{"foo":"bar"},"values":[[1,"1"],[2,"2"]]}
						]`),
			actual: json.RawMessage(`[
							{"metric":{"foo":"bar"},"values":[[1,"1"],[3,"2"]]}
						]`),
			// timestamps are parsed from seconds to ms which are then added to errors as is so adding 3 0s to expected error.
			err: errors.New(`float sample pair does not match for metric {foo="bar"}: expected timestamp 2 but got 3`),
		},
		{
			name: "difference in float sample value",
			expected: json.RawMessage(`[
							{"metric":{"foo":"bar"},"values":[[1,"1"],[2,"2"]]}
						]`),
			actual: json.RawMessage(`[
							{"metric":{"foo":"bar"},"values":[[1,"1"],[2,"3"]]}
						]`),
			err: errors.New(`float sample pair does not match for metric {foo="bar"}: expected value 2 for timestamp 2 but got 3`),
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
			err: errors.New(`expected 0 float sample(s) and 1 histogram sample(s) for metric {foo="bar"} but got 1 float sample(s) and 0 histogram sample(s)`),
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
			err: errors.New(`expected 1 float sample(s) and 0 histogram sample(s) for metric {foo="bar"} but got 0 float sample(s) and 1 histogram sample(s)`),
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
			err: errors.New(`histogram sample pair does not match for metric {foo="bar"}: expected timestamp 1 but got 2`),
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
			err: errors.New(`histogram sample pair does not match for metric {foo="bar"}: expected count 2 for timestamp 1 but got 5`),
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
			err: errors.New(`histogram sample pair does not match for metric {foo="bar"}: expected sum 3 for timestamp 1 but got 5`),
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
			err: errors.New(`histogram sample pair does not match for metric {foo="bar"}: expected buckets [[0,2):2] for timestamp 1 but got [[0,2):2 [2,4):2]`),
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
			err: errors.New(`histogram sample pair does not match for metric {foo="bar"}: expected buckets [[0,2):2] for timestamp 1 but got [(0,2):2]`),
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
			err: errors.New(`histogram sample pair does not match for metric {foo="bar"}: expected buckets [[0,2):2] for timestamp 1 but got [[1,2):2]`),
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
			err: errors.New(`histogram sample pair does not match for metric {foo="bar"}: expected buckets [[0,2):2] for timestamp 1 but got [[0,3):2]`),
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
			err: errors.New(`histogram sample pair does not match for metric {foo="bar"}: expected buckets [[0,2):2] for timestamp 1 but got [[0,2):3]`),
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
			err: errors.New(`histogram sample pair does not match for metric {foo="bar"}: expected sum 5 for timestamp 2 but got 6`),
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
			err: errors.New(`expected 0 float sample(s) and 2 histogram sample(s) for metric {foo="bar"} but got 0 float sample(s) and 1 histogram sample(s)`),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := compareMatrix(tc.expected, tc.actual, SampleComparisonOptions{})
			if tc.err == nil {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			require.Equal(t, tc.err.Error(), err.Error())
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
			err: errors.New(`float sample pair does not match for metric {foo="bar"}: expected timestamp 1 but got 2`),
		},
		{
			name: "difference in float sample value",
			expected: json.RawMessage(`[
							{"metric":{"foo":"bar"},"value":[1,"1"]}
						]`),
			actual: json.RawMessage(`[
							{"metric":{"foo":"bar"},"value":[1,"2"]}
						]`),
			err: errors.New(`float sample pair does not match for metric {foo="bar"}: expected value 1 for timestamp 1 but got 2`),
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
			err := compareVector(tc.expected, tc.actual, SampleComparisonOptions{})
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
			err:      errors.New("expected timestamp 1 but got 2"),
		},
		{
			name:     "difference in value",
			expected: json.RawMessage(`[1,"1"]`),
			actual:   json.RawMessage(`[1,"2"]`),
			err:      errors.New("expected value 1 for timestamp 1 but got 2"),
		},
		{
			name:     "correct values",
			expected: json.RawMessage(`[1,"1"]`),
			actual:   json.RawMessage(`[1,"1"]`),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := compareScalar(tc.expected, tc.actual, SampleComparisonOptions{})
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
	now := model.Now().String()
	for _, tc := range []struct {
		name              string
		tolerance         float64
		expected          json.RawMessage
		actual            json.RawMessage
		err               error
		useRelativeError  bool
		skipRecentSamples time.Duration
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
			err: errors.New(`float sample pair does not match for metric {foo="bar"}: expected value 773054.5916666666 for timestamp 1 but got 773054.789`),
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
			err: errors.New(`float sample pair does not match for metric {foo="bar"}: expected value 492348853678528200000000000000000000000000 for timestamp 1 but got 492348853678528100000000000000000000000000`),
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
			name: "should not fail when the float sample is recent and configured to skip",
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
			name: "should not fail when the histogram sample is recent and configured to skip",
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
	} {
		t.Run(tc.name, func(t *testing.T) {
			samplesComparator := NewSamplesComparator(SampleComparisonOptions{
				Tolerance:         tc.tolerance,
				UseRelativeError:  tc.useRelativeError,
				SkipRecentSamples: tc.skipRecentSamples,
			})
			result, err := samplesComparator.Compare(tc.expected, tc.actual)
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

					err := compareSampleHistogramPair(expected, actual, opts)

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
