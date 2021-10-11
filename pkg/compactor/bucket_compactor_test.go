// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/thanos-io/thanos/blob/2be2db77/pkg/compact/compact_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Thanos Authors.

package compactor

import (
	"testing"

	"github.com/go-kit/kit/log"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/errutil"
)

func TestHaltError(t *testing.T) {
	err := errors.New("test")
	assert.True(t, !IsHaltError(err), "halt error")

	err = halt(errors.New("test"))
	assert.True(t, IsHaltError(err), "not a halt error")

	err = errors.Wrap(halt(errors.New("test")), "something")
	assert.True(t, IsHaltError(err), "not a halt error")

	err = errors.Wrap(errors.Wrap(halt(errors.New("test")), "something"), "something2")
	assert.True(t, IsHaltError(err), "not a halt error")
}

func TestHaltMultiError(t *testing.T) {
	haltErr := halt(errors.New("halt error"))
	nonHaltErr := errors.New("not a halt error")

	errs := errutil.MultiError{nonHaltErr}
	assert.True(t, !IsHaltError(errs.Err()), "should not be a halt error")

	errs.Add(haltErr)
	assert.True(t, IsHaltError(errs.Err()), "if any halt errors are present this should return true")
	assert.True(t, IsHaltError(errors.Wrap(errs.Err(), "wrap")), "halt error with wrap")

}

func TestRetryMultiError(t *testing.T) {
	retryErr := retry(errors.New("retry error"))
	nonRetryErr := errors.New("not a retry error")

	errs := errutil.MultiError{nonRetryErr}
	assert.True(t, !IsRetryError(errs.Err()), "should not be a retry error")

	errs = errutil.MultiError{retryErr}
	assert.True(t, IsRetryError(errs.Err()), "if all errors are retriable this should return true")

	assert.True(t, IsRetryError(errors.Wrap(errs.Err(), "wrap")), "retry error with wrap")

	errs = errutil.MultiError{nonRetryErr, retryErr}
	assert.True(t, !IsRetryError(errs.Err()), "mixed errors should return false")
}

func TestRetryError(t *testing.T) {
	err := errors.New("test")
	assert.True(t, !IsRetryError(err), "retry error")

	err = retry(errors.New("test"))
	assert.True(t, IsRetryError(err), "not a retry error")

	err = errors.Wrap(retry(errors.New("test")), "something")
	assert.True(t, IsRetryError(err), "not a retry error")

	err = errors.Wrap(errors.Wrap(retry(errors.New("test")), "something"), "something2")
	assert.True(t, IsRetryError(err), "not a retry error")

	err = errors.Wrap(retry(errors.Wrap(halt(errors.New("test")), "something")), "something2")
	assert.True(t, IsHaltError(err), "not a halt error. Retry should not hide halt error")
}

func TestGroupKey(t *testing.T) {
	for _, tcase := range []struct {
		input    metadata.Thanos
		expected string
	}{
		{
			input:    metadata.Thanos{},
			expected: "0@17241709254077376921",
		},
		{
			input: metadata.Thanos{
				Labels:     map[string]string{},
				Downsample: metadata.ThanosDownsample{Resolution: 0},
			},
			expected: "0@17241709254077376921",
		},
		{
			input: metadata.Thanos{
				Labels:     map[string]string{"foo": "bar", "foo1": "bar2"},
				Downsample: metadata.ThanosDownsample{Resolution: 0},
			},
			expected: "0@2124638872457683483",
		},
		{
			input: metadata.Thanos{
				Labels:     map[string]string{`foo/some..thing/some.thing/../`: `a_b_c/bar-something-a\metric/a\x`},
				Downsample: metadata.ThanosDownsample{Resolution: 0},
			},
			expected: "0@16590761456214576373",
		},
	} {
		if ok := t.Run("", func(t *testing.T) {
			assert.Equal(t, tcase.expected, DefaultGroupKey(tcase.input))
		}); !ok {
			return
		}
	}
}

func TestGroupMaxMinTime(t *testing.T) {
	g := &Job{
		metasByMinTime: []*metadata.Meta{
			{BlockMeta: tsdb.BlockMeta{MinTime: 0, MaxTime: 10}},
			{BlockMeta: tsdb.BlockMeta{MinTime: 1, MaxTime: 20}},
			{BlockMeta: tsdb.BlockMeta{MinTime: 2, MaxTime: 30}},
		},
	}

	assert.Equal(t, int64(0), g.MinTime())
	assert.Equal(t, int64(30), g.MaxTime())
}

func TestFilterOwnJobs(t *testing.T) {
	jobsFn := func() []*Job {
		return []*Job{
			NewJob("user", "key1", nil, 0, metadata.NoneFunc, false, 0, ""),
			NewJob("user", "key2", nil, 0, metadata.NoneFunc, false, 0, ""),
			NewJob("user", "key3", nil, 0, metadata.NoneFunc, false, 0, ""),
			NewJob("user", "key4", nil, 0, metadata.NoneFunc, false, 0, ""),
		}
	}

	tests := map[string]struct {
		ownJob       ownCompactionJobFunc
		expectedJobs int
	}{
		"should return all planned jobs if the compactor instance owns all of them": {
			ownJob: func(job *Job) (bool, error) {
				return true, nil
			},
			expectedJobs: 4,
		},
		"should return no jobs if the compactor instance owns none of them": {
			ownJob: func(job *Job) (bool, error) {
				return false, nil
			},
			expectedJobs: 0,
		},
		"should return some jobs if the compactor instance owns some of them": {
			ownJob: func() ownCompactionJobFunc {
				count := 0
				return func(job *Job) (bool, error) {
					count++
					return count%2 == 0, nil
				}
			}(),
			expectedJobs: 2,
		},
	}

	m := NewBucketCompactorMetrics(prometheus.NewCounter(prometheus.CounterOpts{}), prometheus.NewCounter(prometheus.CounterOpts{}), nil)
	for testName, testCase := range tests {
		t.Run(testName, func(t *testing.T) {
			bc, err := NewBucketCompactor(log.NewNopLogger(), nil, nil, nil, nil, "", nil, 2, false, testCase.ownJob, m)
			require.NoError(t, err)

			res, err := bc.filterOwnJobs(jobsFn())

			require.NoError(t, err)
			assert.Len(t, res, testCase.expectedJobs)
		})
	}
}
