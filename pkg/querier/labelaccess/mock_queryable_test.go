// SPDX-License-Identifier: AGPL-3.0-only

package labelaccess

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/util/annotations"
)

// MockQueryable can be used as a querier or queryable in test cases, it is
// only intended for unit tests and not for any production use.
// It takes a series of expected querier calls and select calls on the querier,
// it validates whether all the expected calls have been received and none more.
// For each expected call the return value can also be defined.
type MockQueryable struct {
	sync.Mutex
	TB testing.TB

	// List of expected calls to the Queryable's .Querier() method,
	// order is not enforced to allow for multi-threaded use.
	ExpectedQuerierCalls []QuerierCall

	// List of expected calls to the Queryable's .ChunkQuerier() method,
	// order is not enforced to allow for multi-threaded use.
	ExpectedChunkQuerierCalls []QuerierCall

	// List of expected calls to the Querier's .Select() method,
	// order is not enforced to allow for multi-threaded use.
	ExpectedSelectCalls []SelectCall

	// List of expected calls to the ChunkQuerier's .Select() method,
	// order is not enforced to allow for multi-threaded use.
	ExpectedChunkSelectCalls []ChunkSelectCall

	// List of expected calls to the Querier's .LabelValues() method,
	// order is not enforced to allow for multi-threaded use.
	ExpectedLabelValuesCalls []LabelValuesCall

	UnlimitedCalls bool
}

func (m *MockQueryable) Close() error {
	return nil
}

func (m *MockQueryable) LabelNames(context.Context, *storage.LabelHints, ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	return nil, nil, nil
}

type QuerierCall struct {
	ExpectedMinT int64
	ExpectedMaxT int64
}

type SelectCall struct {
	ArgSortSeries bool
	ArgHints      *storage.SelectHints
	ArgMatchers   []*labels.Matcher
	ReturnValue   func() storage.SeriesSet
}

type ChunkSelectCall struct {
	ArgSortSeries bool
	ArgHints      *storage.SelectHints
	ArgMatchers   []*labels.Matcher
	ReturnValue   func() storage.ChunkSeriesSet
}

type LabelValuesCall struct {
	Label        string
	Matchers     []*labels.Matcher
	ReturnValues []string
}

func (m *MockQueryable) Querier(mint, maxt int64) (storage.Querier, error) {
	m.Lock()
	defer m.Unlock()

	for callIdx := range m.ExpectedQuerierCalls {
		if m.ExpectedQuerierCalls[callIdx].ExpectedMinT != mint || m.ExpectedQuerierCalls[callIdx].ExpectedMaxT != maxt {
			continue
		}

		if !m.UnlimitedCalls {
			m.ExpectedQuerierCalls = append(m.ExpectedQuerierCalls[:callIdx], m.ExpectedQuerierCalls[callIdx+1:]...)
		}

		// mockQueryable satisfies both interfaces
		// storage.Queryable and storage.Querier.
		return m, nil
	}

	m.TB.Fatalf("MockQueryable: Unexpected call to .Querier(ctx, %d, %d)", mint, maxt)
	return nil, errors.New("no querier")
}

func (m *MockQueryable) ChunkQuerier(mint, maxt int64) (storage.ChunkQuerier, error) {
	m.Lock()
	defer m.Unlock()

	for callIdx, expCall := range m.ExpectedChunkQuerierCalls {
		if expCall.ExpectedMinT != mint || expCall.ExpectedMaxT != maxt {
			continue
		}

		if !m.UnlimitedCalls {
			m.ExpectedChunkQuerierCalls = append(m.ExpectedChunkQuerierCalls[:callIdx], m.ExpectedChunkQuerierCalls[callIdx+1:]...)
		}

		return &mockChunkQuerier{
			TB:                  m.TB,
			ExpectedSelectCalls: m.ExpectedChunkSelectCalls,
			UnlimitedCalls:      m.UnlimitedCalls,
		}, nil
	}

	m.TB.Fatalf("MockQueryable: Unexpected call to .ChunkQuerier(ctx, %d, %d)", mint, maxt)
	return nil, errors.New("no querier")
}

type mockChunkQuerier struct {
	sync.Mutex
	TB testing.TB

	ExpectedSelectCalls []ChunkSelectCall

	UnlimitedCalls bool
}

func (q *mockChunkQuerier) Select(_ context.Context,
	sortSeries bool,
	hints *storage.SelectHints,
	matchers ...*labels.Matcher) storage.ChunkSeriesSet {
	q.Lock()
	defer q.Unlock()

CALLS:
	for callIdx, expectedCall := range q.ExpectedSelectCalls {
		// Compare all relevant properties of the expected calls to the given
		// arguments to decide whether this call matches an expected call.

		if expectedCall.ArgSortSeries != sortSeries {
			continue
		}
		if (expectedCall.ArgHints == nil) != (hints == nil) {
			continue
		}
		if expectedCall.ArgHints != nil &&
			(expectedCall.ArgHints.Start != hints.Start || expectedCall.ArgHints.End != hints.End) {
			continue
		}
		if len(expectedCall.ArgMatchers) != len(matchers) {
			continue
		}

		for i := range expectedCall.ArgMatchers {
			if expectedCall.ArgMatchers[i].Type != matchers[i].Type {
				continue CALLS
			}
			if expectedCall.ArgMatchers[i].Name != matchers[i].Name {
				continue CALLS
			}
			if expectedCall.ArgMatchers[i].Value != matchers[i].Value {
				continue CALLS
			}
		}

		res := expectedCall.ReturnValue()

		if !q.UnlimitedCalls {
			q.ExpectedSelectCalls = append(q.ExpectedSelectCalls[:callIdx], q.ExpectedSelectCalls[callIdx+1:]...)
		}

		return res
	}

	q.TB.Fatalf("mockChunkQuerier: Unexpected call to .Select(%t, %+v, %+v)", sortSeries, hints, matchers)
	return nil
}

func (q *mockChunkQuerier) LabelValues(context.Context, string, *storage.LabelHints, ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	return nil, nil, nil
}

func (q *mockChunkQuerier) LabelNames(context.Context, *storage.LabelHints, ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	return nil, nil, nil
}

func (q *mockChunkQuerier) Close() error {
	return nil
}

func (m *MockQueryable) Select(_ context.Context, sortSeries bool, hints *storage.SelectHints, matchers ...*labels.Matcher) storage.SeriesSet {
	m.Lock()
	defer m.Unlock()

CALLS:
	for callIdx, expectedCall := range m.ExpectedSelectCalls {
		// Compare all relevant properties of the expected calls to the given
		// arguments to decide whether this call matches an expected call.

		if expectedCall.ArgSortSeries != sortSeries {
			continue
		}
		if (expectedCall.ArgHints == nil) != (hints == nil) {
			continue
		}
		if expectedCall.ArgHints != nil &&
			(expectedCall.ArgHints.Start != hints.Start || expectedCall.ArgHints.End != hints.End) {
			continue
		}
		if len(expectedCall.ArgMatchers) != len(matchers) {
			continue
		}

		for i := range expectedCall.ArgMatchers {
			if expectedCall.ArgMatchers[i].Type != matchers[i].Type {
				continue CALLS
			}
			if expectedCall.ArgMatchers[i].Name != matchers[i].Name {
				continue CALLS
			}
			if expectedCall.ArgMatchers[i].Value != matchers[i].Value {
				continue CALLS
			}
		}

		res := expectedCall.ReturnValue()

		if !m.UnlimitedCalls {
			m.ExpectedSelectCalls = append(m.ExpectedSelectCalls[:callIdx], m.ExpectedSelectCalls[callIdx+1:]...)
		}

		return res
	}

	m.TB.Fatalf("MockQuerier: Unexpected call to .Select(%t, %+v, %+v)", sortSeries, hints, matchers)
	return nil
}

func (m *MockQueryable) LabelValues(_ context.Context, labelName string, _ *storage.LabelHints, matchers ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	m.Lock()
	defer m.Unlock()

	for callIdx, expectedCall := range m.ExpectedLabelValuesCalls {
		if expectedCall.Label != labelName {
			continue
		}

		if !reflect.DeepEqual(expectedCall.Matchers, matchers) {
			continue
		}

		res := expectedCall.ReturnValues

		if !m.UnlimitedCalls {
			m.ExpectedLabelValuesCalls = append(m.ExpectedLabelValuesCalls[:callIdx],
				m.ExpectedLabelValuesCalls[callIdx+1:]...)
		}

		return res, nil, nil
	}

	m.TB.Fatalf("MockQuerier: Unexpected call to .Labelvalues(%s)", labelName)
	return nil, nil, nil
}

// ValidateAllCalls checks if all the expected calls have been made, if not it
// raises a fatal error.
func (m *MockQueryable) ValidateAllCalls() {
	m.TB.Helper()

	// Checking whether the mock querier was expecting more calls
	// to its .Querier() method.
	if len(m.ExpectedQuerierCalls) > 0 {
		m.TB.Fatalf("Expected querier calls have not been made: %+v", m.ExpectedQuerierCalls)
	}

	// Checking whether the mock querier was expecting more calls
	// to its .Select() method.
	if len(m.ExpectedSelectCalls) > 0 {
		m.TB.Fatalf("Expected select calls: %+v", m.ExpectedSelectCalls)
	}

	// Checking whether the mock querier was expecting more calls
	// to its .LabelValues() method.
	if len(m.ExpectedLabelValuesCalls) > 0 {
		m.TB.Fatalf("Expected label values calls: %+v", m.ExpectedLabelValuesCalls)
	}
}
