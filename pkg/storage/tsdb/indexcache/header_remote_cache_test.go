package indexcache

import (
	"context"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/oklog/ulid/v2"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb/index"
	"github.com/stretchr/testify/assert"

	"github.com/grafana/mimir/pkg/storage/fixtures"
)

type mockedPostingsOffset struct {
	tenantID string
	blockID  ulid.ULID
	lbl      labels.Label
	rng      index.Range
}

type mockedPostingsOffsetsForMatcher struct {
	tenantID   string
	blockID    ulid.ULID
	m          *labels.Matcher
	isSubtract bool
	//rngs       []index.Range
}

func TestRemotePostingsOffsetTableCache_FetchPostingsOffset(t *testing.T) {
	t.Parallel()

	tenant0 := "tenant-0"
	tenant1 := "tenant1"
	block1 := ulid.MustNew(1, nil)
	block2 := ulid.MustNew(2, nil)
	label0 := labels.Label{Name: "instance", Value: "a"}
	label1 := labels.Label{Name: "instance", Value: "b"}
	label2 := labels.Label{Name: "instance", Value: "c"}
	rng0 := index.Range{Start: 4, End: 16}
	rng1 := index.Range{Start: 20, End: 32}
	rng2 := index.Range{Start: 36, End: 60}

	tests := map[string]struct {
		setup          []mockedPostingsOffset
		mockedErr      error
		fetchTenantID  string
		fetchBlockID   ulid.ULID
		fetchLabels    []labels.Label
		expectedHits   map[labels.Label]index.Range
		expectedMisses []labels.Label
	}{
		"should return no hits on empty cache": {
			setup:          []mockedPostingsOffset{},
			fetchTenantID:  tenant0,
			fetchBlockID:   block1,
			fetchLabels:    []labels.Label{label0, label1},
			expectedHits:   nil,
			expectedMisses: []labels.Label{label0, label1},
		},
		"should return no misses on 100% hit ratio": {
			setup: []mockedPostingsOffset{
				{tenantID: tenant0, blockID: block1, lbl: label0, rng: rng0},
				{tenantID: tenant0, blockID: block1, lbl: label1, rng: rng1},
				{tenantID: tenant1, blockID: block1, lbl: label0, rng: rng0},
				{tenantID: tenant1, blockID: block2, lbl: label1, rng: rng1},
			},
			fetchTenantID: tenant0,
			fetchBlockID:  block1,
			fetchLabels:   []labels.Label{label0, label1},
			expectedHits: map[labels.Label]index.Range{
				label0: rng0,
				label1: rng1,
			},
			expectedMisses: []labels.Label{},
		},
		"should return hits and misses on partial hits": {
			setup: []mockedPostingsOffset{
				{tenantID: tenant0, blockID: block1, lbl: label0, rng: rng0},
				{tenantID: tenant0, blockID: block2, lbl: label2, rng: rng2},
			},
			fetchTenantID:  tenant0,
			fetchBlockID:   block1,
			fetchLabels:    []labels.Label{label0, label1},
			expectedHits:   map[labels.Label]index.Range{label0: rng0},
			expectedMisses: []labels.Label{label1},
		},
		"should return no hits on remote cache error": {
			setup: []mockedPostingsOffset{
				{tenantID: tenant0, blockID: block1, lbl: label0, rng: rng1},
				{tenantID: tenant0, blockID: block1, lbl: label1, rng: rng1},
				{tenantID: tenant0, blockID: block2, lbl: label0, rng: rng2},
			},
			mockedErr:      errors.New("mocked error"),
			fetchTenantID:  tenant0,
			fetchBlockID:   block1,
			fetchLabels:    []labels.Label{label0, label1},
			expectedHits:   nil,
			expectedMisses: []labels.Label{label0, label1},
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			cacheClient := newMockedRemoteCacheClient(testData.mockedErr)
			cache := NewRemotePostingsOffsetTableCache(cacheClient, log.NewNopLogger())

			// Store the postings expected before running the test.
			ctx := context.Background()
			for _, p := range testData.setup {
				cache.StorePostingsOffset(p.tenantID, p.blockID, p.lbl, p.rng, time.Hour)
			}

			// Fetch postings from cached and assert on it.
			testFetchFetchPostingsOffsets(
				ctx, t, cache,
				testData.fetchTenantID, testData.fetchBlockID, testData.fetchLabels,
				testData.expectedHits,
			)
		})
	}
}

func TestRemotePostingsOffsetTableCache_FetchPostingsOffsetsForMatcher(t *testing.T) {
	t.Parallel()

	tenant0 := "tenant-0"
	tenant1 := "tenant1"
	block0 := ulid.MustNew(0, nil)
	block1 := ulid.MustNew(1, nil)

	matchAnyPlus := labels.MustNewMatcher(labels.MatchRegexp, "instance", ".+")
	matchEqual := labels.MustNewMatcher(labels.MatchEqual, "instance", fixtures.LabelLongSuffix)

	rng0 := index.Range{Start: 4, End: 16}
	rng1 := index.Range{Start: 20, End: 32}
	rng2 := index.Range{Start: 36, End: 60}

	allKeyValues := map[mockedPostingsOffsetsForMatcher][]index.Range{
		{tenantID: tenant0, blockID: block0, m: matchAnyPlus, isSubtract: false}: {rng0, rng1, rng2},
		{tenantID: tenant0, blockID: block1, m: matchAnyPlus, isSubtract: false}: {rng0, rng1},
		{tenantID: tenant0, blockID: block0, m: matchAnyPlus, isSubtract: true}:  {},
		{tenantID: tenant0, blockID: block1, m: matchAnyPlus, isSubtract: true}:  {},
		{tenantID: tenant0, blockID: block0, m: matchEqual, isSubtract: false}:   {rng0},
		{tenantID: tenant0, blockID: block1, m: matchEqual, isSubtract: false}:   {rng1},
		{tenantID: tenant0, blockID: block0, m: matchEqual, isSubtract: true}:    {rng1, rng2},
		{tenantID: tenant0, blockID: block1, m: matchEqual, isSubtract: true}:    {rng0},

		{tenantID: tenant1, blockID: block0, m: matchAnyPlus, isSubtract: false}: {rng0, rng1},
		{tenantID: tenant1, blockID: block1, m: matchAnyPlus, isSubtract: false}: {rng1, rng2},
		{tenantID: tenant1, blockID: block0, m: matchAnyPlus, isSubtract: true}:  {},
		{tenantID: tenant1, blockID: block1, m: matchAnyPlus, isSubtract: true}:  {},
		{tenantID: tenant1, blockID: block0, m: matchEqual, isSubtract: false}:   {rng1},
		{tenantID: tenant1, blockID: block1, m: matchEqual, isSubtract: false}:   {rng0},
		{tenantID: tenant1, blockID: block0, m: matchEqual, isSubtract: true}:    {rng0},
		{tenantID: tenant1, blockID: block1, m: matchEqual, isSubtract: true}:    {rng1},
	}
	allKeys := make([]mockedPostingsOffsetsForMatcher, 0, len(allKeyValues))
	for k := range allKeyValues {
		allKeys = append(allKeys, k)
	}

	tests := map[string]struct {
		setupSetVals   map[mockedPostingsOffsetsForMatcher][]index.Range
		mockedErr      error
		fetches        []mockedPostingsOffsetsForMatcher // no need to set rngs values
		expectedHits   map[mockedPostingsOffsetsForMatcher][]index.Range
		expectedMisses []mockedPostingsOffsetsForMatcher
	}{
		"should return no hits on empty cache": {
			setupSetVals:   map[mockedPostingsOffsetsForMatcher][]index.Range{},
			fetches:        allKeys,
			expectedHits:   nil,
			expectedMisses: allKeys,
		},
		"should return no misses on 100% hit ratio": {
			setupSetVals:   allKeyValues,
			fetches:        allKeys,
			expectedHits:   allKeyValues,
			expectedMisses: nil,
		},
		"should return hits and misses on partial hits": {
			setupSetVals: map[mockedPostingsOffsetsForMatcher][]index.Range{
				{tenantID: tenant0, blockID: block0, m: matchAnyPlus, isSubtract: false}: {rng0, rng1, rng2},
				{tenantID: tenant0, blockID: block1, m: matchAnyPlus, isSubtract: false}: {rng0, rng1},
				{tenantID: tenant0, blockID: block0, m: matchAnyPlus, isSubtract: true}:  {},
				{tenantID: tenant0, blockID: block1, m: matchAnyPlus, isSubtract: true}:  {},
				{tenantID: tenant1, blockID: block0, m: matchEqual, isSubtract: false}:   {rng1},
				{tenantID: tenant1, blockID: block1, m: matchEqual, isSubtract: false}:   {rng0},
				{tenantID: tenant1, blockID: block0, m: matchEqual, isSubtract: true}:    {rng0},
				{tenantID: tenant1, blockID: block1, m: matchEqual, isSubtract: true}:    {rng1},
			},
			fetches: allKeys,
			expectedHits: map[mockedPostingsOffsetsForMatcher][]index.Range{
				{tenantID: tenant0, blockID: block0, m: matchAnyPlus, isSubtract: false}: {rng0, rng1, rng2},
				{tenantID: tenant0, blockID: block1, m: matchAnyPlus, isSubtract: false}: {rng0, rng1},
				{tenantID: tenant0, blockID: block0, m: matchAnyPlus, isSubtract: true}:  {},
				{tenantID: tenant0, blockID: block1, m: matchAnyPlus, isSubtract: true}:  {},
				{tenantID: tenant1, blockID: block0, m: matchEqual, isSubtract: false}:   {rng1},
				{tenantID: tenant1, blockID: block1, m: matchEqual, isSubtract: false}:   {rng0},
				{tenantID: tenant1, blockID: block0, m: matchEqual, isSubtract: true}:    {rng0},
				{tenantID: tenant1, blockID: block1, m: matchEqual, isSubtract: true}:    {rng1},
			},
			expectedMisses: []mockedPostingsOffsetsForMatcher{
				{tenantID: tenant0, blockID: block0, m: matchEqual, isSubtract: false},
				{tenantID: tenant0, blockID: block1, m: matchEqual, isSubtract: false},
				{tenantID: tenant0, blockID: block0, m: matchEqual, isSubtract: true},
				{tenantID: tenant0, blockID: block1, m: matchEqual, isSubtract: true},
				{tenantID: tenant1, blockID: block0, m: matchAnyPlus, isSubtract: false},
				{tenantID: tenant1, blockID: block1, m: matchAnyPlus, isSubtract: false},
				{tenantID: tenant1, blockID: block0, m: matchAnyPlus, isSubtract: true},
				{tenantID: tenant1, blockID: block1, m: matchAnyPlus, isSubtract: true},
			},
		},
		"should return no hits on remote cache error": {
			setupSetVals:   allKeyValues,
			mockedErr:      errors.New("mocked error"),
			fetches:        allKeys,
			expectedHits:   nil,
			expectedMisses: allKeys,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			cacheClient := newMockedRemoteCacheClient(testData.mockedErr)
			cache := NewRemotePostingsOffsetTableCache(cacheClient, log.NewNopLogger())

			ctx := context.Background()
			for k, v := range testData.setupSetVals {
				cache.StorePostingsOffsetsForMatcher(k.tenantID, k.blockID, k.m, k.isSubtract, v, time.Hour)
			}

			testFetchFetchPostingsOffsetsForMatcher(
				ctx, t, cache,
				testData.fetches,
				testData.expectedHits,
				testData.expectedMisses,
			)
		})
	}
}

func testFetchFetchPostingsOffsets(
	ctx context.Context,
	t *testing.T,
	cache PostingsOffsetTableCache,
	tenantID string,
	blockID ulid.ULID,
	keys []labels.Label,
	expectedHits map[labels.Label]index.Range,
) {
	t.Helper()

	hits := make(map[labels.Label]index.Range)
	for _, key := range keys {
		if rng, ok := cache.FetchPostingsOffset(ctx, tenantID, blockID, key); ok {
			hits[key] = rng
		}
	}

	if len(expectedHits) == 0 {
		assert.Equal(t, len(hits), 0)
	} else {
		assert.Equal(t, expectedHits, hits)
	}
}

func testFetchFetchPostingsOffsetsForMatcher(
	ctx context.Context,
	t *testing.T,
	cache PostingsOffsetTableCache,
	keys []mockedPostingsOffsetsForMatcher,
	expectedHits map[mockedPostingsOffsetsForMatcher][]index.Range,
	expectedMisses []mockedPostingsOffsetsForMatcher,
) {
	t.Helper()

	hits := make(map[mockedPostingsOffsetsForMatcher][]index.Range)
	misses := make([]mockedPostingsOffsetsForMatcher, 0, len(keys))
	for _, key := range keys {
		rngs, ok := cache.FetchPostingsOffsetsForMatcher(
			ctx, key.tenantID, key.blockID, key.m, key.isSubtract)
		if ok {
			hits[key] = rngs
		} else {
			misses = append(misses, key)
		}
	}

	assert.Equal(t, len(expectedHits), len(hits))
	assert.Equal(t, len(expectedMisses), len(misses))
	assert.Equal(t, len(keys)-len(expectedHits), len(expectedMisses))

	if len(expectedHits) != 0 {
		assert.Equal(t, len(expectedHits), len(hits))
		for k, expectedV := range expectedHits {
			assert.EqualValues(t, expectedV, hits[k])
		}
	}

}
