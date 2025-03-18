// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/ingester/client/compat_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package client

import (
	"reflect"
	"testing"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestQueryRequest(t *testing.T) {
	from, to := model.Time(int64(0)), model.Time(int64(10))
	matchers := []*labels.Matcher{}
	matcher1, err := labels.NewMatcher(labels.MatchEqual, "foo", "1")
	if err != nil {
		t.Fatal(err)
	}
	matchers = append(matchers, matcher1)

	matcher2, err := labels.NewMatcher(labels.MatchNotEqual, "bar", "2")
	if err != nil {
		t.Fatal(err)
	}
	matchers = append(matchers, matcher2)

	matcher3, err := labels.NewMatcher(labels.MatchRegexp, "baz", "3")
	if err != nil {
		t.Fatal(err)
	}
	matchers = append(matchers, matcher3)

	matcher4, err := labels.NewMatcher(labels.MatchNotRegexp, "bop", "4")
	if err != nil {
		t.Fatal(err)
	}
	matchers = append(matchers, matcher4)

	req, err := ToQueryRequest(from, to, matchers)
	if err != nil {
		t.Fatal(err)
	}

	haveFrom, haveTo, haveMatchers, err := FromQueryRequest(req)
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(haveFrom, from) {
		t.Fatalf("Bad from FromQueryRequest(ToQueryRequest) round trip")
	}
	if !reflect.DeepEqual(haveTo, to) {
		t.Fatalf("Bad to FromQueryRequest(ToQueryRequest) round trip")
	}

	// Assert same matchers. We do some optimizations in mimir-prometheus which make
	// the label matchers not comparable with reflect.DeepEqual() so we're going to
	// compare their string representation.
	require.Len(t, haveMatchers, len(matchers))
	for i := 0; i < len(matchers); i++ {
		assert.Equal(t, matchers[i].String(), haveMatchers[i].String())
	}
}

func TestLabelNamesRequest(t *testing.T) {
	const (
		mint, maxt = 0, 10
	)

	hints := &storage.LabelHints{Limit: 10}
	matchers := []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "foo", "bar")}

	req, err := ToLabelNamesRequest(mint, maxt, hints, matchers)
	require.NoError(t, err)

	actualMinT, actualMaxT, actualHints, actualMatchers, err := FromLabelNamesRequest(req)
	require.NoError(t, err)

	assert.Equal(t, int64(mint), actualMinT)
	assert.Equal(t, int64(maxt), actualMaxT)
	assert.Equal(t, hints, actualHints)
	assert.Equal(t, matchers, actualMatchers)
}
