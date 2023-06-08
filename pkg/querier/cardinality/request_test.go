// SPDX-License-Identifier: AGPL-3.0-only

package cardinality

import (
	"testing"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/assert"
)

func TestLabelNamesRequest_String(t *testing.T) {
	req := &LabelNamesRequest{
		Matchers: []*labels.Matcher{
			labels.MustNewMatcher(labels.MatchEqual, "first", "1"),
			labels.MustNewMatcher(labels.MatchNotEqual, "second", "2"),
		},
		Limit: 100,
	}

	assert.Equal(t, `first="1",second!="2"|100`, req.String())
}

func TestLabelValuesRequest_String(t *testing.T) {
	req := &LabelValuesRequest{
		LabelNames: []model.LabelName{"foo", "bar"},
		Matchers: []*labels.Matcher{
			labels.MustNewMatcher(labels.MatchEqual, "first", "1"),
			labels.MustNewMatcher(labels.MatchNotEqual, "second", "2"),
		},
		Limit: 100,
	}

	assert.Equal(t, `foo,bar|first="1",second!="2"|100`, req.String())
}
