// SPDX-License-Identifier: AGPL-3.0-only

package validation

import (
	"fmt"

	"github.com/prometheus/prometheus/model/labels"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/util/promqlext"
)

// DelayedSeriesRule selects series that are not kept in the ingester head. They remain durable in Kafka
// and become queryable once the block-builder has published them.
type DelayedSeriesRule struct {
	Match  string   `yaml:"match" json:"match" doc:"description=Series selector for the series to delay, for example {__name__=\"http_request_duration_seconds_bucket\"}."`
	Except []string `yaml:"except,omitempty" json:"except,omitempty" doc:"description=Series selectors excluded from the rule. Matching series stay on the standard path."`

	match  []*labels.Matcher
	except [][]*labels.Matcher
}

type DelayedSeriesConfig []DelayedSeriesRule

// Validate parses the selectors and caches the compiled matchers.
func (c DelayedSeriesConfig) Validate() error {
	parser := promqlext.NewPromQLParser()
	for i := range c {
		match, err := parser.ParseMetricSelector(c[i].Match)
		if err != nil {
			return fmt.Errorf("delayed_series[%d]: invalid match selector %q: %w", i, c[i].Match, err)
		}
		except := make([][]*labels.Matcher, 0, len(c[i].Except))
		for j, sel := range c[i].Except {
			m, err := parser.ParseMetricSelector(sel)
			if err != nil {
				return fmt.Errorf("delayed_series[%d].except[%d]: invalid selector %q: %w", i, j, sel, err)
			}
			except = append(except, m)
		}
		c[i].match = match
		c[i].except = except
	}
	return nil
}

// IsDelayed returns whether the series matches any rule and none of that rule's exceptions.
func (c DelayedSeriesConfig) IsDelayed(lbls []mimirpb.LabelAdapter) bool {
	for i := range c {
		if matchesAll(c[i].match, lbls) && !matchesAny(c[i].except, lbls) {
			return true
		}
	}
	return false
}

func matchesAny(sets [][]*labels.Matcher, lbls []mimirpb.LabelAdapter) bool {
	for _, set := range sets {
		if matchesAll(set, lbls) {
			return true
		}
	}
	return false
}

func matchesAll(matchers []*labels.Matcher, lbls []mimirpb.LabelAdapter) bool {
	if len(matchers) == 0 {
		return false
	}
	for _, m := range matchers {
		if !m.Matches(labelValue(lbls, m.Name)) {
			return false
		}
	}
	return true
}

func labelValue(lbls []mimirpb.LabelAdapter, name string) string {
	for _, l := range lbls {
		if l.Name == name {
			return l.Value
		}
	}
	return ""
}

func (c *DelayedSeriesConfig) ExampleDoc() (comment string, yaml any) {
	return `The following configuration delays a histogram for every cluster except the one queried in real time.`,
		[]DelayedSeriesRule{
			{
				Match:  `{__name__="http_request_duration_seconds_bucket"}`,
				Except: []string{`{cluster="prod-us-east-0"}`},
			},
		}
}
