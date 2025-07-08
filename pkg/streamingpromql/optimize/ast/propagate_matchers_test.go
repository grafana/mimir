// SPDX-License-Identifier: AGPL-3.0-only

package ast

import (
	"context"
	"testing"

	"github.com/prometheus/prometheus/promql/parser"
	"github.com/stretchr/testify/require"
)

func TestPropagateMatchers(t *testing.T) {
	testCases := map[string]string{
		`up`:                                 `up`,
		`up{foo="bar"}`:                      `up{foo="bar"}`,
		`up * on(foo) group_left down`:       `up * on(foo) group_left down`,
		`up * ignoring(foo) group_left down`: `up * ignoring(foo) group_left down`,
		`up{foo="bar"} * 1`:                  `up{foo="bar"} * 1`,
		`up * down`:                          `up * down`,
		`up{foo="bar"} * down{foo="bar"}`:    `up{foo="bar"} * down{foo="bar"}`,
		`up{foo="bar"} * down`:               `up{foo="bar"} * down{foo="bar"}`,
		`up * down{foo="bar"}`:               `up{foo="bar"} * down{foo="bar"}`,
		`up{foo="bar"} * down{bar="foo"}`:    `up{foo="bar", bar="foo"} * down{foo="bar", bar="foo"}`,
		`up{foo="bar"} * down{foo!="bar2"}`:  `up{foo="bar", foo!="bar2"} * down{foo="bar", foo!="bar2"}`,
		`up{foo="bar"} * ignoring(foo) down`: `up{foo="bar"} * ignoring(foo) down`,
		`up{foo="bar", bar="foo", boo="far", far="boo"} * ignoring(foo, bar) down`: `up{foo="bar", bar="foo", boo="far", far="boo"} * ignoring(foo, bar) down{boo="far", far="boo"}`,
		`up * ignoring(foo) down{foo="bar"}`:                                       `up * ignoring(foo) down{foo="bar"}`,
		`(up{foo="bar"} * down)`:                                                   `(up{foo="bar"} * down{foo="bar"})`,
		`((((up{foo="bar"} * down))))`:                                             `((((up{foo="bar"} * down{foo="bar"}))))`,
		`((((((up{foo="bar"})) * (((down)))))))`:                                   `((((((up{foo="bar"})) * (((down{foo="bar"})))))))`,
		`up{foo="bar"} * down * 3`:                                                 `up{foo="bar"} * down{foo="bar"} * 3`,
		`up * down{foo="bar"} * 3`:                                                 `up{foo="bar"} * down{foo="bar"} * 3`,
		`(up{foo="bar"} * down) * 3`:                                               `(up{foo="bar"} * down{foo="bar"}) * 3`,
		`(up * down{foo="bar"}) * 3`:                                               `(up{foo="bar"} * down{foo="bar"}) * 3`,
		`3 * (up{foo="bar"} * down)`:                                               `3 * (up{foo="bar"} * down{foo="bar"})`,
		`3 * (up * down{foo="bar"})`:                                               `3 * (up{foo="bar"} * down{foo="bar"})`,
		`3 * up{foo="bar"} * down`:                                                 `3 * up{foo="bar"} * down{foo="bar"}`,
		`3 * up * down{foo="bar"}`:                                                 `3 * up{foo="bar"} * down{foo="bar"}`,
		`up{foo="bar"} * 3 * down`:                                                 `up{foo="bar"} * 3 * down{foo="bar"}`,
		`up * 3 * down{foo="bar"}`:                                                 `up{foo="bar"} * 3 * down{foo="bar"}`,
		`(up{foo="bar"} * 3) * down`:                                               `(up{foo="bar"} * 3) * down{foo="bar"}`,
		`up{foo="bar"} * (3 * down)`:                                               `up{foo="bar"} * (3 * down{foo="bar"})`,
		`(up * 3) * down{foo="bar"}`:                                               `(up{foo="bar"} * 3) * down{foo="bar"}`,
		`up * (3 * down{foo="bar"})`:                                               `up{foo="bar"} * (3 * down{foo="bar"})`,
		`up * on(foo) down{foo="bar", bar="foo"}`:                                  `up{foo="bar"} * on(foo) down{foo="bar", bar="foo"}`,
		`up * on(foo, bar) down{foo="bar", bar="foo", boo="far", far="boo"}`:       `up{foo="bar", bar="foo"} * on(foo, bar) down{foo="bar", bar="foo", boo="far", far="boo"}`,
		`up{foo="bar"} * down * left`:                                              `up{foo="bar"} * down{foo="bar"} * left{foo="bar"}`,
		`up * down{foo="bar"} * left`:                                              `up{foo="bar"} * down{foo="bar"} * left{foo="bar"}`,
		`up * down * left{foo="bar"}`:                                              `up{foo="bar"} * down{foo="bar"} * left{foo="bar"}`,
		`vector(3) / up{foo!="bar"} + down`:                                        `vector(3) / up{foo!="bar"} + down{foo!="bar"}`,
		`vector(3) / on(foo) up{foo!="bar"} + down`:                                `vector(3) / on(foo) up{foo!="bar"} + down{foo!="bar"}`,
		`vector(3) / on(foo) up{foo!="bar"} + ignoring(foo) down`:                  `vector(3) / on(foo) up{foo!="bar"} + ignoring(foo) down`,
		`left / ignoring(foo) up{foo!="bar"} + on(foo) down`:                       `left / ignoring(foo) up{foo!="bar"} + on(foo) down`,
		`left + ignoring(foo) up{foo!="bar"} / on(foo) down`:                       `left + ignoring(foo) up{foo!="bar"} / on(foo) down{foo!="bar"}`,
		`up and vector(3) / down{foo=~"bar"}`:                                      `up{foo=~"bar"} and vector(3) / down{foo=~"bar"}`,
		`up and vector(3) / ignoring(foo) down{foo=~"bar"}`:                        `up{foo=~"bar"} and vector(3) / ignoring(foo) down{foo=~"bar"}`,
		`(up and vector(3)) / ignoring(foo) down{foo=~"bar"}`:                      `(up and vector(3)) / ignoring(foo) down{foo=~"bar"}`,
		`up and vector(3) or down{foo=~"bar"}`:                                     `up and vector(3) or down{foo=~"bar"}`,
		`up{foo="bar"} or down`:                                                    `up{foo="bar"} or down`,
		`up{foo="bar"} unless down`:                                                `up{foo="bar"} unless down`,
		`{__name__=~"left_side.*"} == ignoring(env) right_side`:                    `{__name__=~"left_side.*"} == ignoring(env) right_side`,
		`sum(up) / sum(down)`:                                                      `sum(up) / sum(down)`,
		`sum(up{foo!="bar"}) / sum(down)`:                                          `sum(up{foo!="bar"}) / sum(down)`,
		`sum by (foo) (up) / sum by (foo) (down)`:                                  `sum by (foo) (up) / sum by (foo) (down)`,
		`sum by (foo) (up{foo!="bar"}) / sum by (foo) (down)`:                      `sum by (foo) (up{foo!="bar"}) / sum by (foo) (down{foo!="bar"})`,
		`sum by (foo) (up) / sum by (foo) (down{boo="far"})`:                       `sum by (foo) (up) / sum by (foo) (down{boo="far"})`,
		`sum by (foo) (up{foo!="bar", boo="far"}) / sum by (foo) (down)`:           `sum by (foo) (up{foo!="bar", boo="far"}) / sum by (foo) (down{foo!="bar"})`,
		`sum by (foo) (up) / sum(down)`:                                            `sum by (foo) (up) / sum(down)`,
		`sum by (foo) (up{foo!="bar"}) / sum(down)`:                                `sum by (foo) (up{foo!="bar"}) / sum(down)`,
		`sum by (foo) (up{foo!="bar"}) / sum by (foo) (down{boo="far"})`:           `sum by (foo) (up{foo!="bar"}) / sum by (foo) (down{foo!="bar", boo="far"})`,
	}

	optimizer := &PropagateMatchers{}
	ctx := context.Background()

	for input, expected := range testCases {
		t.Run(input, func(t *testing.T) {
			expectedExpr, err := parser.ParseExpr(expected)
			require.NoError(t, err)

			inputExpr, err := parser.ParseExpr(input)
			require.NoError(t, err)
			outputExpr, err := optimizer.Apply(ctx, inputExpr)
			require.NoError(t, err)

			require.Equal(t, expectedExpr.String(), outputExpr.String())
		})
	}
}
