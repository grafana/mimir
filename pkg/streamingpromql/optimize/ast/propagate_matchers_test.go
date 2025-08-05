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
		`up{foo="bar"} * down{baz="fob"}`:    `up{foo="bar", baz="fob"} * down{foo="bar", baz="fob"}`,
		`up{foo="bar"} * down{foo!="bar2"}`:  `up{foo="bar", foo!="bar2"} * down{foo="bar", foo!="bar2"}`,
		`up{foo="bar"} * ignoring(foo) down`: `up{foo="bar"} * ignoring(foo) down`,
		`up{foo="bar", baz="fob", boo="far", faf="bob"} * ignoring(foo, baz) down`: `up{foo="bar", baz="fob", boo="far", faf="bob"} * ignoring(foo, baz) down{boo="far", faf="bob"}`,
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
		`up * on(foo) down{foo="bar", baz="fob"}`:                                  `up{foo="bar"} * on(foo) down{foo="bar", baz="fob"}`,
		`up * on(foo, baz) down{foo="bar", baz="fob", boo="far", faf="bob"}`:       `up{foo="bar", baz="fob"} * on(foo, baz) down{foo="bar", baz="fob", boo="far", faf="bob"}`,
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
		`up{foo="bar"} unless down`:                                                `up{foo="bar"} unless down{foo="bar"}`,
		`up unless down{foo="bar"}`:                                                `up unless down{foo="bar"}`,
		`up{foo="bar"} unless down{baz="fob"}`:                                     `up{foo="bar"} unless down{foo="bar", baz="fob"}`,
		`{__name__=~"left_side.*"} == ignoring(env) right_side`:                    `{__name__=~"left_side.*"} == ignoring(env) right_side`,
		`abs(up{foo="bar"}) + sin(down) - left{baz="fob"}`:                         `abs(up{foo="bar", baz="fob"}) + sin(down{foo="bar", baz="fob"}) - left{foo="bar", baz="fob"}`,
		`up{foo="bar"} / round(down{baz="fob"}, 1)`:                                `up{foo="bar", baz="fob"} / round(down{foo="bar", baz="fob"}, 1)`,
		`rate(up{foo="bar"}[5m]) + delta(down{baz="fob"}[10m])`:                    `rate(up{foo="bar", baz="fob"}[5m]) + delta(down{foo="bar", baz="fob"}[10m])`,
		`up * -down{foo="bar"}`:                                                    `up{foo="bar"} * -down{foo="bar"}`,

		// Aggregations
		`sum(up) / sum(down)`:                                                                                 `sum(up) / sum(down)`,
		`sum(up{foo!="bar"}) / sum(down)`:                                                                     `sum(up{foo!="bar"}) / sum(down)`,
		`sum by (foo) (up) / sum by (foo) (down)`:                                                             `sum by (foo) (up) / sum by (foo) (down)`,
		`sum by (foo) (up{foo!="bar"}) / sum by (foo) (down)`:                                                 `sum by (foo) (up{foo!="bar"}) / sum by (foo) (down{foo!="bar"})`,
		`sum by (foo) (up) / sum by (foo) (down{boo="far"})`:                                                  `sum by (foo) (up) / sum by (foo) (down{boo="far"})`,
		`sum by (foo) (up{foo!="bar", boo="far"}) / sum by (foo) (down)`:                                      `sum by (foo) (up{foo!="bar", boo="far"}) / sum by (foo) (down{foo!="bar"})`,
		`sum by (foo) (up) / sum(down)`:                                                                       `sum by (foo) (up) / sum(down)`,
		`sum by (foo) (up{foo!="bar"}) / sum(down)`:                                                           `sum by (foo) (up{foo!="bar"}) / sum(down)`,
		`sum by (foo) (up{foo!="bar"}) / sum by (foo) (down{boo="far"})`:                                      `sum by (foo) (up{foo!="bar"}) / sum by (foo) (down{foo!="bar", boo="far"})`,
		`sum by (foo) (up{foo!="bar"}) / sum by (boo) (down{boo="far"})`:                                      `sum by (foo) (up{foo!="bar"}) / sum by (boo) (down{boo="far"})`,
		`sum by (foo, boo) (up{foo!="bar"}) / sum by (foo, boo) (down{boo="far"})`:                            `sum by (foo, boo) (up{foo!="bar", boo="far"}) / sum by (foo, boo) (down{foo!="bar", boo="far"})`,
		`sum by (foo, boo) (up{foo!="bar"}) / sum by (foo, soo) (down{boo="far"})`:                            `sum by (foo, boo) (up{foo!="bar"}) / sum by (foo, soo) (down{foo!="bar", boo="far"})`,
		`sum by (foo, boo) (up{foo!="bar"}) / sum by (boo, soo) (down{boo="far"})`:                            `sum by (foo, boo) (up{foo!="bar", boo="far"}) / sum by (boo, soo) (down{boo="far"})`,
		`sum by (boo) (up) / sum without (foo) (down{foo!="bar", boo="far"})`:                                 `sum by (boo) (up{boo="far"}) / sum without (foo) (down{foo!="bar", boo="far"})`,
		`sum without (foo) (up) / sum without (foo) (down{foo!="bar", boo="far"})`:                            `sum without (foo) (up{boo="far"}) / sum without (foo) (down{foo!="bar", boo="far"})`,
		`sum without (foo) (up{foo="bar", soo="sar"}) / sum without (foo) (down{foo!="bar", boo="far"})`:      `sum without (foo) (up{foo="bar", soo="sar", boo="far"}) / sum without (foo) (down{foo!="bar", boo="far", soo="sar"})`,
		`sum without (soo) (up{foo="bar", soo="sar"}) / sum without (foo) (down{foo!="bar", boo="far"})`:      `sum without (soo) (up{foo="bar", soo="sar", boo="far"}) / sum without (foo) (down{foo!="bar", boo="far"})`,
		`sum without (soo, boo) (up{foo="bar", soo="sar"}) / sum without (foo) (down{foo!="bar", boo="far"})`: `sum without (soo, boo) (up{foo="bar", soo="sar"}) / sum without (foo) (down{foo!="bar", boo="far"})`,
		`up * on(foo) sum(down{foo="bar", baz="fob"})`:                                                        `up * on(foo) sum(down{foo="bar", baz="fob"})`,
		`up * on(foo) sum by (foo) (down{foo="bar", baz="fob"})`:                                              `up{foo="bar"} * on(foo) sum by (foo) (down{foo="bar", baz="fob"})`,
		`up * on(foo) sum by (baz) (down{foo="bar", baz="fob"})`:                                              `up * on(foo) sum by (baz) (down{foo="bar", baz="fob"})`,
		`up * ignoring(foo) sum by (baz) (down{foo="bar", baz="fob"})`:                                        `up{baz="fob"} * ignoring(foo) sum by (baz) (down{foo="bar", baz="fob"})`,
		`up * ignoring(foo) sum without (baz) (down{foo="bar", baz="fob"})`:                                   `up * ignoring(foo) sum without (baz) (down{foo="bar", baz="fob"})`,
		`up{soo="sar"} * ignoring(foo) sum without (baz) (down{foo="bar", baz="fob"})`:                        `up{soo="sar"} * ignoring(foo) sum without (baz) (down{foo="bar", baz="fob", soo="sar"})`,
		`topk by (env) (5, foo{env="prod"}) / bottomk by (region) (5, bar{region="eu"})`:                      `topk by (env) (5, foo{env="prod"}) / bottomk by (region) (5, bar{region="eu"})`,
		`sum by (env) (abs(foo{env="prod"})) / sum by (env) (sin(bar))`:                                       `sum by (env) (abs(foo{env="prod"})) / sum by (env) (sin(bar{env="prod"}))`,
		`max by (baz) (avg by (foo, baz) (left)) / right{foo="bar"}`:                                          `max by (baz) (avg by (foo, baz) (left)) / right{foo="bar"}`,
		`max by (foo) (avg by (foo, baz) (left{foo="bar"})) / right`:                                          `max by (foo) (avg by (foo, baz) (left{foo="bar"})) / right{foo="bar"}`,
		`max by (foo) (avg by (foo, baz) (left)) / right{foo="bar"}`:                                          `max by (foo) (avg by (foo, baz) (left{foo="bar"})) / right{foo="bar"}`,
		`max by (foo, baz) (avg by (foo) (left)) / right{foo="bar"}`:                                          `max by (foo, baz) (avg by (foo) (left{foo="bar"})) / right{foo="bar"}`,
		`max without (foo, baz) (avg without (foo) (left)) / right{foo="bar", baz="fob", soo="sar"}`:          `max without (foo, baz) (avg without (foo) (left{soo="sar"})) / right{foo="bar", baz="fob", soo="sar"}`,
		`max by (foo, baz) (avg without (foo) (left)) / right{foo="bar", baz="fob"}`:                          `max by (foo, baz) (avg without (foo) (left{baz="fob"})) / right{foo="bar", baz="fob"}`,
		`max without (baz) (avg by (foo, baz) (left)) / right{foo="bar", baz="fob"}`:                          `max without (baz) (avg by (foo, baz) (left{foo="bar"})) / right{foo="bar", baz="fob"}`,
	}

	optimizer := NewPropagateMatchers()
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
