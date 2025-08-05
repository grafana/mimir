// SPDX-License-Identifier: AGPL-3.0-only

package testdata

var TestCasesReorderHistogramAggregation = map[string]string{
	`histogram_sum(sum(foo))`:                   `sum(histogram_sum(foo))`,
	`sum(histogram_sum(foo))`:                   `sum(histogram_sum(foo))`,
	`histogram_sum(avg(foo))`:                   `avg(histogram_sum(foo))`,
	`avg(histogram_sum(foo))`:                   `avg(histogram_sum(foo))`,
	`histogram_sum(sum(rate(foo[2m])))`:         `sum(histogram_sum(rate(foo[2m])))`,
	`sum(histogram_sum(rate(foo[2m])))`:         `sum(histogram_sum(rate(foo[2m])))`,
	`histogram_sum(avg(rate(foo[2m])))`:         `avg(histogram_sum(rate(foo[2m])))`,
	`avg(histogram_sum(rate(foo[2m])))`:         `avg(histogram_sum(rate(foo[2m])))`,
	`histogram_count(sum(foo))`:                 `sum(histogram_count(foo))`,
	`sum(histogram_count(foo))`:                 `sum(histogram_count(foo))`,
	`histogram_count(avg(foo))`:                 `avg(histogram_count(foo))`,
	`avg(histogram_count(foo))`:                 `avg(histogram_count(foo))`,
	`histogram_count(sum(rate(foo[2m])))`:       `sum(histogram_count(rate(foo[2m])))`,
	`sum(histogram_count(rate(foo[2m])))`:       `sum(histogram_count(rate(foo[2m])))`,
	`histogram_count(avg(rate(foo[2m])))`:       `avg(histogram_count(rate(foo[2m])))`,
	`avg(histogram_count(rate(foo[2m])))`:       `avg(histogram_count(rate(foo[2m])))`,
	`(((histogram_sum(sum(foo)))))`:             `(((sum(histogram_sum(foo)))))`,
	`histogram_sum(sum(foo+bar))`:               `sum(histogram_sum(foo+bar))`,
	`histogram_sum(sum(foo)+sum(bar))`:          `histogram_sum(sum(foo)+sum(bar))`,
	"histogram_sum(sum by (job) (foo))":         "sum by (job) (histogram_sum(foo))",
	"histogram_sum(sum without (job) (foo))":    "sum without (job) (histogram_sum(foo))",
	"histogram_sum(rate(foo[2m]))":              "histogram_sum(rate(foo[2m]))",
	`3 + (((histogram_sum(sum(foo)))))`:         `3 + (((sum(histogram_sum(foo)))))`,
	`vector(3) + (((histogram_sum(sum(foo)))))`: `vector(3) + (((sum(histogram_sum(foo)))))`,
}
