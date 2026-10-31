// SPDX-License-Identifier: AGPL-3.0-only

package analyze

import (
	"testing"

	"github.com/go-kit/log"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/mimirtool/minisdk"
	"github.com/grafana/mimir/pkg/mimirtool/util"
)

func TestMetricsFromTemplating(t *testing.T) {
	t.Run("multiline query_result", func(t *testing.T) {
		metrics := make(map[string]struct{})
		in := minisdk.Templating{
			List: []minisdk.TemplateVar{
				{
					Name:       "variable",
					Type:       "query",
					Datasource: nil,
					Query: `query_result(
  quantile_over_time(0.95, foo{lbl="$var"}[$__range])
)`,
				},
			},
		}

		errs := metricsFromTemplating(in, metrics, util.CreatePromQLParser(false), log.NewNopLogger())
		require.Empty(t, errs)
		require.Len(t, metrics, 1)
		require.Equal(t, map[string]struct{}{"foo": {}}, metrics)
	})

	t.Run("query is a metric containing 'query_result'", func(t *testing.T) {
		metrics := make(map[string]struct{})
		in := minisdk.Templating{
			List: []minisdk.TemplateVar{
				{
					Name:       "variable",
					Type:       "query",
					Datasource: nil,
					Query:      `foo_bar_query_result_total{}`,
				},
			},
		}

		errs := metricsFromTemplating(in, metrics, util.CreatePromQLParser(false), log.NewNopLogger())
		require.Empty(t, errs)
		require.Len(t, metrics, 1)
		require.Equal(t, map[string]struct{}{"foo_bar_query_result_total": {}}, metrics)
	})

	t.Run("query is a metric containing 'label_values'", func(t *testing.T) {
		metrics := make(map[string]struct{})
		in := minisdk.Templating{
			List: []minisdk.TemplateVar{
				{
					Name:       "variable",
					Type:       "query",
					Datasource: nil,
					Query:      `foo_bar_label_values_total{}`,
				},
			},
		}

		errs := metricsFromTemplating(in, metrics, util.CreatePromQLParser(false), log.NewNopLogger())
		require.Empty(t, errs)
		require.Len(t, metrics, 1)
		require.Equal(t, map[string]struct{}{"foo_bar_label_values_total": {}}, metrics)
	})

	t.Run(`label_values query with invalid metric name in __name__`, func(t *testing.T) {
		metrics := make(map[string]struct{})
		in := minisdk.Templating{
			List: []minisdk.TemplateVar{
				{
					Name:       "variable",
					Type:       "query",
					Datasource: nil,
					Query:      `label_values({__name__=~"$prefix\\\\_?last_updated_time_seconds", job=~"$job", instance=~"$instance"}, entity)`,
				},
			},
		}

		errs := metricsFromTemplating(in, metrics, util.CreatePromQLParser(false), log.NewNopLogger())
		require.Empty(t, errs)
		require.Empty(t, metrics)
	})

	t.Run(`label_values query with metric name in  __name__ label`, func(t *testing.T) {
		metrics := make(map[string]struct{})
		in := minisdk.Templating{
			List: []minisdk.TemplateVar{
				{
					Name:       "variable",
					Type:       "query",
					Datasource: nil,
					Query:      `label_values({__name__=~"metric", job=~"$job", instance=~"$instance"}, entity)`,
				},
			},
		}

		errs := metricsFromTemplating(in, metrics, util.CreatePromQLParser(false), log.NewNopLogger())
		require.Empty(t, errs)
		require.Len(t, metrics, 1)
		require.Equal(t, map[string]struct{}{"metric": {}}, metrics)
	})

	t.Run(`label_values query with multiline metric`, func(t *testing.T) {
		metrics := make(map[string]struct{})
		in := minisdk.Templating{
			List: []minisdk.TemplateVar{
				{
					Name:       "variable",
					Type:       "query",
					Datasource: nil,
					Query: `label_values(
	metric,
	label
)`,
				},
			},
		}

		errs := metricsFromTemplating(in, metrics, util.CreatePromQLParser(false), log.NewNopLogger())
		require.Empty(t, errs)
		require.Len(t, metrics, 1)
		require.Equal(t, map[string]struct{}{"metric": {}}, metrics)
	})

	t.Run(`label_values query with no metric selector single line`, func(t *testing.T) {
		metrics := make(map[string]struct{})
		in := minisdk.Templating{
			List: []minisdk.TemplateVar{
				{
					Name:       "variable",
					Type:       "query",
					Datasource: nil,
					Query:      `label_values(entity)`,
				},
			},
		}

		errs := metricsFromTemplating(in, metrics, util.CreatePromQLParser(false), log.NewNopLogger())
		require.Empty(t, errs)
		require.Empty(t, metrics)
	})

	t.Run(`label_values query with no metric selector multiline`, func(t *testing.T) {
		metrics := make(map[string]struct{})
		in := minisdk.Templating{
			List: []minisdk.TemplateVar{
				{
					Name:       "variable",
					Type:       "query",
					Datasource: nil,
					Query: `label_values(
	entity
)`,
				},
			},
		}

		errs := metricsFromTemplating(in, metrics, util.CreatePromQLParser(false), log.NewNopLogger())
		require.Empty(t, errs)
		require.Empty(t, metrics)
	})

	t.Run(`query contains a subquery with the $__interval variable`, func(t *testing.T) {
		metrics := make(map[string]struct{})
		in := minisdk.Templating{
			List: []minisdk.TemplateVar{
				{
					Name:       "variable",
					Type:       "query",
					Datasource: nil,
					Query:      `increase(varnish_main_threads_failed{job=~"$job",instance=~"$instance"}[$__interval:])`,
				},
			},
		}

		errs := metricsFromTemplating(in, metrics, util.CreatePromQLParser(false), log.NewNopLogger())
		require.Empty(t, errs)
		require.Len(t, metrics, 1)
		require.Equal(t, map[string]struct{}{"varnish_main_threads_failed": {}}, metrics)
	})

	t.Run(`query uses offset with $__interval variable`, func(t *testing.T) {
		metrics := make(map[string]struct{})
		in := minisdk.Templating{
			List: []minisdk.TemplateVar{
				{
					Name:       "variable",
					Type:       "query",
					Datasource: nil,
					Query:      `increase(tomcat_session_processingtime_total{job=~"$job", instance=~"$instance", host=~"$host", context=~"$context"}[$__interval:] offset -$__interval)`,
				},
			},
		}

		errs := metricsFromTemplating(in, metrics, util.CreatePromQLParser(false), log.NewNopLogger())
		require.Empty(t, errs)
		require.Len(t, metrics, 1)
		require.Equal(t, map[string]struct{}{"tomcat_session_processingtime_total": {}}, metrics)
	})

	t.Run(`query uses $latency_metrics variable`, func(t *testing.T) {
		metrics := make(map[string]struct{})
		in := minisdk.Templating{
			List: []minisdk.TemplateVar{
				{
					Name:       "variable",
					Type:       "query",
					Datasource: nil,
					Query:      `request_duration_second{} and on() (vector($latency_metrics) == -1)`,
				},
			},
		}

		errs := metricsFromTemplating(in, metrics, util.CreatePromQLParser(false), log.NewNopLogger())
		require.Empty(t, errs)
		require.Len(t, metrics, 1)
		require.Equal(t, map[string]struct{}{"request_duration_second": {}}, metrics)
	})

	t.Run(`query contains range with other variables`, func(t *testing.T) {
		metrics := make(map[string]struct{})
		in := minisdk.Templating{
			List: []minisdk.TemplateVar{
				{
					Name:       "variable",
					Type:       "query",
					Datasource: nil,
					Query:      `myapp_metric_foo[$__from:$__to]`,
				},
			},
		}

		errs := metricsFromTemplating(in, metrics, util.CreatePromQLParser(false), log.NewNopLogger())
		require.Empty(t, errs)
		require.Len(t, metrics, 1)
		require.Equal(t, map[string]struct{}{"myapp_metric_foo": {}}, metrics)
	})
}

func TestParseQueryWithDashboardVariables(t *testing.T) {
	testCases := map[string]struct {
		query           string
		expectedMetrics map[string]struct{}
		expectedErr     bool
	}{
		"variable in comparison": {
			query:           `request_duration > $target`,
			expectedMetrics: map[string]struct{}{"request_duration": {}},
		},
		"variable in comparison and label matcher": {
			query:           `sum(rabbitmq_queue_messages{tenant=~"$tenant"}) by (tenant, queue) > $threshold`,
			expectedMetrics: map[string]struct{}{"rabbitmq_queue_messages": {}},
		},
		"variable as function argument": {
			query:           `histogram_fraction(0, $le, request_duration)`,
			expectedMetrics: map[string]struct{}{"request_duration": {}},
		},
		"variable in function arguments and label matchers": {
			query:           `topk($limit, sum by (rule_group) (cortex_prometheus_rule_group_rules{cluster=~"$cluster", job=~"($namespace)/((ruler|cortex|mimir))", user="$user"}))`,
			expectedMetrics: map[string]struct{}{"cortex_prometheus_rule_group_rules": {}},
		},
		"braced variable in comparison": {
			query:           `request_duration > ${threshold}`,
			expectedMetrics: map[string]struct{}{"request_duration": {}},
		},
		"label_replace capture reference is not a dashboard variable": {
			query:           `label_replace(request_duration, "dst", "$1", "src", "(.*)")`,
			expectedMetrics: map[string]struct{}{"request_duration": {}},
		},
		"variable embedded in metric name is not replaced": {
			query:       `vault_route_create_${mountpoint}__sum`,
			expectedErr: true,
		},
	}
	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			metrics := map[string]struct{}{}
			err := parseQuery(tc.query, metrics, util.CreatePromQLParser(false))
			if tc.expectedErr {
				require.Error(t, err)
				require.Empty(t, metrics)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.expectedMetrics, metrics)
		})
	}
}

func TestReplaceVariables(t *testing.T) {
	testCases := map[string]struct {
		query    string
		expected string
	}{
		"variable in a comparison is replaced": {
			query:    `request_duration > $target`,
			expected: `request_duration > 0`,
		},
		"braced variable is replaced": {
			query:    `request_duration > ${threshold}`,
			expected: `request_duration > 0`,
		},
		"variable in a scalar argument is replaced": {
			query:    `histogram_fraction(0, $le, metric_one)`,
			expected: `histogram_fraction(0, 0, metric_one)`,
		},
		"multiple occurrences are all replaced": {
			query:    `$left > $right`,
			expected: `0 > 0`,
		},
		"skipped and replaced occurrences in the same query keep the skipped text intact": {
			query:    `vault_route_create_${mountpoint}__sum > $threshold`,
			expected: `vault_route_create_${mountpoint}__sum > 0`,
		},
		"label_replace capture reference is not treated as a variable": {
			query:    `label_replace(metric_one, "dst", "$1", "src", "(.*)")`,
			expected: `label_replace(metric_one, "dst", "$1", "src", "(.*)")`,
		},
		"anchored regex is left untouched": {
			query:    `metric_one{label=~"foo$"}`,
			expected: `metric_one{label=~"foo$"}`,
		},
		"variable glued to an identifier is left untouched": {
			query:    `metric_$name`,
			expected: `metric_$name`,
		},
		// Known limitation: :format modifiers are not matched by the regexp.
		"variable with a format modifier is left untouched": {
			query:    `topk(${limit:raw}, metric_one)`,
			expected: `topk(${limit:raw}, metric_one)`,
		},
	}
	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			require.Equal(t, tc.expected, replaceVariables(tc.query))
		})
	}
}
