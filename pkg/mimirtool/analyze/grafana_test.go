// SPDX-License-Identifier: AGPL-3.0-only

package analyze

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/mimirtool/minisdk"
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

		errs := metricsFromTemplating(in, metrics)
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

		errs := metricsFromTemplating(in, metrics)
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

		errs := metricsFromTemplating(in, metrics)
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

		errs := metricsFromTemplating(in, metrics)
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

		errs := metricsFromTemplating(in, metrics)
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

		errs := metricsFromTemplating(in, metrics)
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

		errs := metricsFromTemplating(in, metrics)
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

		errs := metricsFromTemplating(in, metrics)
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

		errs := metricsFromTemplating(in, metrics)
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

		errs := metricsFromTemplating(in, metrics)
		require.Empty(t, errs)
		require.Len(t, metrics, 1)
		require.Equal(t, map[string]struct{}{"tomcat_session_processingtime_total": {}}, metrics)
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

		errs := metricsFromTemplating(in, metrics)
		require.Empty(t, errs)
		require.Len(t, metrics, 1)
		require.Equal(t, map[string]struct{}{"myapp_metric_foo": {}}, metrics)
	})
}
