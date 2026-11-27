// SPDX-License-Identifier: AGPL-3.0-only

package ruler

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/rulefmt"
	"github.com/stretchr/testify/require"
	"go.yaml.in/yaml/v3"

	"github.com/grafana/mimir/pkg/ruler/rulespb"
)

const (
	listRulesBenchNamespaces    = 50
	listRulesBenchGroupsPerNS   = 40
	listRulesBenchRulesPerGroup = 20
)

// listRulesBenchFixture builds one tenant with 50 namespaces, 2,000 groups and 40,000 rules.
// Even rules are recording rules.
// Odd rules are alerting rules.
func listRulesBenchFixture() rulespb.RuleGroupList {
	// %[1]d is the tenant-wide rule index.
	// %[2]d is the rule index modulo 7.
	templates := []struct {
		alert, record, expr string
	}{
		{
			alert:  "HighErrorRate",
			record: "job_instance:http_requests_errors:ratio_rate5m",
			expr:   `sum by (job, instance) (rate(http_requests_total{job="api-%[1]d", status=~"5.."}[5m])) / sum by (job, instance) (rate(http_requests_total{job="api-%[1]d"}[5m]))`,
		},
		{
			alert:  "HighRequestLatency",
			record: "job_route:http_request_duration_seconds:p99_rate5m",
			expr:   `histogram_quantile(0.99, sum by (le, job, route) (rate(http_request_duration_seconds_bucket{job="svc-%[1]d", cluster="prod-eu-west-%[2]d", route!~"/health.*|/ready"}[5m])))`,
		},
		{
			alert:  "NodeMemoryPressure",
			record: "cluster_instance:node_memory_utilisation:ratio",
			expr:   `(node_memory_MemTotal_bytes{cluster="prod-%[1]d", job="node-exporter"} - node_memory_MemAvailable_bytes{cluster="prod-%[1]d", job="node-exporter"}) / node_memory_MemTotal_bytes{cluster="prod-%[1]d", job="node-exporter"}`,
		},
		{
			alert:  "PodRestartingOften",
			record: "namespace_pod_owner:kube_pod_container_restarts:increase15m",
			expr:   `sum by (namespace, pod) (increase(kube_pod_container_status_restarts_total{namespace="team-%[1]d", container!="POD"}[15m])) * on (namespace, pod) group_left (owner_name) kube_pod_owner{namespace="team-%[1]d"}`,
		},
		{
			alert:  "WorkerCPUSaturation",
			record: "job:process_cpu_seconds:avg_rate5m_30m",
			expr:   `avg_over_time(sum without (instance) (rate(process_cpu_seconds_total{job="worker-%[1]d", env="production"}[5m]))[30m:1m]) / count without (instance) (up{job="worker-%[1]d", env="production"} == 1)`,
		},
	}

	intervals := []model.Duration{model.Duration(30 * time.Second), model.Duration(time.Minute), model.Duration(2 * time.Minute), model.Duration(5 * time.Minute)}
	forDurations := []model.Duration{model.Duration(time.Minute), model.Duration(5 * time.Minute), model.Duration(10 * time.Minute), model.Duration(15 * time.Minute)}

	list := make(rulespb.RuleGroupList, 0, listRulesBenchNamespaces*listRulesBenchGroupsPerNS)
	i := 0
	for ns := 0; ns < listRulesBenchNamespaces; ns++ {
		namespace := fmt.Sprintf("namespace-%02d", ns)
		for g := 0; g < listRulesBenchGroupsPerNS; g++ {
			rg := rulefmt.RuleGroup{
				Name:     fmt.Sprintf("group-%02d-%02d", ns, g),
				Interval: intervals[g%len(intervals)],
				Rules:    make([]rulefmt.Rule, 0, listRulesBenchRulesPerGroup),
			}
			for r := 0; r < listRulesBenchRulesPerGroup; r++ {
				tmpl := templates[i%len(templates)]
				expr := fmt.Sprintf(tmpl.expr, i, i%7)
				if i%2 == 0 {
					rg.Rules = append(rg.Rules, rulefmt.Rule{
						Record: fmt.Sprintf("%s_%d", tmpl.record, i),
						Expr:   expr,
					})
				} else {
					forDuration := forDurations[i%len(forDurations)]
					rule := rulefmt.Rule{
						Alert: fmt.Sprintf("%s%d", tmpl.alert, i),
						Expr:  fmt.Sprintf("%s > %d.%02d", expr, i%3, i%100),
						For:   forDuration,
						Labels: map[string]string{
							"severity": []string{"warning", "critical"}[i%4/2],
							"team":     fmt.Sprintf("team-%d", i%40),
						},
						Annotations: map[string]string{
							"summary": fmt.Sprintf("%s on {{ $labels.job }} is above threshold", tmpl.alert),
							"description": fmt.Sprintf("The value for {{ $labels.job }} on {{ $labels.instance }} is {{ $value | humanize }}.\n"+
								"This is above the threshold of %d.%02d for more than %s.\n"+
								"Check the service dashboard and recent deploys for team-%d.", i%3, i%100, forDuration, i%40),
						},
					}
					if i%4 == 1 {
						rule.Labels["service"] = fmt.Sprintf("service-%d", i%200)
						rule.Annotations["runbook_url"] = fmt.Sprintf("https://runbooks.example.com/alerts/%s#%d", tmpl.alert, i%50)
					}
					rg.Rules = append(rg.Rules, rule)
				}
				i++
			}
			list = append(list, rulespb.ToProto("user-1", namespace, rg))
		}
	}
	return list
}

func BenchmarkListRulesEncode(b *testing.B) {
	rgs := listRulesBenchFixture()
	logger := log.NewNopLogger()

	var sizeOutput interface{} = rgs.Formatted()
	yamlBytes, err := yaml.Marshal(&sizeOutput)
	require.NoError(b, err)
	yamlSize := float64(len(yamlBytes))

	b.Run("formatted", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			_ = rgs.Formatted()
		}
	})

	b.Run("yaml_marshal", func(b *testing.B) {
		b.ReportAllocs()
		var n int
		for b.Loop() {
			var output interface{} = rgs.Formatted()
			d, err := yaml.Marshal(&output)
			if err != nil {
				b.Fatal(err)
			}
			n = len(d)
		}
		b.ReportMetric(float64(n), "out-bytes")
	})

	b.Run("marshalAndSend_recorder", func(b *testing.B) {
		b.ReportAllocs()
		var n int
		for b.Loop() {
			rec := httptest.NewRecorder()
			marshalAndSend(rgs.Formatted(), rec, logger)
			n = rec.Body.Len()
		}
		b.ReportMetric(float64(n), "out-bytes")
	})

	// Concurrency 0 is the default path with one yaml.Marshal call.
	for _, concurrency := range []int{0, 2, 4, 8} {
		b.Run(fmt.Sprintf("encode/concurrency=%d", concurrency), func(b *testing.B) {
			b.ReportAllocs()
			var n int
			for b.Loop() {
				rec := httptest.NewRecorder()
				marshalNamespacesAndSend(rgs.Formatted(), concurrency, rec, logger)
				n = rec.Body.Len()
			}
			b.ReportMetric(float64(n), "out-bytes")
		})
	}

	b.Run("encoder_discard", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			enc := yaml.NewEncoder(io.Discard)
			if err := enc.Encode(rgs.Formatted()); err != nil {
				b.Fatal(err)
			}
			if err := enc.Close(); err != nil {
				b.Fatal(err)
			}
		}
		b.ReportMetric(yamlSize, "out-bytes")
	})

	b.Run("encoder_recorder", func(b *testing.B) {
		b.ReportAllocs()
		var n int
		for b.Loop() {
			rec := httptest.NewRecorder()
			enc := yaml.NewEncoder(rec)
			if err := enc.Encode(rgs.Formatted()); err != nil {
				b.Fatal(err)
			}
			if err := enc.Close(); err != nil {
				b.Fatal(err)
			}
			n = rec.Body.Len()
		}
		b.ReportMetric(float64(n), "out-bytes")
	})

	// Reference point for sec/op and allocs/op only.
	// rulefmt has no json tags, so this output is not API compatible.
	b.Run("json_marshal", func(b *testing.B) {
		b.ReportAllocs()
		var n int
		for b.Loop() {
			d, err := json.Marshal(rgs.Formatted())
			if err != nil {
				b.Fatal(err)
			}
			n = len(d)
		}
		b.ReportMetric(float64(n), "out-bytes")
	})
}

// listRulesShapeFixture builds groups rule groups in namespaces namespaces, with rulesPerGroup rules in each group.
// If largestShare is more than 0, the first namespace gets that share of the groups.
// The other namespaces share the rest.
func listRulesShapeFixture(groups, namespaces, rulesPerGroup int, largestShare float64) map[string][]rulefmt.RuleGroup {
	first := groups / namespaces
	if largestShare > 0 {
		first = int(float64(groups) * largestShare)
	}
	rest := groups - first
	out := make(map[string][]rulefmt.RuleGroup, namespaces)
	r := 0
	for n := 0; n < namespaces; n++ {
		count := first
		if n > 0 {
			count = rest / (namespaces - 1)
			if n <= rest%(namespaces-1) {
				count++
			}
		}
		gs := make([]rulefmt.RuleGroup, 0, count)
		for g := 0; g < count; g++ {
			rg := rulefmt.RuleGroup{Name: fmt.Sprintf("group-%d", g), Interval: model.Duration(time.Minute)}
			for j := 0; j < rulesPerGroup; j++ {
				r++
				if r%2 == 0 {
					rg.Rules = append(rg.Rules, rulefmt.Rule{
						Record: fmt.Sprintf("job_instance:http_requests_errors:ratio_rate5m_%d", r),
						Expr:   fmt.Sprintf(`sum by (job, instance) (rate(http_requests_total{job="api-%d", status=~"5.."}[5m])) / sum by (job, instance) (rate(http_requests_total{job="api-%d"}[5m]))`, r, r),
					})
					continue
				}
				rg.Rules = append(rg.Rules, rulefmt.Rule{
					Alert:       fmt.Sprintf("HighErrorRate%d", r),
					Expr:        fmt.Sprintf(`job_instance:http_requests_errors:ratio_rate5m_%d > 0.05`, r-1),
					For:         model.Duration(5 * time.Minute),
					Labels:      map[string]string{"severity": "warning", "team": fmt.Sprintf("team-%d", r%40)},
					Annotations: map[string]string{"summary": "{{ $labels.job }} on {{ $labels.instance }} is above threshold"},
				})
			}
			gs = append(gs, rg)
		}
		out[fmt.Sprintf("namespace-%05d", n)] = gs
	}
	return out
}

// discardResponseWriter counts the body bytes and keeps none of them.
type discardResponseWriter struct {
	header http.Header
	n      int
}

func (w *discardResponseWriter) Header() http.Header {
	if w.header == nil {
		w.header = http.Header{}
	}
	return w.header
}

func (w *discardResponseWriter) Write(b []byte) (int, error) {
	w.n += len(b)
	return len(b), nil
}

func (w *discardResponseWriter) WriteHeader(int) {}

// BenchmarkListRulesEncodeShapes encodes synthetic tenants of different sizes with each value of concurrency.
// One iteration of the largest shape takes seconds.
// Run it with -benchtime=1x and a -count of 6 or more.
// The cpu-ns/op metric is the CPU time of the process.
// More goroutines can use more CPU in total.
func BenchmarkListRulesEncodeShapes(b *testing.B) {
	large := []int{0, 2, 4, 8, 16}
	for _, shape := range []struct {
		name                              string
		groups, namespaces, rulesPerGroup int
		largestShare                      float64
		concurrency                       []int
	}{
		{name: "g100k_ns10k", groups: 100_000, namespaces: 10_000, rulesPerGroup: 20, concurrency: large},
		{name: "g75k_ns2k", groups: 75_000, namespaces: 2_000, rulesPerGroup: 20, concurrency: large},
		{name: "g50k_ns1k", groups: 50_000, namespaces: 1_000, rulesPerGroup: 20, concurrency: large},
		{name: "g10k_ns50", groups: 10_000, namespaces: 50, rulesPerGroup: 20, concurrency: large},
		{name: "g10k_ns50_skew", groups: 10_000, namespaces: 50, rulesPerGroup: 20, largestShare: 0.4, concurrency: large},
		{name: "g200_ns20", groups: 200, namespaces: 20, rulesPerGroup: 20, concurrency: []int{0, 2, 4}},
	} {
		b.Run("shape="+shape.name, func(b *testing.B) {
			formatted := listRulesShapeFixture(shape.groups, shape.namespaces, shape.rulesPerGroup, shape.largestShare)
			for _, concurrency := range shape.concurrency {
				b.Run(fmt.Sprintf("concurrency=%d", concurrency), func(b *testing.B) {
					b.ReportAllocs()
					start := processCPUTime()
					for b.Loop() {
						marshalNamespacesAndSend(formatted, concurrency, &discardResponseWriter{}, log.NewNopLogger())
					}
					if cpu := processCPUTime() - start; cpu > 0 {
						b.ReportMetric(float64(cpu.Nanoseconds())/float64(b.N), "cpu-ns/op")
					}
				})
			}
		})
	}
}
