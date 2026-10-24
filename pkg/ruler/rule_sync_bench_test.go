// SPDX-License-Identifier: AGPL-3.0-only

package ruler

import (
	"errors"
	"fmt"
	"runtime"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/rulefmt"
	promRules "github.com/prometheus/prometheus/rules"

	"github.com/grafana/mimir/pkg/ruler/rulespb"
)

const ruleSyncUser = "user-1"

// ruleSync does one rule sync for one tenant with one RuleFileStore.
type ruleSync struct {
	store   RuleFileStore
	manager *promRules.Manager
}

func newRuleSync(registryEnabled bool) ruleSync {
	store := NewRuleFileStore(Config{RulePath: "/rules", RuleRegistryEnabled: registryEnabled}, log.NewNopLogger())
	return ruleSync{
		store:   store,
		manager: promRules.NewManager(&promRules.ManagerOptions{GroupLoader: store.groupLoader(ruleSyncUser)}),
	}
}

// sync does the part of DefaultMultiTenantManager.syncRulesToManager up to LoadGroups, for a tenant that has a manager.
// It calls LoadGroups only when the rules changed.
func (s ruleSync) sync(groups rulespb.RuleGroupList) (bool, map[string]*promRules.Group, error) {
	update, files, err := s.store.mapRules(ruleSyncUser, groups)
	if err != nil || !update {
		return update, nil, err
	}
	loaded, errs := s.manager.LoadGroups(time.Minute, labels.EmptyLabels(), "", nil, false, files...)
	return update, loaded, errors.Join(errs...)
}

// BenchmarkRuleSync measures one rule sync of one tenant, from the rule group protos to LoadGroups.
// Each sync gets protos that the store does not hold.
// The protos come from Unmarshal, as in the ruler.
// So their label strings point into a read buffer.
// The unchanged scenario alternates between two lists with the same content.
// The one-namespace-changed scenario alternates between two versions of namespace-00.
func BenchmarkRuleSync(b *testing.B) {
	sizes := []struct {
		name                      string
		namespaces, groups, rules int
	}{
		{name: "S", namespaces: 50, groups: 1000, rules: 10000},
		{name: "L", namespaces: 50, groups: 4450, rules: 29000},
	}

	for _, size := range sizes {
		build := func() rulespb.RuleGroupList {
			return unmarshalEach(b, ruleSyncFixture(size.namespaces, size.groups, size.rules))
		}
		changed := build()
		// The first group of the list is the first group of namespace-00.
		changed[0].Rules[0].Expr += " * 2"

		scenarios := []struct {
			name       string
			versions   [2]rulespb.RuleGroupList
			wantUpdate bool
		}{
			{name: "unchanged", versions: [2]rulespb.RuleGroupList{build(), build()}, wantUpdate: false},
			{name: "one-namespace-changed", versions: [2]rulespb.RuleGroupList{build(), changed}, wantUpdate: true},
		}

		for _, sc := range scenarios {
			for _, registryEnabled := range []bool{false, true} {
				b.Run(fmt.Sprintf("size=%s/scenario=%s/registry_enabled=%v", size.name, sc.name, registryEnabled), func(b *testing.B) {
					before := heapAllocAfterGC()

					s := newRuleSync(registryEnabled)
					// Only the part of the input that s keeps counts in the retained heap.
					update, loaded, err := s.sync(build())
					if err != nil {
						b.Fatal(err)
					}
					if !update || len(loaded) != size.groups {
						b.Fatalf("first sync: got update %v and %d groups, want true and %d groups", update, len(loaded), size.groups)
					}
					withGroups := heapAllocAfterGC()
					runtime.KeepAlive(loaded)
					withoutGroups := heapAllocAfterGC()

					if sc.wantUpdate {
						if update, _, err := s.sync(sc.versions[1]); err != nil || !update {
							b.Fatalf("second sync: got update %v and error %v, want true and no error", update, err)
						}
					}

					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						update, _, err := s.sync(sc.versions[i%2])
						if err != nil {
							b.Fatal(err)
						}
						if update != sc.wantUpdate {
							b.Fatalf("got update %v, want %v", update, sc.wantUpdate)
						}
					}
					b.StopTimer()

					// The ResetTimer call deletes the metrics reported before it.
					// The retained heap does not depend on the scenario.
					// So report it once.
					if sc.name == "unchanged" {
						b.ReportMetric(float64(withoutGroups)-float64(before), "retained-B")
						b.ReportMetric(float64(withGroups)-float64(before), "retained-with-groups-B")
					}
					runtime.KeepAlive(s)
				})
			}
		}
	}
}

// unmarshalEach replaces each group with a copy that Unmarshal reads from its own buffer.
// The ruler also reads each group from its own object.
func unmarshalEach(b *testing.B, list rulespb.RuleGroupList) rulespb.RuleGroupList {
	for i, g := range list {
		buf, err := g.Marshal()
		if err != nil {
			b.Fatal(err)
		}
		list[i] = &rulespb.RuleGroupDesc{}
		if err := list[i].Unmarshal(buf); err != nil {
			b.Fatal(err)
		}
	}
	return list
}

func heapAllocAfterGC() uint64 {
	runtime.GC()
	runtime.GC()
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	return ms.HeapAlloc
}

// ruleSyncFixture builds one tenant with numGroups groups and numRules rules.
// It spreads the groups evenly over the namespaces, and the rules evenly over the groups.
// Even rules are recording rules.
// Odd rules are alerting rules.
func ruleSyncFixture(numNamespaces, numGroups, numRules int) rulespb.RuleGroupList {
	// %[1]d is the rule index.
	// %[2]d is a small per-rule number.
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

	list := make(rulespb.RuleGroupList, 0, numGroups)
	i := 0
	for ns := 0; ns < numNamespaces; ns++ {
		namespace := fmt.Sprintf("namespace-%02d", ns)
		groupsInNS := (ns+1)*numGroups/numNamespaces - ns*numGroups/numNamespaces
		for g := 0; g < groupsInNS; g++ {
			j := len(list)
			rulesInGroup := (j+1)*numRules/numGroups - j*numRules/numGroups
			rg := rulefmt.RuleGroup{
				Name:     fmt.Sprintf("group-%02d-%02d", ns, g),
				Interval: intervals[g%len(intervals)],
				Rules:    make([]rulefmt.Rule, 0, rulesInGroup),
			}
			for r := 0; r < rulesInGroup; r++ {
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
			list = append(list, rulespb.ToProto(ruleSyncUser, namespace, rg))
		}
	}
	return list
}
