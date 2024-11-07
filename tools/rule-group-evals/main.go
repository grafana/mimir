package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/prometheus/rules"
	"golang.org/x/sync/errgroup"
	"gopkg.in/yaml.v3"
)

// The estimated latency overhead to execute 1 strong read consistency query.
const estimatedStrongConsistencyLatencyOverhead = 3 * time.Second

type Result struct {
	Data Data `json:"data"`
}

type Data struct {
	Groups []*Group `json:"groups"`
}

type Group struct {
	Name            string  `json:"name"`
	IntervalSeconds float64 `json:"interval"`
	Rules           []*Rule `json:"rules"`

	// Total duration to evaluate rules in this group.
	rulesTotalEvaluationDuration time.Duration

	// Estimated total duration to evaluate rules in this group, factoring the strong consistency overhead.
	rulesEstimatedTotalEvaluationWithStrongConsistencyDuration time.Duration

	// Number of rules, in this group, that will be executed with strong read consistency.
	rulesStrongConsistencyCount int
}

func (g *Group) EvaluationInterval() time.Duration {
	return time.Duration(g.IntervalSeconds * float64(time.Second))
}

// estimatedDurationPercentage returns the estimated total evaluation duration after strong consistency is enforced
// as a percentage of the evaluation interval. The higher the percentage, the higher the risk of missing evaluations.
func (g *Group) estimatedDurationPercentage() float64 {
	return float64(g.rulesEstimatedTotalEvaluationWithStrongConsistencyDuration) / float64(g.EvaluationInterval()) * 100
}

func main() {
	mode := os.Args[1]
	switch mode {
	case "download":
		downloadRules(os.Args[2])
	case "analyze":
		dir := os.Args[2]
		analyseRuleGroups(readRuleGroups(dir))
	case "analyze-exprs":
		dir := os.Args[2]
		analyzeStaticExpressions(readRuleGroups(dir), dir)
	}
}

func readRuleGroups(dir string) []*Group {
	var groups []*Group

	entries, err := os.ReadDir(dir)
	noErr(err)

	for _, entry := range entries {
		if !strings.HasSuffix(entry.Name(), ".json") {
			fmt.Printf("Ignoring non rule file %s\n", entry.Name())
			continue
		}
		groupsFile, err := os.Open(filepath.Join(dir, entry.Name()))
		noErr(err)

		groupsBytes, err := io.ReadAll(groupsFile)
		noErr(err)

		result := &Result{}
		noErr(json.Unmarshal(groupsBytes, result))

		groups = append(groups, result.Data.Groups...)
	}

	return groups
}

func analyseRuleGroups(groups []*Group) {
	for _, group := range groups {
		analyseRuleGroup(group)
	}

	printAnalysisResultsCSV(groups)
}

func analyseRuleGroup(group *Group) {
	// Cast the rules slice to the Prometheus Rule.
	promRules := make([]rules.Rule, 0, len(group.Rules))
	for _, rule := range group.Rules {
		promRules = append(promRules, rule)
	}

	// Analyse rules dependencies. This function will set the dependency information on the rule itself.
	rules.AnalyseRulesDependencies(promRules)

	// Gather statistics.
	group.rulesStrongConsistencyCount = 0
	group.rulesTotalEvaluationDuration = 0

	for _, rule := range group.Rules {
		group.rulesTotalEvaluationDuration += rule.GetEvaluationDuration()
		if !rule.NoDependencyRules() {
			group.rulesStrongConsistencyCount++
		}
	}

	// Compute the estimated evaluation duration after strong read consistency is enforced.
	group.rulesEstimatedTotalEvaluationWithStrongConsistencyDuration = group.rulesTotalEvaluationDuration +
		(estimatedStrongConsistencyLatencyOverhead * time.Duration(group.rulesStrongConsistencyCount))
}

func printAnalysisResultsCSV(groups []*Group) {
	// Sort groups by estimated duration % desc.
	slices.SortFunc(groups, func(a, b *Group) int {
		return int(b.estimatedDurationPercentage() - a.estimatedDurationPercentage())
	})

	fmt.Println(`"Rule group","Evaluation interval (sec)","Num rules","Num strong consistency rules","Current duration (sec)","Estimated duration with strong read consistency (sec)", "Estimated duration with strong read consistency (%)"`)
	for _, group := range groups {
		// Skip rule groups with no strong consistency rules, since they're not affected.
		if group.rulesStrongConsistencyCount == 0 {
			continue
		}

		fmt.Println(strings.Join([]string{
			group.Name,
			fmt.Sprintf("%.0f", group.EvaluationInterval().Seconds()),
			strconv.Itoa(len(group.Rules)),
			strconv.Itoa(group.rulesStrongConsistencyCount),
			fmt.Sprintf("%.2f", group.rulesTotalEvaluationDuration.Seconds()),
			fmt.Sprintf("%.2f", group.rulesEstimatedTotalEvaluationWithStrongConsistencyDuration.Seconds()),
			fmt.Sprintf("%.0f%%", group.estimatedDurationPercentage()),
		}, ","))
	}

	// Print global stats.
	fmt.Println("")
	fmt.Println(`"Total number of strong consistency rules"`)
	fmt.Println(strings.Join([]string{
		strconv.Itoa(strongConsistencyRulesCount(groups)),
	}, ","))
}

func analyzeStaticExpressions(groups []*Group, outDir string) {

	file := filepath.Join(outDir, "exprs.csv")
	err := os.Remove(file)
	if errors.Is(err, os.ErrNotExist) {
		err = nil
	} else {
		noErr(err)
	}
	//noErr(os.WriteFile(filepath.Join(desitnation, namespace, fmt.Sprintf("%s.json", tenant)), respBody, 0600))
	f, err := os.Create(file)
	noErr(err)
	defer f.Close()

	f.WriteString(`"Rule Group","Record","Expr","HasDependents"`)
	f.WriteString("\n")
	for _, group := range groups {
		// Cast the rules slice to the Prometheus Rule.
		promRules := make([]rules.Rule, 0, len(group.Rules))
		for _, rule := range group.Rules {
			promRules = append(promRules, rule)
		}

		rules.AnalyseRulesDependencies(promRules)

		for _, rule := range promRules {
			query := rule.Query().String()

			// Warn us if we detect any anomalous SLO rule groups

			// Warn if we see meta:objective or meta:window_size_days used in a context that's unexpected (normally they are never used)
			if strings.Contains(query, "sailpoint_slo:meta:objective") {
				fmt.Println("sailpoint_slo:meta:objective was used in group " + group.Name)
			}
			if strings.Contains(query, "sailpoint_slo:meta:window_size_days") {
				fmt.Println("sailpoint_slo:meta:window_size_days was used in group " + group.Name)
			}

			// Warn if we see meta:error_budget being used anywhere outside of burn rate rules
			if strings.Contains(query, "sailpoint_slo:meta:error_budget") {
				if !(rule.Name() == "sailpoint_slo:meta:current_burn_rate" || rule.Name() == "sailpoint_slo:meta:period_burn_rate") {
					fmt.Println("sailpoint_slo:meta:error_budget was used in an atypical query! " + group.Name)
				}
			}

			// Warn if we see any burn rate rules that use error_budget, that don't also combine it with one of the error_ratio series
			if strings.Contains(query, "sailpoint_slo:meta:error_budget") {
				if strings.Contains(query, "sailpoint_slo:sli:error_ratio:") {
					// The rule queried error_budget, but sadly combined it with error_ratio -- nothing can be improved.
				} else {
					fmt.Println("A rule queried error_budget and not error_ratio! " + group.Name)
				}
			}

			// Uncomment to exclude all SLO and SLO-test rules.
			/*if strings.HasPrefix(rule.Name(), "sailpoint_slo:meta:") {
				continue
			}
			if strings.HasPrefix(rule.Name(), "sailpoint_slo:sli:error_ratio:") {
				continue
			}*/

			f.WriteString(strings.Join([]string{
				group.Name,
				rule.Name(),
				// CSV-escape PromQL
				`"` + strings.Replace(query, "\"", "\"\"", -1) + `"`,
				fmt.Sprintf("%t", !rule.NoDependentRules()),
			}, ","))
			f.WriteString("\n")
		}
	}
}

func strongConsistencyRulesCount(groups []*Group) (count int) {
	for _, group := range groups {
		count += group.rulesStrongConsistencyCount
	}
	return
}

func noErr(err error) {
	if err != nil {
		panic(err)
	}
}

func downloadRules(destination string) {
	// The mapping should be <namespace>: <cluster>.
	namespaces := map[string]string{
		"mimir-dedicated-42": "prod-us-east-0",
	}

	for namespace, cluster := range namespaces {
		defer fmt.Println("done", namespace)
		downloadNamespaceRules(destination, cluster, namespace)
	}
}

func downloadNamespaceRules(desitnation string, cluster string, namespace string) {
	fmt.Println("downloading", cluster, namespace)
	kubefwd := exec.Command("kubectl", "--context", cluster, "--namespace", namespace, "port-forward", "svc/ruler", "8080:80")
	noErr(kubefwd.Start())
	defer func() { _, _ = kubefwd.Process.Wait() }()
	defer func() { noErr(kubefwd.Process.Kill()) }()

	time.Sleep(time.Second * 10)

	g := errgroup.Group{}
	g.SetLimit(10)
	tenants := tenantNames()
	noErr(os.MkdirAll(filepath.Join(desitnation, namespace), 0777))
	for tenantIdx, tenant := range tenants {
		tenant := tenant
		tenantIdx := tenantIdx
		g.Go(func() error {
			fmt.Println("downloading tenant", tenantIdx, "/", len(tenants))
			req, err := http.NewRequest(http.MethodGet, "http://localhost:8080/prometheus/api/v1/rules", nil)
			noErr(err)
			req.Header.Set("x-scope-orgid", tenant)
			resp, err := http.DefaultClient.Do(req)
			noErr(err)
			respBody, err := io.ReadAll(resp.Body)
			noErr(err)
			noErr(os.WriteFile(filepath.Join(desitnation, namespace, fmt.Sprintf("%s.json", tenant)), respBody, 0600))
			return nil
		})
	}
	noErr(g.Wait())
}

func tenantNames() []string {
	respMap := map[string]interface{}{}
	resp, err := http.Get("http://localhost:8080/ruler/rule_groups")
	noErr(err)
	respBody, err := io.ReadAll(resp.Body)
	noErr(err)
	//fmt.Println(string(respBody))
	noErr(yaml.Unmarshal(respBody, &respMap))
	names := make([]string, 0, len(respMap))
	for tenant := range respMap {
		names = append(names, tenant)
	}
	return names
}
