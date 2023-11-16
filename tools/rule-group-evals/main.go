package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	"github.com/prometheus/prometheus/promql/parser"
	"golang.org/x/sync/errgroup"
	"gopkg.in/yaml.v3"
)

type Result struct {
	Data Data `json:"data"`
}

type Data struct {
	Groups []Group `json:"groups"`
}

type Group struct {
	Name            string  `json:"name"`
	IntervalSeconds float64 `json:"interval"`
	Rules           []Rule  `json:"rules"`

	RecordingRules map[string]*Rule
}

type Rule struct {
	Name                  string  `json:"name"` // name of the series if type == recording
	Type                  string  `json:"type"` // "alerting" or "recording"
	EvaluationTimeSeconds float64 `json:"evaluationTime"`
	Query                 string  `json:"query"`

	// Longest time it will take to evaluate this rule + all of its dependencies.
	// If this rule depends on multiple other rules, then this is the max of the others + this.EvaluationTimeSeconds.
	// If this rule doesn't depend on other rules, then this is equal to EvaluationTimeSeconds.
	MaxEvalTime time.Duration

	// Length of chain for MaxEvalTime. If rule doesn't depend on other rules, this is 0.
	MaxEvalTimeChain int

	// An element from DependsOn which leads to MaxEvalTime and MaxEvalTimeChain
	MaxEvalTimeDep *Rule

	// Only same rule group
	DependsOn []*Rule
}

func (r Rule) NameStr() string {
	return r.Name
}

func main() {
	mode := os.Args[1]
	switch mode {
	case "download":
		downloadRules(os.Args[2])
	case "analyze":
		dir := os.Args[2]
		entries, err := os.ReadDir(dir)
		noErr(err)
		for _, entry := range entries {
			analyseGroups(filepath.Join(dir, entry.Name()))
		}
	}
}

func analyseGroups(file string) {
	groupsFile, err := os.Open(file)
	noErr(err)

	groupsBytes, err := io.ReadAll(groupsFile)
	noErr(err)

	result := &Result{}
	noErr(json.Unmarshal(groupsBytes, result))

	parseRecordingRules(result.Data.Groups)
	discoverDependencies(result.Data.Groups)
	findDependencyChains(result.Data.Groups)
	printEvaluationSlack(result.Data.Groups)
}

func parseRecordingRules(groups []Group) {
	for groupIdx, group := range groups {
		for ruleIdx, rule := range group.Rules {
			if rule.Type != "recording" {
				continue
			}
			if group.RecordingRules == nil {
				groups[groupIdx].RecordingRules = make(map[string]*Rule)
				group = groups[groupIdx]
			}
			group.RecordingRules[rule.Name] = &(group.Rules[ruleIdx])
			//fmt.Println("DEBUG: found recording rule: " + rule.Name)
		}
	}
}

// discoverDependencies discovers which recording rules each rule depends on directly. It doesn't account for transitive dependencies.
// This uses Group.RecordingRules and the result is saved in Rule.DependsOn
func discoverDependencies(groups []Group) {
	for _, group := range groups {
		//fmt.Println()
		//fmt.Println(collectRuleNames(group.Rules))
		for ruleIdx := range group.Rules {
			rule := &group.Rules[ruleIdx]
			vectorSelectors := map[string]struct{}{}
			expr, err := parser.ParseExpr(rule.Query)
			if err != nil {
				fmt.Println("DEBUG: ignoring wrong query query: " + err.Error())
				continue
			}

			parser.Inspect(expr, func(node parser.Node, nodes []parser.Node) error {
				if vectorSelector, ok := node.(*parser.VectorSelector); ok {
					vectorSelectors[vectorSelector.Name] = struct{}{}
				}
				return nil
			})
			for vectorSelector := range vectorSelectors {
				if recordingRule, ok := group.RecordingRules[vectorSelector]; ok {
					rule.DependsOn = append(rule.DependsOn, recordingRule)
				}
			}
		}
	}
}

func collectRuleNames[T interface{ NameStr() string }](rules []T) []string {
	names := make([]string, 0, len(rules))
	for _, rule := range rules {
		names = append(names, rule.NameStr())
	}
	return names
}

// findDependencyChains calculates Rule.MaxEvalTime and Rule.MaxEvalTimeChain
func findDependencyChains(groups []Group) {
	for _, group := range groups {
		for ruleIdx := range group.Rules {
			group.Rules[ruleIdx].MaxEvalTime = -1
		}
	}

	for _, group := range groups {
		for ruleIdx := range group.Rules {
			findDependencyChainsForRule(&(group.Rules[ruleIdx]), make(map[*Rule]bool))
			//fmt.Println(collectRuleNames(group.Rules[ruleIdx].Deps.Rules), group.Rules[ruleIdx].Query)
		}
	}
}

func findDependencyChainsForRule(rule *Rule, visited map[*Rule]bool) {
	if rule.MaxEvalTime != -1 {
		return
	}
	if len(rule.DependsOn) == 0 {
		rule.MaxEvalTime = time.Duration(rule.EvaluationTimeSeconds * float64(time.Second))
		return
	}
	if visited[rule] {
		// Rule groups can be recursive.
		// Some valid reasons for this is when the rule value falls back to an average of its previous values.
		// In this case we pretend like the dependency doesn't exist, and it's just a rule which takes 0 time to evaluate.
		fmt.Println("DEBUG: found a recursive rule")
		return
	}
	visited[rule] = true
	defer func() {
		visited[rule] = false
	}()

	maxDepEval := time.Duration(-1)
	maxDepEvalChain := 0
	var maxDepEvalRule *Rule
	for _, dep := range rule.DependsOn {
		if dep.MaxEvalTime == -1 {
			findDependencyChainsForRule(dep, visited)
		}
		if maxDepEval < dep.MaxEvalTime {
			maxDepEval, maxDepEvalChain, maxDepEvalRule = dep.MaxEvalTime, dep.MaxEvalTimeChain, dep
		}
	}

	rule.MaxEvalTimeChain = maxDepEvalChain + 1
	rule.MaxEvalTime = maxDepEval + time.Duration(rule.EvaluationTimeSeconds*float64(time.Second))
	rule.MaxEvalTimeDep = maxDepEvalRule
}

// printEvaluationSlack takes each rule in a group and prints the latency you can add to every rule evaluation
func printEvaluationSlack(groups []Group) {
	for _, group := range groups {
		maxDepEval := group.Rules[0].MaxEvalTime
		maxDepEvalChain := group.Rules[0].MaxEvalTimeChain
		maxDepEvalRule := &group.Rules[0]
		for ruleIdx, rule := range group.Rules[1:] {
			if rule.MaxEvalTime > maxDepEval {
				maxDepEval, maxDepEvalChain, maxDepEvalRule = rule.MaxEvalTime, rule.MaxEvalTimeChain, &group.Rules[ruleIdx+1]
			}
		}
		if maxDepEvalChain == 0 {
			continue
		}

		evalInterval := time.Duration(group.IntervalSeconds * float64(time.Second))
		fmt.Println(
			((evalInterval - maxDepEval) / time.Duration(maxDepEvalChain)).Seconds(),
			evalInterval,
			//len(group.Rules),
			maxDepEvalRule.MaxEvalTime,
			maxDepEvalChain,
			formatMaxEvalTimeRuleChain(maxDepEvalRule),
			//group.Name,
		)
	}
}

func formatMaxEvalTimeRuleChain(rule *Rule) string {
	const delim = " -> "
	var str string
	for rule != nil {
		str += fmt.Sprintf("%s (%s)%s", rule.Name, time.Duration(float64(time.Second)*rule.EvaluationTimeSeconds), delim)
		rule = rule.MaxEvalTimeDep
	}
	str = str[:len(str)-len(delim)]
	return str
}

func noErr(err error) {
	if err != nil {
		panic(err)
	}
}

func downloadRules(destiantion string) {
	namespaces := map[string]string{}

	for namespace, cluster := range namespaces {
		defer fmt.Println("done", namespace)
		downloadNamespaceRules(destiantion, cluster, namespace)
	}
}

func downloadNamespaceRules(desitnation string, cluster string, namespace string) {
	fmt.Println("downloading", cluster, namespace)
	noErr(exec.Command("kubectx", cluster).Run())
	noErr(exec.Command("kubens", namespace).Run())
	kubefwd := exec.Command("kubectl", "port-forward", "svc/ruler", "8080:80", "-n", namespace)
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
