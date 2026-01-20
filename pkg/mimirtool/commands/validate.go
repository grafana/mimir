// SPDX-License-Identifier: AGPL-3.0-only

package commands

import (
	"fmt"
	"sort"
	"strings"

	"github.com/alecthomas/kingpin/v2"
	"github.com/go-kit/log"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/rulefmt"
	"github.com/prometheus/prometheus/promql/parser"

	"github.com/grafana/mimir/pkg/mimirtool/rules"
)

// ValidateCommand is the parent command for validation operations.
type ValidateCommand struct{}

// ValidateAlertFilesCommand validates alert rule files.
type ValidateAlertFilesCommand struct {
	Files       []string
	Verbose     bool
	SetExitCode bool

	logger log.Logger
}

// Register registers the validate command and its subcommands with the kingpin application.
func (cmd *ValidateCommand) Register(app *kingpin.Application, _ EnvVarNames, logConfig *LoggerConfig) {
	validateCmd := app.Command("validate", "Validate Prometheus files.")

	alertsCmd := &ValidateAlertFilesCommand{}
	validateAlertsCmd := validateCmd.Command("alerts-file", "Load alert rule files and run validations on them.").PreAction(func(_ *kingpin.ParseContext) error {
		alertsCmd.logger = logConfig.Logger()
		return nil
	}).Action(alertsCmd.run)
	validateAlertsCmd.Arg("files", "Alert rule files to validate").Required().ExistingFilesVar(&alertsCmd.Files)
	validateAlertsCmd.Flag("verbose", "Print verbose output").Default("false").BoolVar(&alertsCmd.Verbose)
	validateAlertsCmd.Flag("set-exit-code", "Set exit code 1 on failures").Default("false").BoolVar(&alertsCmd.SetExitCode)
}

type alertCheckResult struct {
	failure   bool
	alertName string
	message   string
}

type alertCheckFunc func([]rulefmt.Rule) []alertCheckResult

func (cmd *ValidateAlertFilesCommand) run(_ *kingpin.ParseContext) error {
	namespaces, err := rules.ParseFiles(rules.MimirBackend, cmd.Files, model.UTF8Validation, cmd.logger)
	if err != nil {
		return fmt.Errorf("failed to parse rule files: %w", err)
	}

	checks := map[string]alertCheckFunc{
		"native-versions-exists": alertsCheckNativeVersionExists,
	}

	var failures int
	for _, ns := range namespaces {
		for _, group := range ns.Groups {
			results := map[string][]alertCheckResult{}
			for name, checkFunc := range checks {
				res := checkFunc(group.Rules)
				if len(res) > 0 {
					results[name] = res
				}
			}

			for checkName, res := range results {
				for _, r := range res {
					if !cmd.Verbose && !r.failure {
						continue
					}
					icon := "✅"
					if r.failure {
						icon = "❌"
						failures++
					}
					fmt.Printf("%s%s/%s: %q: %s\n", icon, checkName, group.Name, r.alertName, r.message)
				}
			}
		}
	}

	if failures > 0 && cmd.SetExitCode {
		return fmt.Errorf("detected %d failures", failures)
	} else if failures > 0 {
		fmt.Printf("\nDetected %d failures\n", failures)
	}

	return nil
}

// alertsCheckNativeVersionExists ensures that alerts using classic histograms
// have an equivalent native histogram version following them. This is a very
// basic check that can't ensure two promql expressions are semantically
// identical.
//
// Classic histogram usage is determined by searching for vector selectors with
// _bucket, _sum, or _count suffixes in the alert expression.
//
// Two rules are considered equivalent when they have:
//   - The same alert name, and
//   - Compatible vector selectors: the native version must have a vector selector
//     with the same metric name (after removing histogram suffixes, e.g.
//     some_metric_count == some_metric) and identical label matchers.
func alertsCheckNativeVersionExists(rules []rulefmt.Rule) []alertCheckResult {
	var failures []alertCheckResult
	for i := 0; i < len(rules); i++ {
		classicSelectors, err := findClassicHistogramSelectors(rules[i].Expr)
		if err != nil {
			failures = append(failures, alertCheckResult{
				failure:   true,
				alertName: rules[i].Alert,
				message:   fmt.Sprintf("failed to parse rule expression: %v", err),
			})
			continue
		}

		// Skip rules without classic histogram selectors
		if len(classicSelectors) == 0 {
			failures = append(failures, alertCheckResult{
				alertName: rules[i].Alert,
				message:   "skipped (no classic histogram selectors)",
			})
			continue
		}

		// Check if this is the last rule - no native version can follow
		if i == len(rules)-1 {
			failures = append(failures, alertCheckResult{
				failure:   true,
				alertName: rules[i].Alert,
				message:   fmt.Sprintf("rule with metric %q should have a native version, but is the last rule", formatSelectorList(classicSelectors)),
			})
			continue
		}
		if rules[i].Alert != rules[i+1].Alert {
			failures = append(failures, alertCheckResult{
				failure:   true,
				alertName: rules[i].Alert,
				message:   fmt.Sprintf("rule with metric %q should have a native version, but got %q", formatSelectorList(classicSelectors), rules[i+1].Alert),
			})
			continue
		}
		nextRule, err := parser.ParseExpr(rules[i+1].Expr)
		if err != nil {
			failures = append(failures, alertCheckResult{
				failure:   true,
				alertName: rules[i+1].Alert,
				message:   fmt.Sprintf("failed to parse rule expression: %v", err),
			})
			continue
		}
		found := map[int]*parser.VectorSelector{}
		parser.Inspect(nextRule, func(node parser.Node, _ []parser.Node) error {
			if vs, ok := node.(*parser.VectorSelector); ok {
				for index, classicSelector := range classicSelectors {
					for _, suffix := range classicHistogramSuffixes {
						if vs.Name == strings.TrimSuffix(classicSelector.Name, suffix) {
							if nonNameLabelMatchersEqual(vs.LabelMatchers, classicSelector.LabelMatchers) {
								found[index] = vs
							}
							break
						}
					}
				}
			}
			return nil
		})
		if len(found) < len(classicSelectors) {
			failures = append(failures, alertCheckResult{
				failure:   true,
				alertName: rules[i].Alert,
				message:   fmt.Sprintf("found %d matching rules in vector selector", len(found)),
			})
			continue
		}

		// Report this rule and the next rule as valid.
		failures = append(failures,
			alertCheckResult{
				alertName: rules[i].Alert,
				message:   "valid",
			}, alertCheckResult{
				alertName: rules[i+1].Alert,
				message:   "valid",
			},
		)
		i += 1
	}

	return failures
}

// classicHistogramSuffixes are the metric name suffixes that indicate classic histogram usage.
var classicHistogramSuffixes = []string{"_bucket", "_count", "_sum"}

// findClassicHistogramSelectors parses the expression and returns all vector selectors
// that reference classic histogram metrics (identified by _bucket, _count, _sum suffixes).
func findClassicHistogramSelectors(expr string) ([]*parser.VectorSelector, error) {
	parsed, err := parser.ParseExpr(expr)
	if err != nil {
		return nil, err
	}

	var selectors []*parser.VectorSelector
	parser.Inspect(parsed, func(node parser.Node, _ []parser.Node) error {
		if vs, ok := node.(*parser.VectorSelector); ok {
			for _, suffix := range classicHistogramSuffixes {
				if strings.HasSuffix(vs.Name, suffix) {
					selectors = append(selectors, vs)
					break
				}
			}
		}
		return nil
	})
	return selectors, nil
}

// formatSelectorList formats a list of vector selectors as a comma-separated string.
func formatSelectorList(selectors []*parser.VectorSelector) string {
	var result string
	for i, s := range selectors {
		if i == 0 {
			result = s.String()
		} else {
			result += ", " + s.String()
		}
	}
	return result
}

func nonNameLabelMatchersEqual(a, b []*labels.Matcher) bool {
	if len(a) != len(b) {
		return false
	}
	sortLabelMatchers(a)
	sortLabelMatchers(b)
	for i := range a {
		if a[i].Name == model.MetricNameLabel || b[i].Name == model.MetricNameLabel {
			continue
		}
		if a[i].Name != b[i].Name {
			return false
		}
		if a[i].Value != b[i].Value {
			return false
		}
		if a[i].Type != b[i].Type {
			return false
		}
	}
	return true
}

func sortLabelMatchers(a []*labels.Matcher) {
	sort.Slice(a, func(i, j int) bool {
		return a[i].Name < a[j].Name
	})
}
