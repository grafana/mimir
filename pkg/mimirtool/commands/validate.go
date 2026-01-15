// SPDX-License-Identifier: AGPL-3.0-only

package commands

import (
	"fmt"
	"strings"

	"github.com/alecthomas/kingpin/v2"
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
}

// Register registers the validate command and its subcommands with the kingpin application.
func (cmd *ValidateCommand) Register(app *kingpin.Application, _ EnvVarNames) {
	validateCmd := app.Command("validate", "Validate Prometheus rule files.")

	alertsCmd := &ValidateAlertFilesCommand{}
	validateAlertsCmd := validateCmd.Command("alerts-file", "Load alert rule files and print alert names with their expressions.").Action(alertsCmd.run)
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
	namespaces, err := rules.ParseFiles(rules.MimirBackend, cmd.Files, model.UTF8Validation)
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

func alertsCheckNativeVersionExists(rules []rulefmt.Rule) []alertCheckResult {
	var failures []alertCheckResult
	for i := 0; i < len(rules)-1; i++ {
		thisRule, err := parser.ParseExpr(rules[i].Expr)
		if err != nil {
			failures = append(failures, alertCheckResult{
				failure:   true,
				alertName: rules[i].Alert,
				message:   fmt.Sprintf("failed to parse rule expression: %v", err),
			})
			continue
		}
		var nativeSelectors []*parser.VectorSelector
		parser.Inspect(thisRule, func(node parser.Node, nodes []parser.Node) error {
			if vs, ok := node.(*parser.VectorSelector); ok {
				for _, suffix := range []string{"_bucket", "_count", "_sum"} {
					if strings.HasSuffix(vs.Name, suffix) {
						nativeSelectors = append(nativeSelectors, vs)
						break
					}
				}
			}
			return nil
		})
		if len(nativeSelectors) == 0 {
			failures = append(failures, alertCheckResult{
				alertName: rules[i].Alert,
				message:   "skipped (no native selectors)",
			})
			continue
		}
		if rules[i].Alert != rules[i+1].Alert {
			var selector string
			for j, ns := range nativeSelectors {
				if j == 0 {
					selector = ns.String()
				} else {
					selector += ", " + ns.String()
				}
			}
			failures = append(failures, alertCheckResult{
				failure:   true,
				alertName: rules[i].Alert,
				message:   fmt.Sprintf("rule with metric %q should have a native version, but got %q", selector, rules[i+1].Alert),
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
		parser.Inspect(nextRule, func(node parser.Node, nodes []parser.Node) error {
			if vs, ok := node.(*parser.VectorSelector); ok {
				for index, nvs := range nativeSelectors {
					for _, suffix := range []string{"_bucket", "_count", "_sum"} {
						if vs.Name == strings.TrimSuffix(nvs.Name, suffix) {
							if labelMatchersEqual(vs.LabelMatchers, nvs.LabelMatchers) {
								found[index] = vs
							}
							break
						}
					}
				}
			}
			return nil
		})
		if len(found) < len(nativeSelectors) {
			failures = append(failures, alertCheckResult{
				failure:   true,
				alertName: rules[i].Alert,
				message:   fmt.Sprintf("found %d matching rules in vector selector", len(found)),
			})
			continue
		}
		failures = append(failures, alertCheckResult{
			alertName: rules[i].Alert,
			message:   "valid",
		})
	}

	failures = append(failures, alertCheckResult{
		alertName: rules[len(rules)-1].Alert,
		message:   "nothing to validate",
	})

	return failures
}

func labelMatchersEqual(a, b []*labels.Matcher) bool {
	if len(a) != len(b) {
		return false
	}
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
