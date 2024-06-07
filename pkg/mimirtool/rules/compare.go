// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/grafana/cortex-tools/blob/main/pkg/rules/compare.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package rules

import (
	"errors"
	"fmt"
	"reflect"
	"strings"

	"github.com/mitchellh/colorstring"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/rulefmt"
	yaml "gopkg.in/yaml.v3"

	"github.com/grafana/mimir/pkg/mimirtool/rules/rwrulefmt"
)

var (
	errNameDiff            = errors.New("rule groups are named differently")
	errIntervalDiff        = errors.New("rule groups have different intervals")
	errDiffRuleLen         = errors.New("rule groups have a different number of rules")
	errDiffRWConfigs       = errors.New("rule groups have different remote write configs")
	errDiffSourceTenants   = errors.New("rule groups have different source tenants")
	errDiffEvaluationDelay = errors.New("rule groups have different evaluation delay")
	errDiffQueryOffset     = errors.New("rule groups have different query offset")
)

// NamespaceState is used to denote the difference between the staged namespace
// and active namespace for the mimir tenant
type NamespaceState int

func (s NamespaceState) String() string {
	switch s {
	case Created:
		return "created"
	case Updated:
		return "updated"
	case Deleted:
		return "deleted"
	default:
		return "unknown"
	}
}

const (
	// Unchanged denotes the active namespace is identical to the staged namespace
	Unchanged NamespaceState = iota
	// Created denotes there is no active namespace for the staged namespace
	Created
	// Updated denotes the active namespace is different than the staged namespace
	Updated
	// Deleted denotes there is no staged namespace for the active namespace
	Deleted
)

// NamespaceChange stores the various changes between a staged set of changes
// and the active rules configs.
type NamespaceChange struct {
	Namespace     string
	State         NamespaceState
	GroupsUpdated []UpdatedRuleGroup
	GroupsCreated []rwrulefmt.RuleGroup
	GroupsDeleted []rwrulefmt.RuleGroup
}

// ToOperations returns a list of operations to run to apply the given change.
// Each operation addresses a single rule group.
func (c NamespaceChange) ToOperations() []NamespaceChangeOperation {
	ops := make([]NamespaceChangeOperation, 0, len(c.GroupsCreated)+len(c.GroupsUpdated)+len(c.GroupsDeleted))

	for _, g := range c.GroupsCreated {
		ops = append(ops, NamespaceChangeOperation{State: Created, Namespace: c.Namespace, RuleGroup: g})
	}

	for _, g := range c.GroupsUpdated {
		ops = append(ops, NamespaceChangeOperation{State: Updated, Namespace: c.Namespace, RuleGroup: g.New})
	}

	for _, g := range c.GroupsDeleted {
		ops = append(ops, NamespaceChangeOperation{State: Deleted, Namespace: c.Namespace, RuleGroup: g})
	}

	return ops
}

// NamespaceChangeOperation holds an operation on a single rule group.
type NamespaceChangeOperation struct {
	Namespace string
	State     NamespaceState
	RuleGroup rwrulefmt.RuleGroup
}

// SummarizeChanges returns the number of each type of change in a set of changes
func SummarizeChanges(changes []NamespaceChange) (created, updated, deleted int) {
	// Cycle through the results to determine which types of changes have been made
	for _, change := range changes {
		if len(change.GroupsCreated) > 0 {
			created += len(change.GroupsCreated)
		}
		if len(change.GroupsUpdated) > 0 {
			updated += len(change.GroupsUpdated)
		}
		if len(change.GroupsDeleted) > 0 {
			deleted += len(change.GroupsDeleted)
		}
	}
	return
}

// UpdatedRuleGroup is used to store an change between a rule group
type UpdatedRuleGroup struct {
	New      rwrulefmt.RuleGroup
	Original rwrulefmt.RuleGroup
}

// CompareGroups differentiates between two rule groups
func CompareGroups(groupOne, groupTwo rwrulefmt.RuleGroup) error {
	if groupOne.Name != groupTwo.Name {
		return errNameDiff
	}

	if groupOne.Interval != groupTwo.Interval {
		return errIntervalDiff
	}

	if len(groupOne.Rules) != len(groupTwo.Rules) {
		return errDiffRuleLen
	}

	if len(groupOne.RWConfigs) != len(groupTwo.RWConfigs) {
		return errDiffRWConfigs
	}

	//nolint:staticcheck // We want to intentionally access a deprecated field
	if getEvaluationDelayOrQueryOffsetValue(groupOne.EvaluationDelay) != getEvaluationDelayOrQueryOffsetValue(groupTwo.EvaluationDelay) {
		return errDiffEvaluationDelay
	}

	if getEvaluationDelayOrQueryOffsetValue(groupOne.QueryOffset) != getEvaluationDelayOrQueryOffsetValue(groupTwo.QueryOffset) {
		return errDiffQueryOffset
	}

	for i := range groupOne.RWConfigs {
		if groupOne.RWConfigs[i].URL != groupTwo.RWConfigs[i].URL {
			return errDiffRWConfigs
		}
	}

	if !stringSlicesElementsMatch(groupOne.SourceTenants, groupTwo.SourceTenants) {
		return errDiffSourceTenants
	}

	for i := range groupOne.Rules {
		eq := rulesEqual(&groupOne.Rules[i], &groupTwo.Rules[i])
		if !eq {
			return fmt.Errorf("rule #%v does not match %v != %v", i, groupOne.Rules[i], groupTwo.Rules[i])
		}
	}

	return nil
}

// stringSlicesElementsMatch returns true if the two slices have completely overlapping elements.
// For example, `stringSlicesElementsMatch([a, b], [a, b]) == true`
// and `stringSlicesElementsMatch([a, b], [a, b, b]) == true`
func stringSlicesElementsMatch(s1, s2 []string) bool {
	toMap := func(input []string) map[string]bool {
		result := map[string]bool{}
		for _, e := range input {
			result[e] = true
		}
		return result
	}

	m1 := toMap(s1)
	m2 := toMap(s2)

	if len(m1) != len(m2) {
		return false
	}

	for k := range m1 {
		if !m2[k] {
			return false
		}
	}

	return true
}

func rulesEqual(a, b *rulefmt.RuleNode) bool {
	if a.Alert.Value != b.Alert.Value ||
		a.Record.Value != b.Record.Value ||
		a.Expr.Value != b.Expr.Value ||
		a.For != b.For {
		return false
	}

	if a.Annotations != nil && b.Annotations != nil {
		if !reflect.DeepEqual(a.Annotations, b.Annotations) {
			return false
		}
	} else if a.Annotations != nil || b.Annotations != nil {
		return false
	}

	if a.Labels != nil && b.Labels != nil {
		if !reflect.DeepEqual(a.Labels, b.Labels) {
			return false
		}
	} else if a.Labels != nil || b.Labels != nil {
		return false
	}

	return true
}

// CompareNamespaces returns the differences between the two provided
// namespaces
func CompareNamespaces(original, new RuleNamespace) NamespaceChange {
	result := NamespaceChange{
		Namespace:     new.Namespace,
		State:         Unchanged,
		GroupsUpdated: []UpdatedRuleGroup{},
		GroupsCreated: []rwrulefmt.RuleGroup{},
		GroupsDeleted: []rwrulefmt.RuleGroup{},
	}

	origMap := map[string]rwrulefmt.RuleGroup{}
	for _, g := range original.Groups {
		origMap[g.Name] = g
	}

	for _, newGroup := range new.Groups {
		origGroup, found := origMap[newGroup.Name]
		if !found {
			result.State = Updated
			result.GroupsCreated = append(result.GroupsCreated, newGroup)
			continue
		}
		diff := CompareGroups(newGroup, origGroup)
		if diff != nil {
			result.State = Updated
			result.GroupsUpdated = append(result.GroupsUpdated, UpdatedRuleGroup{
				Original: origGroup,
				New:      newGroup,
			})
		}
		delete(origMap, newGroup.Name)
	}

	for _, group := range origMap {
		result.State = Updated
		result.GroupsDeleted = append(result.GroupsDeleted, group)
	}

	return result
}

// PrintComparisonResult prints the differences between the staged namespace
// and active namespace
func PrintComparisonResult(results []NamespaceChange, verbose bool) error {
	created, updated, deleted := SummarizeChanges(results)

	// If any changes are detected, print the symbol legend
	if (created + updated + deleted) > 0 {
		fmt.Println("Changes are indicated with the following symbols:")
		if created > 0 {
			colorstring.Println("[green]  +[reset] created") //nolint
		}
		if updated > 0 {
			colorstring.Println("[yellow]  ~[reset] updated") //nolint
		}
		if deleted > 0 {
			colorstring.Println("[red]  -[reset] deleted") //nolint
		}
		fmt.Println()
		fmt.Println("The following changes will be made if the provided rule set is synced:")
	} else {
		fmt.Println("no changes detected")
		return nil
	}

	for _, change := range results {
		switch change.State {
		case Created:
			colorstring.Printf("[green]+ Namespace: %v\n", change.Namespace)
			for _, c := range change.GroupsCreated {
				colorstring.Printf("[green]  + Group: %v\n", c.Name)
			}
		case Updated:
			colorstring.Printf("[yellow]~ Namespace: %v\n", change.Namespace)
			for _, c := range change.GroupsCreated {
				colorstring.Printf("[green]  + Group: %v\n", c.Name)
			}

			for _, c := range change.GroupsUpdated {
				colorstring.Printf("[yellow]  ~ Group: %v\n", c.New.Name)

				// Print the full diff of the rules if verbose is set
				if verbose {
					newYaml, _ := yaml.Marshal(c.New)
					separated := strings.Split(string(newYaml), "\n")
					for _, l := range separated {
						colorstring.Printf("[green]+ %v\n", l)
					}

					oldYaml, _ := yaml.Marshal(c.Original)
					separated = strings.Split(string(oldYaml), "\n")
					for _, l := range separated {
						colorstring.Printf("[red]- %v\n", l)
					}
				}
			}

			for _, c := range change.GroupsDeleted {
				colorstring.Printf("[red]  - Group: %v\n", c.Name)
			}
		case Deleted:
			colorstring.Printf("[red]- Namespace: %v\n", change.Namespace)
			for _, c := range change.GroupsDeleted {
				colorstring.Printf("[red]  - Group: %v\n", c.Name)
			}
		}
	}

	fmt.Println()
	fmt.Printf("Diff Summary: %v Groups Created, %v Groups Updated, %v Groups Deleted\n", created, updated, deleted)
	return nil
}

func getEvaluationDelayOrQueryOffsetValue(value *model.Duration) model.Duration {
	if value == nil {
		// Mimir ruler considers a value of 0 as if the EvaluationDelay has not been set (nil),
		// so when we compare the configured value we consider nil as 0.
		return 0
	}

	return *value
}
