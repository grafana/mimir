package rules

import (
	"errors"
	"fmt"
	"reflect"
	"strings"

	"github.com/mitchellh/colorstring"
	"github.com/prometheus/prometheus/pkg/rulefmt"
	"sigs.k8s.io/yaml"
)

var (
	errNameDiff     = errors.New("rule groups are named differently")
	errIntervalDiff = errors.New("rule groups have different intervals")
	errDiffRuleLen  = errors.New("rule groups have a different number of rules")
)

// NamespaceState is used to denote the difference between the staged namespace
// and active namespace for the cortex tenant
type NamespaceState int

const (
	// Unchanged denotes the active namespace is identical to the staged namespace
	Unchanged NamespaceState = iota
	// Created denotes their is not active namespace for the staged namespace
	Created
	// Updated denotes the active namespace is different than the staged namespace
	Updated
	// Deleted denotes their is no staged namespace for the active namespace
	Deleted
)

// NamespaceChange stores the various changes between a staged set of changes
// and the active rules configs.
type NamespaceChange struct {
	Namespace     string
	State         NamespaceState
	GroupsUpdated []UpdatedRuleGroup
	GroupsCreated []rulefmt.RuleGroup
	GroupsDeleted []rulefmt.RuleGroup
}

// UpdatedRuleGroup is used to store an change between a rule group
type UpdatedRuleGroup struct {
	New      rulefmt.RuleGroup
	Original rulefmt.RuleGroup
}

// CompareGroups differentiates between two rule groups
func CompareGroups(groupOne, groupTwo rulefmt.RuleGroup) error {
	if groupOne.Name != groupTwo.Name {
		return errNameDiff
	}

	if groupOne.Interval != groupTwo.Interval {
		return errIntervalDiff
	}

	if len(groupOne.Rules) != len(groupTwo.Rules) {
		return errDiffRuleLen
	}

	for i := range groupOne.Rules {
		eq := reflect.DeepEqual(&groupOne.Rules[i], &groupTwo.Rules[i])
		if !eq {
			return fmt.Errorf("rule #%v does not match %v != %v", i, groupOne.Rules[i], groupTwo.Rules[i])
		}
	}

	return nil
}

// CompareNamespaces returns the differences between the two provided
// namespaces
func CompareNamespaces(original, new RuleNamespace) NamespaceChange {
	result := NamespaceChange{
		Namespace:     new.Namespace,
		State:         Unchanged,
		GroupsUpdated: []UpdatedRuleGroup{},
		GroupsCreated: []rulefmt.RuleGroup{},
		GroupsDeleted: []rulefmt.RuleGroup{},
	}

	origMap := map[string]rulefmt.RuleGroup{}
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
	// Cycle through the results to determine which types of changes have been made
	var updated, created, deleted bool
	var updatedTotal, createdTotal, deletedTotal int
	for _, result := range results {
		if len(result.GroupsCreated) > 0 {
			created = true
			createdTotal += len(result.GroupsCreated)
		}
		if len(result.GroupsUpdated) > 0 {
			updated = true
			updatedTotal += len(result.GroupsUpdated)
		}
		if len(result.GroupsDeleted) > 0 {
			deleted = true
			deletedTotal += len(result.GroupsDeleted)
		}
	}

	// If any changes are detected, print the symbol legend
	if created || updated || deleted {
		fmt.Println("Changes are indicated with the following symbols:")
		if created {
			colorstring.Println("[green]  +[reset] created") //nolint
		}
		if updated {
			colorstring.Println("[yellow]  +[reset] updated") //nolint
		}
		if deleted {
			colorstring.Println("[red]  +[reset] deleted") //nolint
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
			for _, g := range change.GroupsCreated {
				colorstring.Printf("[green]  + Group: %v\n", g.Name)
			}
		case Updated:
			colorstring.Printf("[yellow]~ Namespace: %v\n", change.Namespace)
			for _, created := range change.GroupsCreated {
				colorstring.Printf("[green]  + Group: %v\n", created.Name)
			}

			for _, updated := range change.GroupsUpdated {
				colorstring.Printf("[yellow]  ~ Group: %v\n", updated.New.Name)

				// Print the full diff of the rules if verbose is set
				if verbose {
					newYaml, _ := yaml.Marshal(updated.New)
					seperated := strings.Split(string(newYaml), "\n")
					for _, l := range seperated {
						colorstring.Printf("[green]+ %v\n", l)
					}

					oldYaml, _ := yaml.Marshal(updated.Original)
					seperated = strings.Split(string(oldYaml), "\n")
					for _, l := range seperated {
						colorstring.Printf("[red]+ %v\n", l)
					}
				}
			}

			for _, deleted := range change.GroupsDeleted {
				colorstring.Printf("[red]  - Group: %v\n", deleted.Name)
			}
		case Deleted:
			colorstring.Printf("[red]- Namespace: %v\n", change.Namespace)
			for _, g := range change.GroupsDeleted {
				colorstring.Printf("[red]  - Group: %v\n", g.Name)
			}
		}
	}

	fmt.Println()
	fmt.Printf("Summary: %v Groups Created, %v Groups Updated, %v Groups Deleted", createdTotal, updatedTotal, deletedTotal)

	return nil
}
