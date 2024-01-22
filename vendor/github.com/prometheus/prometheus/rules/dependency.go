// Copyright 2013 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package rules

import "github.com/prometheus/prometheus/promql/parser"

// dependencyMap is a data-structure which contains the relationships between rules within a group.
// It is used to describe the dependency associations between rules in a group whereby one rule uses the
// output metric produced by another rule in its expression (i.e. as its "input").
type dependencyMap map[Rule][]Rule

// dependents returns the count of rules which use the output of the given rule as one of their inputs.
func (m dependencyMap) dependents(r Rule) int {
	return len(m[r])
}

// dependencies returns the count of rules on which the given rule is dependent for input.
func (m dependencyMap) dependencies(r Rule) int {
	if len(m) == 0 {
		return 0
	}

	var count int
	for _, children := range m {
		for _, child := range children {
			if child == r {
				count++
			}
		}
	}

	return count
}

// isIndependent determines whether the given rule is not dependent on another rule for its input, nor is any other rule
// dependent on its output.
func (m dependencyMap) isIndependent(r Rule) bool {
	if m == nil {
		return false
	}

	return m.dependents(r)+m.dependencies(r) == 0
}

// buildDependencyMap builds a data-structure which contains the relationships between rules within a group.
//
// Alert rules, by definition, cannot have any dependents - but they can have dependencies. Any recording rule on whose
// output an Alert rule depends will not be able to run concurrently.
//
// There is a class of rule expressions which are considered "indeterminate", because either relationships cannot be
// inferred, or concurrent evaluation of rules depending on these series would produce undefined/unexpected behaviour:
//   - wildcard queriers like {cluster="prod1"} which would match every series with that label selector
//   - any "meta" series (series produced by Prometheus itself) like ALERTS, ALERTS_FOR_STATE
//
// Rules which are independent can run concurrently with no side-effects.
func buildDependencyMap(rules []Rule) dependencyMap {
	dependencies := make(dependencyMap)

	if len(rules) <= 1 {
		// No relationships if group has 1 or fewer rules.
		return nil
	}

	inputs := make(map[string][]Rule, len(rules))
	outputs := make(map[string][]Rule, len(rules))

	var indeterminate bool

	for _, rule := range rules {
		rule := rule

		name := rule.Name()
		outputs[name] = append(outputs[name], rule)

		parser.Inspect(rule.Query(), func(node parser.Node, path []parser.Node) error {
			if n, ok := node.(*parser.VectorSelector); ok {
				// A wildcard metric expression means we cannot reliably determine if this rule depends on any other,
				// which means we cannot safely run any rules concurrently.
				if n.Name == "" && len(n.LabelMatchers) > 0 {
					indeterminate = true
					return nil
				}

				// Rules which depend on "meta-metrics" like ALERTS and ALERTS_FOR_STATE will have undefined behaviour
				// if they run concurrently.
				if n.Name == alertMetricName || n.Name == alertForStateMetricName {
					indeterminate = true
					return nil
				}

				inputs[n.Name] = append(inputs[n.Name], rule)
			}
			return nil
		})
	}

	if indeterminate {
		return nil
	}

	if len(inputs) == 0 || len(outputs) == 0 {
		// No relationships can be inferred.
		return nil
	}

	for output, outRules := range outputs {
		for _, outRule := range outRules {
			if rs, found := inputs[output]; found && len(rs) > 0 {
				dependencies[outRule] = append(dependencies[outRule], rs...)
			}
		}
	}

	return dependencies
}
