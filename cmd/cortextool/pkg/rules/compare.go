package rules

import (
	"errors"
	"fmt"
	"reflect"

	"github.com/prometheus/prometheus/pkg/rulefmt"
)

var (
	errNameDiff     = errors.New("rule groups are named differently")
	errIntervalDiff = errors.New("rule groups have different intervals")
	errDiffRuleLen  = errors.New("rule groups have a different number of rules")
)

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
