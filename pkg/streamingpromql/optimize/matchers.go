// SPDX-License-Identifier: AGPL-3.0-only

package optimize

import (
	"strings"

	"github.com/prometheus/prometheus/model/labels"
)

func CompareMatchers(firstName, secondName string, firstType, secondType labels.MatchType, firstValue, secondValue string) int {
	if firstName != secondName {
		return strings.Compare(firstName, secondName)
	}

	if firstType != secondType {
		return int(firstType - secondType)
	}

	return strings.Compare(firstValue, secondValue)
}
