// SPDX-License-Identifier: AGPL-3.0-only

package labelaccess

import (
	"fmt"
	"net/http"
)

// LabelPolicies is a common interface to be used for injecting label policies from external sources.
type LabelPolicies interface {
	GetLabelPolicySet() (LabelPolicySet, error)
}

// InjectMatchers injects label matchers from labelPolicies into the HTTP request headers.
func InjectMatchers(req *http.Request, labelPolicies LabelPolicies) error {
	labelPolicySet, err := labelPolicies.GetLabelPolicySet()
	if err != nil {
		return fmt.Errorf("unable to parse label policies: %w", err)
	}

	// Avoid overriding existing "X-Prom-Label-Policy" header when
	// there are no label policies.
	noPolicies := true
	for _, policies := range labelPolicySet {
		if len(policies) != 0 {
			noPolicies = false
			break
		}
	}

	if noPolicies {
		return nil
	}

	if err := InjectLabelMatchersHTTP(req, labelPolicySet); err != nil {
		return fmt.Errorf("unable to inject matchers: %w", err)
	}

	return nil
}
