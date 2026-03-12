// SPDX-License-Identifier: AGPL-3.0-only

package labelaccess

import (
	"fmt"
	"net/http"
)

// LabelPolicies is the interface for types that can provide a LabelPolicySet.
type LabelPolicies interface {
	GetLabelPolicySet() (LabelPolicySet, error)
}

// InjectMatchers sets X-Prom-Label-Policy on req from labelPolicies.
// If labelPolicies returns an empty set the header is left unchanged.
func InjectMatchers(req *http.Request, labelPolicies LabelPolicies) error {
	labelPolicySet, err := labelPolicies.GetLabelPolicySet()
	if err != nil {
		return fmt.Errorf("unable to parse label policies: %w", err)
	}

	// Avoid overriding existing "X-Prom-Label-Policy" header when
	// there are no label policies. Now that the Cloud Auth API can be used
	// for label policies, we would like to migrate users away from specifying
	// this header themselves. Progress is being tracked here:
	// https://github.com/grafana/cloud-auth-squad/issues/25.
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
