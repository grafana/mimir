// SPDX-License-Identifier: AGPL-3.0-only

package labelaccess

import (
	"crypto/sha1"
	"encoding/base32"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strings"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser"
)

const (
	// HTTPHeaderKey is used as the header to store label matcher values.
	HTTPHeaderKey = "X-Prom-Label-Policy"
)

var (
	errInvalidHeaderParse  = errors.New("invalid label policy header value, unable to parse tenant and selector")
	errInvalidHeaderEscape = errors.New("invalid label policy header value unable to url escape selector string")
	errInvalidSelector     = errors.New("invalid label policy header value unable to parse selector string")
)

// LabelPolicy represents a single policy with a set of label matchers.
type LabelPolicy struct {
	Selector []*labels.Matcher
}

// LabelPolicySet is a map of tenant names to their label policies.
type LabelPolicySet map[string][]*LabelPolicy

// Hash iterates through the tenant map in ascending order and returns a base32 string of
// an sha1 hash of all of the tenants and their policies.
func (l LabelPolicySet) Hash() string {
	tenants := make([]string, 0, len(l))
	for t := range l {
		tenants = append(tenants, t)
	}
	h := sha1.New()
	sort.Strings(tenants)
	for _, t := range tenants {
		_, _ = h.Write([]byte(t))
		for _, p := range l[t] {
			for i := range p.Selector {
				_, _ = h.Write([]byte(p.Selector[i].String()))
			}
		}
	}
	return base32.StdEncoding.EncodeToString(h.Sum(nil))
}

// policyToHeaderValue encodes a policy as a header value in the format "tenant:{matcher1,matcher2,...}".
func policyToHeaderValue(instanceName string, policy *LabelPolicy) (string, error) {
	matchers := make([]string, len(policy.Selector))

	for i := range policy.Selector {
		matchers[i] = policy.Selector[i].String()
	}

	return instanceName + ":" + url.PathEscape("{"+strings.Join(matchers, ", ")+"}"), nil
}

// policyFromHeaderValue parses a header value in the format "tenant:{matcher1,matcher2,...}" into a policy.
func policyFromHeaderValue(headerString string) (string, *LabelPolicy, error) {
	components := strings.SplitN(headerString, ":", 2)
	if len(components) != 2 {
		return "", nil, errInvalidHeaderParse
	}
	selectorString, err := url.PathUnescape(components[1])
	if err != nil {
		return "", nil, fmt.Errorf("%w, '%v'", errInvalidHeaderEscape, err)
	}
	matchers, err := parser.ParseMetricSelector(selectorString)
	if err != nil {
		return "", nil, fmt.Errorf("%w '%s', error: '%v'", errInvalidSelector, selectorString, err)
	}

	for _, m := range matchers {
		if m.Name == "" {
			return "", nil, fmt.Errorf("%w '%s', error: 'a label name was empty'", errInvalidSelector, selectorString)
		}
	}

	policy := &LabelPolicy{
		Selector: matchers,
	}

	return components[0], policy, nil
}

// InjectLabelMatchersHTTP takes the provided label policy set and stores them as HTTP headers.
func InjectLabelMatchersHTTP(r *http.Request, instancePolicyMap LabelPolicySet) error {
	// Ensure any existing policy sets are erased.
	r.Header.Del(HTTPHeaderKey)

	values, err := InjectLabelMatchersSlice(instancePolicyMap)
	if err != nil {
		return err
	}

	r.Header[HTTPHeaderKey] = values

	return nil
}

// InjectLabelMatchersSlice takes the provided label policy set and encodes them as a slice of strings, suitable for use as header values.
func InjectLabelMatchersSlice(instancePolicyMap LabelPolicySet) ([]string, error) {
	var values []string

	for instanceName, policies := range instancePolicyMap {
		for _, policy := range policies {
			encodedPolicy, err := policyToHeaderValue(instanceName, policy)
			if err != nil {
				return nil, err
			}

			values = append(values, encodedPolicy)
		}
	}

	return values, nil
}

// ExtractLabelMatchersHTTP takes the provided HTTP request and parses out label policy set.
func ExtractLabelMatchersHTTP(r *http.Request) (LabelPolicySet, error) {
	headerValues := r.Header.Values(HTTPHeaderKey)

	return ExtractLabelMatchersSlice(headerValues)
}

// ExtractLabelMatchersSlice takes a slice of label matcher header values and parses a label policy set from them.
func ExtractLabelMatchersSlice(values []string) (LabelPolicySet, error) {
	instancePolicyMap := LabelPolicySet{}

	// Iterate through each set header value.
	for _, headerValue := range values {
		// Split the header value on a comma and iterate through each policy.
		for _, v := range strings.Split(headerValue, ",") {
			instanceName, policy, err := policyFromHeaderValue(v)
			if err != nil {
				return nil, err
			}

			currentPolicies, exists := instancePolicyMap[instanceName]
			if exists {
				instancePolicyMap[instanceName] = append(currentPolicies, policy)
			} else {
				instancePolicyMap[instanceName] = []*LabelPolicy{policy}
			}
		}
	}

	return instancePolicyMap, nil
}
