// SPDX-License-Identifier: AGPL-3.0-only

package labelaccess

import (
	"context"
	"crypto/sha1"
	"encoding/base32"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strings"

	"github.com/prometheus/prometheus/model/labels"

	"github.com/grafana/mimir/pkg/util/promqlext"
)

type key int

const (
	// HTTPHeaderKey is used as the header to store label matcher values
	HTTPHeaderKey = "X-Prom-Label-Policy"

	contextKey key = 0
)

var (
	errNoMatcherSource     = errors.New("no label matcher source")
	errInvalidHeaderParse  = errors.New("invalid label policy header value, unable to parse tenant and selector")
	errInvalidHeaderEscape = errors.New("invalid label policy header value unable to url escape selector string")
	errInvalidSelector     = errors.New("invalid label policy header value unable to parse selector string")
)

type LabelPolicy struct {
	Selector []*labels.Matcher
}

// LabelPolicySet is used
type LabelPolicySet map[string][]*LabelPolicy

func (l LabelPolicySet) GetLabelPolicySet() (LabelPolicySet, error) {
	return l, nil
}

// Hash iterates through the tenant map in ascending order and returns a base32 string of
// and sha1 hash of all of the tenants
func (l LabelPolicySet) Hash() string {
	tenants := make([]string, 0, len(l))
	for t := range l {
		tenants = append(tenants, t)
	}
	h := sha1.New() //nolint:gosec // SHA1 is used for non-cryptographic hashing
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

func policyToHeaderValue(instanceName string, policy *LabelPolicy) (string, error) {
	matchers := make([]string, len(policy.Selector))

	for i := range policy.Selector {
		matchers[i] = policy.Selector[i].String()
	}

	return instanceName + ":" + url.PathEscape("{"+strings.Join(matchers, ", ")+"}"), nil
}

func policyFromHeaderValue(headerString string) (string, *LabelPolicy, error) {
	components := strings.SplitN(headerString, ":", 2)
	if len(components) != 2 {
		return "", nil, errInvalidHeaderParse
	}
	selectorString, err := url.PathUnescape(components[1])
	if err != nil {
		return "", nil, fmt.Errorf("%w, '%v'", errInvalidHeaderEscape, err)
	}
	matchers, err := promqlext.NewPromQLParser().ParseMetricSelector(selectorString)
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

// InjectLabelMatchersHTTP takes the provided matcher protobufs and stores them as HTTP headers
func InjectLabelMatchersHTTP(r *http.Request, instancePolicyMap LabelPolicySet) error {
	// Ensure any existing policy sets are erased
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

// ExtractLabelMatchersHTTP takes the provided HTTP request and parses out label matcher protobufs
func ExtractLabelMatchersHTTP(r *http.Request) (LabelPolicySet, error) {
	headerValues := r.Header.Values(HTTPHeaderKey)

	return ExtractLabelMatchersSlice(headerValues)
}

// ExtractLabelMatchersSlice takes a slice of label matcher header values and parses a label policy set from them.
func ExtractLabelMatchersSlice(values []string) (LabelPolicySet, error) {
	instancePolicyMap := LabelPolicySet{}

	// Iterate through each set header value
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

type matcherSource interface {
	GetMatchers() LabelPolicySet
}

type labelMatcherCarrier struct {
	matcher LabelPolicySet
}

func (l *labelMatcherCarrier) GetMatchers() LabelPolicySet {
	return l.matcher
}

// ExtractLabelMatchersContext gets the embedded label matchers from the context
func ExtractLabelMatchersContext(ctx context.Context) (LabelPolicySet, error) {
	source, ok := ctx.Value(contextKey).(matcherSource)

	if !ok {
		return nil, errNoMatcherSource
	}

	return source.GetMatchers(), nil
}

// InjectLabelMatchersContext returns a derived context containing the provided label matchers
func InjectLabelMatchersContext(ctx context.Context, matchers LabelPolicySet) context.Context {
	return context.WithValue(ctx, interface{}(contextKey), &labelMatcherCarrier{matchers})
}
