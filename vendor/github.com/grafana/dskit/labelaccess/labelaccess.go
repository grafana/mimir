// SPDX-License-Identifier: AGPL-3.0-only

// Package labelaccess implements Label-Based Access Control (LBAC) propagation.
//
// LBAC restricts which time-series a tenant can access by attaching Prometheus
// label-selector policies to requests. A LabelPolicySet maps each tenant ID to
// one or more LabelPolicy values; each LabelPolicy holds a set of matchers that
// are ANDed together, while multiple policies for the same tenant are ORed.
//
// Policies are carried over HTTP using the X-Prom-Label-Policy header and over
// gRPC using the same header name in the gRPC metadata. Helper functions inject
// and extract policies from HTTP requests, from gRPC metadata, and from
// context.Context values.
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
)

var parseMetricSelectorFunc func(input string) (m []*labels.Matcher, err error)

// SetSelectorParser sets the function used to parse metric selector strings from header values.
// It must be called during application initialisation before any labelaccess functions are used.
func SetSelectorParser(f func(string) ([]*labels.Matcher, error)) {
	parseMetricSelectorFunc = f
}

// HTTPHeaderKey is the HTTP (and gRPC metadata) header used to carry label policy values.
const HTTPHeaderKey = "X-Prom-Label-Policy"

type contextKeyType int

const contextKey contextKeyType = 0

var (
	errNoMatcherSource     = errors.New("no label matcher source")
	errInvalidHeaderParse  = errors.New("invalid label policy header value, unable to parse tenant and selector")
	errInvalidHeaderEscape = errors.New("invalid label policy header value unable to url escape selector string")
	errInvalidSelector     = errors.New("invalid label policy header value unable to parse selector string")
)

// LabelPolicy holds a set of Prometheus label matchers representing one access policy selector.
// All matchers within a single policy are ANDed together.
type LabelPolicy struct {
	Selector []*labels.Matcher
}

// LabelPolicySet maps tenant IDs to their label policies.
// Multiple policies for the same tenant are ORed together.
type LabelPolicySet map[string][]*LabelPolicy

// GetLabelPolicySet implements LabelPolicies.
func (l LabelPolicySet) GetLabelPolicySet() (LabelPolicySet, error) {
	return l, nil
}

// Hash returns a deterministic base32-encoded SHA1 hash of all tenants and their label policies,
// iterating in ascending key order. It can be used as a cache-key component to ensure that
// requests with different policies never share cached results.
//
// Delimiters used (none of these can appear in tenant IDs, label names, or label values):
//
//	\x00 separates matchers within a single policy (AND boundary)
//	\x01 separates policies within a tenant (OR boundary)
//	\x02 separates the tenant name from its policies
func (l LabelPolicySet) Hash() string {
	tenants := make([]string, 0, len(l))
	for t := range l {
		tenants = append(tenants, t)
	}
	sort.Strings(tenants)

	h := sha1.New() //nolint:gosec // SHA1 is used for non-cryptographic hashing
	for _, t := range tenants {
		_, _ = h.Write([]byte(t))
		_, _ = h.Write([]byte{'\x02'}) // tenant/policies boundary
		for i, p := range l[t] {
			if i > 0 {
				_, _ = h.Write([]byte{'\x01'}) // policy (OR) boundary
			}
			for j := range p.Selector {
				if j > 0 {
					_, _ = h.Write([]byte{'\x00'}) // matcher (AND) boundary
				}
				_, _ = h.Write([]byte(p.Selector[j].String()))
			}
		}
	}
	return base32.StdEncoding.EncodeToString(h.Sum(nil))
}

// String returns a human-readable representation of the policy set, sorted by tenant ID.
func (l LabelPolicySet) String() string {
	tenants := make([]string, 0, len(l))
	for t := range l {
		tenants = append(tenants, t)
	}
	sort.Strings(tenants)

	var sb strings.Builder
	for _, t := range tenants {
		sb.WriteString(t)
		sb.WriteString(":[")
		for _, p := range l[t] {
			sb.WriteString("{")
			for _, m := range p.Selector {
				sb.WriteString(m.String())
				sb.WriteString(",")
			}
			sb.WriteString("},")
		}
		sb.WriteString("] ")
	}
	return sb.String()
}

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

	for _, policies := range labelPolicySet {
		if len(policies) != 0 {
			if err := InjectLabelMatchersHTTP(req, labelPolicySet); err != nil {
				return fmt.Errorf("unable to inject matchers: %w", err)
			}
			return nil
		}
	}
	return nil
}

// InjectLabelMatchersHTTP encodes the policy set as X-Prom-Label-Policy header values on r.
// Any existing header values are replaced.
func InjectLabelMatchersHTTP(r *http.Request, instancePolicyMap LabelPolicySet) error {
	r.Header.Del(HTTPHeaderKey)

	values, err := InjectLabelMatchersSlice(instancePolicyMap)
	if err != nil {
		return err
	}

	r.Header[HTTPHeaderKey] = values
	return nil
}

// InjectLabelMatchersSlice encodes the policy set as a slice of header value strings.
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

// ExtractLabelMatchersHTTP parses X-Prom-Label-Policy header values from r into a LabelPolicySet.
func ExtractLabelMatchersHTTP(r *http.Request) (LabelPolicySet, error) {
	return ExtractLabelMatchersSlice(r.Header.Values(HTTPHeaderKey))
}

// ExtractLabelMatchersSlice parses a slice of X-Prom-Label-Policy header values into a LabelPolicySet.
func ExtractLabelMatchersSlice(values []string) (LabelPolicySet, error) {
	instancePolicyMap := LabelPolicySet{}
	for _, headerValue := range values {
		for _, v := range strings.Split(headerValue, ",") {
			instanceName, policy, err := policyFromHeaderValue(v)
			if err != nil {
				return nil, err
			}
			instancePolicyMap[instanceName] = append(instancePolicyMap[instanceName], policy)
		}
	}
	return instancePolicyMap, nil
}

// InjectLabelMatchersContext returns a derived context containing matchers.
func InjectLabelMatchersContext(ctx context.Context, matchers LabelPolicySet) context.Context {
	return context.WithValue(ctx, contextKey, &labelMatcherCarrier{matcher: matchers})
}

// ExtractLabelMatchersContext retrieves the label policy set embedded in ctx.
func ExtractLabelMatchersContext(ctx context.Context) (LabelPolicySet, error) {
	source, ok := ctx.Value(contextKey).(matcherSource)
	if !ok {
		return nil, errNoMatcherSource
	}
	return source.GetMatchers(), nil
}

// --- internal helpers ---

type matcherSource interface {
	GetMatchers() LabelPolicySet
}

type labelMatcherCarrier struct {
	matcher LabelPolicySet
}

func (l *labelMatcherCarrier) GetMatchers() LabelPolicySet {
	return l.matcher
}

func policyToHeaderValue(instanceName string, policy *LabelPolicy) (string, error) {
	matchers := make([]string, len(policy.Selector))
	for i, m := range policy.Selector {
		matchers[i] = m.String()
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

	if parseMetricSelectorFunc == nil {
		panic("labelaccess: SetSelectorParser must be called before using labelaccess functions")
	}
	matchers, err := parseMetricSelectorFunc(selectorString)
	if err != nil {
		return "", nil, fmt.Errorf("%w '%s', error: '%v'", errInvalidSelector, selectorString, err)
	}

	for _, m := range matchers {
		if m.Name == "" {
			return "", nil, fmt.Errorf("%w '%s', error: 'a label name was empty'", errInvalidSelector, selectorString)
		}
	}

	return components[0], &LabelPolicy{Selector: matchers}, nil
}
