// SPDX-License-Identifier: AGPL-3.0-only

package ruler

import (
	"context"
	"net/http"
	"net/url"
	"path/filepath"
	"strings"

	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/rules"
)

// Headers identifying the rule being evaluated. Name, type, and source share their names with the
// headers Grafana attaches to queries issued by Grafana-managed alert rules; the source tells the
// two apart. Namespace and group are Mimir concepts with no Grafana equivalent.
const (
	ruleNameHeader      = "X-Rule-Name"
	ruleTypeHeader      = "X-Rule-Type"
	ruleSourceHeader    = "X-Rule-Source"
	ruleNamespaceHeader = "X-Rule-Namespace"
	ruleGroupHeader     = "X-Rule-Group"

	ruleSourceMimirRuler = "mimir-ruler"

	// maxRuleDetailHeaderValueLen bounds the size of each header value, because rule, group, and
	// namespace names are tenant-supplied. It matches the limit Grafana applies to its rule headers.
	maxRuleDetailHeaderValueLen = 128
)

// WithRuleDetailMiddleware attaches headers identifying the rule being evaluated to the outgoing
// request, so that queries issued by the ruler can be attributed downstream, for example in the
// query-frontend query stats logs. It's a no-op for queries not issued by a rule evaluation, such
// as the ones restoring the alerts' for state.
func WithRuleDetailMiddleware(ctx context.Context, req *http.Request) error {
	detail := rules.FromOriginContext(ctx)
	if detail.Name == "" {
		return nil
	}

	setRuleDetailHeader(req, ruleNameHeader, detail.Name)
	setRuleDetailHeader(req, ruleTypeHeader, detail.Kind)
	setRuleDetailHeader(req, ruleSourceHeader, ruleSourceMimirRuler)

	// The group's file is the path of the rule file the ruler writes to disk, named after the
	// path-escaped namespace (see mapper.MapRules), so the escaped name never contains a separator.
	if origin, ok := ctx.Value(promql.QueryOrigin{}).(map[string]any); ok {
		if group, ok := origin["ruleGroup"].(map[string]string); ok {
			if file := group["file"]; file != "" {
				if namespace, err := url.PathUnescape(filepath.Base(file)); err == nil {
					setRuleDetailHeader(req, ruleNamespaceHeader, namespace)
				}
			}
			setRuleDetailHeader(req, ruleGroupHeader, group["name"])
		}
	}

	return nil
}

// setRuleDetailHeader sets the header to a tenant-supplied value. Control characters are stripped
// because the HTTP client rejects them in header values, and the httpgrpc transport copies values
// unvalidated. The value is truncated to maxRuleDetailHeaderValueLen bytes, and omitted if empty.
func setRuleDetailHeader(req *http.Request, name, value string) {
	value = strings.Map(func(r rune) rune {
		if r < 0x20 || r == 0x7f {
			return -1
		}
		return r
	}, value)

	if len(value) > maxRuleDetailHeaderValueLen {
		value = strings.ToValidUTF8(value[:maxRuleDetailHeaderValueLen], "")
	}

	if value == "" {
		return
	}
	req.Header.Set(name, value)
}
