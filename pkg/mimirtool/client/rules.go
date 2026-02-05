// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/grafana/cortex-tools/blob/main/pkg/client/rules.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package client

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/url"

	"github.com/go-kit/log/level"
	"github.com/pkg/errors"
	"go.yaml.in/yaml/v3"

	"github.com/grafana/mimir/pkg/mimirtool/rules/rwrulefmt"
)

// CreateRuleGroup creates a new rule group
func (c *MimirClient) CreateRuleGroup(ctx context.Context, namespace string, rg rwrulefmt.RuleGroup) error {
	payload, err := yaml.Marshal(&rg)
	if err != nil {
		return err
	}

	escapedNamespace := url.PathEscape(namespace)
	path := c.apiPath + "/" + escapedNamespace

	res, err := c.doRequest(ctx, path, "POST", bytes.NewBuffer(payload), int64(len(payload)))
	if err != nil {
		return err
	}

	res.Body.Close()

	return nil
}

// DeleteRuleGroup deletes a rule group
func (c *MimirClient) DeleteRuleGroup(ctx context.Context, namespace, groupName string) error {
	escapedNamespace := url.PathEscape(namespace)
	escapedGroupName := url.PathEscape(groupName)
	path := c.apiPath + "/" + escapedNamespace + "/" + escapedGroupName

	res, err := c.doRequest(ctx, path, "DELETE", nil, -1)
	if err != nil {
		return err
	}

	res.Body.Close()

	return nil
}

// GetRuleGroup retrieves a rule group
func (c *MimirClient) GetRuleGroup(ctx context.Context, namespace, groupName string) (*rwrulefmt.RuleGroup, error) {
	escapedNamespace := url.PathEscape(namespace)
	escapedGroupName := url.PathEscape(groupName)
	path := c.apiPath + "/" + escapedNamespace + "/" + escapedGroupName

	fmt.Println(path)
	res, err := c.doRequest(ctx, path, "GET", nil, -1)
	if err != nil {
		return nil, err
	}

	defer res.Body.Close()
	body, err := io.ReadAll(res.Body)

	if err != nil {
		return nil, err
	}

	rg := rwrulefmt.RuleGroup{}
	err = yaml.Unmarshal(body, &rg)
	if err != nil {
		level.Debug(c.logger).Log("msg", "failed to unmarshal rule group from response", "body", string(body))

		return nil, errors.Wrap(err, "unable to unmarshal response")
	}

	return &rg, nil
}

// ListRules retrieves a rule group
func (c *MimirClient) ListRules(ctx context.Context, namespace string) (map[string][]rwrulefmt.RuleGroup, error) {
	path := c.apiPath
	if namespace != "" {
		path = path + "/" + namespace
	}

	res, err := c.doRequest(ctx, path, "GET", nil, -1)
	if err != nil {
		return nil, err
	}

	defer res.Body.Close()
	body, err := io.ReadAll(res.Body)

	if err != nil {
		return nil, err
	}

	ruleSet := map[string][]rwrulefmt.RuleGroup{}
	err = yaml.Unmarshal(body, &ruleSet)
	if err != nil {
		return nil, err
	}

	return ruleSet, nil
}

// DeleteNamespace delete all the rule groups in a namespace including the namespace itself
func (c *MimirClient) DeleteNamespace(ctx context.Context, namespace string) error {
	escapedNamespace := url.PathEscape(namespace)
	path := c.apiPath + "/" + escapedNamespace

	res, err := c.doRequest(ctx, path, "DELETE", nil, -1)
	if err != nil {
		return err
	}

	res.Body.Close()

	return nil
}
