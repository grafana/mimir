package client

import (
	"context"
	"fmt"
	"io/ioutil"

	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/pkg/rulefmt"
	log "github.com/sirupsen/logrus"
	"gopkg.in/yaml.v2"
)

// CreateRuleGroup creates a new rule group
func (r *CortexClient) CreateRuleGroup(ctx context.Context, namespace string, rg rulefmt.RuleGroup) error {
	payload, err := yaml.Marshal(&rg)
	if err != nil {
		return err
	}

	res, err := r.doRequest("/api/prom/rules/"+namespace, "POST", payload)
	if err != nil {
		return err
	}

	defer res.Body.Close()

	return nil
}

// DeleteRuleGroup creates a new rule group
func (r *CortexClient) DeleteRuleGroup(ctx context.Context, namespace, groupName string) error {
	res, err := r.doRequest("/api/prom/rules/"+namespace, "DELETE", nil)
	if err != nil {
		return err
	}

	defer res.Body.Close()
	body, err := ioutil.ReadAll(res.Body)

	if err != nil {
		return err
	}

	if res.StatusCode%2 > 0 {
		return fmt.Errorf("error occured, %v", string(body))
	}

	return nil
}

// GetRuleGroup retrieves a rule group
func (r *CortexClient) GetRuleGroup(ctx context.Context, namespace, groupName string) (*rulefmt.RuleGroup, error) {
	res, err := r.doRequest(fmt.Sprintf("/api/prom/rules/%s/%s", namespace, groupName), "GET", nil)
	if err != nil {
		return nil, err
	}

	defer res.Body.Close()
	body, err := ioutil.ReadAll(res.Body)

	if err != nil {
		return nil, err
	}

	rg := rulefmt.RuleGroup{}
	err = yaml.Unmarshal(body, &rg)
	if err != nil {
		log.WithFields(log.Fields{
			"body": string(body),
		}).Debugln("failed to unmarshal rule group from response")

		return nil, errors.Wrap(err, "unable to unmarshal response")
	}

	return &rg, nil
}

// ListRules retrieves a rule group
func (r *CortexClient) ListRules(ctx context.Context, namespace string) (map[string][]rulefmt.RuleGroup, error) {
	path := "/api/prom/rules"
	if namespace != "" {
		path = path + "/" + namespace
	}

	res, err := r.doRequest(path, "GET", nil)
	if err != nil {
		return nil, err
	}

	defer res.Body.Close()
	body, err := ioutil.ReadAll(res.Body)

	if err != nil {
		return nil, err
	}

	ruleSet := map[string][]rulefmt.RuleGroup{}
	err = yaml.Unmarshal(body, &ruleSet)
	if err != nil {
		return nil, err
	}

	return ruleSet, nil
}
