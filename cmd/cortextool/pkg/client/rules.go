package client

import (
	"context"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"

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

	escapedNamespace := url.PathEscape(namespace)
	res, err := r.doRequest("/api/prom/rules/"+escapedNamespace, "POST", payload)
	if err != nil {
		return err
	}

	defer res.Body.Close()

	return nil
}

// DeleteRuleGroup creates a new rule group
func (r *CortexClient) DeleteRuleGroup(ctx context.Context, namespace, groupName string) error {
	escapedNamespace := url.PathEscape(namespace)
	escapedGroupName := url.PathEscape(groupName)

	res, err := r.doRequest("/api/prom/rules/"+escapedNamespace+"/"+escapedGroupName, "DELETE", nil)
	if err != nil {
		return err
	}

	defer res.Body.Close()
	body, err := ioutil.ReadAll(res.Body)

	if err != nil {
		return err
	}

	switch res.StatusCode {
	case http.StatusAccepted, http.StatusOK:
		return nil
	case http.StatusNotFound:
		log.Debugln("alertmanager config not found, already deleted")
		return nil
	}

	return fmt.Errorf("error occured, %v", string(body))
}

// GetRuleGroup retrieves a rule group
func (r *CortexClient) GetRuleGroup(ctx context.Context, namespace, groupName string) (*rulefmt.RuleGroup, error) {
	escapedNamespace := url.PathEscape(namespace)
	escapedGroupName := url.PathEscape(groupName)
	path := "/api/prom/rules/" + escapedNamespace + "/" + escapedGroupName

	log.WithFields(log.Fields{
		"url": path,
	}).Debugln("path built to request rule group")

	res, err := r.doRequest(path, "GET", nil)
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
