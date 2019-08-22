package client

import (
	"context"
	"fmt"
	"io/ioutil"

	"github.com/pkg/errors"
	"github.com/prometheus/alertmanager/config"
	log "github.com/sirupsen/logrus"
	"gopkg.in/yaml.v2"
)

type configCompat struct {
	TemplateFiles      map[string]string `yaml:"template_files"`
	AlertmanagerConfig config.Config     `yaml:"alertmanager_config"`
}

// CreateAlertmanagerConfig creates a new alertmanager config
func (r *CortexClient) CreateAlertmanagerConfig(ctx context.Context, cfg config.Config, templates map[string]string) error {
	payload, err := yaml.Marshal(&configCompat{
		TemplateFiles:      templates,
		AlertmanagerConfig: cfg,
	})
	if err != nil {
		return err
	}

	res, err := r.doRequest("/api/alerts", "POST", payload)
	if err != nil {
		return err
	}

	defer res.Body.Close()
	err = checkResponse(res)
	if err != nil {
		return err
	}

	return nil
}

// DeleteAlermanagerConfig deletes the users alertmanagerconfig
func (r *CortexClient) DeleteAlermanagerConfig(ctx context.Context) error {
	res, err := r.doRequest("/api/alerts", "DELETE", nil)
	if err != nil {
		return err
	}

	defer res.Body.Close()
	err = checkResponse(res)
	if err != nil {
		return err
	}
	body, err := ioutil.ReadAll(res.Body)

	if err != nil {
		return err
	}

	if res.StatusCode%2 > 0 {
		return fmt.Errorf("error occured, %v", string(body))
	}

	return nil
}

// GetAlertmanagerConfig retrieves a rule group
func (r *CortexClient) GetAlertmanagerConfig(ctx context.Context) (*config.Config, map[string]string, error) {
	res, err := r.doRequest("/api/alerts", "GET", nil)
	if err != nil {
		return nil, nil, err
	}

	defer res.Body.Close()
	err = checkResponse(res)
	if err != nil {
		return nil, nil, err
	}

	body, err := ioutil.ReadAll(res.Body)

	if err != nil {
		return nil, nil, err
	}

	compat := configCompat{}
	err = yaml.Unmarshal(body, &compat)
	if err != nil {
		log.WithFields(log.Fields{
			"body": string(body),
		}).Debugln("failed to unmarshal rule group from response")

		return nil, nil, errors.Wrap(err, "unable to unmarshal response")
	}

	return &compat.AlertmanagerConfig, compat.TemplateFiles, nil
}
