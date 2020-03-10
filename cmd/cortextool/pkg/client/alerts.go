package client

import (
	"context"
	"fmt"
	"io/ioutil"
	"net/http"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"gopkg.in/yaml.v2"
)

type configCompat struct {
	TemplateFiles      map[string]string `yaml:"template_files"`
	AlertmanagerConfig string            `yaml:"alertmanager_config"`
}

// CreateAlertmanagerConfig creates a new alertmanager config
func (r *CortexClient) CreateAlertmanagerConfig(ctx context.Context, cfg string, templates map[string]string) error {
	payload, err := yaml.Marshal(&configCompat{
		TemplateFiles:      templates,
		AlertmanagerConfig: cfg,
	})
	if err != nil {
		return err
	}

	res, err := r.doRequest("/alertmanager/alerts", "POST", payload)
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
	res, err := r.doRequest("/alertmanager/alerts", "DELETE", nil)
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

	switch res.StatusCode {
	case http.StatusAccepted, http.StatusOK:
		return nil
	case http.StatusNotFound:
		log.Debugln("alertmanager config not found, already deleted")
		return nil
	}

	return fmt.Errorf("error occured, %v", string(body))
}

// GetAlertmanagerConfig retrieves a rule group
func (r *CortexClient) GetAlertmanagerConfig(ctx context.Context) (string, map[string]string, error) {
	res, err := r.doRequest("/alertmanager/alerts", "GET", nil)
	if err != nil {
		log.Debugln("no alert config present in response")
		return "", nil, err
	}

	defer res.Body.Close()
	body, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return "", nil, err
	}

	compat := configCompat{}
	err = yaml.Unmarshal(body, &compat)
	if err != nil {
		log.WithFields(log.Fields{
			"body": string(body),
		}).Debugln("failed to unmarshal rule group from response")

		return "", nil, errors.Wrap(err, "unable to unmarshal response")
	}

	return compat.AlertmanagerConfig, compat.TemplateFiles, nil
}
