// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/grafana/cortex-tools/blob/main/pkg/client/alerts.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package client

import (
	"bytes"
	"context"
	"io"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"gopkg.in/yaml.v3"
)

const alertmanagerAPIPath = "/api/v1/alerts"

type configCompat struct {
	TemplateFiles      map[string]string `yaml:"template_files"`
	AlertmanagerConfig string            `yaml:"alertmanager_config"`
}

// CreateAlertmanagerConfig creates a new alertmanager config
func (r *MimirClient) CreateAlertmanagerConfig(ctx context.Context, cfg string, templates map[string]string) error {
	payload, err := yaml.Marshal(&configCompat{
		TemplateFiles:      templates,
		AlertmanagerConfig: cfg,
	})
	if err != nil {
		return err
	}

	res, err := r.doRequest(alertmanagerAPIPath, "POST", bytes.NewBuffer(payload), int64(len(payload)))
	if err != nil {
		return err
	}

	res.Body.Close()

	return nil
}

// DeleteAlermanagerConfig deletes the users alertmanagerconfig
func (r *MimirClient) DeleteAlermanagerConfig(ctx context.Context) error {
	res, err := r.doRequest(alertmanagerAPIPath, "DELETE", nil, -1)
	if err != nil {
		return err
	}

	res.Body.Close()

	return nil
}

// GetAlertmanagerConfig retrieves a Mimir cluster's Alertmanager config.
func (r *MimirClient) GetAlertmanagerConfig(ctx context.Context) (string, map[string]string, error) {
	res, err := r.doRequest(alertmanagerAPIPath, "GET", nil, -1)
	if err != nil {
		log.Debugln("no alert config present in response")
		return "", nil, err
	}

	defer res.Body.Close()
	body, err := io.ReadAll(res.Body)
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
