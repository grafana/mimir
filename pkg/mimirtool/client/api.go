// SPDX-License-Identifier: AGPL-3.0-only

package client

import (
	"github.com/prometheus/client_golang/api"
	"github.com/prometheus/common/config"
	v1 "github.com/prometheus/client_golang/api/prometheus/v1"
)

func NewAPI(address, username, password string) (v1.API, error) {
	rt := api.DefaultRoundTripper
	rt = config.NewUserAgentRoundTripper(UserAgent, rt)
	if username != "" {
		rt = config.NewBasicAuthRoundTripper(username, config.Secret(password), "", rt)
	}

	client, err := api.NewClient(api.Config{
		Address:      address,
		RoundTripper: rt,
	})
	if err != nil {
		return nil, err
	}

	return v1.NewAPI(client), nil
}
