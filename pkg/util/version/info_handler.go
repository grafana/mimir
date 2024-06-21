// SPDX-License-Identifier: AGPL-3.0-only

package version

import (
	"net/http"

	"github.com/grafana/mimir/pkg/util"
)

type BuildInfoResponse struct {
	Status    string    `json:"status"`
	BuildInfo BuildInfo `json:"data"`
}

type BuildInfo struct {
	Application string      `json:"application"`
	Version     string      `json:"version"`
	Revision    string      `json:"revision"`
	Branch      string      `json:"branch"`
	GoVersion   string      `json:"goVersion"`
	Features    interface{} `json:"features"`
}

type BuildInfoFeatures struct {
	RulerConfigAPI        string `json:"ruler_config_api,omitempty"`
	AlertmanagerConfigAPI string `json:"alertmanager_config_api,omitempty"`
	QuerySharding         string `json:"query_sharding,omitempty"`
	FederatedRules        string `json:"federated_rules,omitempty"`
}

func BuildInfoHandler(application string, features interface{}) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		response := BuildInfoResponse{
			Status: "success",
			BuildInfo: BuildInfo{
				Application: application,
				Version:     Version,
				Revision:    Revision,
				Branch:      Branch,
				GoVersion:   GoVersion,
				Features:    features,
			},
		}

		util.WriteJSONResponse(w, response)
	})
}
