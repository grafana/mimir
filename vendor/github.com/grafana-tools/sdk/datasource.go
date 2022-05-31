package sdk

import (
	"encoding/json"
)

/*
   Copyright 2016 Alexander I.Grafov <grafov@gmail.com>
   Copyright 2016-2019 The Grafana SDK authors

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

	   http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

   ॐ तारे तुत्तारे तुरे स्व
*/

// Datasource as described in the doc
// http://docs.grafana.org/reference/http_api/#get-all-datasources
type Datasource struct {
	ID                uint        `json:"id"`
	OrgID             uint        `json:"orgId"`
	UID               string      `json:"uid"`
	Name              string      `json:"name"`
	Type              string      `json:"type"`
	TypeLogoURL       string      `json:"typeLogoUrl"`
	Access            string      `json:"access"` // direct or proxy
	URL               string      `json:"url"`
	Password          *string     `json:"password,omitempty"`
	User              *string     `json:"user,omitempty"`
	Database          *string     `json:"database,omitempty"`
	BasicAuth         *bool       `json:"basicAuth,omitempty"`
	ReadOnly          *bool       `json:"readOnly,omitempty"`
	BasicAuthUser     *string     `json:"basicAuthUser,omitempty"`
	BasicAuthPassword *string     `json:"basicAuthPassword,omitempty"`
	IsDefault         bool        `json:"isDefault"`
	JSONData          interface{} `json:"jsonData"`
	SecureJSONData    interface{} `json:"secureJsonData"`
}

// Datasource type as described in
// http://docs.grafana.org/reference/http_api/#available-data-source-types
type DatasourceType struct {
	Metrics  bool   `json:"metrics"`
	Module   string `json:"module"`
	Name     string `json:"name"`
	Partials struct {
		Query string `json:"query"`
	} `json:"datasource"`
	PluginType  string `json:"pluginType"`
	ServiceName string `json:"serviceName"`
	Type        string `json:"type"`
}

// DatasourceRef is used to reference a datasource from panels, queries, etc.
type DatasourceRef struct {
	// Type describes the type of the datasource, like "prometheus", "graphite", etc.
	// Datasources of the same type should support same queries.
	// If Type is empty in an unmarshaled DatasourceRef, check the LegacyName field.
	Type string `json:"type"`
	// UID is the uid of the specific datasource this references to.
	UID string `json:"UID"`
	// LegacyName is the old way of referencing a datasource by its name, replaced in Grafana v8.4.3 by Type and UID referencing.
	// If datasource is encoded as a string, then it's unmarshaled into this LegacyName field (Type and UID will be empty).
	// If LegacyName is not empty, then this DatasourceRef will be marshaled as a string, ignoring the values of Type and UID.
	LegacyName string `json:"-"`
}

func (ref DatasourceRef) MarshalJSON() ([]byte, error) {
	if ref.LegacyName != "" {
		return json.Marshal(ref.LegacyName)
	}
	type plain DatasourceRef
	return json.Marshal(plain(ref))
}

func (ref *DatasourceRef) UnmarshalJSON(data []byte) error {
	type plain DatasourceRef
	err := json.Unmarshal(data, (*plain)(ref))
	if err != nil {
		if err := json.Unmarshal(data, &ref.LegacyName); err == nil {
			// We could check here if it's `-- Mixed --` and in that case set ref.Type="mixed".
			return nil
		}
	}
	return err
}
