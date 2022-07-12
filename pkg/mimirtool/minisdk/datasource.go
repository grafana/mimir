// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/grafana-tools/sdk/blob/master/datasource.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: 2016 Alexander I.Grafov <grafov@gmail.com>.
// Provenance-includes-copyright: 2016-2019 The Grafana SDK authors

package minisdk

import (
	"encoding/json"
)

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
