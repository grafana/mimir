// SPDX-License-Identifier: AGPL-3.0-only

package purger

import (
	"flag"
)

type DeleteStoreConfig struct {
	Store             string                  `yaml:"store"`
	RequestsTableName string                  `yaml:"requests_table_name"`
	ProvisionConfig   TableProvisioningConfig `yaml:"table_provisioning"`
}

func (cfg *DeleteStoreConfig) RegisterFlags(f *flag.FlagSet) {
	cfg.ProvisionConfig.RegisterFlags("deletes.table", f)
	f.StringVar(&cfg.Store, "deletes.store", "", "Store for keeping delete request")
	f.StringVar(&cfg.RequestsTableName, "deletes.requests-table-name", "delete_requests", "Name of the table which stores delete requests")
}
