// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/grafana/cortex-tools/blob/main/pkg/commands/access_control.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package commands

import (
	"fmt"
	"net/url"
	"strings"

	"github.com/alecthomas/kingpin/v2"
	"github.com/pkg/errors"

	"github.com/grafana/mimir/pkg/mimirtool/config"
)

// AccessControlCommand is the kingpin command for ACLs.
type AccessControlCommand struct {
	InstanceID string
	ACLs       []string
}

// Register is used to register the command to a parent command.
func (a *AccessControlCommand) Register(app *kingpin.Application, envVars EnvVarNames) {
	aclCmd := app.Command("acl", "Generate and view ACL options in GrafanaCloud.")

	generateHeaderCmd := aclCmd.Command("generate-header", "Generate the header that needs to be passed to the datasource for setting ACLs.").Action(a.generateHeader)
	generateHeaderCmd.Flag("id", "Grafana Mimir tenant ID, alternatively set "+envVars.TenantID+".").Envar(envVars.TenantID).Required().StringVar(&a.InstanceID)
	generateHeaderCmd.Flag("rule", "The access control rules (Prometheus selectors). Set it multiple times to set multiple rules.").Required().StringsVar(&a.ACLs)
}

func (a *AccessControlCommand) generateHeader(_ *kingpin.ParseContext) error {
	parser := config.CreateParser()

	for _, acl := range a.ACLs {
		_, err := parser.ParseMetricSelector(acl)
		if err != nil {
			return errors.Wrapf(err, "cant parse metric selector for: %s", acl)
		}
	}

	headerValues := []string{}
	for _, acl := range a.ACLs {
		headerValues = append(headerValues, fmt.Sprintf("%s:%s", a.InstanceID, url.PathEscape(acl)))
	}

	fmt.Println("The header to set:")
	fmt.Println("X-Prom-Label-Policy:", strings.Join(headerValues, ","))

	return nil
}
