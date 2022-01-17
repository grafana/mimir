package commands

import (
	"fmt"
	"net/url"
	"strings"

	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/promql/parser"
	"gopkg.in/alecthomas/kingpin.v2"
)

// AccessControlCommand is the kingpin command for ACLs.
type AccessControlCommand struct {
	InstanceID string
	ACLs       []string
}

// Register is used to register the command to a parent command.
func (a *AccessControlCommand) Register(app *kingpin.Application) {
	aclCmd := app.Command("acl", "Generate and view ACL options in GrafanaCloud.")

	generateHeaderCmd := aclCmd.Command("generate-header", "Generate the header that needs to be passed to the datasource for setting ACLs.").Action(a.generateHeader)
	generateHeaderCmd.Flag("id", "Cortex tenant ID, alternatively set CORTEX_TENANT_ID.").Envar("CORTEX_TENANT_ID").Required().StringVar(&a.InstanceID)
	generateHeaderCmd.Flag("rule", "The access control rules (Prometheus selectors). Set it multiple times to set multiple rules.").Required().StringsVar(&a.ACLs)
}

func (a *AccessControlCommand) generateHeader(k *kingpin.ParseContext) error {
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
