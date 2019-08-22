package commands

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/prometheus/alertmanager/config"

	"github.com/grafana/cortex-tool/pkg/client"
	log "github.com/sirupsen/logrus"
	"gopkg.in/alecthomas/kingpin.v2"
)

// AlertCommand configures and executes rule related cortex api operations
type AlertCommand struct {
	ClientConfig           client.Config
	AlertmanagerConfigFile string
	TemplateFiles          []string

	cli *client.CortexClient
}

// Register rule related commands and flags with the kingpin application
func (a *AlertCommand) Register(app *kingpin.Application) {
	alertCmd := app.Command("alerts", "View & edit alerts stored in cortex.").PreAction(a.setup)
	alertCmd.Flag("address", "Address of the cortex cluster, alternatively set GRAFANACLOUD_ADDRESS.").Envar("GRAFANACLOUD_ADDRESS").Required().StringVar(&a.ClientConfig.Address)
	alertCmd.Flag("id", "Cortex tenant id, alternatively set GRAFANACLOUD_INSTANCE_ID.").Envar("GRAFANACLOUD_INSTANCE_ID").Required().StringVar(&a.ClientConfig.ID)
	alertCmd.Flag("key", "Api key to use when contacting cortex, alternatively set $GRAFANACLOUD_API_KEY.").Default("").Envar("GRAFANACLOUD_API_KEY").StringVar(&a.ClientConfig.Key)

	// List Rules Command
	alertCmd.Command("get", "List the rules currently in the cortex ruler.").Action(a.getConfig)

	loadalertCmd := alertCmd.Command("load", "load a set of rules to a designated cortex endpoint").Action(a.loadConfig)
	loadalertCmd.Arg("config", "alertmanager configuration to load").Required().StringVar(&a.AlertmanagerConfigFile)
	loadalertCmd.Arg("template-files", "The template files to load").ExistingFilesVar(&a.TemplateFiles)
}

func (a *AlertCommand) setup(k *kingpin.ParseContext) error {
	cli, err := client.New(a.ClientConfig)
	if err != nil {
		return err
	}
	a.cli = cli

	return nil
}

func (a *AlertCommand) getConfig(k *kingpin.ParseContext) error {
	cfg, templates, err := a.cli.GetAlertmanagerConfig(context.Background())
	if err != nil {
		log.Fatalf("unable to read rules from cortex, %v", err)
	}
	d, err := json.Marshal(&cfg)
	if err != nil {
		return err
	}
	fmt.Printf("---\n%s\n", string(d))

	for fn, template := range templates {
		fmt.Println(fn)
		fmt.Println(template)
	}

	return nil
}

func (a *AlertCommand) loadConfig(k *kingpin.ParseContext) error {
	cfg, _, err := config.LoadFile(a.AlertmanagerConfigFile)
	if err != nil {
		return err
	}

	return a.cli.CreateAlertmanagerConfig(context.Background(), *cfg, nil)
}
