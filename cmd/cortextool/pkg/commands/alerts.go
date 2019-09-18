package commands

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"

	"github.com/alecthomas/chroma/quick"
	"github.com/grafana/cortex-tool/pkg/client"
	"github.com/prometheus/alertmanager/config"
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
	alertCmd := app.Command("alertmanager", "View & edit alertmanager configs stored in cortex.").PreAction(a.setup)
	alertCmd.Flag("address", "Address of the cortex cluster, alternatively set CORTEX_ADDRESS.").Envar("CORTEX_ADDRESS").Required().StringVar(&a.ClientConfig.Address)
	alertCmd.Flag("id", "Cortex tenant id, alternatively set CORTEX_TENTANT_ID.").Envar("CORTEX_TENTANT_ID").Required().StringVar(&a.ClientConfig.ID)
	alertCmd.Flag("key", "Api key to use when contacting cortex, alternatively set CORTEX_API_KEY.").Default("").Envar("CORTEX_API_KEY").StringVar(&a.ClientConfig.Key)

	// List Rules Command
	alertCmd.Command("get", "Get the alertmanager config currently in the cortex alertmanager.").Action(a.getConfig)

	alertCmd.Command("delete", "Delete the alertmanager config currently in the cortex alertmanager.").Action(a.deleteConfig)

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
		return err
	}

	err = quick.Highlight(os.Stdout, cfg, "yaml", "terminal", "swapoff")
	if err != nil {
		return err
	}

	// fmt.Println(cfg)
	for fn, template := range templates {
		fmt.Println(fn)
		fmt.Println(template)
	}

	return nil
}

func (a *AlertCommand) loadConfig(k *kingpin.ParseContext) error {
	content, err := ioutil.ReadFile(a.AlertmanagerConfigFile)
	if err != nil {
		return err
	}

	cfg := string(content)
	_, err = config.Load(cfg)
	if err != nil {
		return err
	}

	return a.cli.CreateAlertmanagerConfig(context.Background(), cfg, nil)
}

func (a *AlertCommand) deleteConfig(k *kingpin.ParseContext) error {
	return a.cli.DeleteAlermanagerConfig(context.Background())
}
