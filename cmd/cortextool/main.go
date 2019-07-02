package main

import (
	"context"
	"fmt"
	"os"

	"github.com/grafana/cortex-tools/pkg/client"
	"github.com/grafana/cortex-tools/pkg/parser"
	log "github.com/sirupsen/logrus"
	"gopkg.in/alecthomas/kingpin.v2"
	"gopkg.in/yaml.v2"
)

var (
	clientConfig client.Config

	debug bool
)

func main() {
	kingpin.Version("0.0.1")
	app := kingpin.New("cortex-tool", "A command-line tool to manage cortex configs.")
	app.Flag("address", "Address of the cortex cluster, alternatively set GRAFANACLOUD_ADDRESS.").Envar("GRAFANACLOUD_ADDRESS").Required().StringVar(&clientConfig.Address)
	app.Flag("id", "Cortex tenant id, alternatively set GRAFANACLOUD_INSTANCE_ID.").Envar("GRAFANACLOUD_INSTANCE_ID").Required().StringVar(&clientConfig.ID)
	app.Flag("key", "Api key to use when contacting cortex, alternatively set $GRAFANACLOUD_API_KEY.").Default("").Envar("GRAFANACLOUD_API_KEY").StringVar(&clientConfig.Key)
	app.Flag("debug", "Print information useful for debugging the ruler client.").BoolVar(&debug)
	rulesCmd := app.Command("rules", "View & edit rules stored in cortex.")
	listRulegroupsCmd := rulesCmd.Command("list", "List the rules currently in the cortex ruler.")
	getRuleGroupCmd := rulesCmd.Command("get", "Retreive a rulegroup from the ruler.")
	namespace := getRuleGroupCmd.Arg("namespace", "Namespace of the rulegroup to retrieve.").Required().String()
	rulegroup := getRuleGroupCmd.Arg("group", "Name of the rulegroup ot retrieve.").Required().String()
	loadRulesCmd := rulesCmd.Command("load", "load a set of rules to a designated cortex endpoint")
	ruleFiles := loadRulesCmd.Arg("rule-files", "The rule files to check.").Required().ExistingFiles()
	command := kingpin.MustParse(app.Parse(os.Args[1:]))

	if debug {
		log.SetLevel(log.DebugLevel)
	}

	cli, err := client.New(clientConfig)
	if err != nil {
		log.Fatalln(err)
	}

	switch command {
	case listRulegroupsCmd.FullCommand():
		rules, err := cli.ListRules(context.Background(), "")
		if err != nil {
			log.Fatalf("unable to read rules from cortex, %v", err)
		}
		d, err := yaml.Marshal(&rules)
		if err != nil {
			log.Fatalf("error: %v", err)
		}
		fmt.Printf("---\n%s\n", string(d))
	case getRuleGroupCmd.FullCommand():
		rg, err := cli.GetRuleGroup(context.Background(), *namespace, *rulegroup)
		if err != nil {
			log.Fatalf("unable to read rules from cortex, %v", err)
		}
		d, err := yaml.Marshal(&rg)
		if err != nil {
			log.Fatalf("error: %v", err)
		}
		fmt.Printf("---\n%s\n", string(d))
	case loadRulesCmd.FullCommand():
		nss, err := parser.ParseFiles(*ruleFiles)
		if err != nil {
			log.WithError(err).Fatalf("unable to parse rules files")
		}
		for _, ns := range nss {
			for _, rg := range ns.Groups {
				err := cli.CreateRuleGroup(context.Background(), ns.Namespace, rg)
				if err != nil {
					log.WithError(err).WithFields(log.Fields{
						"group":     rg.Name,
						"namespace": ns,
					}).Fatalf("unable to load rule group")
				}
			}
		}
	}
}
