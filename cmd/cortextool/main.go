package main

import (
	"context"
	"fmt"
	"os"

	"github.com/grafana/ruler_cli/pkg/client"
	"github.com/grafana/ruler_cli/pkg/parser"
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
	app := kingpin.New("rulerclient", "A command-line tool to manage the cortex ruler")
	app.Flag("address", "The address of the ruler server").Required().StringVar(&clientConfig.Address)
	app.Flag("id", "cortex tenant id").Required().StringVar(&clientConfig.ID)
	app.Flag("key", "api key, no basic auth if left blank").Default("").StringVar(&clientConfig.Key)
	app.Flag("debug", "print information useful for debugging the ruler client").BoolVar(&debug)

	listRulegroupsCmd := app.Command("list", "list the rules currently in the cortex ruler")

	getRuleGroupCmd := app.Command("get", "retreive a rulegroup from the ruler")
	namespace := getRuleGroupCmd.Arg("namespace", "namespace of the rulegroup to retrieve").Required().String()
	rulegroup := getRuleGroupCmd.Arg("group name", "name of the rulegroup ot retrieve").Required().String()

	loadRulesCmd := app.Command("load", "load a set of rules to a designated cortex endpoint")
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
