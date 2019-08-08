package commands

import (
	"context"
	"fmt"

	"github.com/pkg/errors"

	"github.com/grafana/cortex-tool/pkg/client"
	"github.com/grafana/cortex-tool/pkg/rules"
	log "github.com/sirupsen/logrus"
	"gopkg.in/alecthomas/kingpin.v2"
	"gopkg.in/yaml.v2"
)

// RuleCommand configures and executes rule related cortex api operations
type RuleCommand struct {
	ClientConfig client.Config

	cli *client.RulerClient

	// Get Rule Groups Configs
	Namespace string
	RuleGroup string

	// Load Rules Configs
	RuleFiles []string
}

// Register rule related commands and flags with the kingpin application
func (r *RuleCommand) Register(app *kingpin.Application) {
	rulesCmd := app.Command("rules", "View & edit rules stored in cortex.").PreAction(r.registerClient)
	rulesCmd.Flag("address", "Address of the cortex cluster, alternatively set GRAFANACLOUD_ADDRESS.").Envar("GRAFANACLOUD_ADDRESS").Required().StringVar(&r.ClientConfig.Address)
	rulesCmd.Flag("id", "Cortex tenant id, alternatively set GRAFANACLOUD_INSTANCE_ID.").Envar("GRAFANACLOUD_INSTANCE_ID").Required().StringVar(&r.ClientConfig.ID)
	rulesCmd.Flag("key", "Api key to use when contacting cortex, alternatively set $GRAFANACLOUD_API_KEY.").Default("").Envar("GRAFANACLOUD_API_KEY").StringVar(&r.ClientConfig.Key)

	// List Rules Command
	rulesCmd.Command("list", "List the rules currently in the cortex ruler.").Action(r.listRules)

	// Get RuleGroup Command
	getRuleGroupCmd := rulesCmd.Command("get", "Retreive a rulegroup from the ruler.").Action(r.getRuleGroup)
	getRuleGroupCmd.Arg("namespace", "Namespace of the rulegroup to retrieve.").Required().StringVar(&r.Namespace)
	getRuleGroupCmd.Arg("group", "Name of the rulegroup ot retrieve.").Required().StringVar(&r.RuleGroup)

	loadRulesCmd := rulesCmd.Command("load", "load a set of rules to a designated cortex endpoint").Action(r.loadRules)
	loadRulesCmd.Arg("rule-files", "The rule files to check.").Required().ExistingFilesVar(&r.RuleFiles)
}

func (r *RuleCommand) registerClient(k *kingpin.ParseContext) error {
	var err error
	r.cli, err = client.New(r.ClientConfig)
	return err
}

func (r *RuleCommand) listRules(k *kingpin.ParseContext) error {
	rules, err := r.cli.ListRules(context.Background(), "")
	if err != nil {
		log.Fatalf("unable to read rules from cortex, %v", err)
	}
	d, err := yaml.Marshal(&rules)
	if err != nil {
		return err
	}
	fmt.Printf("---\n%s\n", string(d))

	return nil
}

func (r *RuleCommand) getRuleGroup(k *kingpin.ParseContext) error {
	group, err := r.cli.GetRuleGroup(context.Background(), r.Namespace, r.RuleGroup)
	if err != nil {
		log.Fatalf("unable to read rules from cortex, %v", err)
	}
	d, err := yaml.Marshal(&group)
	if err != nil {
		log.Fatalf("error: %v", err)
	}
	fmt.Printf("---\n%s\n", string(d))

	return nil
}

func (r *RuleCommand) loadRules(k *kingpin.ParseContext) error {
	nss, err := rules.ParseFiles(r.RuleFiles)
	if err != nil {
		log.WithError(err).Fatalf("unable to parse rules files")
	}

	for _, ns := range nss {
		for _, group := range ns.Groups {
			curGroup, err := r.cli.GetRuleGroup(context.Background(), ns.Namespace, group.Name)
			if err != nil && err != client.ErrResourceNotFound {
				return errors.Wrap(err, "unable to contact cortex api")
			}
			if curGroup != nil {
				err = rules.CompareGroups(*curGroup, group)
				if err == nil {
					log.WithFields(log.Fields{
						"group":     group.Name,
						"namespace": ns.Namespace,
					}).Infof("group already exists")
					continue
				}
			}

			err = r.cli.CreateRuleGroup(context.Background(), ns.Namespace, group)
			if err != nil {
				log.WithError(err).WithFields(log.Fields{
					"group":     group.Name,
					"namespace": ns.Namespace,
				}).Fatalf("unable to load rule group")
			}
		}
	}

	return nil
}
