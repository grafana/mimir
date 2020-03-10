package commands

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"text/tabwriter"

	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/grafana/cortextool/pkg/client"
	"github.com/grafana/cortextool/pkg/printer"
	"github.com/grafana/cortextool/pkg/rules"
	log "github.com/sirupsen/logrus"
	"gopkg.in/alecthomas/kingpin.v2"
)

var (
	ruleLoadTimestamp = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "cortex",
		Name:      "last_rule_load_timestamp_seconds",
		Help:      "The timestamp of the last rule load.",
	})
	ruleLoadSuccessTimestamp = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "cortex",
		Name:      "last_rule_load_success_timestamp_seconds",
		Help:      "The timestamp of the last successful rule load.",
	})
)

// RuleCommand configures and executes rule related cortex api operations
type RuleCommand struct {
	ClientConfig client.Config

	cli *client.CortexClient

	// Get Rule Groups Configs
	Namespace string
	RuleGroup string

	// Load Rules Configs
	RuleFilesList []string
	RuleFiles     string
	RuleFilesPath string

	// Sync/Diff Rules Config
	IgnoredNamespaces    string
	ignoredNamespacesMap map[string]struct{}

	DisableColor bool
}

// Register rule related commands and flags with the kingpin application
func (r *RuleCommand) Register(app *kingpin.Application) {
	rulesCmd := app.Command("rules", "View & edit rules stored in cortex.").PreAction(r.setup)
	rulesCmd.Flag("address", "Address of the cortex cluster, alternatively set CORTEX_ADDRESS.").Envar("CORTEX_ADDRESS").Required().StringVar(&r.ClientConfig.Address)
	rulesCmd.Flag("id", "Cortex tenant id, alternatively set CORTEX_TENANT_ID.").Envar("CORTEX_TENANT_ID").Required().StringVar(&r.ClientConfig.ID)
	rulesCmd.Flag("key", "Api key to use when contacting cortex, alternatively set $CORTEX_API_KEY.").Default("").Envar("CORTEX_API_KEY").StringVar(&r.ClientConfig.Key)

	// List Rules Command
	rulesCmd.Command("list", "List the rules currently in the cortex ruler.").Action(r.listRules)

	// Print Rules Command
	printRulesCmd := rulesCmd.Command("print", "Print the rules currently in the cortex ruler.").Action(r.printRules)
	printRulesCmd.Flag("disable-color", "disable colored output").BoolVar(&r.DisableColor)

	// Get RuleGroup Command
	getRuleGroupCmd := rulesCmd.Command("get", "Retreive a rulegroup from the ruler.").Action(r.getRuleGroup)
	getRuleGroupCmd.Arg("namespace", "Namespace of the rulegroup to retrieve.").Required().StringVar(&r.Namespace)
	getRuleGroupCmd.Arg("group", "Name of the rulegroup ot retrieve.").Required().StringVar(&r.RuleGroup)
	getRuleGroupCmd.Flag("disable-color", "disable colored output").BoolVar(&r.DisableColor)

	// Delete RuleGroup Command
	deleteRuleGroupCmd := rulesCmd.Command("delete", "Delete a rulegroup from the ruler.").Action(r.deleteRuleGroup)
	deleteRuleGroupCmd.Arg("namespace", "Namespace of the rulegroup to delete.").Required().StringVar(&r.Namespace)
	deleteRuleGroupCmd.Arg("group", "Name of the rulegroup ot delete.").Required().StringVar(&r.RuleGroup)

	loadRulesCmd := rulesCmd.Command("load", "load a set of rules to a designated cortex endpoint").Action(r.loadRules)
	loadRulesCmd.Arg("rule-files", "The rule files to check.").Required().ExistingFilesVar(&r.RuleFilesList)

	diffRulesCmd := rulesCmd.Command("diff", "diff a set of rules to a designated cortex endpoint").Action(r.diffRules)
	diffRulesCmd.Flag("ignored-namespaces", "comma-separated list of namespaces to ignore during a diff.").StringVar(&r.IgnoredNamespaces)
	diffRulesCmd.Flag("rule-files", "The rule files to check. Flag can be reused to load multiple files.").StringVar(&r.RuleFiles)
	diffRulesCmd.Flag(
		"rule-dirs",
		"Comma seperated list of paths to directories containing rules yaml files. Each file in a directory with a .yml or .yaml suffix will be parsed.",
	).StringVar(&r.RuleFilesPath)
	diffRulesCmd.Flag("disable-color", "disable colored output").BoolVar(&r.DisableColor)

	syncRulesCmd := rulesCmd.Command("sync", "sync a set of rules to a designated cortex endpoint").Action(r.syncRules)
	syncRulesCmd.Flag("ignored-namespaces", "comma-separated list of namespaces to ignore during a sync.").StringVar(&r.IgnoredNamespaces)
	syncRulesCmd.Flag("rule-files", "The rule files to check. Flag can be reused to load multiple files.").StringVar(&r.RuleFiles)
	syncRulesCmd.Flag(
		"rule-dirs",
		"Comma seperated list of paths to directories containing rules yaml files. Each file in a directory with a .yml or .yaml suffix will be parsed.",
	).StringVar(&r.RuleFilesPath)
}

func (r *RuleCommand) setup(k *kingpin.ParseContext) error {
	prometheus.MustRegister(
		ruleLoadTimestamp,
		ruleLoadSuccessTimestamp,
	)

	cli, err := client.New(r.ClientConfig)
	if err != nil {
		return err
	}
	r.cli = cli

	return nil
}

func (r *RuleCommand) setupFiles() error {
	// Set up ignored namespaces map for sync/diff command
	r.ignoredNamespacesMap = map[string]struct{}{}
	for _, ns := range strings.Split(r.IgnoredNamespaces, ",") {
		if ns != "" {
			r.ignoredNamespacesMap[ns] = struct{}{}
		}
	}

	for _, file := range strings.Split(r.RuleFiles, ",") {
		if file != "" {
			log.WithFields(log.Fields{
				"file": file,
			}).Debugf("adding file")
			r.RuleFilesList = append(r.RuleFilesList, file)
		}
	}

	for _, dir := range strings.Split(r.RuleFilesPath, ",") {
		if dir != "" {
			err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
				if err != nil {
					return err
				}
				if info.IsDir() {
					return nil
				}

				if strings.HasSuffix(info.Name(), ".yml") || strings.HasSuffix(info.Name(), ".yaml") {
					log.WithFields(log.Fields{
						"file": info.Name(),
						"path": path,
					}).Debugf("adding file in rule-path")
					r.RuleFilesList = append(r.RuleFilesList, path)
					return nil
				}
				log.WithFields(log.Fields{
					"file": info.Name(),
					"path": path,
				}).Debugf("ignorings file in rule-path")
				return nil
			})
			if err != nil {
				return fmt.Errorf("error walking the path %q: %v", dir, err)
			}
		}
	}

	return nil
}

func (r *RuleCommand) listRules(k *kingpin.ParseContext) error {
	rules, err := r.cli.ListRules(context.Background(), "")
	if err != nil {
		log.Fatalf("unable to read rules from cortex, %v", err)

	}

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 1, ' ', tabwriter.Debug)

	fmt.Fprintln(w, "Namespace\t Rule Group")
	for ns, rulegroups := range rules {
		for _, rg := range rulegroups {
			fmt.Fprintf(w, "%s\t %s\n", ns, rg.Name)
		}
	}

	w.Flush()

	return nil
}

func (r *RuleCommand) printRules(k *kingpin.ParseContext) error {
	rules, err := r.cli.ListRules(context.Background(), "")
	if err != nil {
		if err == client.ErrResourceNotFound {
			log.Infof("no rule groups currently exist for this user")
			return nil
		}
		log.Fatalf("unable to read rules from cortex, %v", err)
	}

	p := printer.New(r.DisableColor)
	return p.PrintRuleGroups(rules)
}

func (r *RuleCommand) getRuleGroup(k *kingpin.ParseContext) error {
	group, err := r.cli.GetRuleGroup(context.Background(), r.Namespace, r.RuleGroup)
	if err != nil {
		if err == client.ErrResourceNotFound {
			log.Infof("this rule group does not currently exist")
			return nil
		}
		log.Fatalf("unable to read rules from cortex, %v", err)
	}

	p := printer.New(r.DisableColor)
	return p.PrintRuleGroup(*group)
}

func (r *RuleCommand) deleteRuleGroup(k *kingpin.ParseContext) error {
	err := r.cli.DeleteRuleGroup(context.Background(), r.Namespace, r.RuleGroup)
	if err != nil && err != client.ErrResourceNotFound {
		log.Fatalf("unable to delete rule group from cortex, %v", err)
	}
	return nil
}

func (r *RuleCommand) loadRules(k *kingpin.ParseContext) error {
	nss, err := rules.ParseFiles(r.RuleFilesList)
	if err != nil {
		return errors.Wrap(err, "load operation unsuccessful, unable to parse rules files")
	}
	ruleLoadTimestamp.SetToCurrentTime()

	for _, ns := range nss {
		for _, group := range ns.Groups {
			curGroup, err := r.cli.GetRuleGroup(context.Background(), ns.Namespace, group.Name)
			if err != nil && err != client.ErrResourceNotFound {
				return errors.Wrap(err, "load operation unsuccessful, unable to contact cortex api")
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
				log.WithFields(log.Fields{
					"group":      group.Name,
					"namespace":  ns.Namespace,
					"difference": err,
				}).Infof("updating group")
			}

			err = r.cli.CreateRuleGroup(context.Background(), ns.Namespace, group)
			if err != nil {
				log.WithError(err).WithFields(log.Fields{
					"group":     group.Name,
					"namespace": ns.Namespace,
				}).Errorf("unable to load rule group")
				return fmt.Errorf("load operation unsuccessful")
			}
		}
	}

	ruleLoadSuccessTimestamp.SetToCurrentTime()
	return nil
}

func (r *RuleCommand) diffRules(k *kingpin.ParseContext) error {
	err := r.setupFiles()
	if err != nil {
		return errors.Wrap(err, "diff operation unsuccessful, unable to load rules files")
	}

	nss, err := rules.ParseFiles(r.RuleFilesList)
	if err != nil {
		return errors.Wrap(err, "diff operation unsuccessful, unable to parse rules files")
	}

	currentNamespaceMap, err := r.cli.ListRules(context.Background(), "")
	if err != nil && err != client.ErrResourceNotFound {
		return errors.Wrap(err, "diff operation unsuccessful, unable to contact cortex api")
	}

	changes := []rules.NamespaceChange{}

	for _, ns := range nss {
		currentNamespace, exists := currentNamespaceMap[ns.Namespace]
		if !exists {
			changes = append(changes, rules.NamespaceChange{
				State:         rules.Created,
				Namespace:     ns.Namespace,
				GroupsCreated: ns.Groups,
			})
			continue
		}

		origNamespace := rules.RuleNamespace{
			Namespace: ns.Namespace,
			Groups:    currentNamespace,
		}

		changes = append(changes, rules.CompareNamespaces(origNamespace, ns))

		// Remove namespace from temp map so namespaces that have been removed can easily be detected
		delete(currentNamespaceMap, ns.Namespace)
	}

	for ns, deletedGroups := range currentNamespaceMap {
		if _, ignored := r.ignoredNamespacesMap[ns]; !ignored {
			changes = append(changes, rules.NamespaceChange{
				State:         rules.Deleted,
				Namespace:     ns,
				GroupsDeleted: deletedGroups,
			})
		}
	}

	p := printer.New(r.DisableColor)
	return p.PrintComparisonResult(changes, false)
}

func (r *RuleCommand) syncRules(k *kingpin.ParseContext) error {
	err := r.setupFiles()
	if err != nil {
		return errors.Wrap(err, "sync operation unsuccessful, unable to load rules files")
	}

	nss, err := rules.ParseFiles(r.RuleFilesList)
	if err != nil {
		return errors.Wrap(err, "sync operation unsuccessful, unable to parse rules files")
	}

	currentNamespaceMap, err := r.cli.ListRules(context.Background(), "")
	if err != nil && err != client.ErrResourceNotFound {
		return errors.Wrap(err, "sync operation unsuccessful, unable to contact cortex api")
	}

	changes := []rules.NamespaceChange{}

	for _, ns := range nss {
		currentNamespace, exists := currentNamespaceMap[ns.Namespace]
		if !exists {
			changes = append(changes, rules.NamespaceChange{
				State:         rules.Created,
				Namespace:     ns.Namespace,
				GroupsCreated: ns.Groups,
			})
			continue
		}

		origNamespace := rules.RuleNamespace{
			Namespace: ns.Namespace,
			Groups:    currentNamespace,
		}

		changes = append(changes, rules.CompareNamespaces(origNamespace, ns))

		// Remove namespace from temp map so namespaces that have been removed can easily be detected
		delete(currentNamespaceMap, ns.Namespace)
	}

	for ns, deletedGroups := range currentNamespaceMap {
		if _, ignored := r.ignoredNamespacesMap[ns]; !ignored {
			changes = append(changes, rules.NamespaceChange{
				State:         rules.Deleted,
				Namespace:     ns,
				GroupsDeleted: deletedGroups,
			})
		}
	}

	err = r.executeChanges(context.Background(), changes)
	if err != nil {
		return errors.Wrap(err, "sync operation unsuccessful, unable to complete executing changes.")
	}

	return nil
}

func (r *RuleCommand) executeChanges(ctx context.Context, changes []rules.NamespaceChange) error {
	var err error
	for _, ch := range changes {
		for _, g := range ch.GroupsCreated {
			log.WithFields(log.Fields{
				"group":     g.Name,
				"namespace": ch.Namespace,
			}).Infof("creating group")
			err = r.cli.CreateRuleGroup(ctx, ch.Namespace, g)
			if err != nil {
				return err
			}
		}

		for _, g := range ch.GroupsUpdated {
			log.WithFields(log.Fields{
				"group":     g.New.Name,
				"namespace": ch.Namespace,
			}).Infof("updating group")
			err = r.cli.CreateRuleGroup(ctx, ch.Namespace, g.New)
			if err != nil {
				return err
			}
		}

		for _, g := range ch.GroupsDeleted {
			log.WithFields(log.Fields{
				"group":     g.Name,
				"namespace": ch.Namespace,
			}).Infof("deleting group")
			err = r.cli.DeleteRuleGroup(ctx, ch.Namespace, g.Name)
			if err != nil && err != client.ErrResourceNotFound {
				return err
			}
		}
	}

	updated, created, deleted := rules.SummarizeChanges(changes)
	fmt.Println()
	fmt.Printf("Sync Summary: %v Groups Created, %v Groups Updated, %v Groups Deleted\n", created, updated, deleted)
	return nil
}
