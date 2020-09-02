package commands

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"text/tabwriter"

	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	log "github.com/sirupsen/logrus"
	"gopkg.in/alecthomas/kingpin.v2"
	yamlv3 "gopkg.in/yaml.v3"

	"github.com/grafana/cortex-tools/pkg/client"
	"github.com/grafana/cortex-tools/pkg/printer"
	"github.com/grafana/cortex-tools/pkg/rules"
)

const (
	defaultPrepareAggregationLabel = "cluster"
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

	backends = []string{rules.CortexBackend, rules.LokiBackend} // list of supported backend types
)

// RuleCommand configures and executes rule related cortex operations
type RuleCommand struct {
	ClientConfig client.Config

	cli *client.CortexClient

	// Backend type (cortex | loki)
	Backend string

	// Get Rule Groups Configs
	Namespace string
	RuleGroup string

	// Load Rules Config
	RuleFilesList []string
	RuleFiles     string
	RuleFilesPath string

	// Sync/Diff Rules Config
	IgnoredNamespaces    string
	ignoredNamespacesMap map[string]struct{}

	// Prepare Rules Config
	InPlaceEdit      bool
	AggregationLabel string

	// Lint Rules Config
	LintDryRun bool

	// Rules check flags
	Strict bool

	DisableColor bool
}

// Register rule related commands and flags with the kingpin application
func (r *RuleCommand) Register(app *kingpin.Application) {
	rulesCmd := app.Command("rules", "View & edit rules stored in cortex.").PreAction(r.setup)
	rulesCmd.Flag("key", "Api key to use when contacting cortex, alternatively set $CORTEX_API_KEY.").Default("").Envar("CORTEX_API_KEY").StringVar(&r.ClientConfig.Key)
	rulesCmd.Flag("backend", "Backend type to interact with: <cortex|loki>").Default("cortex").EnumVar(&r.Backend, backends...)

	// Register rule commands
	listCmd := rulesCmd.
		Command("list", "List the rules currently in the cortex ruler.").
		Action(r.listRules)
	printRulesCmd := rulesCmd.
		Command("print", "Print the rules currently in the cortex ruler.").
		Action(r.printRules)
	getRuleGroupCmd := rulesCmd.
		Command("get", "Retrieve a rulegroup from the ruler.").
		Action(r.getRuleGroup)
	deleteRuleGroupCmd := rulesCmd.
		Command("delete", "Delete a rulegroup from the ruler.").
		Action(r.deleteRuleGroup)
	loadRulesCmd := rulesCmd.
		Command("load", "load a set of rules to a designated cortex endpoint").
		Action(r.loadRules)
	diffRulesCmd := rulesCmd.
		Command("diff", "diff a set of rules to a designated cortex endpoint").
		Action(r.diffRules)
	syncRulesCmd := rulesCmd.
		Command("sync", "sync a set of rules to a designated cortex endpoint").
		Action(r.syncRules)
	prepareCmd := rulesCmd.
		Command("prepare", "modifies a set of rules by including an specific label in aggregations.").
		Action(r.prepare)
	lintCmd := rulesCmd.
		Command("lint", "formats a set of rule files. It reorders keys alphabetically, uses 4 spaces as indentantion, and formats PromQL expressions to a single line.").
		Action(r.lint)
	checkCmd := rulesCmd.
		Command("check", "runs various best practice checks against rules.").
		Action(r.checkRecordingRuleNames)

	// Require Cortex cluster address and tentant ID on all these commands
	for _, c := range []*kingpin.CmdClause{listCmd, printRulesCmd, getRuleGroupCmd, deleteRuleGroupCmd, loadRulesCmd, diffRulesCmd, syncRulesCmd} {
		c.Flag("address", "Address of the cortex cluster, alternatively set CORTEX_ADDRESS.").
			Envar("CORTEX_ADDRESS").
			Required().
			StringVar(&r.ClientConfig.Address)

		c.Flag("id", "Cortex tenant id, alternatively set CORTEX_TENANT_ID.").
			Envar("CORTEX_TENANT_ID").
			Required().
			StringVar(&r.ClientConfig.ID)

		c.Flag("tls-ca-path", "TLS CA certificate to verify cortex API as part of mTLS, alternatively set CORTEX_TLS_CA_PATH.").
			Default("").
			Envar("CORTEX_TLS_CA_CERT").
			StringVar(&r.ClientConfig.TLS.CAPath)

		c.Flag("tls-cert-path", "TLS client certificate to authenticate with cortex API as part of mTLS, alternatively set CORTEX_TLS_CERT_PATH.").
			Default("").
			Envar("CORTEX_TLS_CLIENT_CERT").
			StringVar(&r.ClientConfig.TLS.CertPath)

		c.Flag("tls-key-path", "TLS client certificate private key to authenticate with cortex API as part of mTLS, alternatively set CORTEX_TLS_KEY_PATH.").
			Default("").
			Envar("CORTEX_TLS_CLIENT_KEY").
			StringVar(&r.ClientConfig.TLS.KeyPath)

	}

	// Print Rules Command
	printRulesCmd.Flag("disable-color", "disable colored output").BoolVar(&r.DisableColor)

	// Get RuleGroup Command
	getRuleGroupCmd.Arg("namespace", "Namespace of the rulegroup to retrieve.").Required().StringVar(&r.Namespace)
	getRuleGroupCmd.Arg("group", "Name of the rulegroup ot retrieve.").Required().StringVar(&r.RuleGroup)
	getRuleGroupCmd.Flag("disable-color", "disable colored output").BoolVar(&r.DisableColor)

	// Delete RuleGroup Command
	deleteRuleGroupCmd.Arg("namespace", "Namespace of the rulegroup to delete.").Required().StringVar(&r.Namespace)
	deleteRuleGroupCmd.Arg("group", "Name of the rulegroup ot delete.").Required().StringVar(&r.RuleGroup)

	// Load Rules Command
	loadRulesCmd.Arg("rule-files", "The rule files to check.").Required().ExistingFilesVar(&r.RuleFilesList)

	// Diff Command
	diffRulesCmd.Flag("ignored-namespaces", "comma-separated list of namespaces to ignore during a diff.").StringVar(&r.IgnoredNamespaces)
	diffRulesCmd.Flag("rule-files", "The rule files to check. Flag can be reused to load multiple files.").StringVar(&r.RuleFiles)
	diffRulesCmd.Flag(
		"rule-dirs",
		"Comma separated list of paths to directories containing rules yaml files. Each file in a directory with a .yml or .yaml suffix will be parsed.",
	).StringVar(&r.RuleFilesPath)
	diffRulesCmd.Flag("disable-color", "disable colored output").BoolVar(&r.DisableColor)

	// Sync Command
	syncRulesCmd.Flag("ignored-namespaces", "comma-separated list of namespaces to ignore during a sync.").StringVar(&r.IgnoredNamespaces)
	syncRulesCmd.Flag("rule-files", "The rule files to check. Flag can be reused to load multiple files.").StringVar(&r.RuleFiles)
	syncRulesCmd.Flag(
		"rule-dirs",
		"Comma separated list of paths to directories containing rules yaml files. Each file in a directory with a .yml or .yaml suffix will be parsed.",
	).StringVar(&r.RuleFilesPath)

	// Prepare Command
	prepareCmd.Arg("rule-files", "The rule files to check.").ExistingFilesVar(&r.RuleFilesList)
	prepareCmd.Flag("rule-files", "The rule files to check. Flag can be reused to load multiple files.").StringVar(&r.RuleFiles)
	prepareCmd.Flag(
		"rule-dirs",
		"Comma separated list of paths to directories containing rules yaml files. Each file in a directory with a .yml or .yaml suffix will be parsed.",
	).StringVar(&r.RuleFilesPath)
	prepareCmd.Flag(
		"in-place",
		"edits the rule file in place",
	).Short('i').BoolVar(&r.InPlaceEdit)
	prepareCmd.Flag("label", "label to include as part of the aggregations.").Default(defaultPrepareAggregationLabel).Short('l').StringVar(&r.AggregationLabel)

	// Lint Command
	lintCmd.Arg("rule-files", "The rule files to check.").ExistingFilesVar(&r.RuleFilesList)
	lintCmd.Flag("rule-files", "The rule files to check. Flag can be reused to load multiple files.").StringVar(&r.RuleFiles)
	lintCmd.Flag(
		"rule-dirs",
		"Comma separated list of paths to directories containing rules yaml files. Each file in a directory with a .yml or .yaml suffix will be parsed.",
	).StringVar(&r.RuleFilesPath)
	lintCmd.Flag("dry-run", "Performs a trial run that doesn't make any changes and (mostly) produces the same outpupt as a real run.").Short('n').BoolVar(&r.LintDryRun)

	// Check Command
	checkCmd.Arg("rule-files", "The rule files to check.").ExistingFilesVar(&r.RuleFilesList)
	checkCmd.Flag("rule-files", "The rule files to check. Flag can be reused to load multiple files.").StringVar(&r.RuleFiles)
	checkCmd.Flag(
		"rule-dirs",
		"Comma separated list of paths to directories containing rules yaml files. Each file in a directory with a .yml or .yaml suffix will be parsed.",
	).StringVar(&r.RuleFilesPath)
	checkCmd.Flag("strict", "fails rules checks that do not match best practices exactly").BoolVar(&r.Strict)
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
	nss, err := rules.ParseFiles(r.Backend, r.RuleFilesList)
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

	nss, err := rules.ParseFiles(r.Backend, r.RuleFilesList)
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

	nss, err := rules.ParseFiles(r.Backend, r.RuleFilesList)
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

func (r *RuleCommand) prepare(k *kingpin.ParseContext) error {
	err := r.setupFiles()
	if err != nil {
		return errors.Wrap(err, "prepare operation unsuccessful, unable to load rules files")
	}

	namespaces, err := rules.ParseFiles(r.Backend, r.RuleFilesList)
	if err != nil {
		return errors.Wrap(err, "prepare operation unsuccessful, unable to parse rules files")
	}

	var count, mod int
	for _, ruleNamespace := range namespaces {
		c, m, err := ruleNamespace.AggregateBy(r.AggregationLabel)
		if err != nil {
			return err
		}

		count += c
		mod += m
	}

	// now, save all the files
	if err := save(namespaces, r.InPlaceEdit); err != nil {
		return err
	}

	log.Infof("SUCCESS: %d rules found, %d modified expressions", count, mod)

	return nil
}

func (r *RuleCommand) lint(k *kingpin.ParseContext) error {
	err := r.setupFiles()
	if err != nil {
		return errors.Wrap(err, "prepare operation unsuccessful, unable to load rules files")
	}

	namespaces, err := rules.ParseFiles(r.Backend, r.RuleFilesList)
	if err != nil {
		return errors.Wrap(err, "prepare operation unsuccessful, unable to parse rules files")
	}

	var count, mod int
	for _, ruleNamespace := range namespaces {
		c, m, err := ruleNamespace.LintPromQLExpressions()
		if err != nil {
			return err
		}

		count += c
		mod += m
	}

	if !r.LintDryRun {
		// linting will always in-place edit unless is a dry-run.
		if err := save(namespaces, true); err != nil {
			return err
		}
	}

	log.Infof("SUCCESS: %d rules found, %d linted expressions", count, mod)

	return nil
}

func (r *RuleCommand) checkRecordingRuleNames(k *kingpin.ParseContext) error {
	err := r.setupFiles()
	if err != nil {
		return errors.Wrap(err, "check operation unsuccessful, unable to load rules files")
	}

	namespaces, err := rules.ParseFiles(r.Backend, r.RuleFilesList)
	if err != nil {
		return errors.Wrap(err, "check operation unsuccessful, unable to parse rules files")
	}

	for _, ruleNamespace := range namespaces {
		n := ruleNamespace.CheckRecordingRules(r.Strict)
		if n != 0 {
			return fmt.Errorf("%d erroneous recording rule names", n)
		}
	}

	return nil
}

// save saves a set of rule files to to disk. You can specify whenever you want the
// file(s) to be edited in-place.
func save(nss map[string]rules.RuleNamespace, i bool) error {
	for _, ns := range nss {
		payload, err := yamlv3.Marshal(ns)
		if err != nil {
			return err
		}

		filepath := ns.Filepath
		if !i {
			filepath = filepath + ".result"
		}

		if err := ioutil.WriteFile(filepath, payload, 0644); err != nil {
			return err
		}
	}

	return nil
}
