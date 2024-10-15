// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/grafana/cortex-tools/blob/main/pkg/commands/rules.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package commands

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"regexp" //lint:ignore faillint Required by kingpin for regexp flags
	"strings"

	"github.com/alecthomas/kingpin/v2"
	"github.com/grafana/dskit/concurrency"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/prometheus/model/rulefmt"
	log "github.com/sirupsen/logrus"
	"golang.org/x/term"
	yamlv3 "gopkg.in/yaml.v3"

	"github.com/grafana/mimir/pkg/mimirtool/client"
	"github.com/grafana/mimir/pkg/mimirtool/printer"
	"github.com/grafana/mimir/pkg/mimirtool/rules"
	"github.com/grafana/mimir/pkg/mimirtool/rules/rwrulefmt"
)

const (
	defaultPrepareAggregationLabel = "cluster"

	// maxSyncConcurrency is the upper bound limit on the concurrency value that can be set.
	maxSyncConcurrency = 32
)

var (
	backends = []string{rules.MimirBackend}      // list of supported backend types
	formats  = []string{"json", "yaml", "table"} // list of supported formats for the list command
	// disallowedNamespaceChars is a regex pattern that matches characters that are not allowed in namespaces. They are characters not allow in Linux and Windows file names.
	disallowedNamespaceChars = regexp.MustCompile(`[<>:"/\\|?*\x00-\x1F]`)
)

// ruleCommandClient defines the interface that should be implemented by the API client used by
// the RuleCommand. This is useful for testing purposes.
type ruleCommandClient interface {
	// CreateRuleGroup creates a new rule group.
	CreateRuleGroup(ctx context.Context, namespace string, rg rwrulefmt.RuleGroup) error

	// DeleteRuleGroup deletes a rule group.
	DeleteRuleGroup(ctx context.Context, namespace, groupName string) error

	// GetRuleGroup retrieves a rule group.
	GetRuleGroup(ctx context.Context, namespace, groupName string) (*rwrulefmt.RuleGroup, error)

	// ListRules retrieves a rule group.
	ListRules(ctx context.Context, namespace string) (map[string][]rwrulefmt.RuleGroup, error)

	// DeleteNamespace delete all the rule groups in a namespace including the namespace itself.
	DeleteNamespace(ctx context.Context, namespace string) error
}

// RuleCommand configures and executes rule related mimir operations
type RuleCommand struct {
	ClientConfig client.Config

	cli ruleCommandClient

	// Backend type (cortex | loki)
	Backend string

	// Get Rule Groups Configs
	Namespace string
	RuleGroup string
	OutputDir string

	// Load Rules Config
	RuleFilesList []string
	RuleFilesPath string

	// Sync/Diff Rules Config
	Namespaces             string
	namespacesMap          map[string]struct{}
	NamespacesRegex        *regexp.Regexp
	IgnoredNamespaces      string
	IgnoredNamespacesRegex *regexp.Regexp
	ignoredNamespacesMap   map[string]struct{}

	// Sync Rules Config
	SyncConcurrency int

	// Prepare Rules Config
	InPlaceEdit                            bool
	AggregationLabel                       string
	AggregationLabelExcludedRuleGroups     string
	aggregationLabelExcludedRuleGroupsList map[string]struct{}

	// Lint Rules Config
	LintDryRun bool

	// Rules check flags
	Strict bool

	// List Rules Config
	Format string

	DisableColor bool
	ForceColor   bool

	// Diff Rules Config
	Verbose bool

	// Metrics.
	ruleLoadTimestamp        prometheus.Gauge
	ruleLoadSuccessTimestamp prometheus.Gauge
}

// Register rule related commands and flags with the kingpin application
func (r *RuleCommand) Register(app *kingpin.Application, envVars EnvVarNames, reg prometheus.Registerer) {
	rulesCmd := app.Command("rules", "View and edit rules stored in Grafana Mimir.").PreAction(func(k *kingpin.ParseContext) error { return r.setup(k, reg) })
	rulesCmd.Flag("user", fmt.Sprintf("Basic auth username to use when contacting Grafana Mimir; alternatively, set %s. If empty, %s is used instead.", envVars.APIUser, envVars.TenantID)).Default("").Envar(envVars.APIUser).StringVar(&r.ClientConfig.User)
	rulesCmd.Flag("key", "Basic auth password to use when contacting Grafana Mimir; alternatively, set "+envVars.APIKey+".").Default("").Envar(envVars.APIKey).StringVar(&r.ClientConfig.Key)
	rulesCmd.Flag("backend", "Backend type to interact with (deprecated)").Default(rules.MimirBackend).EnumVar(&r.Backend, backends...)
	rulesCmd.Flag("auth-token", "Authentication token for bearer token or JWT auth, alternatively set "+envVars.AuthToken+".").Default("").Envar(envVars.AuthToken).StringVar(&r.ClientConfig.AuthToken)
	r.ClientConfig.ExtraHeaders = map[string]string{}
	rulesCmd.Flag("extra-headers", "Extra headers to add to the requests in header=value format, alternatively set newline separated "+envVars.ExtraHeaders+".").Envar(envVars.ExtraHeaders).StringMapVar(&r.ClientConfig.ExtraHeaders)

	// Register rule commands
	listCmd := rulesCmd.
		Command("list", "List the rules currently in the Grafana Mimir ruler.").
		Action(r.listRules)
	printRulesCmd := rulesCmd.
		Command("print", "Print the rules currently in the Grafana Mimir ruler.").
		Action(r.printRules)
	getRuleGroupCmd := rulesCmd.
		Command("get", "Retrieve a rulegroup from the ruler.").
		Action(r.getRuleGroup)
	deleteRuleGroupCmd := rulesCmd.
		Command("delete", "Delete a rulegroup from the ruler.").
		Action(r.deleteRuleGroup)
	loadRulesCmd := rulesCmd.
		Command("load", "Load a set of rules to a designated Grafana Mimir endpoint.").
		Action(r.loadRules)
	diffRulesCmd := rulesCmd.
		Command("diff", "Diff a set of rules to a designated Grafana Mimir endpoint.").
		Action(r.diffRules)
	syncRulesCmd := rulesCmd.
		Command("sync", "Sync a set of rules to a designated Grafana Mimir endpoint.").
		Action(r.syncRules)
	prepareCmd := rulesCmd.
		Command("prepare", "Modify a set of rules by including an specific label in aggregations.").
		Action(r.prepare)
	lintCmd := rulesCmd.
		Command("lint", "Format a set of rule files. Keys are sorted alphabetically, with 4 spaces as indentantion, and PromQL expressions are formatted to a single line.").
		Action(r.lint)
	checkCmd := rulesCmd.
		Command("check", "Run various best practice checks against rules.").
		Action(r.checkRules)
	deleteNamespaceCmd := rulesCmd.
		Command("delete-namespace", "Delete a namespace from the ruler.").
		Action(r.deleteNamespace)

	// Require Mimir cluster address and tenant ID on all these commands
	for _, c := range []*kingpin.CmdClause{listCmd, printRulesCmd, getRuleGroupCmd, deleteRuleGroupCmd, loadRulesCmd, diffRulesCmd, syncRulesCmd, deleteNamespaceCmd} {
		c.Flag("address", "Address of the Grafana Mimir cluster; alternatively, set "+envVars.Address+".").
			Envar(envVars.Address).
			Required().
			StringVar(&r.ClientConfig.Address)

		c.Flag("id", "Grafana Mimir tenant ID; alternatively, set "+envVars.TenantID+".").
			Envar(envVars.TenantID).
			Required().
			StringVar(&r.ClientConfig.ID)

		c.Flag("use-legacy-routes", "If set, the API requests to Grafana Mimir use the legacy /api/v1/rules routes instead of /prometheus/config/v1/rules; alternatively, set "+envVars.UseLegacyRoutes+".").
			Default("false").
			Envar(envVars.UseLegacyRoutes).
			BoolVar(&r.ClientConfig.UseLegacyRoutes)

		c.Flag("mimir-http-prefix", "Used when use-legacy-routes is set to true. The prefix to use for the url when contacting Grafana Mimir; alternatively, set "+envVars.MimirHTTPPrefix+".").
			Default("/prometheus").
			Envar(envVars.MimirHTTPPrefix).
			StringVar(&r.ClientConfig.MimirHTTPPrefix)

		c.Flag("tls-ca-path", "TLS CA certificate to verify Grafana Mimir API as part of mTLS; alternatively, set "+envVars.TLSCAPath+".").
			Default("").
			Envar(envVars.TLSCAPath).
			StringVar(&r.ClientConfig.TLS.CAPath)

		c.Flag("tls-cert-path", "TLS client certificate to authenticate with the Grafana Mimir API as part of mTLS; alternatively, set "+envVars.TLSCertPath+".").
			Default("").
			Envar(envVars.TLSCertPath).
			StringVar(&r.ClientConfig.TLS.CertPath)

		c.Flag("tls-key-path", "TLS client certificate private key to authenticate with the Grafana Mimir API as part of mTLS; alternatively, set "+envVars.TLSKeyPath+".").
			Default("").
			Envar(envVars.TLSKeyPath).
			StringVar(&r.ClientConfig.TLS.KeyPath)

		c.Flag("tls-insecure-skip-verify", "Skip TLS certificate verification; alternatively, set "+envVars.TLSInsecureSkipVerify+".").
			Default("false").
			Envar(envVars.TLSInsecureSkipVerify).
			BoolVar(&r.ClientConfig.TLS.InsecureSkipVerify)
	}

	// Print Rules Command
	printRulesCmd.Flag("disable-color", "disable colored output").BoolVar(&r.DisableColor)
	printRulesCmd.Flag("force-color", "force colored output").BoolVar(&r.ForceColor)
	printRulesCmd.Flag("output-dir", "The directory where the rules will be written to.").ExistingDirVar(&r.OutputDir)

	// Get RuleGroup Command
	getRuleGroupCmd.Arg("namespace", "Namespace of the rulegroup to retrieve.").Required().StringVar(&r.Namespace)
	getRuleGroupCmd.Arg("group", "Name of the rulegroup to retrieve.").Required().StringVar(&r.RuleGroup)
	getRuleGroupCmd.Flag("disable-color", "disable colored output").BoolVar(&r.DisableColor)
	getRuleGroupCmd.Flag("force-color", "force colored output").BoolVar(&r.ForceColor)
	getRuleGroupCmd.Flag("output-dir", "The directory where the rules will be written to.").ExistingDirVar(&r.OutputDir)

	// Delete RuleGroup Command
	deleteRuleGroupCmd.Arg("namespace", "Namespace of the rulegroup to delete.").Required().StringVar(&r.Namespace)
	deleteRuleGroupCmd.Arg("group", "Name of the rulegroup to delete.").Required().StringVar(&r.RuleGroup)

	// Load Rules Command
	loadRulesCmd.Arg("rule-files", "The rule files to check.").Required().ExistingFilesVar(&r.RuleFilesList)

	// Diff Command
	diffRulesCmd.Arg("rule-files", "The rule files to check.").ExistingFilesVar(&r.RuleFilesList)
	diffRulesCmd.Flag("namespaces", "comma-separated list of namespaces to check during a diff. Cannot be used together with other namespaces options.").StringVar(&r.Namespaces)
	diffRulesCmd.Flag("ignored-namespaces", "comma-separated list of namespaces to ignore during a diff. Cannot be used together with other namespaces options.").StringVar(&r.IgnoredNamespaces)
	diffRulesCmd.Flag("namespaces-regex", "regex matching namespaces to check during a diff. Cannot be used together with other namespaces options.").RegexpVar(&r.NamespacesRegex)
	diffRulesCmd.Flag("ignored-namespaces-regex", "regex matching namespaces to ignore during a diff. Cannot be used together with other namespaces options.").RegexpVar(&r.IgnoredNamespacesRegex)
	diffRulesCmd.Flag(
		"rule-dirs",
		"Comma separated list of paths to directories containing rules yaml files. Each file in a directory with a .yml or .yaml suffix will be parsed.",
	).StringVar(&r.RuleFilesPath)
	diffRulesCmd.Flag("disable-color", "disable colored output").BoolVar(&r.DisableColor)
	diffRulesCmd.Flag("force-color", "force colored output").BoolVar(&r.ForceColor)
	diffRulesCmd.Flag("verbose", "show diff output with rules changes").BoolVar(&r.Verbose)

	// Sync Command
	syncRulesCmd.Arg("rule-files", "The rule files to check.").ExistingFilesVar(&r.RuleFilesList)
	syncRulesCmd.Flag("namespaces", "comma-separated list of namespaces to check during a sync. Cannot be used together with other namespaces options.").StringVar(&r.Namespaces)
	syncRulesCmd.Flag("ignored-namespaces", "comma-separated list of namespaces to ignore during a sync. Cannot be used together with other namespaces options.").StringVar(&r.IgnoredNamespaces)
	syncRulesCmd.Flag("namespaces-regex", "regex matching namespaces to check during a sync. Cannot be used together with other namespaces options.").RegexpVar(&r.NamespacesRegex)
	syncRulesCmd.Flag("ignored-namespaces-regex", "regex matching namespaces to ignore during a sync. Cannot be used together with other namespaces options.").RegexpVar(&r.IgnoredNamespacesRegex)
	syncRulesCmd.Flag(
		"rule-dirs",
		"Comma separated list of paths to directories containing rules yaml files. Each file in a directory with a .yml or .yaml suffix will be parsed.",
	).StringVar(&r.RuleFilesPath)
	syncRulesCmd.Flag(
		"concurrency",
		fmt.Sprintf("How many concurrent rule groups to sync. The maximum accepted value is %d.", maxSyncConcurrency),
	).Default("8").IntVar(&r.SyncConcurrency)

	// Prepare Command
	prepareCmd.Arg("rule-files", "The rule files to check.").ExistingFilesVar(&r.RuleFilesList)
	prepareCmd.Flag(
		"rule-dirs",
		"Comma separated list of paths to directories containing rules yaml files. Each file in a directory with a .yml or .yaml suffix will be parsed.",
	).StringVar(&r.RuleFilesPath)
	prepareCmd.Flag(
		"in-place",
		"edits the rule file in place",
	).Short('i').BoolVar(&r.InPlaceEdit)
	prepareCmd.Flag("label", "label to include as part of the aggregations.").Default(defaultPrepareAggregationLabel).Short('l').StringVar(&r.AggregationLabel)
	prepareCmd.Flag("label-excluded-rule-groups", "Comma separated list of rule group names to exclude when including the configured label to aggregations.").StringVar(&r.AggregationLabelExcludedRuleGroups)

	// Lint Command
	lintCmd.Arg("rule-files", "The rule files to check.").ExistingFilesVar(&r.RuleFilesList)
	lintCmd.Flag(
		"rule-dirs",
		"Comma separated list of paths to directories containing rules yaml files. Each file in a directory with a .yml or .yaml suffix will be parsed.",
	).StringVar(&r.RuleFilesPath)
	lintCmd.Flag("dry-run", "Performs a trial run that doesn't make any changes and (mostly) produces the same outpupt as a real run.").Short('n').BoolVar(&r.LintDryRun)

	// Check Command
	checkCmd.Arg("rule-files", "The rule files to check.").ExistingFilesVar(&r.RuleFilesList)
	checkCmd.Flag(
		"rule-dirs",
		"Comma separated list of paths to directories containing rules yaml files. Each file in a directory with a .yml or .yaml suffix will be parsed.",
	).StringVar(&r.RuleFilesPath)
	checkCmd.Flag("strict", "fails rules checks that do not match best practices exactly").BoolVar(&r.Strict)

	// List Command
	listCmd.Flag("format", "Backend type to interact with: <json|yaml|table>").Default("table").EnumVar(&r.Format, formats...)
	listCmd.Flag("disable-color", "disable colored output").BoolVar(&r.DisableColor)
	listCmd.Flag("force-color", "force colored output").BoolVar(&r.ForceColor)

	// Delete Namespace Command
	deleteNamespaceCmd.Arg("namespace", "Namespace to delete.").Required().StringVar(&r.Namespace)

}

func (r *RuleCommand) setup(_ *kingpin.ParseContext, reg prometheus.Registerer) error {
	r.ruleLoadTimestamp = promauto.With(reg).NewGauge(prometheus.GaugeOpts{
		Namespace: "cortex",
		Name:      "last_rule_load_timestamp_seconds",
		Help:      "The timestamp of the last rule load.",
	})
	r.ruleLoadSuccessTimestamp = promauto.With(reg).NewGauge(prometheus.GaugeOpts{
		Namespace: "cortex",
		Name:      "last_rule_load_success_timestamp_seconds",
		Help:      "The timestamp of the last successful rule load.",
	})

	cli, err := client.New(r.ClientConfig)
	if err != nil {
		return err
	}
	r.cli = cli

	return nil
}

func (r *RuleCommand) setupArgs() error {
	if (r.Namespaces != "" && r.IgnoredNamespaces != "") ||
		(r.NamespacesRegex != nil && r.IgnoredNamespacesRegex != nil) ||
		(r.Namespaces != "" || r.IgnoredNamespaces != "") && (r.NamespacesRegex != nil || r.IgnoredNamespacesRegex != nil) {
		return errors.New("only one namespace option can be specified")
	}

	// Set up ignored namespaces map for sync/diff command
	if r.IgnoredNamespaces != "" {
		r.ignoredNamespacesMap = map[string]struct{}{}
		for _, ns := range strings.Split(r.IgnoredNamespaces, ",") {
			if ns != "" {
				r.ignoredNamespacesMap[ns] = struct{}{}
			}
		}
	}

	// Set up allowed namespaces map for sync/diff command
	if r.Namespaces != "" {
		r.namespacesMap = map[string]struct{}{}
		for _, ns := range strings.Split(r.Namespaces, ",") {
			if ns != "" {
				r.namespacesMap[ns] = struct{}{}
			}
		}
	}

	// Set up rule groups excluded from label aggregation.
	r.aggregationLabelExcludedRuleGroupsList = map[string]struct{}{}
	for _, name := range strings.Split(r.AggregationLabelExcludedRuleGroups, ",") {
		if name = strings.TrimSpace(name); name != "" {
			r.aggregationLabelExcludedRuleGroupsList[name] = struct{}{}
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

func (r *RuleCommand) listRules(_ *kingpin.ParseContext) error {
	rules, err := r.cli.ListRules(context.Background(), "")
	if err != nil {
		log.Fatalf("Unable to read rules from Grafana Mimir, %v", err)

	}

	p := printer.New(r.DisableColor, r.ForceColor, term.IsTerminal(int(os.Stdout.Fd())))
	return p.PrintRuleSet(rules, r.Format, os.Stdout)
}

func (r *RuleCommand) printRules(_ *kingpin.ParseContext) error {
	ruleNS, err := r.cli.ListRules(context.Background(), "")
	if err != nil {
		if errors.Is(err, client.ErrResourceNotFound) {
			log.Infof("no rule groups currently exist for this user")
			return nil
		}
		log.Fatalf("Unable to read rules from Grafana Mimir, %v", err)
	}

	p := printer.New(r.DisableColor, r.ForceColor, term.IsTerminal(int(os.Stdout.Fd())))

	if r.OutputDir != "" {
		log.Infof("Output dir detected writing rules to directory: %s", r.OutputDir)
		for namespace, rule := range ruleNS {
			if err = saveNamespaceRuleGroup(namespace, rule, r.OutputDir); err != nil {
				return err
			}
		}

		// Don't print the rule set if we've specified an output directory to save the rule files. It gets too noisy.
		return nil
	}

	return p.PrintRuleGroups(ruleNS)
}

func saveNamespaceRuleGroup(ns string, ruleGroup []rwrulefmt.RuleGroup, dir string) error {
	baseDir, err := filepath.Abs(dir)
	if err != nil {
		return err
	}

	if disallowedNamespaceChars.Match([]byte(ns)) {
		oldNs := ns
		ns = strings.TrimSpace(disallowedNamespaceChars.ReplaceAllString(ns, "_"))
		log.Warnf("We found disallowed characters in the namespace name '%s', replacing them with underscores '%s'", oldNs, ns)
	}

	file := filepath.Join(baseDir, fmt.Sprintf("%s.yaml", ns))
	rule := map[string]rules.RuleNamespace{ns: {
		Namespace: ns,
		Filepath:  file,
		Groups:    ruleGroup,
	}}
	log.Infof("Saving namespace group rules to file %s", file)
	if err := save(rule, true); err != nil {
		return err
	}
	return nil
}

func (r *RuleCommand) getRuleGroup(_ *kingpin.ParseContext) error {
	group, err := r.cli.GetRuleGroup(context.Background(), r.Namespace, r.RuleGroup)
	if err != nil {
		if errors.Is(err, client.ErrResourceNotFound) {
			log.Infof("this rule group does not currently exist")
			return nil
		}
		log.Fatalf("Unable to read rules from Grafana Mimir, %v", err)
	}

	if r.OutputDir != "" {
		log.Infof("Output dir detected, writing group '%s' of namespace '%s' to directory: %s", r.RuleGroup, r.Namespace, r.OutputDir)
		err := saveNamespaceRuleGroup(r.Namespace, []rwrulefmt.RuleGroup{*group}, r.OutputDir)

		return err
	}

	p := printer.New(r.DisableColor, r.ForceColor, term.IsTerminal(int(os.Stdout.Fd())))
	return p.PrintRuleGroup(*group)
}

func (r *RuleCommand) deleteRuleGroup(_ *kingpin.ParseContext) error {
	err := r.cli.DeleteRuleGroup(context.Background(), r.Namespace, r.RuleGroup)
	if err != nil && !errors.Is(err, client.ErrResourceNotFound) {
		log.Fatalf("Unable to delete rule group from Grafana Mimir, %v", err)
	}
	return nil
}

func (r *RuleCommand) loadRules(_ *kingpin.ParseContext) error {
	nss, err := rules.ParseFiles(r.Backend, r.RuleFilesList)
	if err != nil {
		return errors.Wrap(err, "load operation unsuccessful, unable to parse rules files")
	}
	r.ruleLoadTimestamp.SetToCurrentTime()

	for _, ns := range nss {
		for _, group := range ns.Groups {
			fmt.Printf("group: '%v', ns: '%v'\n", group.Name, ns.Namespace)
			curGroup, err := r.cli.GetRuleGroup(context.Background(), ns.Namespace, group.Name)
			if err != nil && !errors.Is(err, client.ErrResourceNotFound) {
				return errors.Wrap(err, "load operation unsuccessful, unable to contact Grafana Mimir API")
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

	r.ruleLoadSuccessTimestamp.SetToCurrentTime()
	return nil
}

// shouldCheckNamespace returns whether the namespace should be checked according to the allowed and ignored namespaces
func (r *RuleCommand) shouldCheckNamespace(namespace string) bool {
	if r.NamespacesRegex != nil {
		return r.NamespacesRegex.MatchString(namespace)
	}
	if r.IgnoredNamespacesRegex != nil {
		return !r.IgnoredNamespacesRegex.MatchString(namespace)
	}

	// when we have an allow list, only check those that we have explicitly defined.
	if r.namespacesMap != nil {
		_, allowed := r.namespacesMap[namespace]
		return allowed
	}

	_, ignored := r.ignoredNamespacesMap[namespace]
	return !ignored
}

func (r *RuleCommand) diffRules(_ *kingpin.ParseContext) error {
	err := r.setupArgs()
	if err != nil {
		return errors.Wrap(err, "diff operation unsuccessful, invalid arguments")
	}

	nss, err := rules.ParseFiles(r.Backend, r.RuleFilesList)
	if err != nil {
		return errors.Wrap(err, "diff operation unsuccessful, unable to parse rules files")
	}

	currentNamespaceMap, err := r.cli.ListRules(context.Background(), "")
	// TODO: Skipping the 404s here might end up in an unsual scenario.
	// If we're unable to reach the Mimir API due to a bad URL, we'll assume no rules are
	// part of the namespace and provide a diff of the whole ruleset.
	if err != nil && !errors.Is(err, client.ErrResourceNotFound) {
		return errors.Wrap(err, "diff operation unsuccessful, unable to contact Grafana Mimir API")
	}

	changes := []rules.NamespaceChange{}

	for _, ns := range nss {
		if !r.shouldCheckNamespace(ns.Namespace) {
			continue
		}

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
		if !r.shouldCheckNamespace(ns) {
			continue
		}

		changes = append(changes, rules.NamespaceChange{
			State:         rules.Deleted,
			Namespace:     ns,
			GroupsDeleted: deletedGroups,
		})
	}

	p := printer.New(r.DisableColor, r.ForceColor, term.IsTerminal(int(os.Stdout.Fd())))
	return p.PrintComparisonResult(changes, r.Verbose)
}

func (r *RuleCommand) syncRules(_ *kingpin.ParseContext) error {
	// Check the configuration.
	if r.SyncConcurrency < 1 || r.SyncConcurrency > maxSyncConcurrency {
		return fmt.Errorf("the configured concurrency (%d) must be a value between 1 and %d", r.SyncConcurrency, maxSyncConcurrency)
	}

	err := r.setupArgs()
	if err != nil {
		return errors.Wrap(err, "sync operation unsuccessful, invalid arguments")
	}

	nss, err := rules.ParseFiles(r.Backend, r.RuleFilesList)
	if err != nil {
		return errors.Wrap(err, "sync operation unsuccessful, unable to parse rules files")
	}

	currentNamespaceMap, err := r.cli.ListRules(context.Background(), "")
	// TODO: Skipping the 404s here might end up in an unsual scenario.
	// If we're unable to reach the Mimir API due to a bad URL, we'll assume no rules are
	// part of the namespace and provide a diff of the whole ruleset.
	if err != nil && !errors.Is(err, client.ErrResourceNotFound) {
		return errors.Wrap(err, "sync operation unsuccessful, unable to contact the Grafana Mimir API")
	}

	changes := []rules.NamespaceChange{}

	for _, ns := range nss {
		if !r.shouldCheckNamespace(ns.Namespace) {
			continue
		}

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
		if !r.shouldCheckNamespace(ns) {
			continue
		}

		changes = append(changes, rules.NamespaceChange{
			State:         rules.Deleted,
			Namespace:     ns,
			GroupsDeleted: deletedGroups,
		})
	}

	err = r.executeChanges(context.Background(), changes, r.SyncConcurrency)
	if err != nil {
		return errors.Wrap(err, "sync operation unsuccessful, unable to complete executing changes")
	}

	return nil
}

func (r *RuleCommand) executeChanges(ctx context.Context, changes []rules.NamespaceChange, concurrencyLimit int) error {
	// Prepare operations to run, so that we can easily parallelize them.
	var ops []rules.NamespaceChangeOperation
	for _, ch := range changes {
		ops = append(ops, ch.ToOperations()...)
	}

	// The execution breaks on first error encountered.
	err := concurrency.ForEachJob(ctx, len(ops), concurrencyLimit, func(ctx context.Context, idx int) error {
		op := ops[idx]

		log.WithFields(log.Fields{
			"group":     op.RuleGroup.Name,
			"namespace": op.Namespace,
		}).Infof("synching %s group", op.State.String())

		switch op.State {
		case rules.Created:
			return r.cli.CreateRuleGroup(ctx, op.Namespace, op.RuleGroup)

		case rules.Updated:
			return r.cli.CreateRuleGroup(ctx, op.Namespace, op.RuleGroup)

		case rules.Deleted:
			return r.cli.DeleteRuleGroup(ctx, op.Namespace, op.RuleGroup.Name)

		default:
			return fmt.Errorf("internal error: unexpected operation %q", op.State)
		}
	})

	if err != nil {
		return err
	}

	created, updated, deleted := rules.SummarizeChanges(changes)
	fmt.Println()
	fmt.Printf("Sync Summary: %v Groups Created, %v Groups Updated, %v Groups Deleted\n", created, updated, deleted)
	return nil
}

func (r *RuleCommand) prepare(_ *kingpin.ParseContext) error {
	err := r.setupArgs()
	if err != nil {
		return errors.Wrap(err, "prepare operation unsuccessful, invalid arguments")
	}

	namespaces, err := rules.ParseFiles(r.Backend, r.RuleFilesList)
	if err != nil {
		return errors.Wrap(err, "prepare operation unsuccessful, unable to parse rules files")
	}

	// Do not apply the aggregation label to excluded rule groups.
	applyTo := func(group rwrulefmt.RuleGroup, _ rulefmt.RuleNode) bool {
		_, excluded := r.aggregationLabelExcludedRuleGroupsList[group.Name]
		return !excluded
	}

	var count, mod int
	for _, ruleNamespace := range namespaces {
		c, m, err := ruleNamespace.AggregateBy(r.AggregationLabel, applyTo)
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

func (r *RuleCommand) lint(_ *kingpin.ParseContext) error {
	err := r.setupArgs()
	if err != nil {
		return errors.Wrap(err, "prepare operation unsuccessful, invalid arguments")
	}

	namespaces, err := rules.ParseFiles(r.Backend, r.RuleFilesList)
	if err != nil {
		return errors.Wrap(err, "prepare operation unsuccessful, unable to parse rules files")
	}

	var count, mod int
	for _, ruleNamespace := range namespaces {
		c, m, err := ruleNamespace.LintExpressions(r.Backend)
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

func (r *RuleCommand) checkRules(_ *kingpin.ParseContext) error {
	err := r.setupArgs()
	if err != nil {
		return errors.Wrap(err, "check operation unsuccessful, invalid arguments")
	}

	namespaces, err := rules.ParseFiles(r.Backend, r.RuleFilesList)
	if err != nil {
		return errors.Wrap(err, "check operation unsuccessful, unable to parse rules files")
	}

	lvl := log.WarnLevel
	if r.Strict {
		lvl = log.ErrorLevel
	}

	for _, ruleNamespace := range namespaces {
		n := ruleNamespace.CheckRecordingRules(r.Strict)
		if n != 0 {
			return fmt.Errorf("%d erroneous recording rule names", n)
		}
		duplicateRules := checkDuplicates(ruleNamespace.Groups)
		if len(duplicateRules) != 0 {
			log.WithFields(log.Fields{
				"namespace":       ruleNamespace.Namespace,
				"error":           "rules should emit unique series, to avoid producing inconsistencies while recording expressions",
				"duplicate_rules": len(duplicateRules),
			}).Logf(lvl, "duplicate rules found")
			for _, n := range duplicateRules {
				fields := log.Fields{
					"namespace": ruleNamespace.Namespace,
					"metric":    n.metric,
				}
				for i, l := range n.label {
					fields["label["+i+"]"] = l
				}
				log.WithFields(fields).Logf(lvl, "duplicate rule")
			}
			if r.Strict {
				return fmt.Errorf("%d duplicate rules found in namespace %q", len(duplicateRules), ruleNamespace.Namespace)
			}
		}
	}

	return nil
}

// Taken from https://github.com/prometheus/prometheus/blob/8c8de46003d1800c9d40121b4a5e5de8582ef6e1/cmd/promtool/main.go#L403
type compareRuleType struct {
	metric string
	label  map[string]string
}

func checkDuplicates(groups []rwrulefmt.RuleGroup) []compareRuleType {
	var duplicates []compareRuleType

	for _, group := range groups {
		for index, rule := range group.Rules {
			inst := compareRuleType{
				metric: ruleMetric(rule),
				label:  rule.Labels,
			}
			for i := 0; i < index; i++ {
				t := compareRuleType{
					metric: ruleMetric(group.Rules[i]),
					label:  group.Rules[i].Labels,
				}
				if reflect.DeepEqual(t, inst) {
					duplicates = append(duplicates, t)
				}
			}
		}
	}
	return duplicates
}

func ruleMetric(rule rulefmt.RuleNode) string {
	if rule.Alert.Value != "" {
		return rule.Alert.Value
	}
	return rule.Record.Value
}

// End taken from https://github.com/prometheus/prometheus/blob/8c8de46003d1800c9d40121b4a5e5de8582ef6e1/cmd/promtool/main.go#L403

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

		if err := os.WriteFile(filepath, payload, 0644); err != nil {
			return err
		}
	}

	return nil
}

func (r *RuleCommand) deleteNamespace(_ *kingpin.ParseContext) error {
	err := r.cli.DeleteNamespace(context.Background(), r.Namespace)
	if err != nil && !errors.Is(err, client.ErrResourceNotFound) {
		log.Fatalf("Unable to delete namespace from Grafana Mimir, %v", err)
	}
	return nil
}
