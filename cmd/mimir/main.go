// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/cmd/cortex/main.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package main

import (
	"bytes"
	"crypto/sha256"
	"flag"
	"fmt"
	"io"
	"os"
	"runtime"
	"sort"
	"strings"

	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/ballast"
	"github.com/grafana/dskit/flagext"
	"github.com/grafana/dskit/tracing"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	jaegercfg "github.com/uber/jaeger-client-go/config"
	"gopkg.in/yaml.v3"

	"github.com/grafana/mimir/pkg/mimir"
	util_log "github.com/grafana/mimir/pkg/util/log"
	"github.com/grafana/mimir/pkg/util/usage"
	"github.com/grafana/mimir/pkg/util/version"
)

// configHash exposes information about the loaded config
var configHash = prometheus.NewGaugeVec(
	prometheus.GaugeOpts{
		Name: "cortex_config_hash",
		Help: "Hash of the currently active config file.",
	},
	[]string{"sha256"},
)

func init() {
	prometheus.MustRegister(version.NewCollector("mimir"))
	prometheus.MustRegister(version.NewCollector("cortex"))
	prometheus.MustRegister(configHash)
}

const (
	configFileOption = "config.file"
	configExpandEnv  = "config.expand-env"
)

var testMode = false

type mainFlags struct {
	ballastBytes             int     `category:"advanced"`
	mutexProfileFraction     int     `category:"advanced"`
	blockProfileRate         int     `category:"advanced"`
	rateLimitedLogsEnabled   bool    `category:"experimental"`
	rateLimitedLogsPerSecond float64 `category:"experimental"`
	rateLimitedLogsBurstSize int     `category:"experimental"`
	printVersion             bool
	printModules             bool
	printHelp                bool
	printHelpAll             bool
}

func (mf *mainFlags) registerFlags(fs *flag.FlagSet) {
	fs.IntVar(&mf.ballastBytes, "mem-ballast-size-bytes", 0, "Size of memory ballast to allocate.")
	fs.IntVar(&mf.mutexProfileFraction, "debug.mutex-profile-fraction", 0, "Fraction of mutex contention events that are reported in the mutex profile. On average 1/rate events are reported. 0 to disable.")
	fs.IntVar(&mf.blockProfileRate, "debug.block-profile-rate", 0, "Fraction of goroutine blocking events that are reported in the blocking profile. 1 to include every blocking event in the profile, 0 to disable.")
	fs.BoolVar(&mf.rateLimitedLogsEnabled, "log.rate-limit-enabled", false, "Use rate limited logger to reduce the number of logged messages per second.")
	fs.Float64Var(&mf.rateLimitedLogsPerSecond, "log.rate-limit-logs-per-second", 10000, "Maximum number of messages per second to be logged.")
	fs.IntVar(&mf.rateLimitedLogsBurstSize, "log.rate-limit-logs-burst-size", 1000, "Burst size, i.e., maximum number of messages that can be logged at once, temporarily exceeding the configured maximum logs per second.")
	fs.BoolVar(&mf.printVersion, "version", false, "Print application version and exit.")
	fs.BoolVar(&mf.printModules, "modules", false, "List available values that can be used as target.")
	fs.BoolVar(&mf.printHelp, "help", false, "Print basic help.")
	fs.BoolVar(&mf.printHelp, "h", false, "Print basic help.")
	fs.BoolVar(&mf.printHelpAll, "help-all", false, "Print help, also including advanced and experimental parameters.")
}

func main() {
	// Cleanup all flags registered via init() methods of 3rd-party libraries.
	flag.CommandLine = flag.NewFlagSet(os.Args[0], flag.ExitOnError)

	var (
		cfg       mimir.Config
		mainFlags mainFlags
	)

	configFile, expandEnv := parseConfigFileParameter(os.Args[1:])

	// This sets default values from flags to the config.
	// It needs to be called before parsing the config file!
	cfg.RegisterFlags(flag.CommandLine, util_log.Logger)

	if configFile != "" {
		if err := LoadConfig(configFile, expandEnv, &cfg); err != nil {
			fmt.Fprintf(os.Stderr, "error loading config from %s: %v\n", configFile, err)
			if testMode {
				return
			}
			exit(1)
		}
	}

	// Ignore -config.file and -config.expand-env here, since they are parsed separately, but are still present on the command line.
	flagext.IgnoredFlag(flag.CommandLine, configFileOption, "Configuration file to load.")
	_ = flag.CommandLine.Bool(configExpandEnv, false, "Expands ${var} or $var in config according to the values of the environment variables.")

	mainFlags.registerFlags(flag.CommandLine)

	flag.CommandLine.Usage = func() { /* don't do anything by default, we will print usage ourselves, but only when requested. */ }
	flag.CommandLine.Init(flag.CommandLine.Name(), flag.ContinueOnError)

	if err := flag.CommandLine.Parse(os.Args[1:]); err != nil {
		fmt.Fprintln(flag.CommandLine.Output(), "Run with -help to get a list of available parameters")
		if !testMode {
			exit(2)
		}
	}

	if mainFlags.printHelp || mainFlags.printHelpAll {
		// Print available parameters to stdout, so that users can grep/less them easily.
		flag.CommandLine.SetOutput(os.Stdout)
		if err := usage.Usage(mainFlags.printHelpAll, &mainFlags, &cfg); err != nil {
			fmt.Fprintf(os.Stderr, "error printing usage: %s\n", err)
			exit(1)
		}

		if !testMode {
			exit(2)
		}
		return
	}

	if mainFlags.printVersion {
		fmt.Fprintln(os.Stdout, version.Print("Mimir"))
		return
	}

	if err := mimir.InheritCommonFlagValues(util_log.Logger, flag.CommandLine, cfg.Common, &cfg); err != nil {
		fmt.Fprintf(os.Stderr, "error inheriting common flag values: %v\n", err)
		if !testMode {
			exit(1)
		}
	}

	// Validate the config once both the config file has been loaded
	// and CLI flags parsed.
	if err := cfg.Validate(util_log.Logger); err != nil {
		fmt.Fprintf(os.Stderr, "error validating config: %v\n", err)
		if !testMode {
			exit(1)
		}
	}

	// Continue on if -modules flag is given. Code handling the
	// -modules flag will not start mimir.
	if testMode && !mainFlags.printModules {
		DumpYaml(&cfg)
		return
	}

	if mainFlags.mutexProfileFraction > 0 {
		runtime.SetMutexProfileFraction(mainFlags.mutexProfileFraction)
	}
	if mainFlags.blockProfileRate > 0 {
		runtime.SetBlockProfileRate(mainFlags.blockProfileRate)
	}
	clampGOMAXPROCS()

	reg := prometheus.DefaultRegisterer
	cfg.Server.Log = util_log.InitLogger(cfg.Server.LogFormat, cfg.Server.LogLevel, true, util_log.RateLimitedLoggerCfg{
		Enabled:       mainFlags.rateLimitedLogsEnabled,
		LogsPerSecond: mainFlags.rateLimitedLogsPerSecond,
		LogsBurstSize: mainFlags.rateLimitedLogsBurstSize,
		Registry:      reg,
	})

	var ballast = ballast.Allocate(mainFlags.ballastBytes)

	// In testing mode skip JAEGER setup to avoid panic due to
	// "duplicate metrics collector registration attempted"
	if !testMode {
		name := os.Getenv("JAEGER_SERVICE_NAME")
		if name == "" {
			name = "mimir"
			if len(cfg.Target) == 1 {
				name += "-" + cfg.Target[0]
			}
		}

		// Setting the environment variable JAEGER_AGENT_HOST enables tracing.
		if trace, err := tracing.NewFromEnv(name, jaegercfg.MaxTagValueLength(16e3)); err != nil {
			level.Error(util_log.Logger).Log("msg", "Failed to setup tracing", "err", err.Error())
		} else {
			defer trace.Close()
		}
	}

	t, err := mimir.New(cfg, reg)
	util_log.CheckFatal("initializing application", err)

	if mainFlags.printModules {
		allDeps := t.ModuleManager.DependenciesForModule(mimir.All)

		for _, m := range t.ModuleManager.UserVisibleModuleNames() {
			ix := sort.SearchStrings(allDeps, m)
			included := ix < len(allDeps) && allDeps[ix] == m

			if included {
				fmt.Fprintln(os.Stdout, m, "*")
			} else {
				fmt.Fprintln(os.Stdout, m)
			}
		}

		fmt.Fprintln(os.Stdout)
		fmt.Fprintln(os.Stdout, "Modules marked with * are included in target All.")
		return
	}

	level.Info(util_log.Logger).Log("msg", "Starting application", "version", version.Info())

	err = t.Run()

	runtime.KeepAlive(ballast)
	util_log.CheckFatal("running application", err)
}

func clampGOMAXPROCS() {
	if runtime.GOMAXPROCS(0) <= runtime.NumCPU() {
		return
	}
	level.Warn(util_log.Logger).Log(
		"msg", "GOMAXPROCS is higher than the number of CPUs; clamping it to NumCPU; please report if this doesn't fit your use case",
		"GOMAXPROCS", runtime.GOMAXPROCS(0),
		"NumCPU", runtime.NumCPU(),
	)
	runtime.GOMAXPROCS(runtime.NumCPU())
}

func exit(code int) {
	if err := util_log.Flush(); err != nil {
		fmt.Fprintln(os.Stderr, "Could not flush logger", err)
	}
	os.Exit(code)
}

// Parse -config.file and -config.expand-env option via separate flag set, to avoid polluting default one and calling flag.Parse on it twice.
func parseConfigFileParameter(args []string) (configFile string, expandEnv bool) {
	// ignore errors and any output here. Any flag errors will be reported by main flag.Parse() call.
	fs := flag.NewFlagSet("", flag.ContinueOnError)
	fs.SetOutput(io.Discard)

	// usage not used in these functions.
	fs.StringVar(&configFile, configFileOption, "", "")
	fs.BoolVar(&expandEnv, configExpandEnv, false, "")

	// Try to find -config.file and -config.expand-env option in the flags. As Parsing stops on the first error, eg. unknown flag, we simply
	// try remaining parameters until we find config flag, or there are no params left.
	// (ContinueOnError just means that flag.Parse doesn't call panic or os.Exit, but it returns error, which we ignore)
	for len(args) > 0 {
		_ = fs.Parse(args)
		args = args[1:]
	}

	return
}

// LoadConfig read YAML-formatted config from filename into cfg.
func LoadConfig(filename string, expandEnv bool, cfg *mimir.Config) error {
	buf, err := os.ReadFile(filename)
	if err != nil {
		return errors.Wrap(err, "Error reading config file")
	}

	// create a sha256 hash of the config before expansion and expose it via
	// the config_info metric
	hash := sha256.Sum256(buf)
	configHash.Reset()
	configHash.WithLabelValues(fmt.Sprintf("%x", hash)).Set(1)

	if expandEnv {
		buf = expandEnvironmentVariables(buf)
	}

	dec := yaml.NewDecoder(bytes.NewReader(buf))
	dec.KnownFields(true)

	// Unmarshal with common config unmarshaler.
	if err := dec.Decode((*mimir.ConfigWithCommon)(cfg)); err != nil {
		return errors.Wrap(err, "Error parsing config file")
	}

	return nil
}

func DumpYaml(cfg *mimir.Config) {
	out, err := yaml.Marshal(cfg)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
	} else {
		fmt.Printf("%s\n", out)
	}
}

// expandEnvironmentVariables replaces ${var} or $var in config according to the values of the current environment variables.
// The replacement is case-sensitive. References to undefined variables are replaced by the empty string.
// A default value can be given by using the form ${var:default value}.
func expandEnvironmentVariables(config []byte) []byte {
	return []byte(os.Expand(string(config), func(key string) string {
		keyAndDefault := strings.SplitN(key, ":", 2)
		key = keyAndDefault[0]

		v := os.Getenv(key)
		if v == "" && len(keyAndDefault) == 2 {
			v = keyAndDefault[1] // Set value to the default.
		}

		if strings.Contains(v, "\n") {
			return strings.Replace(v, "\n", "", -1)
		}

		return v
	}))
}
