// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/cmd/cortex/main_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package main

import (
	"bytes"
	"flag"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/go-kit/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v2"

	"github.com/grafana/mimir/pkg/mimir"
	"github.com/grafana/mimir/pkg/util/fieldcategory"
)

func TestFlagParsing(t *testing.T) {
	for name, tc := range map[string]struct {
		arguments      []string
		yaml           string
		stdoutMessage  string                                // string that must be included in stdout
		stderrMessage  string                                // string that must be included in stderr
		stdoutExcluded string                                // string that must NOT be included in stdout
		stderrExcluded string                                // string that must NOT be included in stderr
		assertConfig   func(t *testing.T, cfg *mimir.Config) // if not nil, will assert that stdout is config, unmarshal it as YAML and invoke this function.
	}{
		"help-short": {
			arguments:      []string{"-h"},
			stdoutMessage:  "Usage of", // Usage must be on stdout, not stderr.
			stderrExcluded: "Usage of",
		},

		"help": {
			arguments:      []string{"-help"},
			stdoutMessage:  "Usage of", // Usage must be on stdout, not stderr.
			stderrExcluded: "Usage of",
		},

		"help-all": {
			arguments:      []string{"-help-all"},
			stdoutMessage:  "Usage of", // Usage must be on stdout, not stderr.
			stderrExcluded: "Usage of",
		},

		"unknown flag": {
			arguments:      []string{"-unknown.flag"},
			stderrMessage:  "Run with -help to get a list of available parameters",
			stdoutExcluded: "Usage of", // No usage description on unknown flag.
			stderrExcluded: "Usage of",
		},

		"new flag, with config": {
			arguments:     []string{"-mem-ballast-size-bytes=100000"},
			yaml:          "target: ingester",
			stdoutMessage: "target: ingester",
		},

		"default values": {
			stdoutMessage: "target: all\n",
		},

		"config": {
			yaml:          "target: ingester",
			stdoutMessage: "target: ingester\n",
		},

		"config with expand-env": {
			arguments:     []string{"-config.expand-env"},
			yaml:          "target: $TARGET",
			stdoutMessage: "target: ingester\n",
		},

		"config with arguments override": {
			yaml:          "target: ingester",
			arguments:     []string{"-target=distributor"},
			stdoutMessage: "target: distributor\n",
		},

		"user visible module listing": {
			arguments:      []string{"-modules"},
			stdoutMessage:  "ingester *\n",
			stderrExcluded: "ingester\n",
		},

		"user visible module listing flag take precedence over target flag": {
			arguments:      []string{"-modules", "-target=blah"},
			stdoutMessage:  "ingester *\n",
			stderrExcluded: "ingester\n",
		},

		"root level configuration option specified as an empty node in YAML": {
			yaml:          "querier:",
			stderrMessage: "the Querier configuration in YAML has been specified as an empty YAML node",
		},

		"version": {
			arguments:     []string{"-version"},
			stdoutMessage: "Mimir, version",
		},

		"common yaml inheritance with common config in the first place": {
			yaml: `
common:
  storage:
    backend: s3
    s3:
      region: common-region
blocks_storage:
  s3:
    region: blocks-storage-region
ruler_storage:
  backend: s3
  s3:
    bucket_name: ruler-bucket
alertmanager_storage:
  backend: local
`,
			assertConfig: func(t *testing.T, cfg *mimir.Config) {
				require.Equal(t, "s3", cfg.BlocksStorage.Bucket.Backend, "Blocks storage bucket backend should be inherited from common")
				require.Equal(t, "blocks-storage-region", cfg.BlocksStorage.Bucket.S3.Region, "Blocks storage bucket s3 region should override common")
				require.Equal(t, "s3", cfg.RulerStorage.Backend, "Ruler storage backend should stay the same (it's explicitly defined)")
				require.Equal(t, "common-region", cfg.RulerStorage.S3.Region, "Ruler storage s3 region should be inherited from common")
				require.Equal(t, "ruler-bucket", cfg.RulerStorage.S3.BucketName, "Ruler storage s3 bucket name should be defined")
				require.Equal(t, "local", cfg.AlertmanagerStorage.Backend, "Alertmanager storage backend should be local (overriding common)")
				require.Equal(t, "common-region", cfg.AlertmanagerStorage.S3.Region, "Alertmanager storage s3 region should be inherited from common as overrides don't know about config semantics")
			},
		},

		"common yaml inheritance with common config in the last place": {
			yaml: `
blocks_storage:
  s3:
    region: blocks-storage-region
ruler_storage:
  backend: s3
  s3:
    bucket_name: ruler-bucket
alertmanager_storage:
  backend: local
common:
  storage:
    backend: s3
    s3:
      region: common-region
`,
			assertConfig: func(t *testing.T, cfg *mimir.Config) {
				require.Equal(t, "s3", cfg.BlocksStorage.Bucket.Backend, "Blocks storage bucket backend should be inherited from common")
				require.Equal(t, "blocks-storage-region", cfg.BlocksStorage.Bucket.S3.Region, "Blocks storage bucket s3 region should override common")
				require.Equal(t, "s3", cfg.RulerStorage.Backend, "Ruler storage backend should stay the same (it's explicitly defined)")
				require.Equal(t, "common-region", cfg.RulerStorage.S3.Region, "Ruler storage s3 region should be inherited from common")
				require.Equal(t, "ruler-bucket", cfg.RulerStorage.S3.BucketName, "Ruler storage s3 bucket name should be defined")
				require.Equal(t, "local", cfg.AlertmanagerStorage.Backend, "Alertmanager storage backend should be local (overriding common)")
				require.Equal(t, "common-region", cfg.AlertmanagerStorage.S3.Region, "Alertmanager storage s3 region should be inherited from common as overrides don't know about config semantics")
			},
		},

		"common yaml sets a value but specific config reverts it back": {
			yaml: `
common:
  storage:
    backend: s3
    s3:
      region: common-region
blocks_storage:
  s3:
    region: ""
`,
			assertConfig: func(t *testing.T, cfg *mimir.Config) {
				require.Equal(t, "", cfg.BlocksStorage.Bucket.S3.Region, "Blocks storage region should be empty since it's explicitly set to be empty")
				require.Equal(t, "common-region", cfg.RulerStorage.S3.Region, "Ruler storage should inherit the common-region")
			},
		},

		"common yaml unmarshaling is strict": {
			yaml: `
common:
  unknown: value
`,
			stderrMessage: "field unknown not found",
		},

		"common yaml overridden by a common flag and specific flag": {
			yaml: `
common:
  storage:
    backend: s3
ruler_storage:
  backend: local
`,
			arguments: []string{
				"-common.storage.backend=swift",
				"-blocks-storage.backend=gcs",
			},
			assertConfig: func(t *testing.T, cfg *mimir.Config) {
				require.Equal(t, "gcs", cfg.BlocksStorage.Bucket.Backend, "Blocks storage bucket should be overridden")
				require.Equal(t, "swift", cfg.RulerStorage.Backend, "Ruler storage should be set from the common flag, as flags prevail over yaml")
			},
		},

		"common flag inheritance": {
			arguments: []string{
				"-common.storage.backend=s3",
				"-common.storage.s3.region=common-region",
				"-blocks-storage.s3.region=blocks-storage-region", // overrides common region
				"-ruler-storage.backend=s3",                       // overrides common backend with same value
				"-ruler-storage.s3.bucket-name=ruler-bucket",      // sets a specific bucket value for ruler (common is not set)
				"-alertmanager-storage.backend=local",             // local alertmanager storage
			},
			assertConfig: func(t *testing.T, cfg *mimir.Config) {
				require.Equal(t, "s3", cfg.BlocksStorage.Bucket.Backend, "Blocks storage bucket backend should be inherited from common")
				require.Equal(t, "blocks-storage-region", cfg.BlocksStorage.Bucket.S3.Region, "Blocks storage bucket s3 region should override common")
				require.Equal(t, "s3", cfg.RulerStorage.Backend, "Ruler storage backend should stay the same (it's explicitly defined)")
				require.Equal(t, "common-region", cfg.RulerStorage.S3.Region, "Ruler storage s3 region should be inherited from common")
				require.Equal(t, "ruler-bucket", cfg.RulerStorage.S3.BucketName, "Ruler storage s3 bucket name should be defined")
				require.Equal(t, "local", cfg.AlertmanagerStorage.Backend, "Alertmanager storage backend should be local (overriding common)")
				require.Equal(t, "common-region", cfg.AlertmanagerStorage.S3.Region, "Alertmanager storage s3 region should be inherited from common as overrides don't know about config semantics")
			},
		},

		// we cannot test the happy path, as mimir would then fully start
	} {
		t.Run(name, func(t *testing.T) {
			_ = os.Setenv("TARGET", "ingester")
			testSingle(t, tc.arguments, tc.yaml, []byte(tc.stdoutMessage), []byte(tc.stderrMessage), []byte(tc.stdoutExcluded), []byte(tc.stderrExcluded), tc.assertConfig)
		})
	}
}

func TestHelp(t *testing.T) {
	for _, tc := range []struct {
		name     string
		arg      string
		filename string
	}{
		{
			name:     "basic",
			arg:      "-h",
			filename: "help.txt.tmpl",
		},
		{
			name:     "all",
			arg:      "-help-all",
			filename: "help-all.txt.tmpl",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			oldArgs, oldStdout, oldStderr, oldTestMode, oldCmdLine := os.Args, os.Stdout, os.Stderr, testMode, flag.CommandLine
			restored := false
			restoreIfNeeded := func() {
				if restored {
					return
				}

				os.Stdout = oldStdout
				os.Stderr = oldStderr
				os.Args = oldArgs
				testMode = oldTestMode
				flag.CommandLine = oldCmdLine
				restored = true
			}
			t.Cleanup(restoreIfNeeded)

			testMode = true
			co := captureOutput(t)

			const cmd = "./cmd/mimir/mimir"
			os.Args = []string{cmd, tc.arg}

			// reset default flags
			flag.CommandLine = flag.NewFlagSet(os.Args[0], flag.ExitOnError)

			main()

			stdout, stderr := co.Done()

			// Restore stdout and stderr before reporting errors to make them visible.
			restoreIfNeeded()

			expected, err := os.ReadFile(tc.filename)
			require.NoError(t, err)
			assert.Equalf(t, string(expected), string(stdout), "%s %s output changed; try `make reference-help`", cmd, tc.arg)
			assert.Empty(t, stderr)
		})
	}
}

func testSingle(t *testing.T, arguments []string, configYAML string, stdoutMessage, stderrMessage, stdoutExcluded, stderrExcluded []byte, assertConfig func(*testing.T, *mimir.Config)) {
	t.Helper()
	oldArgs, oldStdout, oldStderr, oldTestMode := os.Args, os.Stdout, os.Stderr, testMode
	restored := false
	restoreIfNeeded := func() {
		if restored {
			return
		}
		os.Stdout = oldStdout
		os.Stderr = oldStderr
		os.Args = oldArgs
		testMode = oldTestMode
		restored = true
	}
	defer restoreIfNeeded()

	if configYAML != "" {
		tempDir := t.TempDir()
		fpath := filepath.Join(tempDir, "test")
		err := os.WriteFile(fpath, []byte(configYAML), 0600)
		require.NoError(t, err)

		arguments = append(arguments, "-"+configFileOption, fpath)
	}

	arguments = append([]string{"./mimir"}, arguments...)

	testMode = true
	os.Args = arguments
	co := captureOutput(t)

	// reset default flags
	flag.CommandLine = flag.NewFlagSet(arguments[0], flag.ExitOnError)

	main()

	stdout, stderr := co.Done()

	// Restore stdout and stderr before reporting errors to make them visible.
	restoreIfNeeded()
	if !bytes.Contains(stdout, stdoutMessage) {
		t.Errorf("Expected on stdout: %q, stdout: %s\n", stdoutMessage, stdout)
	}
	if !bytes.Contains(stderr, stderrMessage) {
		t.Errorf("Expected on stderr: %q, stderr: %s\n", stderrMessage, stderr)
	}
	if len(stdoutExcluded) > 0 && bytes.Contains(stdout, stdoutExcluded) {
		t.Errorf("Unexpected output on stdout: %q, stdout: %s\n", stdoutExcluded, stdout)
	}
	if len(stderrExcluded) > 0 && bytes.Contains(stderr, stderrExcluded) {
		t.Errorf("Unexpected output on stderr: %q, stderr: %s\n", stderrExcluded, stderr)
	}
	if assertConfig != nil {
		var cfg mimir.Config
		require.NoError(t, yaml.Unmarshal(stdout, &cfg), "Can't unmarshal stdout as yaml config")
		assertConfig(t, &cfg)
	}
}

type capturedOutput struct {
	stdoutBuf bytes.Buffer
	stderrBuf bytes.Buffer

	wg                         sync.WaitGroup
	stdoutReader, stdoutWriter *os.File
	stderrReader, stderrWriter *os.File
}

func captureOutput(t *testing.T) *capturedOutput {
	stdoutR, stdoutW, err := os.Pipe()
	require.NoError(t, err)
	os.Stdout = stdoutW

	stderrR, stderrW, err := os.Pipe()
	require.NoError(t, err)
	os.Stderr = stderrW

	co := &capturedOutput{
		stdoutReader: stdoutR,
		stdoutWriter: stdoutW,
		stderrReader: stderrR,
		stderrWriter: stderrW,
	}
	co.wg.Add(1)
	go func() {
		defer co.wg.Done()
		io.Copy(&co.stdoutBuf, stdoutR)
	}()

	co.wg.Add(1)
	go func() {
		defer co.wg.Done()
		io.Copy(&co.stderrBuf, stderrR)
	}()

	return co
}

func (co *capturedOutput) Done() (stdout []byte, stderr []byte) {
	// we need to close writers for readers to stop
	_ = co.stdoutWriter.Close()
	_ = co.stderrWriter.Close()

	co.wg.Wait()

	return co.stdoutBuf.Bytes(), co.stderrBuf.Bytes()
}

func TestExpandEnvironmentVariables(t *testing.T) {
	var tests = []struct {
		in  string
		out string
	}{
		// Environment variables can be specified as ${env} or $env.
		{"x$y", "xy"},
		{"x${y}", "xy"},

		// Environment variables are case-sensitive. Neither are replaced.
		{"x$Y", "x"},
		{"x${Y}", "x"},

		// Defaults can only be specified when using braces.
		{"x${Z:D}", "xD"},
		{"x${Z:A B C D}", "xA B C D"}, // Spaces are allowed in the default.
		{"x${Z:}", "x"},

		// Defaults don't work unless braces are used.
		{"x$y:D", "xy:D"},
	}

	for _, test := range tests {
		test := test
		t.Run(test.in, func(t *testing.T) {
			_ = os.Setenv("y", "y")
			output := expandEnvironmentVariables([]byte(test.in))
			assert.Equal(t, test.out, string(output), "Input: %s", test.in)
		})
	}
}

func TestParseConfigFileParameter(t *testing.T) {
	var tests = []struct {
		args       string
		configFile string
		expandEnv  bool
	}{
		{"", "", false},
		{"--foo", "", false},
		{"-f -a", "", false},

		{"--config.file=foo", "foo", false},
		{"--config.file foo", "foo", false},
		{"--config.file=foo --config.expand-env", "foo", true},
		{"--config.expand-env --config.file=foo", "foo", true},

		{"--opt1 --config.file=foo", "foo", false},
		{"--opt1 --config.file foo", "foo", false},
		{"--opt1 --config.file=foo --config.expand-env", "foo", true},
		{"--opt1 --config.expand-env --config.file=foo", "foo", true},

		{"--config.file=foo --opt1", "foo", false},
		{"--config.file foo --opt1", "foo", false},
		{"--config.file=foo --config.expand-env --opt1", "foo", true},
		{"--config.expand-env --config.file=foo --opt1", "foo", true},

		{"--config.file=foo --opt1 --config.expand-env", "foo", true},
		{"--config.expand-env --opt1 --config.file=foo", "foo", true},
	}
	for _, test := range tests {
		test := test
		t.Run(test.args, func(t *testing.T) {
			args := strings.Split(test.args, " ")
			configFile, expandEnv := parseConfigFileParameter(args)
			assert.Equal(t, test.configFile, configFile)
			assert.Equal(t, test.expandEnv, expandEnv)
		})
	}
}

func TestFieldCategoryOverridesNotStale(t *testing.T) {
	overrides := make(map[string]struct{})
	fieldcategory.VisitOverrides(func(s string) {
		overrides[s] = struct{}{}
	})

	fs := flag.NewFlagSet("test", flag.PanicOnError)

	var (
		cfg mimir.Config
		mf  mainFlags
	)
	cfg.RegisterFlags(fs, log.NewNopLogger())
	mf.registerFlags(fs)

	fs.VisitAll(func(fl *flag.Flag) {
		delete(overrides, fl.Name)
	})

	require.Empty(t, overrides, "There are category overrides for configuration options that no longer exist")
}
