// SPDX-License-Identifier: AGPL-3.0-only

package commands

import (
	"testing"

	"github.com/alecthomas/kingpin/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// buildBackfillApp returns a kingpin app with BackfillCommand registered and
// the BackfillCommand instance for assertion purposes.
func buildBackfillApp(t *testing.T) (*kingpin.Application, *BackfillCommand) {
	t.Helper()
	app := kingpin.New("mimirtool-test", "test harness for BackfillCommand")
	app.Terminate(nil) // don't os.Exit on parse error
	cmd := &BackfillCommand{}
	envVars := EnvVarNames{
		Address:               "X_ADDR",
		APIUser:               "X_USER",
		TenantID:              "X_ID",
		APIKey:                "X_KEY",
		ExtraHeaders:          "X_EXTRA",
		TLSCAPath:             "X_CA",
		TLSCertPath:           "X_CERT",
		TLSKeyPath:            "X_KEY_PATH",
		TLSInsecureSkipVerify: "X_TLS_INSECURE",
	}
	cmd.Register(app, envVars, &LoggerConfig{})
	return app, cmd
}

// findBackfillCmdModel locates the backfill subcommand in the kingpin model.
// Walking app.Model().Commands is the robust proxy for "mimirtool backfill --help"
// visibility: it asserts the flag is registered regardless of whether the app has
// a `help` subcommand wired up.
func findBackfillCmdModel(t *testing.T, app *kingpin.Application) *kingpin.CmdModel {
	t.Helper()
	for _, c := range app.Model().Commands {
		if c.Name == "backfill" {
			return c
		}
	}
	t.Fatal("backfill command must be registered")
	return nil
}

func TestBackfillCommand_FlagExclusion_BothTrueFails(t *testing.T) {
	app, _ := buildBackfillApp(t)
	validDir := t.TempDir()

	_, err := app.Parse([]string{
		"backfill",
		"--address=http://localhost",
		"--id=tenant-1",
		"--skip-chunk-verification",
		"--full-report",
		validDir,
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "--full-report requires deep analysis",
		"cross-flag mutual exclusion must reject --full-report + --skip-chunk-verification combo")
}

// TestBackfillCommand_FlagExclusion_EitherAlonePasses asserts that the mutual-exclusion
// check only rejects the BOTH-true combination. Note: kingpin's Parse dispatches the
// command Action after flag validation, and our Action runs real verification on the
// supplied block-dir. The empty tempdir here legitimately fails verification, so the
// test asserts that the error (if any) is NOT the mutual-exclusion error.
func TestBackfillCommand_FlagExclusion_EitherAlonePasses(t *testing.T) {
	for _, flag := range []string{"--skip-chunk-verification", "--full-report"} {
		t.Run(flag, func(t *testing.T) {
			app, _ := buildBackfillApp(t)
			validDir := t.TempDir()
			_, err := app.Parse([]string{
				"backfill",
				"--address=http://localhost",
				"--id=tenant-1",
				flag,
				validDir,
			})
			if err != nil {
				assert.NotContains(t, err.Error(), "--full-report requires deep analysis",
					"mutual-exclusion must not fire when only one of the two flags is set")
			}
		})
	}
}

// TestBackfillCommand_NoBypassFlagsRegistered enforces SPEC §7 "no escape hatch".
// Verification is mandatory; no flag may bypass it.
func TestBackfillCommand_NoBypassFlagsRegistered(t *testing.T) {
	app, _ := buildBackfillApp(t)
	backfillCmd := findBackfillCmdModel(t, app)

	forbidden := []string{"skip-verification", "no-verify", "force", "bypass-verification", "no-verification"}
	for _, f := range backfillCmd.Flags {
		for _, bad := range forbidden {
			assert.NotEqual(t, bad, f.Name,
				"SPEC §7: mimirtool backfill must not register a verification-bypass flag (found %q)", f.Name)
		}
	}
}

// TestBackfillCommand_NewFlagsRegistered walks backfillCmd.Flags directly
// rather than parsing a `help backfill` invocation. The `help` subcommand
// isn't registered on a minimal kingpin.New(...) app, so a parse-help
// approach would see empty output. Walking the model is the robust proxy
// for "mimirtool backfill --help" visibility.
func TestBackfillCommand_NewFlagsRegistered(t *testing.T) {
	app, _ := buildBackfillApp(t)
	backfillCmd := findBackfillCmdModel(t, app)

	wanted := map[string]bool{
		"dry-run":                 false,
		"skip-chunk-verification": false,
		"full-report":             false,
		"verify-concurrency":      false,
	}
	for _, f := range backfillCmd.Flags {
		if _, ok := wanted[f.Name]; ok {
			wanted[f.Name] = true
			assert.NotEmpty(t, f.Help, "flag --%s must have a non-empty help string", f.Name)
		}
	}
	for name, found := range wanted {
		assert.True(t, found, "flag --%s must be listed on the backfill command", name)
	}
}

// TestBackfillCommand_VerifyConcurrencyParsesInt asserts --verify-concurrency populates
// the IntVar target. kingpin's Parse populates BoolVar/IntVar BEFORE dispatching the
// Action, so the post-Parse field value is reliable even if the Action returns an error
// on the empty tempdir.
func TestBackfillCommand_VerifyConcurrencyParsesInt(t *testing.T) {
	app, cmd := buildBackfillApp(t)
	validDir := t.TempDir()
	_, _ = app.Parse([]string{
		"backfill",
		"--address=http://localhost",
		"--id=tenant-1",
		"--verify-concurrency=2",
		validDir,
	})
	assert.Equal(t, 2, cmd.verifyConcurrency, "--verify-concurrency must populate the struct field")
}
