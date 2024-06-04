package build

import (
	"bufio"
	"embed"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/magefile/mage/mg"
	"github.com/magefile/mage/sh"
	bra "github.com/unknwon/bra/cmd"
	"github.com/urfave/cli"

	"github.com/grafana/grafana-plugin-sdk-go/build/utils"
	"github.com/grafana/grafana-plugin-sdk-go/experimental/e2e"
	ca "github.com/grafana/grafana-plugin-sdk-go/experimental/e2e/certificate_authority"
	"github.com/grafana/grafana-plugin-sdk-go/experimental/e2e/config"
	"github.com/grafana/grafana-plugin-sdk-go/experimental/e2e/fixture"
	"github.com/grafana/grafana-plugin-sdk-go/experimental/e2e/storage"
	"github.com/grafana/grafana-plugin-sdk-go/internal"
)

var defaultOutputBinaryPath = "dist"
var defaultPluginJSONPath = "src"

// Callbacks give you a way to run custom behavior when things happen
var beforeBuild = func(cfg Config) (Config, error) {
	return cfg, nil
}

// SetBeforeBuildCallback configures a custom callback
func SetBeforeBuildCallback(cb BeforeBuildCallback) error {
	beforeBuild = cb
	return nil
}

var exname string

func getExecutableName(os string, arch string, pluginJSONPath string) (string, error) {
	if exname == "" {
		exename, err := internal.GetExecutableFromPluginJSON(pluginJSONPath)
		if err != nil {
			return "", err
		}

		exname = exename
	}

	exeName := fmt.Sprintf("%s_%s_%s", exname, os, arch)
	if os == "windows" {
		exeName = fmt.Sprintf("%s.exe", exeName)
	}
	return exeName, nil
}

func buildBackend(cfg Config) error {
	cfg, err := beforeBuild(cfg)
	if err != nil {
		return err
	}

	pluginJSONPath := defaultPluginJSONPath
	if cfg.PluginJSONPath != "" {
		pluginJSONPath = cfg.PluginJSONPath
	}
	exeName, err := getExecutableName(cfg.OS, cfg.Arch, pluginJSONPath)
	if err != nil {
		return err
	}

	ldFlags := ""
	if !cfg.EnableCGo {
		// Link statically
		ldFlags = `-extldflags "-static"`
	}

	if !cfg.EnableDebug {
		// Add linker flags to drop debug information
		prefix := ""
		if ldFlags != "" {
			prefix = " "
		}
		ldFlags = fmt.Sprintf("-w -s%s%s", prefix, ldFlags)
	}

	outputPath := cfg.OutputBinaryPath
	if outputPath == "" {
		outputPath = defaultOutputBinaryPath
	}
	args := []string{
		"build", "-o", filepath.Join(outputPath, exeName),
	}

	info := getBuildInfoFromEnvironment()
	pluginID, err := internal.GetStringValueFromJSON(filepath.Join(pluginJSONPath, "plugin.json"), "id")
	if err == nil && len(pluginID) > 0 {
		info.PluginID = pluginID
	}
	version, err := internal.GetStringValueFromJSON("package.json", "version")
	if err == nil && len(version) > 0 {
		info.Version = version
	}

	flags := make(map[string]string, 10)
	info.appendFlags(flags)

	if cfg.CustomVars != nil {
		for k, v := range cfg.CustomVars {
			flags[k] = v
		}
	}

	for k, v := range flags {
		ldFlags = fmt.Sprintf("%s -X '%s=%s'", ldFlags, k, v)
	}
	args = append(args, "-ldflags", ldFlags)

	if cfg.EnableDebug {
		args = append(args, "-gcflags=all=-N -l")
	}
	rootPackage := "./pkg"
	if cfg.RootPackagePath != "" {
		rootPackage = cfg.RootPackagePath
	}
	args = append(args, rootPackage)

	cfg.Env["GOARCH"] = cfg.Arch
	cfg.Env["GOOS"] = cfg.OS
	if !cfg.EnableCGo {
		cfg.Env["CGO_ENABLED"] = "0"
	}

	// TODO: Change to sh.RunWithV once available.
	return sh.RunWith(cfg.Env, "go", args...)
}

func newBuildConfig(os string, arch string) Config {
	return Config{
		OS:          os,
		Arch:        arch,
		EnableDebug: false,
		Env:         map[string]string{},
	}
}

// Build is a namespace.
type Build mg.Namespace

// Linux builds the back-end plugin for Linux.
func (Build) Linux() error {
	return buildBackend(newBuildConfig("linux", "amd64"))
}

// LinuxARM builds the back-end plugin for Linux on ARM.
func (Build) LinuxARM() error {
	return buildBackend(newBuildConfig("linux", "arm"))
}

// LinuxARM64 builds the back-end plugin for Linux on ARM64.
func (Build) LinuxARM64() error {
	return buildBackend(newBuildConfig("linux", "arm64"))
}

// Windows builds the back-end plugin for Windows.
func (Build) Windows() error {
	return buildBackend(newBuildConfig("windows", "amd64"))
}

// Darwin builds the back-end plugin for OSX.
func (Build) Darwin() error {
	return buildBackend(newBuildConfig("darwin", "amd64"))
}

// DarwinARM64 builds the back-end plugin for OSX on ARM (M1).
func (Build) DarwinARM64() error {
	return buildBackend(newBuildConfig("darwin", "arm64"))
}

// GenerateManifestFile generates a manifest file for plugin submissions
func (Build) GenerateManifestFile() error {
	config := Config{}
	config, err := beforeBuild(config)
	if err != nil {
		return err
	}
	outputPath := config.OutputBinaryPath
	if outputPath == "" {
		outputPath = defaultOutputBinaryPath
	}
	manifestContent, err := utils.GenerateManifest()
	if err != nil {
		return err
	}

	manifestFilePath := filepath.Join(outputPath, "go_plugin_build_manifest")
	err = os.MkdirAll(outputPath, 0755)
	if err != nil {
		return err
	}
	// #nosec G306 - we need reading permissions for this file
	err = os.WriteFile(manifestFilePath, []byte(manifestContent), 0755)
	if err != nil {
		return err
	}
	return nil
}

// Debug builds the debug version for the current platform
func (Build) Debug() error {
	cfg := newBuildConfig(runtime.GOOS, runtime.GOARCH)
	cfg.EnableDebug = true
	return buildBackend(cfg)
}

// Backend build a production build for the current platform
func (Build) Backend() error {
	// The M1 platform detection is kinda flakey, so we will just build both
	if runtime.GOOS == "darwin" {
		err := buildBackend(newBuildConfig("darwin", "arm64"))
		if err != nil {
			return err
		}
		return buildBackend(newBuildConfig("darwin", "amd64"))
	}
	cfg := newBuildConfig(runtime.GOOS, runtime.GOARCH)
	return buildBackend(cfg)
}

// BuildAll builds production executables for all supported platforms.
func BuildAll() { //revive:disable-line
	b := Build{}
	mg.Deps(b.Linux, b.Windows, b.Darwin, b.DarwinARM64, b.LinuxARM64, b.LinuxARM, b.GenerateManifestFile)
}

//go:embed tmpl/*
var tmpl embed.FS

// ensureWatchConfig creates a default .bra.toml file in the current directory if it doesn't exist.
func ensureWatchConfig() error {
	exists, err := utils.Exists(".bra.toml")
	if err != nil {
		return err
	}

	if exists {
		return nil
	}

	fmt.Println("No .bra.toml file found. Creating one...")
	config, err := tmpl.ReadFile("tmpl/bra.toml")
	if err != nil {
		return err
	}

	return os.WriteFile(".bra.toml", config, 0600)
}

// Watch rebuilds the plugin backend when files change.
func Watch() error {
	if err := ensureWatchConfig(); err != nil {
		return err
	}

	// this is needed to run `bra run` programmatically
	app := cli.NewApp()
	app.Name = "bra"
	app.Usage = ""
	app.Action = func(c *cli.Context) error {
		return bra.Run.Run(c)
	}
	return app.Run(os.Args)
}

// Test runs backend tests.
func Test() error {
	return sh.RunV("go", "test", "./pkg/...")
}

// Coverage runs backend tests and makes a coverage report.
func Coverage() error {
	// Create a coverage folder if it does not already exist
	if err := os.MkdirAll(filepath.Join(".", "coverage"), os.ModePerm); err != nil {
		return err
	}

	if err := sh.RunV("go", "test", "./pkg/...", "-coverpkg", "./...", "-v", "-cover", "-coverprofile=coverage/backend.out"); err != nil {
		return err
	}

	if err := sh.RunV("go", "tool", "cover", "-func=coverage/backend.out", "-o", "coverage/backend.txt"); err != nil {
		return err
	}

	return sh.RunV("go", "tool", "cover", "-html=coverage/backend.out", "-o", "coverage/backend.html")
}

// Lint audits the source style
func Lint() error {
	return sh.RunV("golangci-lint", "run", "./...")
}

// Format formats the sources.
func Format() error {
	return sh.RunV("gofmt", "-w", ".")
}

// Clean cleans build artifacts, by deleting the dist directory.
func Clean() error {
	err := os.RemoveAll("dist")
	if err != nil {
		return err
	}

	err = os.RemoveAll("coverage")
	if err != nil {
		return err
	}

	err = os.RemoveAll("ci")
	if err != nil {
		return err
	}
	return nil
}

// E2E is a namespace.
type E2E mg.Namespace

// Append starts the E2E proxy in append mode.
func (E2E) Append() error {
	return e2eProxy(e2e.ProxyModeAppend)
}

// Overwrite starts the E2E proxy in overwrite mode.
func (E2E) Overwrite() error {
	return e2eProxy(e2e.ProxyModeOverwrite)
}

// Replay starts the E2E proxy in replay mode.
func (E2E) Replay() error {
	return e2eProxy(e2e.ProxyModeReplay)
}

// Certificate prints the CA certificate to stdout.
func (E2E) Certificate() error {
	cfg, err := config.LoadConfig("proxy.json")
	if err != nil {
		return err
	}

	if cert, _, err := ca.LoadKeyPair(cfg.CAConfig.Cert, cfg.CAConfig.PrivateKey); err == nil {
		fmt.Print(string(cert))
		return nil
	}

	fmt.Print(string(ca.CACertificate))
	return nil
}

func e2eProxy(mode e2e.ProxyMode) error {
	cfg, err := config.LoadConfig("proxy.json")
	if err != nil {
		return err
	}
	fixtures := make([]*fixture.Fixture, 0)
	for _, s := range cfg.Storage {
		switch s.Type {
		case config.StorageTypeHAR:
			store := storage.NewHARStorage(s.Path)
			fixtures = append(fixtures, fixture.NewFixture(store))
		case config.StorageTypeOpenAPI:
			store := storage.NewOpenAPIStorage(s.Path)
			fixtures = append(fixtures, fixture.NewFixture(store))
		}
	}
	proxy := e2e.NewProxy(mode, fixtures, cfg)
	return proxy.Start()
}

// checkLinuxPtraceScope verifies that ptrace is configured as required.
func checkLinuxPtraceScope() error {
	ptracePath := "/proc/sys/kernel/yama/ptrace_scope"
	byteValue, err := os.ReadFile(ptracePath)
	if err != nil {
		return fmt.Errorf("unable to read ptrace_scope: %w", err)
	}
	val := strings.TrimSpace(string(byteValue))
	if val != "0" {
		log.Printf("WARNING:")
		fmt.Printf("ptrace_scope set to value other than 0 (currently: %s), this might prevent debugger from connecting\n", val)
		fmt.Printf("try writing \"0\" to %s\n", ptracePath)
		fmt.Printf("Set ptrace_scope to 0? y/N (default N)\n")

		scanner := bufio.NewScanner(os.Stdin)
		if scanner.Scan() {
			if scanner.Text() == "y" || scanner.Text() == "Y" {
				// if err := sh.RunV("echo", "0", "|", "sudo", "tee", ptracePath); err != nil {
				// 	return // Error?
				// }
				log.Printf("TODO, run: echo 0 | sudo tee /proc/sys/kernel/yama/ptrace_scope")
			} else {
				fmt.Printf("Did not write\n")
			}
		}
	}

	return nil
}
