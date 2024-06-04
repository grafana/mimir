package standalone

import (
	"bytes"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/grafana/grafana-plugin-sdk-go/internal"
)

var (
	standaloneEnabled = flag.Bool("standalone", false, "should this run standalone")
)

func NewServerSettings(address, dir string) ServerSettings {
	return ServerSettings{
		Address: address,
		Dir:     dir,
	}
}

type ServerSettings struct {
	Address string
	Dir     string
}

type ClientSettings struct {
	TargetAddress string
	TargetPID     int
}

// ServerModeEnabled returns true if the plugin should run in standalone server mode.
func ServerModeEnabled(pluginID string) (ServerSettings, bool) {
	flag.Parse() // Parse the flags so that we can check values for -standalone

	if *standaloneEnabled {
		s, err := serverSettings(pluginID)
		if err != nil {
			log.Printf("Error: %s", err.Error())
			return ServerSettings{}, false
		}
		return s, true
	}
	return ServerSettings{}, false
}

// ClientModeEnabled returns standalone server connection settings if a standalone server is running and can be reached.
func ClientModeEnabled(pluginID string) (ClientSettings, bool) {
	s, err := clientSettings(pluginID)
	if err != nil {
		log.Printf("Error: %s", err.Error())
		return ClientSettings{}, false
	}

	return s, true
}

// RunDummyPluginLocator runs the standalone server locator that Grafana uses to connect to the standalone GRPC server.
func RunDummyPluginLocator(address string) {
	fmt.Printf("1|2|tcp|%s|grpc\n", address)
	t := time.NewTicker(time.Second * 10)

	for ts := range t.C {
		fmt.Printf("[%s] using address: %s\n", ts.Format("2006-01-02 15:04:05"), address)
	}
}

func serverSettings(pluginID string) (ServerSettings, error) {
	curProcPath, err := currentProcPath()
	if err != nil {
		return ServerSettings{}, err
	}

	dir := filepath.Dir(curProcPath) // Default to current directory
	if pluginDir, found := findPluginRootDir(curProcPath); found {
		dir = pluginDir
	}

	address, err := createStandaloneAddress(pluginID)
	if err != nil {
		return ServerSettings{}, err
	}
	return ServerSettings{
		Address: address,
		Dir:     dir,
	}, nil
}

// clientSettings will attempt to find a standalone server's address and PID.
func clientSettings(pluginID string) (ClientSettings, error) {
	procPath, err := currentProcPath()
	if err != nil {
		return ClientSettings{}, err
	}

	dir := filepath.Dir(procPath)

	// Determine running standalone address + PID
	standaloneAddress, err := getStandaloneAddress(pluginID, dir)
	if err != nil {
		return ClientSettings{}, err
	}

	if standaloneAddress == "" {
		return ClientSettings{}, errors.New("address is empty")
	}

	standalonePID, err := getStandalonePID(dir)
	if err != nil {
		return ClientSettings{}, err
	}

	return ClientSettings{
		TargetAddress: standaloneAddress,
		TargetPID:     standalonePID,
	}, nil
}

func currentProcPath() (string, error) {
	curProcPath, err := os.Executable()
	if err != nil {
		return "", err
	}
	return curProcPath, nil
}

// findPluginRootDir will attempt to find a plugin directory based on the executing process's file-system path.
func findPluginRootDir(curProcPath string) (string, bool) {
	cwd, _ := os.Getwd()
	if filepath.Base(cwd) == "pkg" {
		cwd = filepath.Join(cwd, "..")
	}

	check := []string{
		filepath.Join(curProcPath, "plugin.json"),
		filepath.Join(curProcPath, "dist", "plugin.json"),
		filepath.Join(filepath.Dir(curProcPath), "plugin.json"),
		filepath.Join(filepath.Dir(curProcPath), "dist", "plugin.json"),
		filepath.Join(cwd, "dist", "plugin.json"),
		filepath.Join(cwd, "plugin.json"),
	}

	for _, path := range check {
		if _, err := os.Stat(path); err == nil {
			return filepath.Dir(path), true
		}
	}

	return "", false
}

func getStandaloneAddress(pluginID string, dir string) (string, error) {
	if addressEnvVar, exists := standaloneAddressFromEnv(pluginID); exists {
		return addressEnvVar, nil
	}

	// Check the local file for address
	fb, err := os.ReadFile(standaloneAddressFilePath(dir))
	addressFileContent := string(bytes.TrimSpace(fb))
	switch {
	case err != nil && !os.IsNotExist(err):
		return "", fmt.Errorf("read standalone file: %w", err)
	case os.IsNotExist(err) || len(addressFileContent) == 0:
		// No standalone file, do not treat as standalone
		return "", nil
	}
	return addressFileContent, nil
}

func createStandaloneAddress(pluginID string) (string, error) {
	if addressEnvVar, exists := standaloneAddressFromEnv(pluginID); exists {
		return addressEnvVar, nil
	}

	port, err := getFreePort()
	if err != nil {
		return "", fmt.Errorf("get free port: %w", err)
	}
	return fmt.Sprintf(":%d", port), nil
}

func standaloneAddressFromEnv(pluginID string) (string, bool) {
	addressEnvVar := "GF_PLUGIN_GRPC_ADDRESS_" + strings.ReplaceAll(strings.ToUpper(pluginID), "-", "_")
	if v, ok := os.LookupEnv(addressEnvVar); ok {
		return v, true
	}
	return "", false
}

func getStandalonePID(dir string) (int, error) {
	// Read PID (optional, as it was introduced later on)
	fb, err := os.ReadFile(standalonePIDFilePath(dir))
	pidFileContent := string(bytes.TrimSpace(fb))
	switch {
	case err != nil && !os.IsNotExist(err):
		return 0, fmt.Errorf("read pid file: %w", err)
	case os.IsNotExist(err) || len(pidFileContent) == 0:
		// No PID, this is optional as it was introduced later, so it's fine.
		// We lose hot switching between debug and non-debug without the pid file,
		// but there's nothing better we can do.
		return 0, nil
	default:
		pid, err := strconv.Atoi(pidFileContent)
		if err != nil {
			return 0, fmt.Errorf("could not parse pid: %w", err)
		}

		if !checkPIDIsRunning(pid) {
			return 0, errors.New("standalone server is not running")
		}

		return pid, nil
	}
}

// getFreePort returns a free port number on localhost.
func getFreePort() (int, error) {
	addr, err := net.ResolveTCPAddr("tcp", "localhost:0")
	if err != nil {
		return 0, err
	}

	l, err := net.ListenTCP("tcp", addr)
	if err != nil {
		return 0, err
	}
	defer func() { _ = l.Close() }()
	return l.Addr().(*net.TCPAddr).Port, nil
}

// FindAndKillCurrentPlugin kills the currently registered plugin, causing Grafana to restart it
// and connect to our new host.
func FindAndKillCurrentPlugin(dir string) {
	defer func() {
		if r := recover(); r != nil {
			fmt.Println("Error finding processes", r)
		}
	}()

	executablePath, err := internal.GetExecutableFromPluginJSON(dir)
	if err != nil {
		fmt.Printf("missing executable in plugin.json (standalone)")
		return
	}

	out, err := exec.Command("pgrep", "-f", executablePath).Output()
	if err != nil {
		fmt.Printf("error running pgrep: %s (%s)", err.Error(), executablePath)
		return
	}
	currentPID := os.Getpid()
	for _, txt := range strings.Fields(string(out)) {
		pid, err := strconv.Atoi(txt)
		if err == nil {
			// Do not kill the plugin process
			if pid == currentPID {
				continue
			}
			log.Printf("Killing process: %d", pid)
			// err := syscall.Kill(pid, 9)
			pidstr := fmt.Sprintf("%d", pid)
			err = exec.Command("kill", "-9", pidstr).Run()
			if err != nil {
				log.Printf("Error: %s", err.Error())
			}
		}
	}
}

// CreateStandaloneAddressFile creates the standalone.txt file containing the address of the GRPC server
func CreateStandaloneAddressFile(address, dir string) error {
	return os.WriteFile(
		standaloneAddressFilePath(dir),
		[]byte(address),
		0600,
	)
}

// CreateStandalonePIDFile creates the pid.txt file containing the PID of the GRPC server process
func CreateStandalonePIDFile(pid int, dir string) error {
	return os.WriteFile(
		standalonePIDFilePath(dir),
		[]byte(strconv.Itoa(pid)),
		0600,
	)
}

// CleanupStandaloneAddressFile attempts to delete standalone.txt from the specified folder.
// If the file does not exist, the function returns nil.
func CleanupStandaloneAddressFile(info ServerSettings) error {
	return deleteFile(standaloneAddressFilePath(info.Dir))
}

// CleanupStandalonePIDFile attempts to delete pid.txt from the specified folder.
// If the file does not exist, the function returns nil.
func CleanupStandalonePIDFile(info ServerSettings) error {
	return deleteFile(standalonePIDFilePath(info.Dir))
}

// deleteFile attempts to delete the specified file.
func deleteFile(fileName string) error {
	err := os.Remove(fileName)
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

// standaloneAddressFilePath returns the path to the standalone.txt file, which contains the standalone GRPC address
func standaloneAddressFilePath(dir string) string {
	return filepath.Join(dir, "standalone.txt")
}

// standalonePIDFilePath returns the path to the pid.txt file, which contains the standalone GRPC's server PID
func standalonePIDFilePath(dir string) string {
	return filepath.Join(dir, "pid.txt")
}

// checkPIDIsRunning returns true if there's a process with the specified PID
func checkPIDIsRunning(pid int) bool {
	if pid == 0 {
		return false
	}
	process, err := os.FindProcess(pid)
	if err != nil {
		return false
	}
	// FindProcess does not return an error if the process does not exist in UNIX.
	//
	// From man kill:
	//	 If  sig  is 0, then no signal is sent, but error checking is still per‐
	//   formed; this can be used to check for the existence of a process ID  or
	//   process group ID.
	//
	// So we send try to send a 0 signal to the process instead to test if it exists.
	if err = process.Signal(syscall.Signal(0)); err != nil {
		return false
	}
	return true
}
