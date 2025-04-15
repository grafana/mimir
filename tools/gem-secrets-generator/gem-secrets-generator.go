package main

import (
	"encoding/base64"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strings"
)

// Config struct holds the secret data
type Config struct {
	AdminUser            string `json:"adminUser"`
	AdminPassword        string `json:"adminPassword"`
	AWSAccessKey         string `json:"AWS_ACCESS_KEY"`
	AWSSecretAccessKey   string `json:"AWS_SECRET_ACCESS_KEY"`
	LicenseFilePath      string `json:"licenseFile"`
}

func printHelp() {
	fmt.Println(`Usage: go run metrics-secrets-generator.go [--config=config.json] [flags]

Available flags to override config.json:
  --adminUser string
  --adminPassword string
  --AWS_ACCESS_KEY string
  --AWS_SECRET_ACCESS_KEY string
  --licenseFile string

Example config.json:
{
  "adminUser": "admin",
  "adminPassword": "s3cr3t",
  "AWS_ACCESS_KEY": "AKIA...",
  "AWS_SECRET_ACCESS_KEY": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
  "licenseFile": "license.jwt"
}`)
	os.Exit(0)
}

func loadConfig(path string) (*Config, error) {
	var config Config
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	err = json.Unmarshal(data, &config)
	return &config, err
}

func base64Encode(value string) string {
	return base64.StdEncoding.EncodeToString([]byte(value))
}

func createSecret(name string, data map[string]string) string {
	var b strings.Builder
	b.WriteString("apiVersion: v1\nkind: Secret\n")
	b.WriteString(fmt.Sprintf("metadata:\n  name: %s\n", name))
	b.WriteString("type: Opaque\n")
	b.WriteString("data:\n")
	for k, v := range data {
		b.WriteString(fmt.Sprintf("  %s: %s\n", k, base64Encode(v)))
	}
	return b.String()
}

func writeFile(filename, content string) error {
	err := os.WriteFile(filename, []byte(content), 0644)
	if err != nil {
		return fmt.Errorf("failed to write file %s: %w", filename, err)
	}
	fmt.Printf("Secret written to: %s\n", filename)
	return nil
}

func main() {
	// Flags
	configPath := flag.String("config", "", "Path to config file")
	adminUser := flag.String("adminUser", "", "Admin username")
	adminPassword := flag.String("adminPassword", "", "Admin password")
	awsKey := flag.String("AWS_ACCESS_KEY", "", "AWS Access Key")
	awsSecret := flag.String("AWS_SECRET_ACCESS_KEY", "", "AWS Secret Access Key")
	licenseFile := flag.String("licenseFile", "", "Path to license.jwt file")

	flag.Parse()

	// Show help if no flags or --help is provided
	if len(os.Args) == 1 || contains(os.Args, "--help") {
		printHelp()
	}

	var cfg Config
	if *configPath != "" {
		loadedCfg, err := loadConfig(*configPath)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error reading config: %v\n", err)
			os.Exit(1)
		}
		cfg = *loadedCfg
	}

	// Override with flags if provided
	if *adminUser != "" {
		cfg.AdminUser = *adminUser
	}
	if *adminPassword != "" {
		cfg.AdminPassword = *adminPassword
	}
	if *awsKey != "" {
		cfg.AWSAccessKey = *awsKey
	}
	if *awsSecret != "" {
		cfg.AWSSecretAccessKey = *awsSecret
	}
	if *licenseFile != "" {
		cfg.LicenseFilePath = *licenseFile
	}

	licenseData := ""
	if cfg.LicenseFilePath != "" {
		content, err := os.ReadFile(cfg.LicenseFilePath)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to read license file: %v\n", err)
			os.Exit(1)
		}
		licenseData = string(content)
	}

	// Generate Secrets
	adminSecret := createSecret("admin-secret", map[string]string{
		"adminUser":     cfg.AdminUser,
		"adminPassword": cfg.AdminPassword,
	})

	bucketSecret := createSecret("metrics-bucket-secret", map[string]string{
		"AWS_ACCESS_KEY":       cfg.AWSAccessKey,
		"AWS_SECRET_ACCESS_KEY": cfg.AWSSecretAccessKey,
	})

	licenseSecret := createSecret("metrics-license-secret", map[string]string{
		"license.jwt": licenseData,
	})

	// Write secrets to files
	if err := writeFile("admin-secret.yaml", adminSecret); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	if err := writeFile("metrics-bucket-secret.yaml", bucketSecret); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	if err := writeFile("metrics-license-secret.yaml", licenseSecret); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

// Helper function to check if a flag is passed
func contains(slice []string, val string) bool {
	for _, s := range slice {
		if s == val {
			return true
		}
	}
	return false
}
