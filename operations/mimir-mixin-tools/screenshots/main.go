// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io/fs"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/chromedp/cdproto/emulation"
	"github.com/chromedp/cdproto/input"
	"github.com/chromedp/chromedp"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
)

const (
	defaultViewportWidth  = 1400
	defaultViewportHeight = 2400
	inputDir              = "/input"
	outputDir             = "/output"
)

// customViewportHeight allows customizing the viewport height for specific dashboards
var customViewportHeight = map[string]int{
	"mimir-alertmanager-resources":        1900,
	"mimir-compactor-resources":           1000,
	"mimir-config":                        800,
	"mimir-object-store":                  1400,
	"mimir-overrides":                     800,
	"mimir-overview":                      1400,
	"mimir-overview-resources":            1700,
	"mimir-overview-networking":           1100,
	"mimir-reads-networking":              2000,
	"mimir-rollout-progress":              1000,
	"mimir-scaling":                       800,
	"mimir-slow-queries":                  600,
	"mimir-tenants":                       1200,
	"mimir-top-tenants":                   2000,
	"mimir-writes-networking":             1000,
	"mimir-writes-resources":              1600,
	"mimir-remote-ruler-reads":            1800,
	"mimir-remote-ruler-reads-resources":  1100,
	"mimir-remote-ruler-reads-networking": 1400,
}

// skippedDashboards contains dashboards we don't generate screenshots for
var skippedDashboards = map[string]bool{
	"mimir-slow-queries": true,
	"mimir-top-tenants":  true,
}

type Dashboard struct {
	Name string
	UID  string
}

type DashboardJSON struct {
	UID string `json:"uid"`
}

func main() {
	logger := log.NewLogfmtLogger(os.Stdout)
	logger = log.With(logger, "ts", log.DefaultTimestampUTC)

	level.Info(logger).Log("msg", "Starting Mimir mixin screenshots generator")

	// Ensure required environment variables are set
	requiredEnvVars := []string{"CLUSTER", "MIMIR_NAMESPACE", "ALERTMANAGER_NAMESPACE", "MIMIR_USER"}
	for _, envVar := range requiredEnvVars {
		if os.Getenv(envVar) == "" {
			level.Error(logger).Log("msg", "Missing required environment variable", "var", envVar)
			os.Exit(1)
		}
	}

	level.Info(logger).Log("msg", "Environment variables validated", "cluster", os.Getenv("CLUSTER"), "namespace", os.Getenv("MIMIR_NAMESPACE"))

	// List dashboards
	dashboards, err := listDashboards(logger)
	if err != nil {
		level.Error(logger).Log("msg", "Failed to list dashboards", "error", err)
		os.Exit(1)
	}

	level.Info(logger).Log("msg", "Found dashboards", "count", len(dashboards))

	// Create chrome options for containerized environment
	opts := []chromedp.ExecAllocatorOption{
		chromedp.NoSandbox,
		chromedp.Headless,
		chromedp.DisableGPU,
		chromedp.Flag("disable-dev-shm-usage", true),
		chromedp.Flag("disable-setuid-sandbox", true),
		chromedp.ExecPath("/usr/bin/chromium"),
	}

	level.Debug(logger).Log("msg", "Configured Chrome options for containerized environment")

	// Create allocator context
	allocCtx, cancel := chromedp.NewExecAllocator(context.Background(), opts...)
	defer cancel()

	// Take screenshots for all dashboards
	for i, dashboard := range dashboards {
		dashLogger := log.With(logger, "dashboard", dashboard.Name, "progress", fmt.Sprintf("%d/%d", i+1, len(dashboards)))
		level.Info(dashLogger).Log("msg", "Taking screenshot")

		// Create a new browser context for each screenshot (avoids shared state issues)
		ctx, cancel := chromedp.NewContext(allocCtx)

		screenshotCtx, cancelTimeout := context.WithTimeout(ctx, 30*time.Second)

		if err := takeScreenshot(screenshotCtx, dashboard, dashLogger); err != nil {
			level.Error(dashLogger).Log("msg", "Failed to take screenshot", "error", err)
			cancelTimeout()
			cancel()
			os.Exit(1)
		}

		level.Info(dashLogger).Log("msg", "Successfully took screenshot")
		cancelTimeout()
		cancel()
	}

	level.Info(logger).Log("msg", "Screenshot generation completed", "total_dashboards", len(dashboards))
}

func listDashboards(logger log.Logger) ([]Dashboard, error) {
	var dashboards []Dashboard

	level.Debug(logger).Log("msg", "Scanning for dashboard files", "input_dir", inputDir)

	err := filepath.WalkDir(inputDir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		if !d.IsDir() && strings.HasSuffix(d.Name(), ".json") {
			// Parse the dashboard
			raw, err := os.ReadFile(path)
			if err != nil {
				return fmt.Errorf("failed to read %s: %w", path, err)
			}

			var data DashboardJSON
			if err := json.Unmarshal(raw, &data); err != nil {
				return fmt.Errorf("failed to parse JSON from %s: %w", path, err)
			}

			name := strings.TrimSuffix(d.Name(), ".json")

			// Skip dashboards in skip list
			if skippedDashboards[name] {
				level.Debug(logger).Log("msg", "Skipping dashboard", "dashboard", name, "reason", "in skip list")
				return nil
			}

			level.Debug(logger).Log("msg", "Found dashboard", "dashboard", name, "uid", data.UID)
			dashboards = append(dashboards, Dashboard{
				Name: name,
				UID:  data.UID,
			})
		}

		return nil
	})

	return dashboards, err
}

func takeScreenshot(ctx context.Context, dashboard Dashboard, logger log.Logger) error {
	// Get viewport height
	height := defaultViewportHeight
	if customHeight, exists := customViewportHeight[dashboard.Name]; exists {
		height = customHeight
		level.Debug(logger).Log("msg", "Using custom viewport height", "height", height)
	}

	// Build dashboard URL
	params := url.Values{}
	params.Set("var-datasource", "Mimir")
	params.Set("var-cluster", os.Getenv("CLUSTER"))

	// Choose namespace based on dashboard name
	namespace := os.Getenv("MIMIR_NAMESPACE")
	if strings.Contains(dashboard.Name, "alertmanager") {
		namespace = os.Getenv("ALERTMANAGER_NAMESPACE")
	}
	params.Set("var-namespace", namespace)
	params.Set("var-user", os.Getenv("MIMIR_USER"))

	dashboardURL := fmt.Sprintf("http://mixin-serve-grafana:3000/d/%s?%s", dashboard.UID, params.Encode())
	level.Debug(logger).Log("msg", "Dashboard URL constructed", "url", dashboardURL)

	// Create output directory
	directoryName := strings.TrimPrefix(dashboard.Name, "mimir-")
	screenshotDir := filepath.Join(outputDir, directoryName)
	if err := os.MkdirAll(screenshotDir, 0755); err != nil {
		return fmt.Errorf("failed to create output directory: %w", err)
	}

	screenshotPath := filepath.Join(screenshotDir, dashboard.Name+".png")
	level.Debug(logger).Log("msg", "Output path determined", "path", screenshotPath)

	var buf []byte

	// Run actions separately to make debugging easier
	actions := []struct {
		name   string
		action chromedp.Action
	}{
		{"SetDeviceMetricsOverride", emulation.SetDeviceMetricsOverride(int64(defaultViewportWidth), int64(height), 1, false)},
		{"Navigate", chromedp.Navigate(dashboardURL)},
		{"WaitVisible", chromedp.WaitVisible("body", chromedp.ByQuery)},
		{"Sleep", chromedp.Sleep(2 * time.Second)},
		{"KeyEvent: d", chromedp.KeyEvent("d")},
		{"KeyEvent: E", chromedp.KeyEvent("E", chromedp.KeyModifiers(input.ModifierShift))},
		{"Sleep", chromedp.Sleep(3 * time.Second)},
		{"CaptureScreenshot", chromedp.CaptureScreenshot(&buf)},
	}

	for _, act := range actions {
		level.Debug(logger).Log("msg", "Executing browser action", "action", act.name)
		if err := chromedp.Run(ctx, act.action); err != nil {
			return fmt.Errorf("failed to run action %s: %w", act.name, err)
		}
	}

	level.Debug(logger).Log("msg", "Screenshot captured", "size_bytes", len(buf))

	// Save screenshot
	if err := os.WriteFile(screenshotPath, buf, 0644); err != nil {
		return fmt.Errorf("failed to save screenshot: %w", err)
	}

	level.Debug(logger).Log("msg", "Screenshot saved to disk")

	// Optimize PNG using pngquant
	level.Debug(logger).Log("msg", "Optimizing PNG with pngquant")
	cmd := exec.Command("pngquant", "--force", "--ext", ".png", "--skip-if-larger", "--speed", "1", "--strip", "--quality", "100", "--verbose", screenshotPath)
	if err := cmd.Run(); err != nil {
		// Check if it's exit status 99 (file not optimized because output would be larger)
		if exitErr, ok := err.(*exec.ExitError); ok && exitErr.ExitCode() == 99 {
			level.Debug(logger).Log("msg", "PNG optimization skipped", "reason", "output would be larger than input")
			return nil
		}
		return fmt.Errorf("pngquant failed: %w", err)
	}

	level.Debug(logger).Log("msg", "PNG optimization completed")
	return nil
}
