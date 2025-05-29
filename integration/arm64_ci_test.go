package integration

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v2"
)

// TestARM64CIWorkflowsExist ensures the ARM64 CI workflows are present and correctly configured
func TestARM64CIWorkflowsExist(t *testing.T) {
	// Test that the benchmark-node-cost.yml workflow exists
	benchmarkWorkflowPath := filepath.Join("..", ".github", "workflows", "benchmark-node-cost.yml")
	require.FileExists(t, benchmarkWorkflowPath, "benchmark-node-cost.yml workflow should exist")

	// Test that the test-build-deploy.yml workflow has ARM64 support
	testBuildDeployPath := filepath.Join("..", ".github", "workflows", "test-build-deploy.yml")
	require.FileExists(t, testBuildDeployPath, "test-build-deploy.yml workflow should exist")

	// Read and validate benchmark workflow
	benchmarkContent, err := os.ReadFile(benchmarkWorkflowPath)
	require.NoError(t, err)

	var benchmarkWorkflow map[string]interface{}
	err = yaml.Unmarshal(benchmarkContent, &benchmarkWorkflow)
	require.NoError(t, err)

	// Validate benchmark workflow structure
	require.Contains(t, benchmarkWorkflow, "on")
	require.Contains(t, benchmarkWorkflow, "jobs")

	jobs := benchmarkWorkflow["jobs"].(map[interface{}]interface{})
	require.Contains(t, jobs, "benchmark_amd64")
	require.Contains(t, jobs, "benchmark_arm64")

	// Validate ARM64 job has QEMU setup
	arm64Job := jobs["benchmark_arm64"].(map[interface{}]interface{})
	steps := arm64Job["steps"].([]interface{})
	
	hasQEMUStep := false
	for _, step := range steps {
		stepMap := step.(map[interface{}]interface{})
		if name, ok := stepMap["name"]; ok && name == "Set up QEMU" {
			hasQEMUStep = true
			break
		}
	}
	assert.True(t, hasQEMUStep, "ARM64 benchmark job should have QEMU setup step")
}

// TestBenchmarkScriptsExist ensures the benchmark orchestration scripts exist
func TestBenchmarkScriptsExist(t *testing.T) {
	scripts := []string{
		"orchestrate_benchmark_run.sh",
		"deploy_prometheus_for_benchmark.sh",
		"run_benchmark_setup.sh",
		"query_benchmark_metrics.sh",
	}

	for _, script := range scripts {
		scriptPath := filepath.Join("..", script)
		require.FileExists(t, scriptPath, "%s should exist", script)

		// Check that script is executable
		info, err := os.Stat(scriptPath)
		require.NoError(t, err)
		
		mode := info.Mode()
		assert.True(t, mode&0111 != 0, "%s should be executable", script)
	}
}

// TestARM64DocumentationExists ensures ARM64 CI documentation is present
func TestARM64DocumentationExists(t *testing.T) {
	docs := []string{
		"docs/internal/ci/arm64-helm-upgrade-test.md",
		"docs/internal/benchmarking/node-cost-delta.md",
	}

	for _, doc := range docs {
		docPath := filepath.Join("..", doc)
		require.FileExists(t, docPath, "%s documentation should exist", doc)

		// Check that file is not empty
		content, err := os.ReadFile(docPath)
		require.NoError(t, err)
		assert.NotEmpty(t, content, "%s should not be empty", doc)
	}
}

// TestARM64HelmUpgradeTestWorkflow validates the test-build-deploy.yml has ARM64 support
func TestARM64HelmUpgradeTestWorkflow(t *testing.T) {
	workflowPath := filepath.Join("..", ".github", "workflows", "test-build-deploy.yml")
	content, err := os.ReadFile(workflowPath)
	require.NoError(t, err)

	var workflow map[string]interface{}
	err = yaml.Unmarshal(content, &workflow)
	require.NoError(t, err)

	jobs := workflow["jobs"].(map[interface{}]interface{})
	
	// Check for ARM64 Helm upgrade test job
	hasARM64HelmTest := false
	for jobName := range jobs {
		if jobName == "helm-upgrade-test-arm64" {
			hasARM64HelmTest = true
			break
		}
	}
	assert.True(t, hasARM64HelmTest, "test-build-deploy.yml should contain helm-upgrade-test-arm64 job")
}

// TestBenchmarkWorkflowInputs validates the benchmark workflow has required inputs
func TestBenchmarkWorkflowInputs(t *testing.T) {
	workflowPath := filepath.Join("..", ".github", "workflows", "benchmark-node-cost.yml")
	content, err := os.ReadFile(workflowPath)
	require.NoError(t, err)

	var workflow map[string]interface{}
	err = yaml.Unmarshal(content, &workflow)
	require.NoError(t, err)

	on := workflow["on"].(map[interface{}]interface{})
	workflowDispatch := on["workflow_dispatch"].(map[interface{}]interface{})
	inputs := workflowDispatch["inputs"].(map[interface{}]interface{})

	// Validate required inputs
	requiredInputs := []string{
		"mimir_image_tag",
		"mimir_continuous_test_image",
		"target_active_series",
		"target_sps",
		"workload_duration",
	}

	for _, input := range requiredInputs {
		assert.Contains(t, inputs, input, "Benchmark workflow should have %s input", input)
	}

	// Validate mimir_image_tag is required
	mimirImageTag := inputs["mimir_image_tag"].(map[interface{}]interface{})
	assert.Equal(t, true, mimirImageTag["required"], "mimir_image_tag should be required")

	// Validate mimir_continuous_test_image is required
	mimirContinuousTestImage := inputs["mimir_continuous_test_image"].(map[interface{}]interface{})
	assert.Equal(t, true, mimirContinuousTestImage["required"], "mimir_continuous_test_image should be required")
} 