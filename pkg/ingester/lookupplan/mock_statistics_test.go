package lookupplan

import (
	"context"
	"fmt"
)

// mockStatistics implements the Statistics interface with hardcoded data for testing
type mockStatistics struct {
	// seriesPerValue maps label name -> label value -> number of series
	seriesPerValue map[string]map[string]uint64
	totalSeries    uint64
}

func newMockStatistics() *mockStatistics {
	return &mockStatistics{
		seriesPerValue: map[string]map[string]uint64{
			"__name__": {
				"http_requests_total": 1000,
				"cpu_usage_percent":   500,
				"memory_usage_bytes":  300,
				"disk_io_operations":  200,
				"network_bytes_sent":  150,
			},
			"job": {
				"prometheus":   800,
				"grafana":      600,
				"alertmanager": 400,
				"node":         300,
			},
			"instance": {
				"localhost:9090": 200,
				"localhost:3000": 150,
				"localhost:9093": 100,
				"localhost:9100": 80,
				"prod-server-1":  300,
				"prod-server-2":  250,
				"prod-server-3":  200,
			},
			"status": {
				"200": 800,
				"404": 300,
				"500": 100,
			},
			"method": {
				"GET":    600,
				"POST":   300,
				"PUT":    150,
				"DELETE": 50,
			},
		},
		totalSeries: 2100,
	}
}

func (m *mockStatistics) TotalSeries() uint64 {
	return m.totalSeries
}

func (m *mockStatistics) LabelValuesCount(_ context.Context, name string) (uint64, error) {
	values := m.seriesPerValue[name]

	count := uint64(0)
	for _, seriesCount := range values {
		if seriesCount > 0 {
			count++
		}
	}
	return count, nil
}

func (m *mockStatistics) LabelValuesCardinality(_ context.Context, name string, values ...string) (uint64, error) {
	labelValues := m.seriesPerValue[name]

	if len(values) == 0 {
		// Return total cardinality for all values of this label
		total := uint64(0)
		for _, seriesCount := range labelValues {
			total += seriesCount
		}
		return total, nil
	}

	// Return cardinality for specific values
	total := uint64(0)
	for _, value := range values {
		if seriesCount, exists := labelValues[value]; exists {
			total += seriesCount
		}
	}
	return total, nil
}

// newHighCardinalityMockStatistics creates a mock statistics with higher cardinality
// to test the planner's behavior with realistic scale data
func newHighCardinalityMockStatistics() *mockStatistics {
	stats := &mockStatistics{
		seriesPerValue: make(map[string]map[string]uint64),
		totalSeries:    5000000, // 5 million series
	}

	// __name__ - similar to existing but with higher cardinality + some low-cardinality ones
	stats.seriesPerValue["__name__"] = map[string]uint64{
		"http_requests_total":      800000,
		"cpu_usage_percent":        650000,
		"memory_usage_bytes":       500000,
		"disk_io_operations":       400000,
		"network_bytes_sent":       350000,
		"network_bytes_received":   350000,
		"process_resident_memory":  300000,
		"process_virtual_memory":   300000,
		"go_memstats_alloc_bytes":  250000,
		"go_memstats_heap_objects": 250000,
		"prometheus_config_reload": 200000,
		"test_metric_small":        50,
		"debug_counter":            25,
		"health_check":             10,
		"startup_duration":         5,
	}

	// job - more job types with higher cardinality + some low-cardinality ones
	stats.seriesPerValue["job"] = map[string]uint64{
		"prometheus":      1200000,
		"grafana":         800000,
		"alertmanager":    600000,
		"node-exporter":   400000,
		"blackbox":        300000,
		"pushgateway":     250000,
		"cortex":          200000,
		"loki":            180000,
		"tempo":           150000,
		"mimir":           120000,
		"test-runner":     100,
		"backup-service":  75,
		"log-shipper":     50,
		"health-checker":  25,
		"config-reloader": 15,
	}

	// instance - auto-generated with high cardinality (this will be the highest cardinality label)
	instanceMap := make(map[string]uint64)

	// Generate instances with realistic patterns
	// Format: hostname-region-az-index
	regions := []string{"us-east-1", "us-west-2", "eu-west-1", "ap-southeast-1"}
	azs := []string{"a", "b", "c"}
	hostTypes := []string{"web", "api", "worker", "db", "cache", "monitor"}

	seriesPerInstance := uint64(50) // Each instance has ~50 series
	instanceCount := 0

	for _, region := range regions {
		for _, az := range azs {
			for _, hostType := range hostTypes {
				for i := 1; i <= 200; i++ { // 200 instances per host type per AZ
					instanceName := fmt.Sprintf("%s-%s-%s%s-%03d", hostType, region, az, az, i)
					instanceMap[instanceName] = seriesPerInstance
					instanceCount++
				}
			}
		}
	}
	stats.seriesPerValue["instance"] = instanceMap

	// status - more HTTP status codes with realistic distribution
	stats.seriesPerValue["status"] = map[string]uint64{
		"200": 2000000,
		"404": 800000,
		"500": 400000,
		"502": 300000,
		"503": 250000,
		"401": 200000,
		"403": 150000,
		"400": 120000,
		"429": 100000,
		"301": 80000,
		"302": 70000,
		"204": 60000,
	}

	// method - more HTTP methods
	stats.seriesPerValue["method"] = map[string]uint64{
		"GET":     2500000,
		"POST":    1200000,
		"PUT":     600000,
		"DELETE":  300000,
		"PATCH":   200000,
		"HEAD":    150000,
		"OPTIONS": 100000,
	}

	// Add some additional high-cardinality labels
	stats.seriesPerValue["pod"] = generatePodNames(1000, 800) // 1000 pods, 800 series each
	// Add low-cardinality pods (singleton services) to the pod map
	stats.seriesPerValue["pod"]["backup-controller-abcdefghij-12345"] = 25
	stats.seriesPerValue["pod"]["config-manager-klmnopqrst-67890"] = 20
	stats.seriesPerValue["pod"]["health-monitor-uvwxyzabcd-54321"] = 15
	stats.seriesPerValue["pod"]["log-aggregator-efghijklmn-98765"] = 10
	stats.seriesPerValue["pod"]["test-runner-opqrstuvwx-11111"] = 5

	stats.seriesPerValue["container"] = map[string]uint64{
		"prometheus":      600000,
		"grafana":         400000,
		"alertmanager":    300000,
		"node-exporter":   250000,
		"nginx":           200000,
		"redis":           180000,
		"postgres":        150000,
		"elasticsearch":   120000,
		"kibana":          100000,
		"fluentd":         80000,
		"istio-proxy":     200,
		"jaeger-agent":    150,
		"config-reloader": 100,
		"log-shipper":     75,
		"health-check":    50,
		"init-container":  25,
	}

	stats.seriesPerValue["namespace"] = map[string]uint64{
		"monitoring":   1500000,
		"default":      1200000,
		"kube-system":  800000,
		"logging":      600000,
		"ingress":      400000,
		"cert-manager": 300000,
		"prometheus":   250000,
		"grafana":      200000,
		"testing":      300,
		"staging":      250,
		"development":  200,
		"backup":       100,
		"security":     75,
		"admin":        50,
	}

	return stats
}

// generatePodNames creates realistic pod names with replicas
func generatePodNames(numDeployments int, seriesPerPod uint64) map[string]uint64 {
	podMap := make(map[string]uint64)

	deployments := []string{
		"prometheus", "grafana", "alertmanager", "node-exporter", "blackbox-exporter",
		"pushgateway", "nginx-ingress", "cert-manager", "external-dns", "cluster-autoscaler",
		"fluentd", "elasticsearch", "kibana", "redis", "postgres", "mysql", "mongodb",
		"rabbitmq", "kafka", "zookeeper", "consul", "vault", "etcd", "coredns",
		"api-gateway", "user-service", "auth-service", "payment-service", "notification-service",
	}

	for i := 0; i < numDeployments; i++ {
		deployment := deployments[i%len(deployments)]
		if i >= len(deployments) {
			deployment = fmt.Sprintf("%s-%d", deployment, i/len(deployments))
		}

		// Each deployment has 1-5 replicas
		replicas := (i % 5) + 1
		for j := 0; j < replicas; j++ {
			// Generate random-looking pod suffix (simulating Kubernetes pod names)
			podName := fmt.Sprintf("%s-%s-%s", deployment,
				generateRandomString(10), generateRandomString(5))
			podMap[podName] = seriesPerPod
		}
	}

	return podMap
}

// generateRandomString creates a pseudo-random string for pod names
func generateRandomString(length int) string {
	chars := "abcdefghijklmnopqrstuvwxyz0123456789"
	result := make([]byte, length)
	for i := range result {
		// Use deterministic "randomness" based on position for consistent test results
		result[i] = chars[(i*7+length*3)%len(chars)]
	}
	return string(result)
}

// errorMockStatistics implements Statistics interface but always returns errors
type errorMockStatistics struct{}

func newErrorMockStatistics() *errorMockStatistics {
	return &errorMockStatistics{}
}

func (e *errorMockStatistics) TotalSeries() uint64 {
	return 1000 // Return some non-zero value so planning can start
}

func (e *errorMockStatistics) LabelValuesCount(context.Context, string) (uint64, error) {
	return 0, fmt.Errorf("mock statistics error: failed to get label values count")
}

func (e *errorMockStatistics) LabelValuesCardinality(context.Context, string, ...string) (uint64, error) {
	return 0, fmt.Errorf("mock statistics error: failed to get label values cardinality")
}
