# Node Cost Delta Benchmark: AMD64 vs ARM64

## 1. Purpose of the Benchmark

This benchmark is designed to compare the resource consumption of Grafana Mimir when deployed on x86 (amd64) versus ARM64 (aarch64) architectures. By subjecting Mimir instances on both architectures to a similar, sustained workload, we aim to measure and compare key performance indicators and resource utilization.

The "node-cost delta" refers to the difference in resources (primarily CPU and memory) required by Mimir to handle the same workload on amd64 compared to ARM64. Understanding this delta can help in:

*   Identifying performance characteristics specific to each architecture.
*   Evaluating potential infrastructure cost savings if one architecture proves to be more efficient for Mimir's workload.
*   Making informed decisions about hardware provisioning for Mimir deployments.

## 2. How to Run the Benchmark

The benchmark is orchestrated by a GitHub Actions workflow.

*   **Workflow File:** `.github/workflows/benchmark-node-cost.yml`
*   **Triggering:** The workflow is manually triggered using `workflow_dispatch`.

To run the benchmark:

1.  Navigate to the "Actions" tab in the Grafana Mimir GitHub repository.
2.  Under "Workflows", find "Benchmark Node Cost".
3.  Click the "Run workflow" button.
4.  You will be prompted to provide the following inputs:
    *   `mimir_image_tag` (string, required): The Mimir image tag to be benchmarked (e.g., `main-abcdef` or `v2.8.0`).
    *   `mimir_continuous_test_image` (string, required): The full image name for the `mimir-continuous-test` workload generator (e.g., `grafana/mimir-continuous-test:main-abcdef`).
    *   `target_active_series` (string, default: "100000"): The target number of active series for the workload generator.
    *   `target_sps` (string, default: "50000"): The target samples per second (ingestion rate) for the workload generator.
    *   `workload_duration` (string, default: "30m"): The duration for which the workload generator will run (e.g., `30m` for quick tests, `2h` or longer for more stable results).
5.  Click "Run workflow" to start the benchmark.

## 3. Benchmark Process Overview

The `benchmark-node-cost.yml` workflow executes two main jobs in parallel: `benchmark_amd64` and `benchmark_arm64`.

Each job performs the following steps for its respective architecture:

1.  **Environment Setup:**
    *   Checks out the repository code.
    *   Sets up necessary tools on an `ubuntu-latest` runner: Go, KinD, kubectl, Helm.
    *   For the `benchmark_arm64` job, QEMU is set up to enable running an ARM64 KinD node on an x86 runner.
2.  **Orchestration Script Execution:**
    *   Runs the `orchestrate_benchmark_run.sh` script. This script coordinates the entire benchmark process for that architecture.
    *   **KinD Cluster:** A Kubernetes in Docker (KinD) cluster is created, configured with a node matching the target architecture (`amd64` or `arm64`).
    *   **Prometheus Deployment:** A dedicated Prometheus instance is deployed into the KinD cluster (using `deploy_prometheus_for_benchmark.sh`) to collect metrics from Mimir and the underlying node/containers.
    *   **Mimir Deployment:** Grafana Mimir is deployed using the `mimir-distributed` Helm chart from the local repository (via `run_benchmark_setup.sh`). The `mimir_image_tag` input is used for Mimir component images.
    *   **Workload Generation:** The `mimir-continuous-test` tool is run as a Kubernetes Job (also via `run_benchmark_setup.sh`), using the `mimir_continuous_test_image` and other workload parameters (`target_active_series`, `target_sps`, `workload_duration`). This job generates load against the Mimir instance.
    *   **Metric Collection:** After the workload finishes, relevant metrics are queried from the in-cluster Prometheus (via `query_benchmark_metrics.sh`) over the period the workload was active.
3.  **Artifact Upload:**
    *   The collected metrics data (JSON files from Prometheus queries) are uploaded as a workflow artifact. The artifact will be named `benchmark-results-amd64` or `benchmark-results-arm64`.

## 4. Benchmark Configuration Details

The benchmark aims to keep configurations as identical as possible between the amd64 and ARM64 runs, with the primary variable being the node architecture.

*   **Mimir Deployment:**
    *   Deployed using the `mimir-distributed` Helm chart from the current repository.
    *   Default chart values are used, with the following key overrides:
        *   **Replica Counts (example):** Ingesters: 3, Distributors: 2-3, QueryFrontend: 2, Queriers: 2-3, StoreGateways: 2-3, Compactor: 1. (Refer to `run_benchmark_setup.sh` for exact defaults).
        *   **Images:** Uses the `mimir_image_tag` provided at workflow trigger.
        *   **Node Selectors:** Mimir components are scheduled onto nodes matching the target architecture.
    *   **Resource Requests/Limits:** The scripts currently use default resource requests/limits from the Helm chart. For precise cost comparison, these should ideally be identical and explicitly set for both architectures, or tuned appropriately for each if the goal is to find the *optimal* cost for each rather than direct comparison with identical settings. *Current scripts use Helm defaults unless overridden in `run_benchmark_setup.sh`.*

*   **Workload:**
    *   Generated by `mimir-continuous-test`.
    *   Parameters (`target_active_series`, `target_sps`, `workload_duration`) are controlled by the workflow inputs.

*   **Metrics Collected:**
    *   A range of metrics are collected to assess resource usage and Mimir performance. These include:
        *   **Pod-level resource metrics (from cAdvisor):**
            *   `container_cpu_usage_seconds_total` (rate)
            *   `container_memory_working_set_bytes`
            *   `container_network_receive_bytes_total` (rate)
            *   `container_network_transmit_bytes_total` (rate)
            *   `container_cpu_cfs_throttled_periods_total` (rate)
            *   (Disk I/O metrics if available and relevant)
        *   **Mimir-specific operational metrics:** Ingestion rates, series counts (e.g., `mimir_ingester_memory_series`, `mimir_tsdb_head_active_series`), query latencies (e.g., `mimir_request_duration_seconds_bucket`), replication factor, etc.
        *   **Go runtime metrics:** Garbage collection statistics from Mimir pods.
    *   For a detailed list of queries, refer to the `query_benchmark_metrics.sh` script.

## 5. Accessing and Interpreting Results

1.  **Download Artifacts:**
    *   Once the workflow run is complete, navigate to the summary page for that run.
    *   Under the "Artifacts" section, download `benchmark-results-amd64` and `benchmark-results-arm64`. These are zip files.

2.  **Structure of Output:**
    *   Each zip file, when unzipped, will contain a directory (e.g., `amd64` or `arm64`).
    *   Inside this directory, there will be a `metrics_data` sub-directory.
    *   The `metrics_data` directory contains multiple JSON files, where each file is the output of a specific PromQL query executed against the in-cluster Prometheus. Filenames correspond to the query's purpose (e.g., `ingester_cpu_usage.json`).
    *   A `benchmark_times.txt` file is also present, recording the actual start and end times of the workload Kubernetes Job.

3.  **Analyzing the Data:**
    *   **Direct Comparison:** The core of the analysis involves comparing the JSON metric files between the `amd64` and `arm64` runs.
    *   **Resource Usage:**
        *   Focus on CPU usage (`container_cpu_usage_seconds_total`) and memory usage (`container_memory_working_set_bytes`) for key Mimir components (ingesters, distributors, queriers, etc.).
        *   Compare average values, peak values, and potentially the overall trend/shape of the time series data.
        *   Look at CPU throttling (`container_cpu_cfs_throttled_periods_total`) to see if either architecture was significantly more constrained.
    *   **Mimir Performance:**
        *   Verify that Mimir operational metrics (e.g., actual ingestion rates, query success rates, latencies) were comparable between the two runs. This ensures that any observed resource differences are not due to one setup underperforming or failing to meet the target load.
    *   **Inferring Node-Cost Delta:**
        *   The "node-cost delta" is inferred from these resource consumption differences. For example:
            *   If ARM64 ingesters use, on average, 20% less CPU than amd64 ingesters to handle the same number of samples per second and active series, this suggests a potential for 20% CPU cost saving for the ingester component on ARM64.
            *   Similar comparisons can be made for memory.
        *   Consider the aggregate resource usage across all Mimir components.
    *   **Advanced Analysis (Optional):**
        *   The raw JSON data can be further processed using custom scripts (e.g., Python with pandas/matplotlib) to:
            *   Calculate precise averages, percentiles (P90, P99).
            *   Plot time series graphs for visual comparison.
            *   Perform statistical analysis.
        *   These advanced analysis scripts are not part of the current benchmark tooling but can be developed based on the collected data.

## 6. Benchmark Scripts Location

The underlying shell scripts that orchestrate and execute the benchmark are located in the Mimir repository:

*   `./tools/benchmark/orchestrate_benchmark_run.sh` (Top-level orchestrator called by the CI job)
*   `./tools/benchmark/run_benchmark_setup.sh` (Deploys Mimir and workload generator)
*   `./tools/benchmark/deploy_prometheus_for_benchmark.sh` (Deploys in-cluster Prometheus)
*   `./tools/benchmark/query_benchmark_metrics.sh` (Queries metrics from Prometheus)
