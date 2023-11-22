package main

poddisruptionbudget_root_test_fixture_dir := "test_fixtures/poddisruptionbudget"

no_pdb_violations {
	missing_pdb_denies := deny_missing_poddisruptionbudget with input as input
	trace(sprintf("Missing PDB denies: %v", [missing_pdb_denies]))
	count(missing_pdb_denies) == 0

	pdb_with_no_workload_denies := deny_poddisruptionbudget_missing_workload with input as input
	trace(sprintf("PDB with missing workload denies: %v", [pdb_with_no_workload_denies]))
	count(pdb_with_no_workload_denies) == 0
}

test_deployment_with_no_pdb {
	input_directory := sprintf("%s/deployment-with-no-pdb", [poddisruptionbudget_root_test_fixture_dir])
	deployment_file := sprintf("%s/deployment.yaml", [input_directory])
	input := parse_combined_config_files([deployment_file])

	denies := deny_missing_poddisruptionbudget with input as input
	trace(sprintf("Denies: %v", [denies]))
	denies["Deployment/test-deployment has no PodDisruptionBudget"]
}

test_deployment_with_matching_pdb {
	input_directory := sprintf("%s/deployment-with-pdb", [poddisruptionbudget_root_test_fixture_dir])
	deployment_file := sprintf("%s/deployment.yaml", [input_directory])
	pdb_file := sprintf("%s/pdb.yaml", [input_directory])
	input := parse_combined_config_files([deployment_file, pdb_file])

	no_pdb_violations with input as input
}

test_deployment_with_many_labels_and_matching_pdb {
	input_directory := sprintf("%s/deployment-with-many-labels-and-pdb", [poddisruptionbudget_root_test_fixture_dir])
	deployment_file := sprintf("%s/deployment.yaml", [input_directory])
	pdb_file := sprintf("%s/pdb.yaml", [input_directory])
	input := parse_combined_config_files([deployment_file, pdb_file])

	no_pdb_violations with input as input
}

test_mismatched_deployment_and_pdb {
	input_directory := sprintf("%s/mismatched-deployment-and-pdb", [poddisruptionbudget_root_test_fixture_dir])
	deployment_file := sprintf("%s/deployment.yaml", [input_directory])
	pdb_file := sprintf("%s/pdb.yaml", [input_directory])
	input := parse_combined_config_files([deployment_file, pdb_file])

	missing_pdb_denies := deny_missing_poddisruptionbudget with input as input
	trace(sprintf("Missing PDB denies: %v", [missing_pdb_denies]))
	missing_pdb_denies["Deployment/test-deployment has no PodDisruptionBudget"]

	pdb_with_no_workload_denies := deny_poddisruptionbudget_missing_workload with input as input
	trace(sprintf("PDB with missing workload denies: %v", [pdb_with_no_workload_denies]))
	pdb_with_no_workload_denies["PodDisruptionBudget/test-pdb has no matching workload"]
}

test_statefulset_with_no_pdb {
	input_directory := sprintf("%s/statefulset-with-no-pdb", [poddisruptionbudget_root_test_fixture_dir])
	statefulset_file := sprintf("%s/statefulset.yaml", [input_directory])
	input := parse_combined_config_files([statefulset_file])

	denies := deny_missing_poddisruptionbudget with input as input
	trace(sprintf("Denies: %v", [denies]))
	denies["StatefulSet/test-set has no PodDisruptionBudget"]
}

test_statefulset_with_matching_pdb {
	input_directory := sprintf("%s/statefulset-with-pdb", [poddisruptionbudget_root_test_fixture_dir])
	statefulset_file := sprintf("%s/statefulset.yaml", [input_directory])
	pdb_file := sprintf("%s/pdb.yaml", [input_directory])
	input := parse_combined_config_files([statefulset_file, pdb_file])

	no_pdb_violations with input as input
}

test_mismatched_deployment_and_pdb {
	input_directory := sprintf("%s/mismatched-statefulset-and-pdb", [poddisruptionbudget_root_test_fixture_dir])
	statefulset_file := sprintf("%s/statefulset.yaml", [input_directory])
	pdb_file := sprintf("%s/pdb.yaml", [input_directory])
	input := parse_combined_config_files([statefulset_file, pdb_file])

	missing_pdb_denies := deny_missing_poddisruptionbudget with input as input
	trace(sprintf("Missing PDB denies: %v", [missing_pdb_denies]))
	missing_pdb_denies["StatefulSet/test-set has no PodDisruptionBudget"]

	pdb_with_no_workload_denies := deny_poddisruptionbudget_missing_workload with input as input
	trace(sprintf("PDB with missing workload denies: %v", [pdb_with_no_workload_denies]))
	pdb_with_no_workload_denies["PodDisruptionBudget/test-pdb has no matching workload"]
}
