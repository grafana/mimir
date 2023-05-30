package main

poddisruptionbudget_root_test_fixture_dir := "test_fixtures/poddisruptionbudget"

no_violations {
	denies := deny_missing_poddisruption_budget with input as input
	trace(sprintf("Denies: %v", [denies]))
	count(deny_missing_poddisruption_budget) == 0
}

test_deployment_with_no_pdb {
	input_directory := sprintf("%s/deployment-with-no-pdb", [poddisruptionbudget_root_test_fixture_dir])
	deployment_file := sprintf("%s/deployment.yaml", [input_directory])
	input := parse_combined_config_files([deployment_file])

	denies := deny_missing_poddisruption_budget with input as input
	trace(sprintf("Denies: %v", [denies]))
	denies["Deployment/test-deployment has no PodDisruptionBudget"]
}

test_deployment_with_matching_pdb {
	input_directory := sprintf("%s/deployment-with-pdb", [poddisruptionbudget_root_test_fixture_dir])
	deployment_file := sprintf("%s/deployment.yaml", [input_directory])
	pdb_file := sprintf("%s/pdb.yaml", [input_directory])
	input := parse_combined_config_files([deployment_file, pdb_file])

	no_violations with input as input
}

test_deployment_with_many_labels_and_matching_pdb {
	input_directory := sprintf("%s/deployment-with-many-labels-and-pdb", [poddisruptionbudget_root_test_fixture_dir])
	deployment_file := sprintf("%s/deployment.yaml", [input_directory])
	pdb_file := sprintf("%s/pdb.yaml", [input_directory])
	input := parse_combined_config_files([deployment_file, pdb_file])

	no_violations with input as input
}

test_mismatched_deployment_and_pdb {
	input_directory := sprintf("%s/mismatched-deployment-and-pdb", [poddisruptionbudget_root_test_fixture_dir])
	deployment_file := sprintf("%s/deployment.yaml", [input_directory])
	pdb_file := sprintf("%s/pdb.yaml", [input_directory])
	input := parse_combined_config_files([deployment_file, pdb_file])

	denies := deny_missing_poddisruption_budget with input as input
	trace(sprintf("Denies: %v", [denies]))
	denies["Deployment/test-deployment has no PodDisruptionBudget"]
}

test_statefulset_with_no_pdb {
	input_directory := sprintf("%s/statefulset-with-no-pdb", [poddisruptionbudget_root_test_fixture_dir])
	statefulset_file := sprintf("%s/statefulset.yaml", [input_directory])
	input := parse_combined_config_files([statefulset_file])

	denies := deny_missing_poddisruption_budget with input as input
	trace(sprintf("Denies: %v", [denies]))
	denies["StatefulSet/test-set has no PodDisruptionBudget"]
}

test_statefulset_with_matching_pdb {
	input_directory := sprintf("%s/statefulset-with-pdb", [poddisruptionbudget_root_test_fixture_dir])
	statefulset_file := sprintf("%s/statefulset.yaml", [input_directory])
	pdb_file := sprintf("%s/pdb.yaml", [input_directory])
	input := parse_combined_config_files([statefulset_file, pdb_file])

	no_violations with input as input
}

test_mismatched_deployment_and_pdb {
	input_directory := sprintf("%s/mismatched-statefulset-and-pdb", [poddisruptionbudget_root_test_fixture_dir])
	statefulset_file := sprintf("%s/statefulset.yaml", [input_directory])
	pdb_file := sprintf("%s/pdb.yaml", [input_directory])
	input := parse_combined_config_files([statefulset_file, pdb_file])

	denies := deny_missing_poddisruption_budget with input as input
	trace(sprintf("Denies: %v", [denies]))
	denies["StatefulSet/test-set has no PodDisruptionBudget"]
}
