package main

deployment_rollingupdate_root_test_fixture_dir := "test_fixtures/deployment_rollingupdate"

no_rollingupdate_violations {
	deployment_rollingupdate_denies := deny_deployment_rollingupdate with input as input
	trace(sprintf("Deployment rolling update configuration denies: %v", [deployment_rollingupdate_denies]))
	count(deployment_rollingupdate_denies) == 0
}

test_deployment_with_no_strategy {
	deployment_file := sprintf("%s/no-strategy.yaml", [deployment_rollingupdate_root_test_fixture_dir])
	input := parse_combined_config_files([deployment_file])

	denies := deny_deployment_rollingupdate with input as input
	trace(sprintf("Denies: %v", [denies]))
	denies["Deployment/test-deployment has no spec.strategy"]
}

test_deployment_with_no_rollingupdate {
	deployment_file := sprintf("%s/no-rollingupdate.yaml", [deployment_rollingupdate_root_test_fixture_dir])
	input := parse_combined_config_files([deployment_file])

	denies := deny_deployment_rollingupdate with input as input
	trace(sprintf("Denies: %v", [denies]))
	denies["Deployment/test-deployment has no spec.strategy.rollingUpdate"]
}

test_deployment_with_no_maxunavailable {
	deployment_file := sprintf("%s/no-maxunavailable.yaml", [deployment_rollingupdate_root_test_fixture_dir])
	input := parse_combined_config_files([deployment_file])

	denies := deny_deployment_rollingupdate with input as input
	trace(sprintf("Denies: %v", [denies]))
	denies["Deployment/test-deployment has no spec.strategy.rollingUpdate.maxUnavailable"]
}

test_deployment_with_non_zero_maxunavailable {
	deployment_file := sprintf("%s/maxunavailable-non-zero.yaml", [deployment_rollingupdate_root_test_fixture_dir])
	input := parse_combined_config_files([deployment_file])

	denies := deny_deployment_rollingupdate with input as input
	trace(sprintf("Denies: %v", [denies]))
	denies["Deployment/test-deployment has spec.strategy.rollingUpdate.maxUnavailable set to 1, but 0 is required"]
}

test_deployment_with_zero_maxunavailable {
	deployment_file := sprintf("%s/maxunavailable-zero.yaml", [deployment_rollingupdate_root_test_fixture_dir])
	input := parse_combined_config_files([deployment_file])

	no_rollingupdate_violations with input as input
}

test_not_a_deployment {
	deployment_file := sprintf("%s/non-deployment.yaml", [deployment_rollingupdate_root_test_fixture_dir])
	input := parse_combined_config_files([deployment_file])

	no_rollingupdate_violations with input as input
}
