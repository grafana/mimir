package main

mimir_tests_dir := "../mimir-tests"

# Extract just the contents from parsed config files (removes file path metadata)
get_contents(parsed) := [obj.contents | some i; obj := parsed[i]]

# Verify that the multi-AZ read-path migration step 8 produces the same output as the final state.
# This ensures the migration procedure is complete at step 8.
test_multi_az_migration_step_8_equals_final {
	step_8_file := sprintf("%s/test-multi-az-read-path-migration-step-8-generated.yaml", [mimir_tests_dir])
	final_file := sprintf("%s/test-multi-az-read-path-migration-step-final-generated.yaml", [mimir_tests_dir])

	step_8 := get_contents(parse_combined_config_files([step_8_file]))
	final := get_contents(parse_combined_config_files([final_file]))

	step_8 == final
}
