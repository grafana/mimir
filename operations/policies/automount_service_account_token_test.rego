package main

import future.keywords

test_automount_service_account_token_unset_not_allowed if {
	deny[reason] with input as [json.remove(passing_deployment, ["contents/spec/template/spec/automountServiceAccountToken"])]
	contains(reason, "The Mimir or GEM Pod doesn't define automountServiceAccountToken")
}

test_automount_service_account_token_not_boolean_not_allowed if {
	deny[reason] with input as [object.union(
		passing_deployment,
		{"contents": {"spec": {"template": {"spec": {"automountServiceAccountToken": "false"}}}}},
	)]

	contains(reason, "The Mimir or GEM Pod doesn't define automountServiceAccountToken")
}

test_automount_service_account_token_false_allowed if {
	no_denies with input as [passing_deployment]
}

test_automount_service_account_token_true_allowed if {
	no_denies with input as [object.union(
		passing_deployment,
		{"contents": {"spec": {"template": {"spec": {"automountServiceAccountToken": true}}}}},
	)]
}

test_automount_service_account_token_ignored_for_non_mimir_images if {
	no_denies with input as [json.remove(
		object.union(
			passing_deployment,
			{"contents": {"spec": {"template": {"spec": {"containers": [object.union(
				passing_container,
				{"image": "nginx"},
			)]}}}}},
		),
		["contents/spec/template/spec/automountServiceAccountToken"],
	)]
}
