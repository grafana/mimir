package main

import future.keywords

test_container_security_context_without_readOnlyRootFS if {
	deny[reason] with input as [object.union(
		passing_deployment,
		{"contents": {"spec": {"template": {"spec": {"containers": [object.union(
			passing_container,
			{"securityContext": {"readOnlyRootFilesystem": false}},
		)]}}}}},
	)]

	contains(reason, "Mimir or GEM containers do not have the restricted security context")
}

test_container_security_context_with_priv_escalation if {
	deny[reason] with input as [object.union(
		passing_deployment,
		{"contents": {"spec": {"template": {"spec": {"containers": [object.union(
			passing_container,
			{"securityContext": {"allowPrivilegeEscalation": true}},
		)]}}}}},
	)]

	contains(reason, "Mimir or GEM containers do not have the restricted security context")
}

test_container_security_context_without_dropping_all if {
	deny[reason] with input as [object.union(
		passing_deployment,
		{"contents": {"spec": {"template": {"spec": {"containers": [object.union(
			passing_container,
			{"securityContext": {"capabilities": {"drop": ["none"]}}},
		)]}}}}},
	)]

	contains(reason, "Mimir or GEM containers do not have the restricted security context")
}

test_container_security_context_without_dropping_all if {
	deny[reason] with input as [object.union(
		passing_deployment,
		{"contents": {"spec": {"template": {"spec": {"containers": [object.union(
			passing_container,
			{"securityContext": {"capabilities": {"add": ["NET_ADMIN", "SYS_TIME"]}}},
		)]}}}}},
	)]

	contains(reason, "Mimir or GEM containers do not have the restricted security context")
}

test_container_security_context_unset_fields if {
	deny[reason] with input as [json.remove(passing_deployment, ["contents/spec/template/spec/containers/0/securityContext/allowPrivilegeEscalation"])]
	contains(reason, "Mimir or GEM containers do not have the restricted security context")
}

test_container_security_context_unset_fields if {
	deny[reason] with input as [json.remove(passing_deployment, ["contents/spec/template/spec/containers/0/securityContext/readOnlyRootFilesystem"])]
	contains(reason, "Mimir or GEM containers do not have the restricted security context")
}

test_container_security_context_unset_fields if {
	deny[reason] with input as [json.remove(passing_deployment, ["contents/spec/template/spec/containers/0/securityContext/capabilities"])]
	contains(reason, "Mimir or GEM containers do not have the restricted security context")
}

test_pod_security_context_not_correct if {
	deny[reason] with input as [object.union(
		passing_deployment,
		{"contents": {"spec": {"template": {"spec": {"securityContext": {"runAsNonRoot": false}}}}}},
	)]

	contains(reason, "The Mimir or GEM Pod doesn't have the restricted security context")
}

test_pod_security_context_not_correct if {
	deny[reason] with input as [object.union(
		passing_deployment,
		{"contents": {"spec": {"template": {"spec": {"securityContext": {"seccompProfile": {"type": "SomethingElse"}}}}}}},
	)]

	contains(reason, "The Mimir or GEM Pod doesn't have the restricted security context")
}

test_pod_security_context_unset_fields if {
	deny[reason] with input as [json.remove(passing_deployment, ["contents/spec/template/spec/securityContext/seccompProfile"])]
	contains(reason, "The Mimir or GEM Pod doesn't have the restricted security context")
}

test_pod_security_context_unset_fields if {
	deny[reason] with input as [json.remove(passing_deployment, ["contents/spec/template/spec/securityContext/runAsNonRoot"])]
	contains(reason, "The Mimir or GEM Pod doesn't have the restricted security context")
}
