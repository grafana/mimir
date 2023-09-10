package main

import future.keywords.in

ignored_deployments = [
	"continuous-test", # We only want to run at most one instance at a time.
	"rollout-operator", # We only want to run at most one instance at a time.
]

deny_deployment_rollingupdate[msg] {
	obj := input[i].contents
	msg = sprintf("%s has no spec.strategy", [object_display_name[i]])

	obj.kind == "Deployment"
	not is_ignored_deployment(obj)
	not "strategy" in object.keys(obj.spec)
}

deny_deployment_rollingupdate[msg] {
	obj := input[i].contents
	msg = sprintf("%s has no spec.strategy.rollingUpdate", [object_display_name[i]])

	obj.kind == "Deployment"
	not is_ignored_deployment(obj)
	not "rollingUpdate" in object.keys(obj.spec.strategy)
}

deny_deployment_rollingupdate[msg] {
	obj := input[i].contents
	msg = sprintf("%s has no spec.strategy.rollingUpdate.maxUnavailable", [object_display_name[i]])

	obj.kind == "Deployment"
	not is_ignored_deployment(obj)
	not "maxUnavailable" in object.keys(obj.spec.strategy.rollingUpdate)
}

deny_deployment_rollingupdate[msg] {
	obj := input[i].contents
	maxUnavailable := obj.spec.strategy.rollingUpdate.maxUnavailable
	msg = sprintf("%s has spec.strategy.rollingUpdate.maxUnavailable set to %v, but 0 is required", [object_display_name[i], maxUnavailable])

	obj.kind == "Deployment"
	not is_ignored_deployment(obj)
	maxUnavailable > 0
}

is_ignored_deployment(obj) {
	obj.metadata.name in ignored_deployments
}
