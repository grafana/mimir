package main

import future.keywords.in

single_instance_deployments = [
	"continuous-test", # We only want to run at most one instance at a time.
	"rollout-operator", # We only want to run at most one instance at a time.
]

deny_deployment_rollingupdate[msg] {
	obj := input[i].contents
	msg = sprintf("%s has no spec.strategy", [object_display_name[i]])

	obj.kind == "Deployment"
	not "strategy" in object.keys(obj.spec)
}

deny_deployment_rollingupdate[msg] {
	obj := input[i].contents
	msg = sprintf("%s has no spec.strategy.rollingUpdate", [object_display_name[i]])

	obj.kind == "Deployment"
	not "rollingUpdate" in object.keys(obj.spec.strategy)
}

deny_deployment_rollingupdate[msg] {
	obj := input[i].contents
	msg = sprintf("%s has no spec.strategy.rollingUpdate.maxUnavailable", [object_display_name[i]])

	obj.kind == "Deployment"
	not "maxUnavailable" in object.keys(obj.spec.strategy.rollingUpdate)
}

deny_deployment_rollingupdate[msg] {
	obj := input[i].contents
	maxUnavailable := obj.spec.strategy.rollingUpdate.maxUnavailable
	msg = sprintf("%s has spec.strategy.rollingUpdate.maxUnavailable set to %v, but 0 is required", [object_display_name[i], maxUnavailable])

	obj.kind == "Deployment"
	not is_single_instance_deployment(obj)
	maxUnavailable > 0
}

deny_deployment_rollingupdate[msg] {
	obj := input[i].contents
	maxSurge := object.get(obj.spec.strategy.rollingUpdate, "maxSurge", "25%")
	msg = sprintf("%s has spec.strategy.rollingUpdate.maxSurge set to %v, but 0 is required", [object_display_name[i], maxSurge])

	obj.kind == "Deployment"
	is_single_instance_deployment(obj)
	maxSurge != 0
}

# for Jsonnet
is_single_instance_deployment(obj) {
	obj.spec.template.metadata.labels.name in single_instance_deployments
}

# for Helm
is_single_instance_deployment(obj) {
	obj.spec.template.metadata.labels["app.kubernetes.io/component"] in single_instance_deployments
}
