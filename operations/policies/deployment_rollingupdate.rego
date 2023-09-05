package main

import future.keywords.in

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
	maxUnavailable > 0
}
