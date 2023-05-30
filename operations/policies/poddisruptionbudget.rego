package main

import future.keywords.every

deny_missing_poddisruption_budget[msg] {
	obj := input[i].contents
	msg = sprintf("%s has no PodDisruptionBudget", [object_display_name[i]])

	should_have_poddisruption_budget(obj)
	not pdb_selects_object(obj)
}

should_have_poddisruption_budget(obj) {
	obj.kind in ["Deployment", "StatefulSet"]
}

pdb_selects_object(obj) {
	input[pdb_object_index].contents.kind == "PodDisruptionBudget"
	pdb := input[pdb_object_index].contents

	every matcher in pdb.spec.selector.matchLabels {
		matcher_matches_object(matcher, obj)
	}
}

matcher_matches_object(matcher, obj) {
	matcher in obj.spec.template.metadata.labels
}
