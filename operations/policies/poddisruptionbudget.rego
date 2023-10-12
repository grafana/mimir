package main

import future.keywords.every

deny_missing_poddisruptionbudget[msg] {
	obj := input[i].contents
	msg = sprintf("%s has no PodDisruptionBudget", [object_display_name[i]])

	should_have_poddisruptionbudget(obj)
	not any_pdb_selects_object(obj)
}

deny_poddisruptionbudget_missing_workload[msg] {
	pdb := input[i].contents
	msg = sprintf("%s has no matching workload", [object_display_name[i]])

	pdb.kind == "PodDisruptionBudget"
	not any_object_matches_pdb(pdb)
}

should_have_poddisruptionbudget(obj) {
	obj.kind in ["Deployment", "StatefulSet"]
}

any_pdb_selects_object(obj) {
	input[pdb_object_index].contents.kind == "PodDisruptionBudget"
	pdb := input[pdb_object_index].contents

	every matcher in pdb.spec.selector.matchLabels {
		matcher_matches_object(matcher, obj)
	}
}

any_object_matches_pdb(pdb) {
	obj := input[i].contents
	should_have_poddisruptionbudget(obj)

	every matcher in pdb.spec.selector.matchLabels {
		matcher_matches_object(matcher, obj)
	}
}

matcher_matches_object(matcher, obj) {
	matcher in obj.spec.template.metadata.labels
}
