package main

valid_job_name(name) {
	is_string(name)
	count(name) <= 63
	regex.match(`^[a-z0-9]([-a-z0-9]*[a-z0-9])?(\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*$`, name)
}

deny[msg] {
	obj := input[i].contents
	obj.kind == "Job"
	name := obj.metadata.name
	name != ""
	not valid_job_name(name)
	msg := sprintf("Job has invalid metadata.name: %v", [name])
}
