package main

import future.keywords.every

job_name_input(name) := [{"contents": {"kind": "Job", "metadata": {"name": name, "namespace": "example"}}}]

test_invalid_job_names {
	every name in [
		"mimir-distributed-vypjoyyveq-make-minio-buckets-5.4.-461d3ead54",
		"mimir-distributed-onj13ckn5a-make-minio-buckets-5.4.-9cc3200e1c",
		"name.-hash",
		"-name",
		"name.",
		"name..hash",
		"UPPERCASE",
		"name_with_underscores",
	] {
		denies := deny with input as job_name_input(name)
		denies[sprintf("Job has invalid metadata.name: %v", [name])]
	}
}

test_valid_job_names {
	every name in ["job", "1", "job-name-0123456789", "valid.dotted.name", "job-5.4.0-hash"] {
		denies := deny with input as job_name_input(name)
		count(denies) == 0
	}
}

test_generate_name_only_unchanged {
	denies := deny with input as [{"contents": {"kind": "Job", "metadata": {"generateName": "job-", "namespace": "example"}}}]
	count(denies) == 0
}

test_non_job_name_unchanged {
	denies := deny with input as [{"contents": {"kind": "ConfigMap", "metadata": {"name": "not_a_job", "namespace": "example"}}}]
	count(denies) == 0
}

test_job_name_at_length_limit {
	plain := sprintf("%063d", [0])
	dotted := sprintf("job.%059d", [0])
	every name in [plain, dotted] {
		denies := deny with input as job_name_input(name)
		count(denies) == 0
	}
}

test_job_name_over_length_limit {
	every length in [64, 253, 254] {
		plain := sprintf("%0*d", [length, 0])
		dotted := sprintf("job.%0*d", [length - 4, 0])
		every name in [plain, dotted] {
			denies := deny with input as job_name_input(name)
			count(denies) == 1
			denies[sprintf("Job has invalid metadata.name: %v", [name])]
		}
	}
}
