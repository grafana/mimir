# You can install Jsonnet by running:
#
#   go install github.com/google/go-jsonnet/cmd/jsonnet@latest
#
# Or follow instructions on https://github.com/google/go-jsonnet

.DEFAULT_GOAL := docker-compose.yml

# intended to update new changes from jsonnet to yml, but not to overwrite if yml is newer
docker-compose.yml: docker-compose.jsonnet
	jsonnet -S docker-compose.jsonnet -o docker-compose.yml

.PHONY: check
check: clean docker-compose.yml
	@git diff --exit-code -- 'docker-compose.yml' || (echo "docker-compose.jsonnet and docker-compose.yml don't match in ${PWD}" && false)

.PHONY: clean
clean:
	rm -f docker-compose.yml
