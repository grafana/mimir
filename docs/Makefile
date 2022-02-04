IMAGE = grafana/docs-base:latest

.PHONY: pull
pull:
	docker pull ${IMAGE}

.PHONY: docs
docs: pull
	docker run  --rm -it -v ${PWD}/sources:/hugo/content/docs/mimir/next -p 3002:3002 $(IMAGE) /bin/bash -c "touch content/docs/mimir/_index.md && make server"
