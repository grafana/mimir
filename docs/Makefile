.ONESHELL:
.DELETE_ON_ERROR:
export SHELL     := bash
export SHELLOPTS := pipefail:errexit
MAKEFLAGS += --warn-undefined-variables
MAKEFLAGS += --no-builtin-rule

include docs.mk

$(GIT_ROOT_PARENT)/mimir-$(MIMIR_DOCS_VERSION):
	git worktree add -f $@ origin/$(MIMIR_DOCS_BRANCH)

docs: $(GIT_ROOT_PARENT)/mimir-$(MIMIR_DOCS_VERSION)
