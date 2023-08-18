.ONESHELL:
.DELETE_ON_ERROR:
export SHELL     := bash
export SHELLOPTS := pipefail:errexit
MAKEFLAGS += --warn-undefined-variables
MAKEFLAGS += --no-builtin-rule

include docs.mk

$(REPOS_PATH)/mimir-$(MIMIR_DOCS_VERSION):
	git worktree add -f $@ origin/$(MIMIR_DOCS_BRANCH)

docs: $(REPOS_PATH)/mimir-$(MIMIR_DOCS_VERSION)
